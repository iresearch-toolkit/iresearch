//
// IResearch search engine 
// 
// Copyright © 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#include "shared.hpp"
#include "format_compress.hpp"
#include "format_utils.hpp"

#include "error/error.hpp"

#include "utils/bit_utils.hpp"
#include "utils/bit_packing.hpp"
#include "utils/type_limits.hpp"

#if defined(_MSC_VER)
  #pragma warning(disable : 4244)
  #pragma warning(disable : 4245)
#elif defined (__GNUC__)
  // NOOP
#endif

#include <boost/crc.hpp>

#if defined(_MSC_VER)
  #pragma warning(default: 4244)
  #pragma warning(default: 4245)
#elif defined (__GNUC__)
  // NOOP
#endif

NS_ROOT

// -----------------------------------------------------------------------------
// --SECTION--                           compressing_index_writer implementation
// -----------------------------------------------------------------------------

const string_ref compressing_index_writer::FORMAT_NAME = "iresearch_10_compressing_index";

compressing_index_writer::compressing_index_writer(size_t block_size)
  : keys_(new uint64_t[2*block_size]),
    offsets_(keys_.get() + block_size) {
}

void compressing_index_writer::prepare(index_output& out) {
  out_ = &out;
  key_ = offsets_; // will trigger 'reset' on write()
  offset_ = offsets_; // will prevent flushing of emtpy block
}

void compressing_index_writer::write_block(
    size_t full_blocks, 
    const uint64_t* begin,
    const uint64_t* end,
    uint64_t median,
    uint32_t bits) {

  // write block header
  out_->write_vlong(median);
  out_->write_vint(bits);

  // write packed blocks 
  if (full_blocks) {
    packed_.clear();
    packed_.resize(full_blocks);

    packed::pack(begin, begin + full_blocks, &packed_[0], bits);

    out_->write_bytes(
      reinterpret_cast<const byte_type*>(&packed_[0]),
      sizeof(uint64_t)*packed::blocks_required_64(full_blocks, bits)
    );

    begin += full_blocks;
  }

  // write tail
  for (; begin < end; ++begin) {
    out_->write_vlong(*begin);
  }
}

void compressing_index_writer::flush() {
  assert(out_);

  // it's important that we determine total number of
  // elements here using 'offset' column, since
  // in prepare we implicitly trigger flush for first
  // element
  // since there are no cached elements yet, such
  // way of getting number of elements will prevent
  // us from the wrong behavior
   
  // total number of elements in block
  const size_t size = std::distance(offsets_, offset_);
  if (!size) {
    return;
  }

  // compute block stats
  doc_id_t avg_chunk_docs = 0; // average number of docs per element
  size_t avg_chunk_size = 0; // average size of element
  if (size > 1) {
    const auto den = size - 1; // -1 since the 1st offset is always 0
    avg_chunk_docs = std::lround(static_cast<float_t>(*(key_-1)) / den);
    avg_chunk_size = *(offset_ - 1) / den;
  }
 
  // total number of elements we can be pack using bit packing
  const size_t full_blocks = size - (size % packed::BLOCK_SIZE_64);

  // convert doc id's and offsets to deltas
  // compute maximum deltas
  uint64_t max_offset = 0; // max delta between average and actual start position
  doc_id_t max_doc_delta = 0; // max delta between average and actual doc id

  for (size_t i = 0; i < full_blocks; ++i) {
    // convert block relative doc_id's to deltas between average and actual doc id
    auto& key = keys_[i];
    key = zig_zag_encode64(key - avg_chunk_docs*i);
    max_doc_delta = std::max(max_doc_delta, key);

    // convert block relative offsets to deltas between average and actual offset
    auto& offset = offsets_[i];
    offset = zig_zag_encode64(offset - avg_chunk_size*i);
    max_offset = std::max(max_offset, offset);
  }

  // convert tail doc id's and offsets to deltas
  for (size_t i = full_blocks; i < size; ++i) {
    // convert block relative doc_id's to deltas between average and actual doc_id
    auto& key = keys_[i];
    key = zig_zag_encode64(key - avg_chunk_docs*i);
    
    // convert block relative offsets to deltas between average and actual offset
    auto& offset = offsets_[i];
    offset = zig_zag_encode64(offset - avg_chunk_size*i);
  }

  // write total number of elements
  out_->write_vlong(size);

  // write document bases
  out_->write_vlong(block_base_);
  write_block(
    full_blocks, 
    keys_.get(),
    key_,
    avg_chunk_docs, 
    packed::bits_required_64(max_doc_delta)
  );

  // write start pointers
  out_->write_vlong(block_offset_);
  write_block(
    full_blocks, 
    offsets_,
    offset_,
    avg_chunk_size, 
    packed::bits_required_64(max_offset)
  );
}

void compressing_index_writer::write(doc_id_t key, uint64_t offset) {
  if (key_ == offsets_) {
    // we've reached the size of the block
    flush();
    // set block base & offset
    block_base_ = key;
    block_offset_ = offset;
    // reset pointers
    key_ = keys_.get();
    offset_ = offsets_;
  }

  *key_++ = key - block_base_; // store block relative id's
  assert(key >= block_base_ + *(key_ - 1));

  *offset_++ = offset - block_offset_; // store block relative offsets
  assert(offset >= block_offset_ + *(offset_ - 1));
}

void compressing_index_writer::finish() {
  assert(out_);
 
  flush(); // should flush the rest of the cached data
  out_->write_vint(0); // end marker
}

// -----------------------------------------------------------------------------
// --SECTION--                           compressing_index_reader implementation
// -----------------------------------------------------------------------------

NS_LOCAL

template<typename Func>
void read_block(
    index_input& in,
    size_t full_chunks,
    std::vector<uint64_t>& packed,
    std::vector<uint64_t>& unpacked,
    std::vector<compressing_index_reader::entry_t>& blocks,
    const Func& get_property) {
  const uint64_t median = in.read_vlong();
  const uint32_t bits = in.read_vint();
  if (bits > bits_required<uint64_t>()) {
    // invalid number of bits per document
    throw index_error();
  }

  // read full chunks 
  if (full_chunks) {
    unpacked.resize(full_chunks);

    packed.resize(packed::blocks_required_64(full_chunks, bits));

    in.read_bytes(
      reinterpret_cast<byte_type*>(&packed[0]),
      sizeof(uint64_t)*packed.size()
    );

    packed::unpack(
      &unpacked[0], 
      &unpacked[0] + unpacked.size(), 
      &packed[0], 
      bits
    );

    for (size_t i = 0; i < full_chunks; ++i) {
      get_property(blocks[i]) = zig_zag_decode64(unpacked[i]) + i * median;
    }
  }

  // read tail 
  for (; full_chunks < blocks.size(); ++full_chunks) {
    get_property(blocks[full_chunks]) = read_zvlong(in) + full_chunks * median;
  }
}

NS_END

compressing_index_reader::block::block(
    uint64_t offset_base, doc_id_t key_base, std::vector<compressing_index_reader::entry_t>&& entries)
  : entries(std::move(entries)), key_base(key_base), offset_base(offset_base) {
}

compressing_index_reader::block::block(compressing_index_reader::block&& rhs)
  : entries(std::move(rhs.entries)), 
    offset_base(rhs.offset_base), 
    key_base(rhs.key_base) {
  rhs.offset_base = type_limits<type_t::address_t>::invalid();
  rhs.key_base = type_limits<type_t::doc_id_t>::invalid();
}

compressing_index_reader::compressing_index_reader(compressing_index_reader&& rhs)
  : data_(std::move(rhs.data_)), max_key_(rhs.max_key_) {
}

bool compressing_index_reader::prepare(index_input& in, doc_id_t max_key) {
  std::vector<uint64_t> packed; 
  std::vector<uint64_t> unpacked;

  std::vector<entry_t> blocks(in.read_vlong());
  for (; !blocks.empty(); blocks.resize(in.read_vlong())) {
    const size_t full_chunks = blocks.size() - (blocks.size() % packed::BLOCK_SIZE_64);

    // read document bases
    const doc_id_t doc_base = in.read_vlong();
    read_block(
      in, full_chunks, packed, unpacked, blocks,
      [] (entry_t& e)->doc_id_t& { return e.first; }
    );

    // read start pointers
    const uint64_t start = in.read_vlong();
    read_block(
      in, full_chunks, packed, unpacked, blocks,
      [] (entry_t& e)->uint64_t& { return e.second; }
    );

    data_.emplace_back(start, doc_base, std::move(blocks));
  }

  max_key_ = max_key;
  return true;
}

uint64_t compressing_index_reader::lower_bound(doc_id_t key) const {
  if (key >= max_key_) {
    return type_limits<type_t::address_t>::invalid();
  }

  // find the right block
  const auto block = std::lower_bound(
    data_.rbegin(),
    data_.rend(),
    key,
    [](const compressing_index_reader::block& lhs, doc_id_t rhs) {
      return lhs.key_base > rhs;
  });
  
  if (block == data_.rend()) {
    return type_limits<type_t::address_t>::invalid();
  }

  // find the right entry in the block 
  const auto entry = std::lower_bound(
    block->entries.rbegin(),
    block->entries.rend(),
    key - block->key_base,
    [](const entry_t& lhs, doc_id_t rhs) {
      return lhs.first > rhs;
  });
  
  if (entry == block->entries.rend()) {
    return type_limits<type_t::address_t>::invalid();
  }

  return block->offset_base + entry->second;
}

uint64_t compressing_index_reader::find(doc_id_t key) const {
  if (key >= max_key_) {
    return type_limits<type_t::address_t>::invalid();
  }

  // find the right block chunk
  const auto block = std::lower_bound(
    data_.rbegin(),
    data_.rend(),
    key,
    [](const compressing_index_reader::block& lhs, doc_id_t rhs) {
      return lhs.key_base > rhs;
  });
  
  if (block == data_.rend()) {
    return type_limits<type_t::address_t>::invalid();
  }

  // find the right block in the chunk
  const auto entry = std::lower_bound(
    block->entries.rbegin(),
    block->entries.rend(),
    key -= block->key_base,
    [](const entry_t& lhs, doc_id_t rhs) {
      return lhs.first > rhs;
  });
  
  if (entry == block->entries.rend() || key > entry->first) {
    return type_limits<type_t::address_t>::invalid();
  }

  return block->offset_base + entry->second;
}

NS_END