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
  reset();
}

void compressing_index_writer::prepare(index_output& out, uint64_t ptr) {
  out_ = &out;
  block_offset_ = ptr;
  docs_ = 0;
  reset();
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

  // total number of elements in block
  const size_t size = std::distance(keys_.get(), key_);
  assert(size);

  // compute block stats
  doc_id_t avg_chunk_docs = 0; // average number of docs per element
  size_t avg_chunk_size = 0; // average size of element
  if (size > 1) {
    const auto den = size - 1;
    avg_chunk_docs = std::lround(
      static_cast<float_t>(block_docs_ - *(key_-1)) / den
    );
    avg_chunk_size = *(offset_ - 1) / den; // -1 since the 1st offset is almost always 0
  }
 
  // total number of elements we can be pack using bit packing
  const size_t full_blocks = size - (size % packed::BLOCK_SIZE_64);

  // convert doc id's and offsets to deltas
  // compute maximum deltas
  doc_id_t delta, base = 0;
  uint64_t max_offset = 0; // max delta between average and actual start position
  doc_id_t max_doc_delta = 0; // max delta between average and actual doc id
  for (size_t i = 0; i < full_blocks; ++i) {
    // convert block relative doc_id's to deltas between average and actual doc id
    auto& key = keys_[i];
    delta = zig_zag_encode64(base - avg_chunk_docs*i);
    base += key;
    key = delta;
    max_doc_delta = std::max(max_doc_delta, delta);

    // convert block relative offsets to deltas between average and actual offset
    auto& offset = offsets_[i];
    offset = zig_zag_encode64(offset - avg_chunk_size*i);
    max_offset = std::max(max_offset, offset);
  }

  // convert tail doc id's and offsets to deltas
  for (size_t i = full_blocks; i < size; ++i) {
    // convert block relative doc_id's to deltas between average and actual doc_id
    auto& key = keys_[i];
    delta = zig_zag_encode64(base - avg_chunk_docs*i);
    base += key;
    key = delta;
    
    // convert block relative offsets to deltas between average and actual offset
    auto& offset = offsets_[i];
    offset = zig_zag_encode64(offset - avg_chunk_size*i);
  }

  // write total number of elements
  out_->write_vlong(size);

  // write document bases
  out_->write_vlong(docs_ - block_docs_);
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

  reset();
}

void compressing_index_writer::write(doc_id_t docs, uint64_t offset) {
  if (key_ == offsets_) {
    // we've reached the size of the block
    flush();
    block_offset_ = offset;
  }

  *key_++ = docs;
  *offset_++ = offset - block_offset_; // store block relative offsets
  assert(offset >= block_offset_ + *(offset_ - 1));

  block_docs_ += docs;
  docs_ += docs;
}

void compressing_index_writer::finish() {
  assert(out_);
 
  if (keys_.get() != key_) {
    // should flush the rest of the cached data
    flush();
  }
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
    std::vector<block>& blocks,
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

block_chunk::block_chunk(
  uint64_t start, doc_id_t base, std::vector<block>&& blocks
): blocks(std::move(blocks)), start(start), base(base) {
}

block_chunk::block_chunk(block_chunk&& rhs)
  : blocks(std::move(rhs.blocks)), 
    start(rhs.start), 
    base(rhs.base) {
  rhs.start = type_limits<type_t::address_t>::invalid();
  rhs.base = type_limits<type_t::doc_id_t>::invalid();
}

bool compressing_index_reader::prepare(index_input& in, doc_id_t docs_count) {
  std::vector<uint64_t> packed; 
  std::vector<uint64_t> unpacked;

  std::vector<block> blocks(in.read_vlong());
  for (; !blocks.empty(); blocks.resize(in.read_vlong())) {
    size_t full_chunks = blocks.size() - (blocks.size() % packed::BLOCK_SIZE_64);

    // read document bases
    const doc_id_t doc_base = in.read_vlong();
    read_block(
      in, full_chunks, packed, unpacked, blocks,
      [] (block& b)->uint64_t& { return b.base; }
    );

    // read start pointers
    const uint64_t start = in.read_vlong();
    read_block(
      in, full_chunks, packed, unpacked, blocks,
      [] (block& b)->uint64_t& { return b.start; }
    );

    data_.emplace_back(start, doc_base, std::move(blocks));
  }

  max_doc_ = docs_count;
  return true;
}

size_t compressing_index_reader::start_ptr(doc_id_t doc) const {
  assert(type_limits<type_t::doc_id_t>::valid(doc));

  doc -= type_limits<type_t::doc_id_t>::min();

  if (doc >= max_doc_) {
    throw illegal_argument();
  }

  // find the right block chunk
  auto chunk = std::lower_bound(
    data_.rbegin(),
    data_.rend(),
    doc,
    [](const block_chunk& lhs, doc_id_t rhs) {
      return lhs.base > rhs;
  });
  assert(chunk != data_.rend());

  // find the right block in the chunk
  auto block = std::lower_bound(
    chunk->blocks.rbegin(),
    chunk->blocks.rend(),
    doc - chunk->base,
    [](const iresearch::block& lhs, doc_id_t rhs) {
      return lhs.base > rhs;
  });
  assert(block != chunk->blocks.rend());

  return chunk->start + block->start;
}

NS_END