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

  if (ALL_EQUAL == bits) { // RLE
    out_->write_vlong(*begin);
    return;
  }

  // write packed blocks 
  if (full_blocks) {
    std::memset(packed_.data(), 0, sizeof(uint64_t)*packed_.size());
    packed_.resize(full_blocks);

    packed::pack(begin, begin + full_blocks, packed_.data(), bits);

    out_->write_bytes(
      reinterpret_cast<const byte_type*>(packed_.data()),
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

  bool keys_equal = true;
  const auto first_key = keys_[0];
  bool offsets_equal = true;
  const auto first_offset = offsets_[0];
  for (size_t i = 0; i < full_blocks; ++i) {
    // convert block relative doc_id's to deltas between average and actual doc id
    auto& key = keys_[i];
    key = zig_zag_encode64(key - avg_chunk_docs*i);
    max_doc_delta = std::max(max_doc_delta, key);
    keys_equal &= (first_key == key);

    // convert block relative offsets to deltas between average and actual offset
    auto& offset = offsets_[i];
    offset = zig_zag_encode64(offset - avg_chunk_size*i);
    max_offset = std::max(max_offset, offset);
    offsets_equal &= (first_offset == offset);
  }

  // convert tail doc id's and offsets to deltas
  for (size_t i = full_blocks; i < size; ++i) {
    // convert block relative doc_id's to deltas between average and actual doc_id
    auto& key = keys_[i];
    key = zig_zag_encode64(key - avg_chunk_docs*i);
    keys_equal &= (first_key == key);
    
    // convert block relative offsets to deltas between average and actual offset
    auto& offset = offsets_[i];
    offset = zig_zag_encode64(offset - avg_chunk_size*i);
    offsets_equal &= (first_offset == offset);
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
    keys_equal ? ALL_EQUAL : packed::bits_required_64(max_doc_delta)
  );

  // write start pointers
  out_->write_vlong(block_offset_);
  write_block(
    full_blocks,
    offsets_,
    offset_,
    avg_chunk_size,
    offsets_equal ? ALL_EQUAL : packed::bits_required_64(max_offset)
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

NS_END
