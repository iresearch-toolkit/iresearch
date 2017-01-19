//
// IResearch search engine 
// 
// Copyright (c) 2016 by EMC Corporation, All Rights Reserved
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

void compressing_index_writer::prepare(index_output& out, uint64_t* packed) {
  out_ = &out;
  packed_ = packed;
  assert(packed_);
  key_ = offsets_; // will trigger 'reset' on write()
  offset_ = offsets_; // will prevent flushing of emtpy block
  count_ = 0;
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

  // adjust number of elements to pack to the nearest value
  // that is multiple to the block size
  const auto block_size = math::ceil64(size, packed::BLOCK_SIZE_64);
  assert(block_size >= size);

  // compute block stats
  const doc_id_t key_median = median_encode(keys_.get(), key_); // average number of docs per element
  const size_t offset_median = median_encode(offsets_, offset_); // average size of element

  // write total number of elements
  out_->write_vlong(size);

  // write document bases
  out_->write_vlong(block_base_);
  out_->write_vlong(key_median);
  write_block(*out_, keys_.get(), block_size, packed_);

  // write start pointers
  out_->write_vlong(block_offset_);
  out_->write_vlong(offset_median);
  write_block(*out_, offsets_, block_size, packed_);
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

  ++count_;
}

void compressing_index_writer::finish() {
  assert(out_);
 
  flush(); // should flush the rest of the cached data
  out_->write_vint(0); // end marker
}

NS_END
