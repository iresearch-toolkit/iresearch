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

#ifndef IRESEARCH_FORMAT_COMPRESS_H
#define IRESEARCH_FORMAT_COMPRESS_H

#include "index/index_meta.hpp"
#include "utils/string.hpp"
#include "store/directory.hpp"
#include "store/data_input.hpp"
#include "store/data_output.hpp"

#include <memory>

NS_ROOT

//////////////////////////////////////////////////////////////////////////////
/// @class compressing_index_writer
//////////////////////////////////////////////////////////////////////////////
class compressing_index_writer: util::noncopyable {
 public:
  static const string_ref FORMAT_NAME;

  static const int32_t FORMAT_MIN = 0;
  static const int32_t FORMAT_MAX = FORMAT_MIN;

  explicit compressing_index_writer(size_t block_size);

  void prepare(index_output& out, uint64_t ptr);
  void write(doc_id_t docs, uint64_t value);
  void finish();

 private:
  void flush();

  void write_block(
    size_t full_chunks,
    const uint64_t* start,
    const uint64_t* end,
    uint64_t median,
    uint32_t bits
  ); 

  void reset() {
    block_docs_ = 0;
    key_ = keys_.get();
    offset_ = offsets_;
  }

  std::vector<uint64_t> packed_; // proxy buffer for bit packing
  std::unique_ptr<uint64_t[]> keys_; // buffer for storing unpacked data & pointer where unpacked keys begins
  doc_id_t* offsets_; // where unpacked offsets begins
  doc_id_t* key_; // current key 
  uint64_t* offset_; // current offset
  index_output* out_{}; // associated output stream
  uint64_t block_offset_; // current block offset in a file
  doc_id_t docs_; // total number of processed docs
  doc_id_t block_docs_;
}; // compressing_index_writer 

//////////////////////////////////////////////////////////////////////////////
/// @struct block
/// @brief stores information about base document & start pointer
//////////////////////////////////////////////////////////////////////////////
struct block {
  doc_id_t base; // document offset
  uint64_t start; // where block starts
}; // block

//////////////////////////////////////////////////////////////////////////////
/// @struct block_chunk
//////////////////////////////////////////////////////////////////////////////
struct block_chunk {
  block_chunk(uint64_t start, doc_id_t base, std::vector<block>&& docs);
  block_chunk(block_chunk&& rhs);

  std::vector<block> blocks;
  uint64_t start; // block start
  doc_id_t base; // document base
}; // block_chunk

//////////////////////////////////////////////////////////////////////////////
/// @class compressing_index_reader
//////////////////////////////////////////////////////////////////////////////
class compressing_index_reader: util::noncopyable {
 public:
  bool prepare(index_input& in, doc_id_t docs_count);
  uint64_t start_ptr(doc_id_t doc) const;

 private:
  std::vector<block_chunk> data_;
  doc_id_t max_doc_;
}; // compressing_index_reader 

NS_END

#endif