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
    uint64_t median,
    uint32_t bits
  );  

  void compute_stats(
    doc_id_t& avg_chunk_docs,
    uint64_t& avg_chunk_size,
    uint32_t& doc_delta_bits, 
    uint32_t& ptr_bits
  );

  std::vector<uint64_t> packed_; // proxy buffer for bit packing
  std::unique_ptr<uint64_t[]> unpacked_; // buffer for storing unpacked data
  doc_id_t* doc_base_deltas_; // where unpacked doc id's starts
  uint64_t* doc_pos_deltas_; // where unpacked offsets starts
  index_output* out_{};
  uint64_t first_pos_;
  size_t block_size_;
  size_t block_chunks_;
  doc_id_t docs_;
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