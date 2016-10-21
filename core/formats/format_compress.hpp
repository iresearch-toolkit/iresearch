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

/* -------------------------------------------------------------------
 * compressing_index_writer
 * ------------------------------------------------------------------*/

class IRESEARCH_API compressing_index_writer : util::noncopyable {
 public:
  static const string_ref FORMAT_NAME;

  static const int32_t FORMAT_MIN = 0;
  static const int32_t FORMAT_MAX = FORMAT_MIN;

  explicit compressing_index_writer(size_t block_size);

  void prepare(directory& dir, const std::string& file, uint64_t ptr);
  void write(uint32_t docs, uint64_t ptr);
  void end(uint64_t ptr);

 private:
  void flush();
  
  void compute_stats(
    uint32_t& avg_chunk_docs,
    uint64_t& avg_chunk_size,
    uint32_t& doc_delta_bits, 
    uint32_t& ptr_bits
  );

  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  std::vector<uint32_t> packed_;
  std::unique_ptr<uint32_t[]> doc_base_deltas_;
  std::unique_ptr<uint64_t[]> doc_pos_deltas_;
  index_output::ptr out_;
  uint64_t first_pos_;
  uint64_t last_pos_;
  size_t block_size_;
  size_t block_chunks_;
  uint32_t docs_;
  uint32_t block_docs_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
};

/* -------------------------------------------------------------------
 * compressing_index_reader
 * ------------------------------------------------------------------*/

struct block {
  uint32_t base; // document offset
  uint64_t start; // where block starts
};

struct block_chunk {
  block_chunk(uint64_t start, doc_id_t base, std::vector<block>&& docs);
  block_chunk(block_chunk&& rhs);

  std::vector<block> blocks;
  uint64_t start; // block start
  doc_id_t base; // document base
}; // block_data

class IRESEARCH_API compressing_index_reader : util::noncopyable {
 public:
  // returns pointer to the meta of the data file
  uint64_t prepare(
    const directory& dir, 
    const std::string& seg_name,
    uint64_t docs_count
  );

  uint64_t start_ptr(doc_id_t doc) const;

 private:
  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  std::vector<block_chunk> data_;
  uint64_t max_doc_;
  uint32_t packed_version_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
};

NS_END

#endif