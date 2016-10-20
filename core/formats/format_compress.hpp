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

  void prepare(index_output& out);
  void write(doc_id_t key, uint64_t value);
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

  std::vector<uint64_t> packed_; // proxy buffer for bit packing
  std::unique_ptr<uint64_t[]> keys_; // buffer for storing unpacked data & pointer where unpacked keys begins
  doc_id_t* offsets_; // where unpacked offsets begins
  doc_id_t* key_; // current key 
  uint64_t* offset_; // current offset
  index_output* out_{}; // associated output stream
  doc_id_t block_base_; // current block base doc id
  uint64_t block_offset_; // current block offset in a file
}; // compressing_index_writer 

//////////////////////////////////////////////////////////////////////////////
/// @class compressing_index_reader
//////////////////////////////////////////////////////////////////////////////
class compressing_index_reader : util::noncopyable {
 public:
  typedef std::pair<
    doc_id_t, // key
    uint64_t // offset 
  > entry_t;

  compressing_index_reader() = default;
  compressing_index_reader(compressing_index_reader&& rhs);

  bool prepare(index_input& in, doc_id_t max_key);
  uint64_t lower_bound(doc_id_t key) const;
  uint64_t find(doc_id_t key) const;

 private:
  struct block {
    block(uint64_t start, doc_id_t base, std::vector<entry_t>&& entries);
    block(block&& rhs);

    std::vector<entry_t> entries;
    uint64_t offset_base; // offset base
    doc_id_t key_base; // document base
  }; // block

  std::vector<block> data_;
  doc_id_t max_key_;
}; // compressing_index_reader 

NS_END

#endif