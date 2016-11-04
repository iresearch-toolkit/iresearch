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
#include "utils/bit_packing.hpp"
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
/// @class compressed_index
//////////////////////////////////////////////////////////////////////////////
template<typename T>
class compressed_index : util::noncopyable {
 public:
  typedef T value_type;
  typedef std::pair<doc_id_t, value_type> entry_t;
 
  struct block {
    block(size_t size, doc_id_t key)
      : entries(size), key_base(key) {
    }

    block(block&&) = default;

    std::vector<entry_t> entries;
    doc_id_t key_base;
  };
 
  class iterator : public std::iterator<std::forward_iterator_tag, entry_t> {
   public:
    typedef const block* block_iterator_t;

    iterator(block_iterator_t bit) : bit_(bit) { }
    iterator(const iterator&) = default;
    iterator& operator=(const iterator&) = default;

    iterator& operator++() {
      assert(!bit_->entries.empty());
      if (offset_ == bit_->entries.size()-1) {
        ++bit_;
        offset_ = 0;
      } else {
        ++offset_;
      }
      return *this; 
    }

    iterator operator++(int) { 
      auto tmp = *this;
      ++*this;
      return tmp;
    }

    const value_type& operator*() const {
      return bit_->entries[offset_];
    }

    const value_type* operator->() const {
      return &operator*();
    }

    bool operator==(const iterator& rhs) const {
      return bit_ == rhs.bit_ && offset_ == rhs.offset_;
    }
    
    bool operator!=(const iterator& rhs) const {
      return !(*this == rhs);
    }

   private:
    block_iterator_t bit_;
    size_t offset_{};
  }; // iterator
  
  compressed_index() = default;

  compressed_index(compressed_index&& rhs)
    : blocks_(std::move(rhs.blocks_)),
      max_(rhs.max_) {
  }

  compressed_index& operator=(compressed_index&& rhs) {
    if (this != &rhs) {
      blocks_ = std::move(rhs.blocks_);
      max_ = rhs.max_;
    }
    return *this;
  }

  template<typename Visitor>
  bool read(index_input& in, doc_id_t max, const Visitor& visitor) {
    std::vector<uint64_t> packed;
    std::vector<uint64_t> unpacked;
    std::vector<block> blocks;
    for (size_t size = in.read_vlong(); size; size = in.read_vlong()) {
      const size_t full_chunks = size - (size % packed::BLOCK_SIZE_64);

      // insert new block
      blocks.emplace_back(size, in.read_vlong());
      auto& block = blocks.back();

      // read document bases
      const auto doc_base = block.key_base;
      auto it = block.entries.begin();
      read_block(
        in, full_chunks, size, packed, unpacked,
        [&it, doc_base] (uint64_t v) {
          it->first = doc_base + v;
          ++it;
      });

      // read start pointers    
      const uint64_t start = in.read_vlong();
      it = block.entries.begin();
      read_block(
        in, full_chunks, size, packed, unpacked,
        [&it, &visitor, start] (uint64_t v) {
          visitor(it->second, start + v);
          ++it;
      });
    }

    // noexcept
    blocks_ = std::move(blocks);
    max_ = max;
    return true;
  }

  const T* find(doc_id_t key) const {
    auto* entry = lower_bound_entry(key);
    return !entry || key > entry->first
      ? nullptr
      : &entry->second;
  }

  const T* lower_bound(doc_id_t key) const {
    auto* entry = lower_bound_entry(key);
    return entry ? &entry->second : nullptr;
  }

  iterator begin() const { return iterator(blocks_.data()); }
  iterator end() const { return iterator(blocks_.data() + blocks_.size()); }

  template<typename Visitor>
  bool visit(Visitor visitor) const {
    for (auto& block : blocks_) {
      for (auto& entry : block.entries) {
        if (!visitor(entry)) {
          return false;
        }
      }
    }
    return true;
  }

 private:
  template<typename Visitor>
  static void read_block(
      index_input& in,
      size_t full_chunks,
      size_t num_chunks,
      std::vector<uint64_t>& packed,
      std::vector<uint64_t>& unpacked,
      const Visitor& visitor) {
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
        visitor(zig_zag_decode64(unpacked[i]) + i * median);
      }
    }

    // read tail 
    for (; full_chunks < num_chunks; ++full_chunks) {
      visitor(read_zvlong(in) + full_chunks * median);
    }
  }

  const entry_t* lower_bound_entry(doc_id_t key) const {
    if (key >= max_) {
      return nullptr;
    }

    // find the right block
    const auto block = std::lower_bound(
      blocks_.rbegin(), 
      blocks_.rend(), 
      key,
      [] (const compressed_index::block& lhs, doc_id_t rhs) {
        return lhs.key_base > rhs;
    });

    if (block == blocks_.rend()) {
      return nullptr;
    }

    // find the right entry in the block
    const auto entry = std::lower_bound(
      block->entries.rbegin(), 
      block->entries.rend(), 
      key,
      [] (const entry_t& lhs, doc_id_t rhs) {
        return lhs.first > rhs;
    });

    if (entry == block->entries.rend()) {
      return nullptr;
    }

    return &*entry;
  }

  std::vector<block> blocks_;
  doc_id_t max_;
}; // compressed_index

NS_END

#endif