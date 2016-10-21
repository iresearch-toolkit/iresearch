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

#ifndef IRESEARCH_INDEX_META_H
#define IRESEARCH_INDEX_META_H

#include "store/directory.hpp"

#include "error/error.hpp"

#include "utils/string.hpp"

#include <algorithm>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <atomic>

NS_ROOT

/* -------------------------------------------------------------------
 * segment_meta
 * ------------------------------------------------------------------*/

class format;
typedef std::shared_ptr<format> format_ptr;

struct IRESEARCH_API segment_meta {
  typedef std::unordered_set<std::string> file_set;

  segment_meta() = default;
  segment_meta(const segment_meta&) = default;
  segment_meta(segment_meta&& rhs);
  segment_meta(const string_ref& name, format_ptr codec);
  segment_meta(
    std::string&& name,
    format_ptr codec,
    uint64_t docs_count,
    file_set&& files
  );

  segment_meta& operator=(segment_meta&& rhs);
  segment_meta& operator=(const segment_meta&) = default;

  file_set files;
  std::string name;
  uint64_t docs_count{};
  format_ptr codec;
  uint64_t version{};
};

/* -------------------------------------------------------------------
 * index_meta
 * ------------------------------------------------------------------*/

struct directory;
class index_writer;

class IRESEARCH_API index_meta {
 public:
  struct IRESEARCH_API index_segment_t {
    index_segment_t() = default;
    index_segment_t(segment_meta&& v_meta);
    index_segment_t(const index_segment_t& other) = default;
    index_segment_t& operator=(const index_segment_t& other) = default;
    index_segment_t(index_segment_t&& other);
    index_segment_t& operator=(index_segment_t&& other);
    
    std::string filename;
    segment_meta meta;
  }; // index_segment_t
  typedef std::unordered_set<string_ref> file_set;
  typedef std::vector<index_segment_t> index_segments_t;
  DECLARE_PTR(index_meta);

  static const uint64_t INVALID_GEN = integer_traits< uint64_t >::const_max;
  static const string_ref FORMAT_NAME;

  index_meta() = default;
  index_meta(index_meta&& rhs);
  index_meta(const index_meta& rhs);
  index_meta& operator=(index_meta&& rhs);
  index_meta& operator=(const index_meta&) = delete;

  template<typename _ForwardIterator>
  void add(_ForwardIterator begin, _ForwardIterator end) {
    segments_.reserve(segments_.size() + std::distance(begin, end));
    gen_dirty_ |= begin != end;
    std::move(begin, end, std::back_inserter(segments_));
  }
  
  template<typename Visitor>
  bool visit_files(const Visitor& visitor) const {
    return const_cast<index_meta&>(*this).visit_files(visitor);
  }

  template<typename Visitor>
  bool visit_files(const Visitor& visitor) {
    for (auto& segment : segments_) {
      if (!visitor(segment.filename)) {
        return false;
      }

      for (auto& file : segment.meta.files) {        
        if (!visitor(const_cast<std::string&>(file))) {
          return false;
        }
      }
    }
    return true;
  }

  uint64_t increment() { return ++seg_counter_; }
  uint64_t counter() const { return seg_counter_; }
  uint64_t generation() const { return gen_; }

  index_segments_t::iterator begin() { return segments_.begin(); }
  index_segments_t::iterator end() { return segments_.end(); }
  
  index_segments_t::const_iterator begin() const { return segments_.begin(); }
  index_segments_t::const_iterator end() const { return segments_.end(); }

  void update_generation(const index_meta& rhs) NOEXCEPT{
    gen_ = rhs.gen_;
    gen_dirty_ = rhs.gen_dirty_;
    last_gen_ = rhs.last_gen_;
  }

  size_t size() const { return segments_.size(); }
  bool empty() const { return segments_.empty(); }

  void clear() {
    segments_.clear();
    pending_ = false;
    // leave version and generation counters unchanged do to possible readers
  }

  void reset(const index_meta& rhs) {
    // leave version and generation counters unchanged
    segments_ = rhs.segments_;
    gen_dirty_ = (last_gen_ == INVALID_GEN);
    pending_ = false;
  }

  const index_segment_t& segment(size_t i) const {
    return segments_[i];
  }

 private:
  friend class index_writer;
  friend struct index_meta_reader;
  friend struct index_meta_writer;

  uint64_t next_generation() const {
    return INVALID_GEN == gen_ ? 1 : gen_ + 1;
  }

  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  index_segments_t segments_;
  std::atomic<uint64_t> seg_counter_{ 0 };
  uint64_t gen_{ INVALID_GEN };
  uint64_t last_gen_{ INVALID_GEN };
  bool gen_dirty_{ true }; // dirty because gen == INVALID_GEN
  bool pending_{ false };
  IRESEARCH_API_PRIVATE_VARIABLES_END
}; // index_meta

ErrorCode find_segments_file( 
  const directory& dir,
  const std::function< void(const string_ref&) >& handler );

NS_END

#endif