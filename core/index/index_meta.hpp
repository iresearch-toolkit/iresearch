////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <optional>
#include <span>

#include "error/error.hpp"
#include "utils/string.hpp"
#include "utils/type_limits.hpp"

#include <absl/container/flat_hash_set.h>

namespace irs {

class format;
class IndexWriter;

struct SegmentInfo {
  bool operator==(const SegmentInfo& other) const noexcept;

  std::string name;
  uint64_t docs_count{};       // Total number of documents in a segment
  uint64_t live_docs_count{};  // Total number of live documents in a segment
  uint64_t version{};
  size_t size_in_bytes{};  // Size of a segment in bytes
};

static_assert(std::is_nothrow_move_constructible_v<SegmentInfo>);
static_assert(std::is_nothrow_move_assignable_v<SegmentInfo>);

struct SegmentMeta : SegmentInfo {
  using FileSet = absl::flat_hash_set<std::string>;

  SegmentMeta() = default;
  SegmentMeta(std::string&& name, std::shared_ptr<const format> codec) noexcept
    : SegmentInfo{.name = std::move(name)}, codec{std::move(codec)} {}

  bool operator==(const SegmentMeta& other) const noexcept;

  FileSet files;
  std::shared_ptr<const format> codec;
  field_id sort{field_limits::invalid()};
  bool column_store{};
};

inline bool HasRemovals(const SegmentInfo& meta) noexcept {
  return meta.live_docs_count != meta.docs_count;
}

static_assert(std::is_nothrow_move_constructible_v<SegmentMeta>);
static_assert(std::is_nothrow_move_assignable_v<SegmentMeta>);

struct IndexSegment {
  IndexSegment() = default;
  // cppcheck-suppress noExplicitConstructor
  IndexSegment(SegmentMeta&& meta) : meta{std::move(meta)} {}
  IndexSegment(const SegmentMeta& meta) : meta{meta} {}
  IndexSegment(const IndexSegment& other) = default;
  IndexSegment& operator=(const IndexSegment& other) = default;
  IndexSegment(IndexSegment&&) = default;
  IndexSegment& operator=(IndexSegment&&) = default;

  bool operator==(const IndexSegment& other) const noexcept {
    return filename == other.filename || meta == other.meta;
  }

  std::string filename;
  SegmentMeta meta;
};

static_assert(std::is_nothrow_move_constructible_v<IndexSegment>);
static_assert(std::is_nothrow_move_assignable_v<IndexSegment>);

class IndexMeta {
 public:
  bool operator==(const IndexMeta& other) const noexcept;

  void add(IndexSegment&& segment) {
    segments_.emplace_back(std::move(segment));
  }

  template<typename Visitor>
  bool visit_files(const Visitor& visitor) const {
    return const_cast<IndexMeta&>(*this).visit_files(visitor);
  }

  template<typename Visitor>
  bool visit_files(const Visitor& visitor) {
    for (auto& curr_segment : segments_) {
      if (!visitor(curr_segment.filename)) {
        return false;
      }

      for (auto& file : curr_segment.meta.files) {
        if (!visitor(const_cast<std::string&>(file))) {
          return false;
        }
      }
    }
    return true;
  }

  uint64_t counter() const noexcept { return seg_counter_; }
  void SetCounter(uint64_t v) noexcept { seg_counter_ = v; }
  uint64_t generation() const noexcept { return gen_; }

  void update_generation(const IndexMeta& rhs) noexcept {
    gen_ = rhs.gen_;
    last_gen_ = rhs.last_gen_;
  }

  size_t size() const noexcept { return segments_.size(); }
  bool empty() const noexcept { return segments_.empty(); }

  const IndexSegment& operator[](size_t i) const noexcept {
    IRS_ASSERT(i < segments_.size());
    return segments_[i];
  }

  std::span<const IndexSegment> segments() const noexcept { return segments_; }

  bytes_view payload() const noexcept {
    return payload_ ? *payload_ : bytes_view{};
  }

  // public for tests
  void payload(bytes_view payload) {
    if (IsNull(payload)) {
      payload_.reset();
    } else {
      payload_.emplace(payload);
    }
  }

  void payload(bstring&& payload) noexcept {
    payload_.emplace(std::move(payload));
  }

 private:
  friend class IndexWriter;
  friend struct index_meta_reader;
  friend struct index_meta_writer;

  uint64_t next_generation() const noexcept;

  uint64_t gen_{index_gen_limits::invalid()};
  uint64_t last_gen_{index_gen_limits::invalid()};
  uint64_t seg_counter_{0};
  std::vector<IndexSegment> segments_;
  std::optional<bstring> payload_;
};

}  // namespace irs
