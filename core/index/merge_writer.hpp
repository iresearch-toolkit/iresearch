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

#include <vector>

#include "index/index_features.hpp"
#include "index/index_meta.hpp"
#include "index/index_reader.hpp"
#include "utils/memory.hpp"
#include "utils/noncopyable.hpp"
#include "utils/string.hpp"

namespace irs {

struct tracking_directory;
class comparer;

class merge_writer : public util::noncopyable {
 public:
  using flush_progress_t = std::function<bool()>;

  struct reader_ctx {
    explicit reader_ctx(sub_reader::ptr reader) noexcept;

    sub_reader::ptr reader;                     // segment reader
    std::vector<doc_id_t> doc_id_map;           // FIXME use bitpacking vector
    std::function<doc_id_t(doc_id_t)> doc_map;  // mapping function
  };

  merge_writer() noexcept;

  explicit merge_writer(directory& dir,
                        const column_info_provider_t& column_info,
                        const feature_info_provider_t& feature_info,
                        const comparer* comparator = nullptr) noexcept
    : dir_(dir),
      column_info_(&column_info),
      feature_info_(&feature_info),
      comparator_(comparator) {
    IRS_ASSERT(column_info);
  }
  merge_writer(merge_writer&&) = default;
  merge_writer& operator=(merge_writer&&) = delete;

  operator bool() const noexcept;

  void add(const sub_reader& reader) {
    // add reference, noexcept aliasing ctor
    readers_.emplace_back(sub_reader::ptr{sub_reader::ptr{}, &reader});
  }

  void add(sub_reader::ptr reader) {
    // add shared pointer
    IRS_ASSERT(reader);
    if (reader) {
      readers_.emplace_back(std::move(reader));
    }
  }

  // Flush all of the added readers into a single segment.
  // `segment` the segment that was flushed.
  // `progress` report flush progress (abort if 'progress' returns false).
  // Return merge successful.
  bool flush(index_meta::index_segment_t& segment,
             const flush_progress_t& progress = {});

  const reader_ctx& operator[](size_t i) const noexcept {
    IRS_ASSERT(i < readers_.size());
    return readers_[i];
  }

  // reserve enough space to hold 'size' readers
  void reserve(size_t size) { readers_.reserve(size); }

 private:
  bool flush_sorted(tracking_directory& dir,
                    index_meta::index_segment_t& segment,
                    const flush_progress_t& progress);

  bool flush(tracking_directory& dir, index_meta::index_segment_t& segment,
             const flush_progress_t& progress);

  directory& dir_;
  std::vector<reader_ctx> readers_;
  const column_info_provider_t* column_info_;
  const feature_info_provider_t* feature_info_;
  const comparer* comparator_;
};

static_assert(std::is_nothrow_move_constructible_v<merge_writer>);

}  // namespace irs
