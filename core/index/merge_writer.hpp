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
class Comparer;

class MergeWriter : public util::noncopyable {
 public:
  using FlushProgress = std::function<bool()>;

  struct ReaderCtx {
    explicit ReaderCtx(SubReader::ptr reader) noexcept;

    SubReader::ptr reader;                      // segment reader
    std::vector<doc_id_t> doc_id_map;           // FIXME use bitpacking vector
    std::function<doc_id_t(doc_id_t)> doc_map;  // mapping function
  };

  MergeWriter() noexcept;

  explicit MergeWriter(directory& dir,
                        const column_info_provider_t& column_info,
                        const feature_info_provider_t& feature_info,
                        const Comparer* comparator = nullptr) noexcept
    : dir_(dir),
      column_info_(&column_info),
      feature_info_(&feature_info),
      comparator_(comparator) {
    IRS_ASSERT(column_info);
  }
  MergeWriter(MergeWriter&&) = default;
  MergeWriter& operator=(MergeWriter&&) = delete;

  operator bool() const noexcept;

  void add(const SubReader& reader) {
    // add reference, noexcept aliasing ctor
    readers_.emplace_back(SubReader::ptr{SubReader::ptr{}, &reader});
  }

  void add(SubReader::ptr reader) {
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
  bool flush(IndexSegment& segment, const FlushProgress& progress = {});

  const ReaderCtx& operator[](size_t i) const noexcept {
    IRS_ASSERT(i < readers_.size());
    return readers_[i];
  }

  // reserve enough space to hold 'size' readers
  void reserve(size_t size) { readers_.reserve(size); }

 private:
  bool flush_sorted(tracking_directory& dir, IndexSegment& segment,
                    const FlushProgress& progress);

  bool flush(tracking_directory& dir, IndexSegment& segment,
             const FlushProgress& progress);

  directory& dir_;
  std::vector<ReaderCtx> readers_;
  const column_info_provider_t* column_info_;
  const feature_info_provider_t* feature_info_;
  const Comparer* comparator_;
};

static_assert(std::is_nothrow_move_constructible_v<MergeWriter>);

}  // namespace irs
