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

#include "index_reader.hpp"
#include "utils/object_pool.hpp"

namespace irs {

class SegmentReaderImpl;

// Interface for a segment reader
class segment_reader final : public sub_reader {
 public:
  static segment_reader open(const directory& dir, const SegmentMeta& meta,
                             const index_reader_options& opts);

  segment_reader() = default;
  segment_reader(const segment_reader& other) noexcept;
  segment_reader& operator=(const segment_reader& other) noexcept;

  bool operator==(const segment_reader& rhs) const noexcept {
    return impl_ == rhs.impl_;
  }

  segment_reader& operator*() noexcept { return *this; }
  const segment_reader& operator*() const noexcept { return *this; }
  segment_reader* operator->() noexcept { return this; }
  const segment_reader* operator->() const noexcept { return this; }

  const SegmentInfo& meta() const override;

  const sub_reader& operator[](size_t i) const noexcept override {
    IRS_ASSERT(!i);
    IRS_IGNORE(i);
    return *this;
  }

  column_iterator::ptr columns() const override;

  using sub_reader::docs_count;
  uint64_t docs_count() const override;

  doc_iterator::ptr docs_iterator() const override;

  const document_mask* docs_mask() const override;

  // FIXME find a better way to mask documents
  doc_iterator::ptr mask(doc_iterator::ptr&& it) const override;

  const term_reader* field(std::string_view name) const override;

  field_iterator::ptr fields() const override;

  uint64_t live_docs_count() const override;

  segment_reader reopen(const SegmentMeta& meta) const;

  size_t size() const override;

  const irs::column_reader* sort() const override;

  const irs::column_reader* column(std::string_view name) const final;

  const irs::column_reader* column(field_id field) const override;

  explicit operator sub_reader::ptr() const noexcept;

  explicit operator bool() const noexcept { return nullptr != impl_; }

 private:
  explicit segment_reader(
    std::shared_ptr<const SegmentReaderImpl> impl) noexcept;

  std::shared_ptr<const SegmentReaderImpl> impl_;
};

}  // namespace irs
