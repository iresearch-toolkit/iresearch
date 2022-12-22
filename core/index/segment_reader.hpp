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

////////////////////////////////////////////////////////////////////////////////
/// @brief interface for a segment reader
////////////////////////////////////////////////////////////////////////////////
class segment_reader final : public sub_reader {
 public:
  static segment_reader open(const directory& dir, const segment_meta& meta,
                             const index_reader_options& opts);

  segment_reader() = default;  // required for context<segment_reader>
  segment_reader(const segment_reader& other) noexcept;
  segment_reader& operator=(const segment_reader& other) noexcept;

  explicit operator bool() const noexcept { return bool(impl_); }

  bool operator==(const segment_reader& rhs) const noexcept {
    return impl_ == rhs.impl_;
  }

  bool operator!=(const segment_reader& rhs) const noexcept {
    return !(*this == rhs);
  }

  segment_reader& operator*() noexcept { return *this; }
  const segment_reader& operator*() const noexcept { return *this; }
  segment_reader* operator->() noexcept { return this; }
  const segment_reader* operator->() const noexcept { return this; }

  const sub_reader& operator[](size_t i) const noexcept override {
    IRS_ASSERT(!i);
    IRS_IGNORE(i);
    return *this;
  }

  column_iterator::ptr columns() const override { return impl_->columns(); }

  using sub_reader::docs_count;
  uint64_t docs_count() const override { return impl_->docs_count(); }

  doc_iterator::ptr docs_iterator() const override {
    return impl_->docs_iterator();
  }

  // FIXME find a better way to mask documents
  doc_iterator::ptr mask(doc_iterator::ptr&& it) const override {
    return impl_->mask(std::move(it));
  }

  const term_reader* field(std::string_view name) const override {
    return impl_->field(name);
  }

  field_iterator::ptr fields() const override { return impl_->fields(); }

  uint64_t live_docs_count() const override { return impl_->live_docs_count(); }

  segment_reader reopen(const segment_meta& meta) const;

  void reset() noexcept { impl_.reset(); }

  size_t size() const override { return impl_->size(); }

  const irs::column_reader* sort() const override { return impl_->sort(); }

  const irs::column_reader* column(std::string_view name) const final {
    return impl_->column(name);
  }

  const irs::column_reader* column(field_id field) const override {
    return impl_->column(field);
  }

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief converts current 'segment_reader' to 'sub_reader::ptr'
  ////////////////////////////////////////////////////////////////////////////////
  explicit operator sub_reader::ptr() const noexcept { return impl_; }

 private:
  std::shared_ptr<const sub_reader> impl_;

  segment_reader(std::shared_ptr<const sub_reader> impl) noexcept;
};

}  // namespace irs
