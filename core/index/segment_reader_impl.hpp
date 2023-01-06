////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <memory>

#include "index/index_reader.hpp"
#include "utils/directory_utils.hpp"
#include "utils/hash_utils.hpp"

#include <absl/container/flat_hash_map.h>

namespace irs {

// Reader holds file refs to files from the segment.
class SegmentReaderImpl final : public SubReader {
 public:
  static std::shared_ptr<const SegmentReaderImpl> Open(
    const directory& dir, const SegmentMeta& meta,
    const IndexReaderOptions& warmup);

  SegmentReaderImpl(const directory& dir, const SegmentMeta& meta,
                    const IndexReaderOptions& opts);

  SegmentReaderImpl(const SegmentReaderImpl& rhs, const SegmentMeta& meta,
                    document_mask&& doc_mask);
  ~SegmentReaderImpl();

  const directory& Dir() const noexcept { return *dir_; }

  const IndexReaderOptions& Options() const noexcept { return opts_; }

  const SegmentInfo& Meta() const override { return info_; }

  std::shared_ptr<const SegmentReaderImpl> Reopen(
    const SegmentMeta& meta) const;

  column_iterator::ptr columns() const override;

  const document_mask* docs_mask() const override { return &docs_mask_; }

  doc_iterator::ptr docs_iterator() const override;

  doc_iterator::ptr mask(doc_iterator::ptr&& it) const override;

  const term_reader* field(std::string_view name) const override {
    return data_->field_reader_->field(name);
  }

  field_iterator::ptr fields() const override {
    return data_->field_reader_->iterator();
  }

  const irs::column_reader* sort() const noexcept override { return sort_; }

  const irs::column_reader* column(field_id field) const override;

  const irs::column_reader* column(std::string_view name) const override;

 private:
  using NamedColumns =
    absl::flat_hash_map<hashed_string_view, const irs::column_reader*>;
  using SortedNamedColumns =
    std::vector<std::reference_wrapper<const irs::column_reader>>;

  struct Data {
    field_reader::ptr field_reader_;
    columnstore_reader::ptr columnstore_reader_;
    NamedColumns named_columns_;
    SortedNamedColumns sorted_named_columns_;
  };

  std::vector<index_file_refs::ref_t> file_refs_;
  document_mask docs_mask_;
  std::shared_ptr<Data> data_;
  SegmentInfo info_;
  const directory* dir_;
  const irs::column_reader* sort_{};
  IndexReaderOptions opts_;
};

}  // namespace irs
