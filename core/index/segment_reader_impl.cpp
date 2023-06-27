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

#include "segment_reader_impl.hpp"

#include <vector>

#include "analysis/token_attributes.hpp"
#include "index/index_meta.hpp"
#include "utils/index_utils.hpp"
#include "utils/type_limits.hpp"

#include <absl/strings/str_cat.h>

namespace irs {
namespace {

class AllIteratorBase : public doc_iterator {
 public:
  explicit AllIteratorBase(uint32_t docs_count) noexcept
    : max_doc_{doc_limits::min() + docs_count - 1} {}

  attribute* get_mutable(irs::type_info::type_id type) noexcept final {
    return irs::type<document>::id() == type ? &doc_ : nullptr;
  }

  doc_id_t value() const noexcept final { return doc_.value; }

 protected:
  document doc_;
  doc_id_t max_doc_;  // largest valid doc_id
};

class AllIterator : public AllIteratorBase {
 public:
  using AllIteratorBase::AllIteratorBase;

  doc_id_t next() noexcept final {
    auto& doc = doc_.value;
    return doc = doc < max_doc_ ? doc + 1 : doc_limits::eof();
  }

  doc_id_t seek(doc_id_t target) noexcept final {
    IRS_ASSERT(doc_.value <= target);
    return doc_.value = target <= max_doc_ ? target : doc_limits::eof();
  }
};

class MaskAllIterator : public AllIteratorBase {
 public:
  explicit MaskAllIterator(uint32_t docs_count,
                           const DocumentMask& mask) noexcept
    : AllIteratorBase{docs_count}, mask_{mask} {
    IRS_ASSERT(!mask.contains(doc_limits::eof()));
  }

  doc_id_t next() noexcept final {
    auto& doc = doc_.value;
    if (IRS_UNLIKELY(doc < max_doc_)) {
      return seek(doc + 1);
    }
    return doc = doc_limits::eof();
  }

  IRS_NO_INLINE doc_id_t seek(doc_id_t target) noexcept final {
    IRS_ASSERT(doc_.value <= target);
    for (; target <= max_doc_; ++target) {
      if (!mask_.contains(target)) {
        return doc_.value = target;
      }
    }
    return doc_.value = doc_limits::eof();
  }

 private:
  const DocumentMask& mask_;  // excluded document ids
};

class MaskDocIterator : public doc_iterator {
 public:
  MaskDocIterator(doc_iterator::ptr&& it, const DocumentMask& mask) noexcept
    : it_{std::move(it)}, mask_{mask} {
    IRS_ASSERT(!mask.contains(doc_limits::eof()));
  }

  attribute* get_mutable(irs::type_info::type_id type) final {
    return it_->get_mutable(type);
  }

  doc_id_t value() const final {
    // TODO(MBkkt) member pointer to avoid double virtual call?
    return it_->value();
  }

  doc_id_t next() final { return NextImpl(it_->next()); }

  doc_id_t seek(doc_id_t target) final { return NextImpl(it_->seek(target)); }

 private:
  IRS_NO_INLINE doc_id_t NextImpl(doc_id_t target) {
    while (mask_.contains(target)) {
      target = it_->next();
    }
    return target;
  }

  doc_iterator::ptr it_;
  const DocumentMask& mask_;  // excluded document ids
};

FileRefs GetRefs(const directory& dir, const SegmentMeta& meta) {
  FileRefs file_refs;
  file_refs.reserve(meta.files.size());

  auto& refs = dir.attributes().refs();
  for (const auto& file : meta.files) {
    // cppcheck-suppress useStlAlgorithm
    file_refs.emplace_back(refs.add(file));
  }

  return file_refs;
}

}  // namespace

SegmentReaderImpl::SegmentReaderImpl(PrivateTag) noexcept {}

SegmentReaderImpl::~SegmentReaderImpl() = default;

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::Open(
  const directory& dir, const SegmentMeta& meta,
  const IndexReaderOptions& options) {
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{});
  // read optional docs_mask
  DocumentMask docs_mask;
  if (options.doc_mask) {
    index_utils::ReadDocumentMask(docs_mask, dir, meta);
  }
  reader->Update(dir, meta, std::move(docs_mask));
  // open index data
  IRS_ASSERT(meta.codec != nullptr);
  // always instantiate to avoid unnecessary checks
  reader->field_reader_ = meta.codec->get_field_reader();
  if (options.index) {
    reader->field_reader_->prepare(
      ReaderState{.dir = &dir, .meta = &meta, .scorers = options.scorers});
  }
  // open column store
  reader->data_ = std::make_shared<ColumnData>();
  reader->sort_ =
    reader->data_->Open(dir, meta, options, *reader->field_reader_);
  return reader;
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::ReopenColumnStore(
  const directory& dir, const SegmentMeta& meta,
  const IndexReaderOptions& options) const {
  IRS_ASSERT(meta == info_);
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{});
  // clone removals
  reader->refs_ = refs_;
  reader->info_ = info_;
  reader->docs_mask_ = docs_mask_;
  // clone index data
  reader->field_reader_ = field_reader_;
  // open column store
  reader->data_ = std::make_shared<ColumnData>();
  reader->sort_ = reader->data_->Open(dir, meta, options, *field_reader_);
  return reader;
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::ReopenDocsMask(
  const directory& dir, const SegmentMeta& meta,
  DocumentMask&& docs_mask) const {
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{});
  // clone field reader
  reader->field_reader_ = field_reader_;
  // clone column store
  reader->data_ = data_;
  reader->sort_ = sort_;
  // update removals
  reader->Update(dir, meta, std::move(docs_mask));
  return reader;
}

void SegmentReaderImpl::Update(const directory& dir, const SegmentMeta& meta,
                               DocumentMask&& docs_mask) noexcept {
  IRS_ASSERT(meta.live_docs_count <= meta.docs_count);
  IRS_ASSERT(docs_mask.size() <= meta.docs_count);
  // TODO(MBkkt) on practice only mask file changed, so it can be optimized
  refs_ = GetRefs(dir, meta);
  info_ = meta;
  docs_mask_ = std::move(docs_mask);
  info_.live_docs_count = info_.docs_count - docs_mask_.size();
}

void SegmentReaderImpl::CountMemory(const MemoryStats& stats) const {
  // TODO(Dronplane) compute stats.pinned_memory
  if (field_reader_ != nullptr) {
    field_reader_->CountMemory(stats);
  }
  if (data_ != nullptr && data_->columnstore_reader_ != nullptr) {
    data_->columnstore_reader_->CountMemory(stats);
  }
}

const irs::column_reader* SegmentReaderImpl::column(
  std::string_view name) const {
  const auto& named_columns = data_->named_columns_;
  const auto it = named_columns.find(name);
  return it == named_columns.end() ? nullptr : it->second;
}

const irs::column_reader* SegmentReaderImpl::column(field_id field) const {
  IRS_ASSERT(data_->columnstore_reader_);
  return data_->columnstore_reader_->column(field);
}

column_iterator::ptr SegmentReaderImpl::columns() const {
  struct Less {
    bool operator()(const irs::column_reader& lhs,
                    std::string_view rhs) const noexcept {
      return lhs.name() < rhs;
    }
  };

  using IteratorT =
    iterator_adaptor<std::string_view, irs::column_reader,
                     decltype(data_->sorted_named_columns_.begin()),
                     column_iterator, Less>;

  return memory::make_managed<IteratorT>(data_->sorted_named_columns_.begin(),
                                         data_->sorted_named_columns_.end());
}

doc_iterator::ptr SegmentReaderImpl::docs_iterator() const {
  // Implementations generate doc_ids sequentially
  const auto docs_count = static_cast<uint32_t>(info_.docs_count);
  if (docs_mask_.empty()) {
    return memory::make_managed<AllIterator>(docs_count);
  }
  // Optimization instead of MaskDocIterator(AllIterator)
  return memory::make_managed<MaskAllIterator>(docs_count, docs_mask_);
}

doc_iterator::ptr SegmentReaderImpl::mask(doc_iterator::ptr&& it) const {
  if (!it || docs_mask_.empty()) {
    return std::move(it);
  }

  return memory::make_managed<MaskDocIterator>(std::move(it), docs_mask_);
}

const irs::column_reader* SegmentReaderImpl::ColumnData::Open(
  const directory& dir, const SegmentMeta& meta,
  const IndexReaderOptions& options, const field_reader& field_reader) {
  IRS_ASSERT(meta.codec != nullptr);
  const auto& codec = *meta.codec;
  // always instantiate to avoid unnecessary checks
  columnstore_reader_ = codec.get_columnstore_reader();

  if (!options.columnstore || !meta.column_store) {
    return {};
  }

  // initialize optional columnstore
  columnstore_reader::options columnstore_opts;
  if (options.warmup_columns) {
    columnstore_opts.warmup_column = [warmup = options.warmup_columns,
                                      &field_reader,
                                      &meta](const column_reader& column) {
      return warmup(meta, field_reader, column);
    };
    columnstore_opts.pinned_memory = options.pinned_memory_accounting;
  }

  if (!columnstore_reader_->prepare(dir, meta, columnstore_opts)) {
    throw index_error{
      absl::StrCat("Failed to find existing (according to meta) "
                   "columnstore in segment '",
                   meta.name, "'")};
  }

  const irs::column_reader* sort{};
  if (field_limits::valid(meta.sort)) {
    sort = columnstore_reader_->column(meta.sort);

    if (sort == nullptr) {
      throw index_error{absl::StrCat(
        "Failed to find sort column '", meta.sort,
        "' (according to meta) in columnstore in segment '", meta.name, "'")};
    }
  }

  // FIXME(gnusi): too rough, we must exclude unnamed columns
  const auto num_columns = columnstore_reader_->size();
  named_columns_.reserve(num_columns);
  sorted_named_columns_.reserve(num_columns);

  columnstore_reader_->visit([this, &meta](const irs::column_reader& column) {
    const auto name = column.name();

    if (!IsNull(name)) {
      const auto [it, is_new] = named_columns_.emplace(name, &column);
      IRS_IGNORE(it);

      if (IRS_UNLIKELY(!is_new)) {
        throw index_error{absl::StrCat("Duplicate named column '", name,
                                       "' in segment '", meta.name, "'")};
      }

      if (!sorted_named_columns_.empty() &&
          sorted_named_columns_.back().get().name() >= name) {
        throw index_error{absl::StrCat(
          "Named columns are out of order in segment '", meta.name, "'")};
      }

      sorted_named_columns_.emplace_back(column);
    }

    return true;
  });
  return sort;
}

}  // namespace irs
