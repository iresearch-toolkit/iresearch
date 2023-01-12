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

class AllIterator final : public doc_iterator {
 public:
  explicit AllIterator(doc_id_t docs_count) noexcept
    : max_doc_{doc_limits::min() + docs_count - 1} {}

  bool next() noexcept override {
    if (doc_.value < max_doc_) {
      ++doc_.value;
      return true;
    } else {
      doc_.value = doc_limits::eof();
      return false;
    }
  }

  doc_id_t seek(doc_id_t target) noexcept override {
    doc_.value = target <= max_doc_ ? target : doc_limits::eof();

    return doc_.value;
  }

  doc_id_t value() const noexcept override { return doc_.value; }

  attribute* get_mutable(irs::type_info::type_id type) noexcept override {
    return irs::type<document>::id() == type ? &doc_ : nullptr;
  }

 private:
  document doc_;
  doc_id_t max_doc_;  // largest valid doc_id
};

class MaskDocIterator final : public doc_iterator {
 public:
  MaskDocIterator(doc_iterator::ptr&& it, const document_mask& mask) noexcept
    : mask_{mask}, it_{std::move(it)} {}

  bool next() override {
    while (it_->next()) {
      if (!mask_.contains(value())) {
        return true;
      }
    }

    return false;
  }

  doc_id_t seek(doc_id_t target) override {
    const auto doc = it_->seek(target);

    if (!mask_.contains(doc)) {
      return doc;
    }

    next();

    return value();
  }

  doc_id_t value() const override { return it_->value(); }

  attribute* get_mutable(irs::type_info::type_id type) noexcept override {
    return it_->get_mutable(type);
  }

 private:
  const document_mask& mask_;  // excluded document ids
  doc_iterator::ptr it_;
};

class MaskedDocIterator final : public doc_iterator, private util::noncopyable {
 public:
  MaskedDocIterator(doc_id_t begin, doc_id_t end,
                    const document_mask& docs_mask) noexcept
    : docs_mask_{docs_mask}, end_{end}, next_{begin} {}

  bool next() override {
    while (next_ < end_) {
      current_.value = next_++;

      if (!docs_mask_.contains(current_.value)) {
        return true;
      }
    }

    current_.value = doc_limits::eof();

    return false;
  }

  doc_id_t seek(doc_id_t target) override {
    next_ = target;
    next();

    return value();
  }

  attribute* get_mutable(irs::type_info::type_id type) noexcept override {
    return irs::type<document>::id() == type ? &current_ : nullptr;
  }

  doc_id_t value() const override { return current_.value; }

 private:
  const document_mask& docs_mask_;
  document current_;
  const doc_id_t end_;  // past last valid doc_id
  doc_id_t next_;
};

std::vector<index_file_refs::ref_t> GetRefs(const directory& dir,
                                            const SegmentMeta& meta) {
  std::vector<index_file_refs::ref_t> file_refs;
  file_refs.reserve(meta.files.size());

  auto& refs = dir.attributes().refs();
  for (auto& file : meta.files) {
    file_refs.emplace_back(refs.add(file));
  }

  return file_refs;
}

}  // namespace

/*static*/ std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::Open(
  const directory& dir, const SegmentMeta& meta,
  const IndexReaderOptions& opts) {
  auto& codec = *meta.codec;

  auto reader = std::make_shared<SegmentReaderImpl>(dir, meta, opts);

  // read document mask
  if (opts.doc_mask) {
    index_utils::ReadDocumentMask(reader->docs_mask_, dir, meta);
  } else {
    reader->info_.live_docs_count = reader->info_.docs_count;
  }

  auto& data = *reader->data_;

  // always instantiate to avoid unnecessary checks
  auto& field_reader = data.field_reader_;
  field_reader = codec.get_field_reader();

  if (opts.index) {
    // initialize optional field reader
    field_reader->prepare(dir, meta, reader->docs_mask_);
  }

  // always instantiate to avoid unnecessary checks
  auto& columnstore_reader = data.columnstore_reader_;
  columnstore_reader = codec.get_columnstore_reader();

  if (opts.columnstore && meta.column_store) {
    // initialize optional columnstore
    columnstore_reader::options columnstore_opts;
    if (const auto& opts = reader->Options(); opts.warmup_columns) {
      columnstore_opts.warmup_column = [warmup = opts.warmup_columns,
                                        &field_reader,
                                        &meta](const column_reader& column) {
        return warmup(meta, *field_reader, column);
      };
      columnstore_opts.pinned_memory = opts.pinned_memory_accounting;
    }

    if (!columnstore_reader->prepare(dir, meta, columnstore_opts)) {
      throw index_error{
        absl::StrCat("Failed to find existing (according to meta) "
                     "columnstore in segment '",
                     meta.name, "'")};
    }

    if (field_limits::valid(meta.sort)) {
      reader->sort_ = columnstore_reader->column(meta.sort);

      if (!reader->sort_) {
        throw index_error{absl::StrCat(
          "Failed to find sort column '", meta.sort,
          "' (according to meta) in columnstore in segment '", meta.name, "'")};
      }
    }

    // FIXME(gnusi): too rough, we must exclude unnamed columns
    const size_t num_columns = columnstore_reader->size();

    auto& named_columns = data.named_columns_;
    named_columns.reserve(num_columns);
    auto& sorted_named_columns = data.sorted_named_columns_;
    sorted_named_columns.reserve(num_columns);

    columnstore_reader->visit([&named_columns, &sorted_named_columns,
                               &meta](const irs::column_reader& column) {
      const auto name = column.name();

      if (!IsNull(name)) {
        const auto [it, is_new] = named_columns.emplace(name, &column);
        IRS_IGNORE(it);

        if (IRS_UNLIKELY(!is_new)) {
          throw index_error{absl::StrCat("Duplicate named column '", name,
                                         "' in segment '", meta.name, "'")};
        }

        if (!sorted_named_columns.empty() &&
            sorted_named_columns.back().get().name() >= name) {
          throw index_error{absl::StrCat(
            "Named columns are out of order in segment '", meta.name, "'")};
        }

        sorted_named_columns.emplace_back(column);
      }

      return true;
    });
  }

  return reader;
}

SegmentReaderImpl::~SegmentReaderImpl() = default;

SegmentReaderImpl::SegmentReaderImpl(const directory& dir,
                                     const SegmentMeta& meta,
                                     const IndexReaderOptions& opts)
  : file_refs_{GetRefs(dir, meta)},
    data_{std::make_shared<Data>()},
    info_{meta},
    dir_{&dir},
    opts_{opts} {}

SegmentReaderImpl::SegmentReaderImpl(const SegmentReaderImpl& rhs,
                                     const SegmentMeta& meta,
                                     document_mask&& docs_mask)
  : file_refs_{GetRefs(rhs.Dir(), meta)},
    docs_mask_{std::move(docs_mask)},
    data_{rhs.data_},
    info_{meta},
    dir_{rhs.dir_},
    sort_{rhs.sort_},
    opts_{rhs.opts_} {}

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
  struct less {
    bool operator()(const irs::column_reader& lhs,
                    std::string_view rhs) const noexcept {
      return lhs.name() < rhs;
    }
  };

  using iterator_t =
    iterator_adaptor<std::string_view, irs::column_reader,
                     decltype(data_->sorted_named_columns_.begin()),
                     column_iterator, less>;

  return memory::make_managed<iterator_t>(
    std::begin(data_->sorted_named_columns_),
    std::end(data_->sorted_named_columns_));
}

doc_iterator::ptr SegmentReaderImpl::docs_iterator() const {
  if (docs_mask_.empty()) {
    return memory::make_managed<AllIterator>(info_.docs_count);
  }

  // the implementation generates doc_ids sequentially
  return memory::make_managed<MaskedDocIterator>(
    doc_limits::min(), doc_limits::min() + info_.docs_count, docs_mask_);
}

doc_iterator::ptr SegmentReaderImpl::mask(doc_iterator::ptr&& it) const {
  if (!it) {
    return nullptr;
  }

  if (docs_mask_.empty()) {
    return std::move(it);
  }

  return memory::make_managed<MaskDocIterator>(std::move(it), docs_mask_);
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::Reopen(
  const SegmentMeta& meta) const {
  IRS_ASSERT(this->Meta().version != meta.version);

  document_mask docs_mask;

  // read document mask
  if (Options().doc_mask) {
    index_utils::ReadDocumentMask(docs_mask, Dir(), meta);
  }

  return std::make_shared<SegmentReaderImpl>(*this, meta, std::move(docs_mask));
}

}  // namespace irs
