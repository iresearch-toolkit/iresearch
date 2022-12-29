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

#include "segment_reader.hpp"

#include "analysis/token_attributes.hpp"
#include "formats/format_utils.hpp"
#include "index/index_meta.hpp"
#include "shared.hpp"
#include "utils/hash_set_utils.hpp"
#include "utils/index_utils.hpp"
#include "utils/singleton.hpp"
#include "utils/type_limits.hpp"

#include <absl/container/flat_hash_map.h>

namespace irs {
namespace {

class all_iterator final : public doc_iterator {
 public:
  explicit all_iterator(doc_id_t docs_count) noexcept
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

class mask_doc_iterator final : public doc_iterator {
 public:
  mask_doc_iterator(doc_iterator::ptr&& it, const document_mask& mask) noexcept
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

class masked_docs_iterator final : public doc_iterator,
                                   private util::noncopyable {
 public:
  masked_docs_iterator(doc_id_t begin, doc_id_t end,
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

  directory_utils::reference(
    dir, meta,
    [&file_refs](index_file_refs::ref_t&& ref) {
      file_refs.emplace_back(std::move(ref));
      return true;
    },
    true);

  return file_refs;
}

}  // namespace

class SegmentReaderImpl final : public sub_reader {
 public:
  static std::shared_ptr<const SegmentReaderImpl> Open(
    const directory& dir, const SegmentMeta& meta,
    const index_reader_options& warmup);

  SegmentReaderImpl(const directory& dir, const SegmentMeta& meta,
                    const index_reader_options& opts);

  SegmentReaderImpl(const SegmentReaderImpl& rhs, const SegmentMeta& meta);

  const directory& Dir() const noexcept { return *dir_; }

  const index_reader_options& Options() const noexcept { return opts_; }

  const SegmentInfo& meta() const override { return info_; }

  std::shared_ptr<const SegmentReaderImpl> Reopen(
    const SegmentMeta& meta) const;

  column_iterator::ptr columns() const override;

  using sub_reader::docs_count;
  uint64_t docs_count() const override { return info_.docs_count; }

  const document_mask* docs_mask() const override { return &docs_mask_; }

  doc_iterator::ptr docs_iterator() const override;

  doc_iterator::ptr mask(doc_iterator::ptr&& it) const override {
    if (!it) {
      return nullptr;
    }

    if (docs_mask_.empty()) {
      return std::move(it);
    }

    return memory::make_managed<mask_doc_iterator>(std::move(it), docs_mask_);
  }

  const term_reader* field(std::string_view name) const override {
    return data_->field_reader_->field(name);
  }

  field_iterator::ptr fields() const override {
    return data_->field_reader_->iterator();
  }

  uint64_t live_docs_count() const noexcept override {
    return info_.live_docs_count;
  }

  const sub_reader& operator[](size_t i) const noexcept override {
    IRS_ASSERT(!i);
    IRS_IGNORE(i);
    return *this;
  }

  size_t size() const noexcept override {
    return 1;  // only 1 segment
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
  index_reader_options opts_;
};

SegmentReaderImpl::SegmentReaderImpl(const directory& dir,
                                     const SegmentMeta& meta,
                                     const index_reader_options& opts)
  : file_refs_{GetRefs(dir, meta)},
    data_{std::make_shared<Data>()},
    info_{meta},
    dir_{&dir},
    opts_{opts} {}

SegmentReaderImpl::SegmentReaderImpl(const SegmentReaderImpl& rhs,
                                     const SegmentMeta& meta)
  : file_refs_{GetRefs(rhs.Dir(), meta)},
    data_{rhs.data_},
    info_{meta},
    dir_{rhs.dir_},
    sort_{rhs.sort_},
    opts_{rhs.opts_} {}

const irs::column_reader* SegmentReaderImpl::column(
  std::string_view name) const {
  const auto& named_columns = data_->named_columns_;
  const auto it = named_columns.find(hashed_string_view{name});
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
    return memory::make_managed<all_iterator>(info_.docs_count);
  }

  // the implementation generates doc_ids sequentially
  return memory::make_managed<masked_docs_iterator>(
    doc_limits::min(), doc_limits::min() + info_.docs_count, docs_mask_);
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::Reopen(
  const SegmentMeta& meta) const {
  IRS_ASSERT(this->meta().version != meta.version);
  auto reader = std::make_shared<SegmentReaderImpl>(*this, meta);

  // read document mask
  if (Options().doc_mask) {
    index_utils::read_document_mask(reader->docs_mask_, Dir(), meta);
  }

  return reader;
}

/*static*/ std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::Open(
  const directory& dir, const SegmentMeta& meta,
  const index_reader_options& opts) {
  auto& codec = *meta.codec;

  auto reader = std::make_shared<SegmentReaderImpl>(dir, meta, opts);

  // read document mask
  if (opts.doc_mask) {
    index_utils::read_document_mask(reader->docs_mask_, dir, meta);
  }

  // always instantiate to avoid unnecessary checks
  auto& field_reader = reader->data_->field_reader_;
  field_reader = codec.get_field_reader();

  if (opts.index) {
    // initialize optional field reader
    field_reader->prepare(dir, meta, reader->docs_mask_);
  }

  // always instantiate to avoid unnecessary checks
  auto& columnstore_reader = reader->data_->columnstore_reader_;
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
      throw index_error(
        string_utils::to_string("failed to find existing (according to meta) "
                                "columnstore in segment '%s'",
                                meta.name.c_str()));
    }

    if (field_limits::valid(meta.sort)) {
      reader->sort_ = columnstore_reader->column(meta.sort);

      if (!reader->sort_) {
        throw index_error(string_utils::to_string(
          "failed to find sort column '" IR_UINT64_T_SPECIFIER
          "' (according to meta) in columnstore in segment '%s'",
          meta.sort, meta.name.c_str()));
      }
    }

    // FIXME(gnusi): too rough, we must exclude unnamed columns
    const size_t num_columns = columnstore_reader->size();

    auto& named_columns = reader->data_->named_columns_;
    named_columns.reserve(num_columns);
    auto& sorted_named_columns = reader->data_->sorted_named_columns_;
    sorted_named_columns.reserve(num_columns);

    columnstore_reader->visit([&named_columns, &sorted_named_columns,
                               &meta](const irs::column_reader& column) {
      const auto name = column.name();

      if (!IsNull(name)) {
        const auto [it, is_new] =
          named_columns.emplace(hashed_string_view{name}, &column);
        IRS_IGNORE(it);

        if (IRS_UNLIKELY(!is_new)) {
          throw index_error(string_utils::to_string(
            "duplicate named column '%s' in segment '%s'",
            static_cast<std::string>(name).c_str(), meta.name.c_str()));
        }

        if (!sorted_named_columns.empty() &&
            sorted_named_columns.back().get().name() >= name) {
          throw index_error(string_utils::to_string(
            "Named columns are out of order in segment '%s'",
            meta.name.c_str()));
        }

        sorted_named_columns.emplace_back(column);
      }

      return true;
    });
  }

  return reader;
}

segment_reader::segment_reader(
  std::shared_ptr<const SegmentReaderImpl> impl) noexcept
  : impl_{std::move(impl)} {}

segment_reader::segment_reader(const segment_reader& other) noexcept
  : impl_{std::atomic_load(&other.impl_)} {}

segment_reader& segment_reader::operator=(
  const segment_reader& other) noexcept {
  if (this != &other) {
    // make a copy
    auto impl = std::atomic_load(&other.impl_);

    std::atomic_store(&impl_, impl);
  }

  return *this;
}

/*static*/ segment_reader segment_reader::open(
  const directory& dir, const SegmentMeta& meta,
  const index_reader_options& opts) {
  return segment_reader{SegmentReaderImpl::Open(dir, meta, opts)};
}

segment_reader segment_reader::reopen(const SegmentMeta& meta) const {
  // make a copy
  auto impl = std::atomic_load(&impl_);

  // reuse self if no changes to meta
  return segment_reader{
    impl->meta().version == meta.version ? impl : impl->Reopen(meta)};
}

field_iterator::ptr segment_reader::fields() const { return impl_->fields(); }

uint64_t segment_reader::live_docs_count() const {
  return impl_->live_docs_count();
}

size_t segment_reader::size() const { return impl_->size(); }

const irs::column_reader* segment_reader::sort() const { return impl_->sort(); }

const irs::column_reader* segment_reader::column(std::string_view name) const {
  return impl_->column(name);
}

const irs::column_reader* segment_reader::column(field_id field) const {
  return impl_->column(field);
}

segment_reader::operator sub_reader::ptr() const noexcept { return impl_; }

// FIXME find a better way to mask documents
doc_iterator::ptr segment_reader::mask(doc_iterator::ptr&& it) const {
  return impl_->mask(std::move(it));
}

const term_reader* segment_reader::field(std::string_view name) const {
  return impl_->field(name);
}

uint64_t segment_reader::docs_count() const { return impl_->docs_count(); }

doc_iterator::ptr segment_reader::docs_iterator() const {
  return impl_->docs_iterator();
}

column_iterator::ptr segment_reader::columns() const {
  return impl_->columns();
}

const SegmentInfo& segment_reader::meta() const { return impl_->meta(); }

const document_mask* segment_reader::docs_mask() const {
  return impl_->docs_mask();
}

}  // namespace irs
