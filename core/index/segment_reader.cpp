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

namespace {

using namespace irs;

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

}  // namespace

namespace irs {

class segment_reader_impl : public sub_reader {
 public:
  static sub_reader::ptr open(const directory& dir, const segment_meta& meta,
                              const index_reader_options& warmup);

  segment_reader_impl(const directory& dir, uint64_t meta_version,
                      uint64_t docs_count, const index_reader_options& opts);

  const directory& dir() const noexcept { return dir_; }

  column_iterator::ptr columns() const override;

  using sub_reader::docs_count;
  uint64_t docs_count() const override { return docs_count_; }

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
    return field_reader_->field(name);
  }

  field_iterator::ptr fields() const override {
    return field_reader_->iterator();
  }

  uint64_t live_docs_count() const noexcept override {
    return docs_count_ - docs_mask_.size();
  }

  uint64_t meta_version() const noexcept { return meta_version_; }

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

  const index_reader_options& opts() const noexcept { return opts_; }

 private:
  using named_columns =
    absl::flat_hash_map<hashed_string_view, const irs::column_reader*>;
  using sorted_named_columns =
    std::vector<std::reference_wrapper<const irs::column_reader>>;

  columnstore_reader::ptr columnstore_reader_;
  const irs::column_reader* sort_{};
  const directory& dir_;
  uint64_t docs_count_;
  document_mask docs_mask_;
  field_reader::ptr field_reader_;
  uint64_t meta_version_;
  named_columns named_columns_;
  sorted_named_columns sorted_named_columns_;
  index_reader_options opts_;
};

segment_reader::segment_reader(impl_ptr&& impl) noexcept
  : impl_(std::move(impl)) {}

segment_reader::segment_reader(const segment_reader& other) noexcept {
  *this = other;
}

segment_reader& segment_reader::operator=(
  const segment_reader& other) noexcept {
  if (this != &other) {
    // make a copy
    impl_ptr impl = std::atomic_load(&other.impl_);

    std::atomic_store(&impl_, impl);
  }

  return *this;
}

/*static*/ segment_reader segment_reader::open(
  const directory& dir, const segment_meta& meta,
  const index_reader_options& opts) {
  return segment_reader_impl::open(dir, meta, opts);
}

segment_reader segment_reader::reopen(const segment_meta& meta) const {
  // make a copy
  impl_ptr impl = std::atomic_load(&impl_);

  auto& reader_impl = down_cast<segment_reader_impl>(*impl);

  // reuse self if no changes to meta
  return reader_impl.meta_version() == meta.version
           ? *this
           : segment_reader_impl::open(reader_impl.dir(), meta,
                                       reader_impl.opts());
}

segment_reader_impl::segment_reader_impl(const directory& dir,
                                         uint64_t meta_version,
                                         uint64_t docs_count,
                                         const index_reader_options& opts)
  : dir_(dir),
    docs_count_(docs_count),
    meta_version_(meta_version),
    opts_(opts) {}

const irs::column_reader* segment_reader_impl::column(
  std::string_view name) const {
  auto it = named_columns_.find(hashed_string_view{name});
  return it == named_columns_.end() ? nullptr : it->second;
}

column_iterator::ptr segment_reader_impl::columns() const {
  struct less {
    bool operator()(const irs::column_reader& lhs,
                    std::string_view rhs) const noexcept {
      return lhs.name() < rhs;
    }
  };  // less

  typedef iterator_adaptor<std::string_view, irs::column_reader,
                           decltype(sorted_named_columns_.begin()),
                           column_iterator, less>
    iterator_t;

  return memory::make_managed<iterator_t>(std::begin(sorted_named_columns_),
                                          std::end(sorted_named_columns_));
}

doc_iterator::ptr segment_reader_impl::docs_iterator() const {
  if (docs_mask_.empty()) {
    return memory::make_managed<::all_iterator>(docs_count_);
  }

  // the implementation generates doc_ids sequentially
  return memory::make_managed<masked_docs_iterator>(
    doc_limits::min(), doc_limits::min() + docs_count_, docs_mask_);
}

/*static*/ sub_reader::ptr segment_reader_impl::open(
  const directory& dir, const segment_meta& meta,
  const index_reader_options& opts) {
  auto& codec = *meta.codec;

  auto reader = std::make_shared<segment_reader_impl>(dir, meta.version,
                                                      meta.docs_count, opts);

  // read document mask
  index_utils::read_document_mask(reader->docs_mask_, dir, meta);

  // initialize mandatory field reader
  auto& field_reader = reader->field_reader_;
  field_reader = codec.get_field_reader();
  field_reader->prepare(dir, meta, reader->docs_mask_);

  // initialize optional columnstore
  if (irs::has_columnstore(meta)) {
    auto& columnstore_reader = reader->columnstore_reader_;
    columnstore_reader::options columnstore_opts;
    if (reader->opts().warmup_columns) {
      columnstore_opts.warmup_column = [warmup = reader->opts().warmup_columns,
                                        &field_reader,
                                        &meta](const column_reader& column) {
        return warmup(meta, *field_reader, column);
      };
      columnstore_opts.pinned_memory = reader->opts().pinned_memory_accounting;
    }
    columnstore_reader = codec.get_columnstore_reader();

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

    auto& named_columns = reader->named_columns_;
    named_columns.reserve(num_columns);
    auto& sorted_named_columns = reader->sorted_named_columns_;
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

const irs::column_reader* segment_reader_impl::column(field_id field) const {
  return columnstore_reader_ ? columnstore_reader_->column(field) : nullptr;
}

}  // namespace irs
