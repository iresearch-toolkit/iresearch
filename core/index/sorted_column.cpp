////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#include "sorted_column.hpp"

#include "analysis/token_attributes.hpp"
#include "index/comparer.hpp"
#include "search/cost.hpp"
#include "shared.hpp"
#include "utils/attribute_helper.hpp"
#include "utils/type_limits.hpp"

namespace irs {

class SortedColumnIterator : public doc_iterator {
 public:
  explicit SortedColumnIterator(
    std::span<const SortedColumn::Value> values) noexcept
    : next_{values.data()}, end_{next_ + values.size()} {
    // Iterator can be created only after flushing the column
    IRS_ASSERT(!values.empty());
    IRS_ASSERT(doc_limits::eof(values.back().key));

    std::get<cost>(attrs_).reset(values.size());
  }

  attribute* get_mutable(irs::type_info::type_id type) noexcept final {
    return irs::get_mutable(attrs_, type);
  }

  doc_id_t value() const noexcept final {
    return std::get<document>(attrs_).value;
  }

  doc_id_t seek(doc_id_t target) noexcept final {
    if (IRS_UNLIKELY(target <= value())) {
      return target;
    }

    next_ = std::lower_bound(next_, end_, target,
                             [](const auto& value, doc_id_t target) noexcept {
                               return value.key < target;
                             });

    return next();
  }

  bool next() noexcept final {
    auto& doc = std::get<document>(attrs_);

    if (IRS_UNLIKELY(next_ == end_)) {
      doc.value = doc_limits::eof();
      return false;
    }

    auto& payload = std::get<irs::payload>(attrs_);

    doc.value = next_->key;
    payload.value = {data_.data() + next_->begin, next_->size};

    ++next_;

    return true;
  }

 private:
  using attributes = std::tuple<document, cost, irs::payload>;

  attributes attrs_;
  const SortedColumn::Value* next_;
  const SortedColumn::Value* end_;
  bytes_view data_;
};

bool SortedColumn::FlushSparsePrimary(
  DocMap& docmap, const columnstore_writer::values_writer_f& writer,
  doc_id_t docs_count, const Comparer& compare) {
  auto comparer = [&](const auto& lhs, const auto& rhs) {
    return compare.Compare(GetPayload(lhs), GetPayload(rhs));
  };

  if (std::is_sorted(index_.begin(), index_.end(),
                     [&](const auto& lhs, const auto& rhs) {
                       return comparer(lhs, rhs) < 0;
                     })) {
    return false;
  }

  docmap.resize(doc_limits::min() + docs_count);

  std::vector<size_t> sorted_index(index_.size());
  std::iota(sorted_index.begin(), sorted_index.end(), 0);
  std::sort(sorted_index.begin(), sorted_index.end(),
            [&](size_t lhs, size_t rhs) {
              IRS_ASSERT(lhs < index_.size());
              IRS_ASSERT(rhs < index_.size());
              if (const auto r = comparer(index_[lhs], index_[rhs]); r) {
                return r < 0;
              }
              return lhs < rhs;
            });

  doc_id_t new_doc = doc_limits::min();

  for (size_t idx : sorted_index) {
    const auto* value = &index_[idx];

    doc_id_t min = doc_limits::min();
    if (IRS_LIKELY(idx)) {
      min += std::prev(value)->key;
    }

    for (const doc_id_t max = value->key; min < max; ++min) {
      docmap[min] = new_doc++;
    }

    docmap[min] = new_doc;
    WriteValue(writer(new_doc), *value);
    ++new_doc;
  }

  // Ensure that all docs up to new_doc are remapped without gaps
  IRS_ASSERT(std::all_of(docmap.begin() + 1, docmap.begin() + new_doc,
                         [](doc_id_t doc) { return doc_limits::valid(doc); }));
  // Ensure we reached the last doc in sort column
  IRS_ASSERT((std::prev(index_.end(), 1)->key + 1) == new_doc);
  // Handle docs without sort value that are placed after last filled sort doc
  for (auto begin = std::next(docmap.begin(), new_doc); begin != docmap.end();
       ++begin) {
    IRS_ASSERT(!doc_limits::valid(*begin));
    *begin = new_doc++;
  }

  return true;
}

std::pair<DocMap, field_id> SortedColumn::Flush(
  columnstore_writer& writer, columnstore_writer::column_finalizer_f finalizer,
  doc_id_t docs_count, const Comparer& compare) {
  IRS_ASSERT(index_.size() <= docs_count);
  IRS_ASSERT(index_.empty() || index_.back().key <= docs_count);

  Prepare(doc_limits::eof());  // Insert last pending value

  if (IRS_UNLIKELY(index_.empty())) {
    return {{}, field_limits::invalid()};
  }

  DocMap docmap;
  auto [column_id, column_writer] =
    writer.push_column(info_, std::move(finalizer));

  if (!FlushSparsePrimary(docmap, column_writer, docs_count, compare)) {
    FlushAlreadySorted(column_writer);
  }

  return {std::move(docmap), column_id};
}

void SortedColumn::FlushAlreadySorted(
  const columnstore_writer::values_writer_f& writer) {
  for (const auto& value : index_) {
    WriteValue(writer(value.key), value);
  }
}

bool SortedColumn::FlushDense(const columnstore_writer::values_writer_f& writer,
                              DocMapView docmap, FlushBuffer& buffer) {
  IRS_ASSERT(!docmap.empty());

  const size_t total = docmap.size() - 1;  // -1 for the first element
  const size_t size = index_.size();

  if (!use_dense_sort(size, total)) {
    return false;
  }

  buffer.clear();
  buffer.resize(total, std::pair{doc_limits::eof(), doc_limits::invalid()});

  for (size_t i = 0; i < size; ++i) {
    buffer[docmap[index_[i].key] - doc_limits::min()].first =
      static_cast<doc_id_t>(i);
  }

  // flush sorted data
  irs::doc_id_t doc = doc_limits::min();
  for (const auto& entry : buffer) {
    if (!doc_limits::eof(entry.first)) {
      WriteValue(writer(doc), index_[entry.first]);
    }
    ++doc;
  };

  return true;
}

void SortedColumn::FlushSparse(
  const columnstore_writer::values_writer_f& writer, DocMapView docmap,
  FlushBuffer& buffer) {
  IRS_ASSERT(!docmap.empty());

  const size_t size = index_.size();

  buffer.resize(size);

  for (size_t i = 0; i < size; ++i) {
    buffer[i] = std::pair{doc_id_t(i), docmap[index_[i].key]};
  }

  std::sort(buffer.begin(), buffer.end(),
            [](std::pair<doc_id_t, doc_id_t> lhs,
               std::pair<doc_id_t, doc_id_t> rhs) noexcept {
              return lhs.second < rhs.second;
            });

  // flush sorted data
  for (const auto& entry : buffer) {
    WriteValue(writer(entry.second), index_[entry.first]);
  }
}

field_id SortedColumn::Flush(columnstore_writer& writer,
                             columnstore_writer::column_finalizer_f finalizer,
                             DocMapView docmap, FlushBuffer& buffer) {
  IRS_ASSERT(docmap.size() < irs::doc_limits::eof());

  Prepare(doc_limits::eof());  // Insert last pending value

  if (IRS_UNLIKELY(index_.empty())) {
    return field_limits::invalid();
  }

  auto [column_id, column_writer] =
    writer.push_column(info_, std::move(finalizer));

  if (docmap.empty()) {
    FlushAlreadySorted(column_writer);
  } else if (!FlushDense(column_writer, docmap, buffer)) {
    FlushSparse(column_writer, docmap, buffer);
  }

  return column_id;
}

doc_iterator::ptr SortedColumn::iterator() const {
  return memory::make_managed<SortedColumnIterator>(index_);
}

}  // namespace irs
