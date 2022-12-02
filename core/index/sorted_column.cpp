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

#include "index/comparer.hpp"
#include "shared.hpp"
#include "utils/type_limits.hpp"

namespace iresearch {

bool sorted_column::flush_sprase_primary(
  doc_map& docmap, const columnstore_writer::values_writer_f& writer,
  doc_id_t docs_count, const comparer& less) {
  auto comparer = [&less, this](const std::pair<doc_id_t, size_t>& lhs,
                                const std::pair<doc_id_t, size_t>& rhs) {
    if (lhs.first == rhs.first) {
      return false;
    }

    return less(get_value(&lhs), get_value(&rhs));
  };

  if (std::is_sorted(index_.begin(), index_.end() - 1, comparer)) {
    return false;
  }

  docmap.resize(doc_limits::min() + docs_count);

  std::vector<size_t> sorted_index(index_.size() - 1);
  std::iota(sorted_index.begin(), sorted_index.end(), 0);
  std::sort(sorted_index.begin(), sorted_index.end(),
            [&comparer, this](size_t lhs, size_t rhs) {
              return comparer(index_[lhs], index_[rhs]);
            });

  doc_id_t new_doc = doc_limits::min();

  for (size_t idx : sorted_index) {
    const auto* value = &index_[idx];

    doc_id_t min =
      doc_limits::min() + static_cast<bool>(idx) * std::prev(value)->first;

    for (const doc_id_t max = value->first; min < max; ++min) {
      docmap[min] = new_doc++;
    }

    docmap[min] = new_doc;
    write_value(writer(new_doc), value);
    ++new_doc;
  }

  for (auto begin = std::next(docmap.begin(), new_doc); begin != docmap.end();
       ++begin) {
    *begin = new_doc++;
  }

  return true;
}

std::pair<doc_map, field_id> sorted_column::flush(
  columnstore_writer& writer, columnstore_writer::column_finalizer_f finalizer,
  doc_id_t docs_count, const comparer& less) {
  assert(index_.size() <= docs_count);
  assert(index_.empty() || index_.back().first <= docs_count);

  if (IRS_UNLIKELY(index_.empty())) {
    return {{}, field_limits::invalid()};
  }

  // temporarily push sentinel
  index_.emplace_back(doc_limits::eof(), data_buf_.size());

  doc_map docmap;
  auto [column_id, column_writer] =
    writer.push_column(info_, std::move(finalizer));

  if (!flush_sprase_primary(docmap, column_writer, docs_count, less)) {
    flush_already_sorted(column_writer);
  }

  clear();  // data have been flushed

  return {std::move(docmap), column_id};
}

void sorted_column::flush_already_sorted(
  const columnstore_writer::values_writer_f& writer) {
  // -1 for sentinel
  for (auto begin = index_.begin(), end = std::prev(index_.end()); begin != end;
       ++begin) {
    write_value(writer(begin->first), &*begin);
  }
}

bool sorted_column::flush_dense(
  const columnstore_writer::values_writer_f& writer, const doc_map& docmap,
  flush_buffer_t& buffer) {
  assert(!docmap.empty());

  const size_t total = docmap.size() - 1;  // -1 for first element
  const size_t size = index_.size() - 1;   // -1 for sentinel

  if (!use_dense_sort(size, total)) {
    return false;
  }

  buffer.clear();
  buffer.resize(total, std::pair{doc_limits::eof(), doc_limits::invalid()});

  for (size_t i = 0; i < size; ++i) {
    buffer[docmap[index_[i].first] - doc_limits::min()].first = doc_id_t(i);
  }

  // flush sorted data
  irs::doc_id_t doc = doc_limits::min();
  for (const auto& entry : buffer) {
    if (!doc_limits::eof(entry.first)) {
      write_value(writer(doc), &index_[entry.first]);
    }
    ++doc;
  };

  return true;
}

void sorted_column::flush_sparse(
  const columnstore_writer::values_writer_f& writer, const doc_map& docmap,
  flush_buffer_t& buffer) {
  assert(!docmap.empty());

  const size_t size = index_.size() - 1;  // -1 for sentinel

  buffer.resize(size);

  for (size_t i = 0; i < size; ++i) {
    buffer[i] = std::pair{doc_id_t(i), docmap[index_[i].first]};
  }

  std::sort(buffer.begin(), buffer.end(),
            [](std::pair<doc_id_t, doc_id_t> lhs,
               std::pair<doc_id_t, doc_id_t> rhs) noexcept {
              return lhs.second < rhs.second;
            });

  // flush sorted data
  for (const auto& entry : buffer) {
    write_value(writer(entry.second), &index_[entry.first]);
  };
}

field_id sorted_column::flush(columnstore_writer& writer,
                              columnstore_writer::column_finalizer_f finalizer,
                              const doc_map& docmap, flush_buffer_t& buffer) {
  assert(docmap.size() < irs::doc_limits::eof());

  if (IRS_UNLIKELY(index_.empty())) {
    return field_limits::invalid();
  }

  auto [column_id, column_writer] =
    writer.push_column(info_, std::move(finalizer));

  // temporarily push sentinel
  index_.emplace_back(doc_limits::eof(), data_buf_.size());

  if (docmap.empty()) {
    flush_already_sorted(column_writer);
  } else if (!flush_dense(column_writer, docmap, buffer)) {
    flush_sparse(column_writer, docmap, buffer);
  }

  clear();  // data have been flushed

  return column_id;
}

}  // namespace iresearch
