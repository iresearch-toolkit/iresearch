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

#pragma once

#include <vector>

#include "formats/formats.hpp"
#include "index/column_info.hpp"
#include "store/store_utils.hpp"

namespace irs {

class Comparer;

class sorted_column final : public column_output, private util::noncopyable {
 public:
  using FlushBuffer = std::vector<std::pair<doc_id_t, doc_id_t>>;

  explicit sorted_column(const ColumnInfo& info) : info_{info} {}

  void prepare(doc_id_t key) {
    IRS_ASSERT(index_.empty() || key >= index_.back().key);

    if (index_.empty() || index_.back().key != key) {
      index_.emplace_back(key, data_buf_.size());
    }
  }

  void write_byte(byte_type b) final { data_buf_ += b; }

  void write_bytes(const byte_type* b, size_t size) final {
    data_buf_.append(b, size);
  }

  void reset() final {
    if (index_.empty()) {
      return;
    }

    data_buf_.resize(index_.back().offset);
    index_.pop_back();
  }

  bool empty() const noexcept { return index_.empty(); }

  size_t size() const noexcept { return index_.size(); }

  void clear() noexcept {
    data_buf_.clear();
    index_.clear();
  }

  // 1st - doc map (old->new), empty -> already sorted
  // 2nd - flushed column identifier
  std::pair<DocMap, field_id> flush(
    columnstore_writer& writer,
    columnstore_writer::column_finalizer_f header_writer,
    doc_id_t docs_count,  // total number of docs in segment
    const Comparer& compare);

  field_id flush(columnstore_writer& writer,
                 columnstore_writer::column_finalizer_f header_writer,
                 DocMapView docmap, FlushBuffer& buffer);

  size_t memory_active() const noexcept {
    return data_buf_.size() +
           index_.size() * sizeof(decltype(index_)::value_type);
  }

  size_t memory_reserved() const noexcept {
    return data_buf_.capacity() +
           index_.capacity() * sizeof(decltype(index_)::value_type);
  }

  const ColumnInfo& info() const noexcept { return info_; }

  doc_iterator::ptr iterator() const;

 private:
  friend class SortedColumnIterator;

  struct Value {
    Value(doc_id_t key, size_t offset) noexcept : key{key}, offset{offset} {}

    doc_id_t key;
    size_t offset;  // offset in 'data_buf_'
  };

  bytes_view get_value(const Value* value) const noexcept {
    IRS_ASSERT(index_.data() <= value);
    IRS_ASSERT(value < (index_.data() + index_.size() - 1));
    IRS_ASSERT(!doc_limits::eof(value->key));

    const auto begin = value->offset;
    const auto end = (value + 1)->offset;

    return {data_buf_.c_str() + begin, end - begin};
  }

  void write_value(data_output& out, const Value* value) const {
    const auto payload = get_value(value);
    out.write_bytes(payload.data(), payload.size());
  }

  bool flush_sparse_primary(DocMap& docmap,
                            const columnstore_writer::values_writer_f& writer,
                            doc_id_t docs_count, const Comparer& compare);

  void flush_already_sorted(const columnstore_writer::values_writer_f& writer);

  bool flush_dense(const columnstore_writer::values_writer_f& writer,
                   DocMapView docmap, FlushBuffer& buffer);

  void flush_sparse(const columnstore_writer::values_writer_f& writer,
                    DocMapView docmap, FlushBuffer& buffer);

  bstring data_buf_;  // FIXME use memory_file or block_pool instead
  std::vector<Value> index_;
  ColumnInfo info_;
};

}  // namespace irs
