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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_SORTED_COLUMN_H
#define IRESEARCH_SORTED_COLUMN_H

#include "formats/formats.hpp"
#include "store/store_utils.hpp"

NS_ROOT

class bitvector;

class sorted_column final : public irs::columnstore_writer::column_output {
 public:
  typedef std::function<bool(const bytes_ref&, const bytes_ref&)> less_f;

  sorted_column() = default;

  void prepare(doc_id_t key) {
    assert(index_.empty() || key >= index_.back().first);

    index_.emplace_back(key, data_buf_.size());
  }

  virtual void close() override {
    // NOOP
  }

  virtual void write_byte(byte_type b) override {
    data_buf_.write_byte(b);
  }

  virtual void write_bytes(const byte_type* b, size_t size) override {
    data_buf_.write_bytes(b, size);
  }

  virtual void reset() override {
    if (index_.empty()) {
      return;
    }

    data_buf_.reset(index_.back().second);
    index_.pop_back();
  }

  bool empty() const NOEXCEPT {
    return index_.empty();
  }

  size_t size() const NOEXCEPT {
    return index_.size();
  }

  void clear() NOEXCEPT {
    data_buf_.reset();
    index_.clear();
  }

  // first - oredered array (new -> old)
  // second - flushed column identifier
  std::pair<std::vector<irs::doc_id_t>, field_id> flush(
    columnstore_writer& writer,
    doc_id_t max,
    const bitvector& docs_mask,
    const less_f& less
  );

  size_t memory() const {
    return data_buf_.size() + index_.size()*sizeof(decltype(index_)::value_type);
  }

 private:
  bytes_output data_buf_;
  std::vector<std::pair<irs::doc_id_t, size_t>> index_; // doc_id + offset in 'data_buf_'
}; // sorted_column

NS_END // ROOT

#endif // IRESEARCH_SORTED_COLUMN_H
