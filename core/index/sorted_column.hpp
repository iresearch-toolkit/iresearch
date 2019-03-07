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

class comparer {
 public:
  virtual ~comparer() = default;

  bool operator()(const bytes_ref& lhs, const bytes_ref& rhs) const {
    return less(lhs, rhs);
  }

 protected:
  virtual bool less(const bytes_ref& lhs, const bytes_ref& rhs) const = 0;
}; // comparer

class bitvector;

class doc_map {
 public:
  enum Type { OLD, NEW };

  doc_map() = default;

  doc_map(
    std::vector<doc_id_t>&& new_old,
    std::vector<doc_id_t>&& old_new
  ) NOEXCEPT;

  doc_map(doc_map&& rhs) NOEXCEPT
    : new_old_(std::move(rhs.new_old_)),
      old_new_(std::move(rhs.old_new_)) {
  }

  doc_map& operator=(doc_map&& rhs) NOEXCEPT {
    if (this != &rhs) {
      new_old_ = std::move(rhs.new_old_);
      old_new_ = std::move(rhs.old_new_);
    }
    return *this;
  }

  template<Type type>
  doc_id_t get(doc_id_t doc) const NOEXCEPT {
    if (type == OLD) {
      assert(doc < new_old_.size());
      return new_old_[doc];
    }

    assert(doc < old_new_.size());
    return old_new_[doc];
  }

  template<Type type, typename Visitor>
  bool visit(Visitor visitor) const {
    const std::vector<doc_id_t>& map = type == OLD
      ? new_old_
      : old_new_;

    for (const auto doc : map) {
      if (!visitor(doc)) {
        return false;
      }
    }

    return true;
  }

  bool empty() const NOEXCEPT {
    return new_old_.empty();
  }

  size_t size() const NOEXCEPT {
    return new_old_.size();
  }

 private:
  std::vector<doc_id_t> new_old_;
  std::vector<doc_id_t> old_new_;
}; // doc_map

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

  // 1st - doc map (old->new, new->old)
  // 2nd - flushed column identifier
  std::pair<doc_map, field_id> flush(
    columnstore_writer& writer,
    doc_id_t max,
    const bitvector& docs_mask,
    const comparer& less
  );

  size_t memory() const NOEXCEPT {
    return data_buf_.size() + index_.size()*sizeof(decltype(index_)::value_type);
  }

 private:
  bytes_output data_buf_;
  std::vector<std::pair<irs::doc_id_t, size_t>> index_; // doc_id + offset in 'data_buf_'
}; // sorted_column

NS_END // ROOT

#endif // IRESEARCH_SORTED_COLUMN_H
