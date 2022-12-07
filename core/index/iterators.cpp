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
////////////////////////////////////////////////////////////////////////////////

#include "index/iterators.hpp"

#include "analysis/token_attributes.hpp"
#include "formats/empty_term_reader.hpp"
#include "index/field_meta.hpp"
#include "search/cost.hpp"
#include "utils/singleton.hpp"
#include "utils/type_limits.hpp"

namespace {

// Represents an iterator with no documents
struct empty_doc_iterator final : irs::doc_iterator {
  empty_doc_iterator() noexcept : cost(0), doc{irs::doc_limits::eof()} {}

  irs::doc_id_t value() const override { return irs::doc_limits::eof(); }
  bool next() override { return false; }
  irs::doc_id_t seek(irs::doc_id_t) override { return irs::doc_limits::eof(); }
  irs::attribute* get_mutable(irs::type_info::type_id type) noexcept override {
    if (irs::type<irs::document>::id() == type) {
      return &doc;
    }

    return irs::type<irs::cost>::id() == type ? &cost : nullptr;
  }

  irs::cost cost;
  irs::document doc{irs::doc_limits::eof()};
};

empty_doc_iterator kEmptyDocIterator;

// Represents an iterator without terms
struct empty_term_iterator : irs::term_iterator {
  irs::bytes_view value() const noexcept final { return {}; }
  irs::doc_iterator::ptr postings(irs::IndexFeatures) const noexcept final {
    return irs::doc_iterator::empty();
  }
  void read() noexcept final {}
  bool next() noexcept final { return false; }
  irs::attribute* get_mutable(irs::type_info::type_id) noexcept final {
    return nullptr;
  }
};

empty_term_iterator kEmptyTermIterator;

// Represents an iterator without terms
struct empty_seek_term_iterator final : irs::seek_term_iterator {
  irs::bytes_view value() const noexcept final { return {}; }
  irs::doc_iterator::ptr postings(irs::IndexFeatures) const noexcept final {
    return irs::doc_iterator::empty();
  }
  void read() noexcept final {}
  bool next() noexcept final { return false; }
  irs::attribute* get_mutable(irs::type_info::type_id) noexcept final {
    return nullptr;
  }
  irs::SeekResult seek_ge(irs::bytes_view) noexcept override {
    return irs::SeekResult::END;
  }
  bool seek(irs::bytes_view) noexcept override { return false; }
  irs::seek_cookie::ptr cookie() const noexcept override { return nullptr; }
};

empty_seek_term_iterator kEmptySeekIterator;

// Represents a reader with no terms
const irs::empty_term_reader kEmptyTermReader{0};

// Represents a reader with no fields
struct empty_field_iterator final : irs::field_iterator {
  const irs::term_reader& value() const override { return kEmptyTermReader; }

  bool seek(std::string_view) override { return false; }

  bool next() override { return false; }
};

empty_field_iterator kEmptyFieldIterator;

struct empty_column_reader final : irs::column_reader {
  irs::field_id id() const override { return irs::field_limits::invalid(); }

  // Returns optional column name.
  std::string_view name() const override { return {}; }

  // Returns column header.
  irs::bytes_view payload() const override { return {}; }

  // Returns the corresponding column iterator.
  // If the column implementation supports document payloads then it
  // can be accessed via the 'payload' attribute.
  irs::doc_iterator::ptr iterator(irs::ColumnHint /*hint*/) const override {
    return irs::doc_iterator::empty();
  }

  irs::doc_id_t size() const override { return 0; }
};

const empty_column_reader kEmptyColumnReader;

// Represents a reader with no columns
struct empty_column_iterator final : irs::column_iterator {
  const irs::column_reader& value() const override {
    return kEmptyColumnReader;
  }

  bool seek(std::string_view) override { return false; }

  bool next() override { return false; }
};

empty_column_iterator kEmptyColumnIterator;

}  // namespace

namespace irs {

term_iterator::ptr term_iterator::empty() {
  return memory::to_managed<irs::term_iterator, false>(&kEmptyTermIterator);
}

seek_term_iterator::ptr seek_term_iterator::empty() {
  return memory::to_managed<irs::seek_term_iterator, false>(
    &kEmptySeekIterator);
}

doc_iterator::ptr doc_iterator::empty() {
  return memory::to_managed<doc_iterator, false>(&kEmptyDocIterator);
}

field_iterator::ptr field_iterator::empty() {
  return memory::to_managed<irs::field_iterator, false>(&kEmptyFieldIterator);
}

column_iterator::ptr column_iterator::empty() {
  return memory::to_managed<irs::column_iterator, false>(&kEmptyColumnIterator);
}

}  // namespace irs
