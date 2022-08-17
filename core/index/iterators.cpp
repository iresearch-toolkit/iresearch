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

  virtual irs::doc_id_t value() const override {
    return irs::doc_limits::eof();
  }
  virtual bool next() override { return false; }
  virtual irs::doc_id_t seek(irs::doc_id_t) override {
    return irs::doc_limits::eof();
  }
  virtual irs::attribute* get_mutable(
    irs::type_info::type_id type) noexcept override {
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
  virtual const irs::bytes_ref& value() const noexcept override final {
    return irs::bytes_ref::NIL;
  }
  virtual irs::doc_iterator::ptr postings(
    irs::IndexFeatures) const noexcept override final {
    return irs::doc_iterator::empty();
  }
  virtual void read() noexcept override final {}
  virtual bool next() noexcept override final { return false; }
  virtual irs::attribute* get_mutable(
    irs::type_info::type_id) noexcept override final {
    return nullptr;
  }
};

empty_term_iterator kEmptyTermIterator;

// Represents an iterator without terms
struct empty_seek_term_iterator final : irs::seek_term_iterator {
  virtual const irs::bytes_ref& value() const noexcept override final {
    return irs::bytes_ref::NIL;
  }
  virtual irs::doc_iterator::ptr postings(
    irs::IndexFeatures) const noexcept override final {
    return irs::doc_iterator::empty();
  }
  virtual void read() noexcept override final {}
  virtual bool next() noexcept override final { return false; }
  virtual irs::attribute* get_mutable(
    irs::type_info::type_id) noexcept override final {
    return nullptr;
  }
  virtual irs::SeekResult seek_ge(const irs::bytes_ref&) noexcept override {
    return irs::SeekResult::END;
  }
  virtual bool seek(const irs::bytes_ref&) noexcept override { return false; }
  virtual irs::seek_cookie::ptr cookie() const noexcept override {
    return nullptr;
  }
};

empty_seek_term_iterator kEmptySeekIterator;

// Represents a reader with no terms
const irs::empty_term_reader kEmptyTermReader{0};

// Represents a reader with no fields
struct empty_field_iterator final : irs::field_iterator {
  virtual const irs::term_reader& value() const override {
    return kEmptyTermReader;
  }

  virtual bool seek(irs::string_ref) override { return false; }

  virtual bool next() override { return false; }
};

empty_field_iterator kEmptyFieldIterator;

struct empty_column_reader final : irs::column_reader {
  virtual irs::field_id id() const override {
    return irs::field_limits::invalid();
  }

  // Returns optional column name.
  virtual irs::string_ref name() const override { return irs::string_ref::NIL; }

  // Returns column header.
  virtual irs::bytes_ref payload() const override {
    return irs::bytes_ref::NIL;
  }

  // Returns the corresponding column iterator.
  // If the column implementation supports document payloads then it
  // can be accessed via the 'payload' attribute.
  virtual irs::doc_iterator::ptr iterator(
    irs::ColumnHint /*hint*/) const override {
    return irs::doc_iterator::empty();
  }

  virtual irs::doc_id_t size() const override { return 0; }
};

const empty_column_reader kEmptyColumnReader;

// Represents a reader with no columns
struct empty_column_iterator final : irs::column_iterator {
  virtual const irs::column_reader& value() const override {
    return kEmptyColumnReader;
  }

  virtual bool seek(irs::string_ref) override { return false; }

  virtual bool next() override { return false; }
};

empty_column_iterator kEmptyColumnIterator;

}  // namespace

namespace iresearch {

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

}  // namespace iresearch
