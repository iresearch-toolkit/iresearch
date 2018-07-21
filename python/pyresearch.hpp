////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_PYRESEARCH_H
#define IRESEARCH_PYRESEARCH_H

#include "index/index_reader.hpp"
#include "index/field_meta.hpp"

class doc_iterator {
 public:
  ~doc_iterator() { }

  bool next() { return it_->next(); }

  uint64_t seek(uint64_t target) {
    return it_->seek(target);
  }

  uint64_t value() const {
    return it_->value();
  }

 private:
  friend class column_reader;
  friend class term_iterator;
  friend class segment_reader;

  doc_iterator(irs::doc_iterator::ptr it)
    : it_(it) {
  }

  irs::doc_iterator::ptr it_;
}; // doc_iterator

class term_iterator {
 public:
  ~term_iterator() { }

  bool next() { return it_->next(); }
  doc_iterator postings() const {
    return it_->postings(irs::flags::empty_instance());
  }
  bool seek(irs::string_ref term) {
    return it_->seek(irs::ref_cast<irs::byte_type>(term));
  }
  uint32_t seek_ge(irs::string_ref term) {
    typedef std::underlying_type<irs::SeekResult>::type type;

    static_assert(
      std::is_convertible<type, uint32_t>::value,
      "types are not equal"
    );

    return static_cast<type>(
      it_->seek_ge(irs::ref_cast<irs::byte_type>(term))
    );
  }
  iresearch::bytes_ref value() const { return it_->value(); }

 private:
  friend class field_reader;

  term_iterator(irs::seek_term_iterator::ptr&& it)
    : it_(std::move(it)) {
  }

  std::shared_ptr<irs::seek_term_iterator> it_;
}; // term_iterator

class column_meta {
 public:
  ~column_meta() { }

  irs::string_ref name() const { return meta_->name; }
  uint64_t id() const { return meta_->id; }

 private:
  friend class column_iterator;

  column_meta(const irs::column_meta* meta)
    : meta_(meta) {
  }

  const irs::column_meta* meta_;
}; // column_meta

class column_iterator {
 public:
  ~column_iterator() { }

  bool next() { return it_->next(); }
  bool seek(irs::string_ref column) {
    return it_->seek(column);
  }
  column_meta value() const { return &it_->value(); }

 private:
  friend class segment_reader;

  column_iterator(irs::column_iterator::ptr&& it)
    : it_(it.release(), std::move(it.get_deleter())) {
  }

  std::shared_ptr<irs::column_iterator> it_;
}; // column_iterator

class column_values_reader {
 public:
  ~column_values_reader() { }

  std::pair<bool, irs::bytes_ref> get(uint64_t key) {
    irs::bytes_ref value;
    const bool found = reader_(key, value);
    return std::make_pair(found, value);
  }

  bool has(uint64_t key) const {
    irs::bytes_ref value;
    return reader_(key, value);
  }

 private:
  friend class column_reader;

  column_values_reader(irs::columnstore_reader::values_reader_f&& reader)
    : reader_(std::move(reader)) {
  }

  irs::columnstore_reader::values_reader_f reader_;
};

class column_reader {
 public:
  ~column_reader() { }

  doc_iterator iterator() const {
    return reader_->iterator();
  }

  column_values_reader values() const {
    return reader_->values();
  }

 private:
  friend class segment_reader;

  column_reader(const irs::columnstore_reader::column_reader* reader)
    : reader_(reader) {
  }

  const irs::columnstore_reader::column_reader* reader_;
}; // column_reader

class field_reader {
 public:
  ~field_reader() { }

  size_t docs_count() const { return field_->docs_count(); }
  std::vector<std::string> features() const;
  term_iterator iterator() const { return field_->iterator(); }
  irs::string_ref name() const { return field_->meta().name; }
  uint64_t norm() const { return field_->meta().norm; }
  size_t size() const { return field_->size(); }
  iresearch::bytes_ref min() const { return field_->min(); }
  iresearch::bytes_ref max() const { return field_->max(); }

 private:
  friend class segment_reader;
  friend class field_iterator;

  field_reader(const irs::term_reader* field)
    : field_(field) {
  }

  const irs::term_reader* field_;
}; // field_reader

class field_iterator {
 public:
  ~field_iterator() { }

  bool seek(irs::string_ref field) {
    return it_->seek(field);
  }
  bool next() { return it_->next(); }
  field_reader value() const { return &it_->value(); }

 private:
  friend class segment_reader;

  field_iterator(irs::field_iterator::ptr&& it)
    : it_(it.release(), std::move(it.get_deleter())) {
  }

  std::shared_ptr<irs::field_iterator> it_;
}; // field_iterator

class segment_reader {
 public:
  ~segment_reader() { }

  column_iterator columns() const { return reader_->columns(); }
  column_reader column(uint64_t id) const { return reader_->column_reader(id); }
  column_reader column(irs::string_ref column) const {
    return reader_->column_reader(column);
  }
  size_t docs_count() const { return reader_->docs_count(); }
  size_t docs_count(irs::string_ref field) {
    return reader_->docs_count(field);
  }
  doc_iterator docs_iterator() const { return reader_->mask(reader_->docs_iterator()); }
  field_reader field(irs::string_ref field) const {
    return reader_->field(field);
  }
  field_iterator fields() const { return reader_->fields(); }
  size_t live_docs_count() const { return reader_->live_docs_count(); }
  doc_iterator live_docs_iterator() const { return reader_->docs_iterator(); }

 private:
  friend class index_reader;

  segment_reader(irs::sub_reader::ptr reader)
    : reader_(reader) {
  }

  irs::sub_reader::ptr reader_;
}; // segment_reader

class index_reader {
 public:
  static index_reader open(const char* path);

  ~index_reader() { }

  segment_reader segment(size_t i) const;
  size_t docs_count() const { return reader_->docs_count(); }
  size_t docs_count(irs::string_ref field) {
    return reader_->docs_count(field);
  }
  size_t live_docs_count() const { return reader_->live_docs_count(); }
  size_t size() const { return reader_->size(); }

  index_reader(irs::index_reader::ptr reader)
    : reader_(reader) {
  }

 private:
  irs::index_reader::ptr reader_;
}; // index_reader

#endif // IRESEARCH_PYRESEARCH_H
