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

#ifndef PYRESEARCH_H
#define PYRESEARCH_H

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
  friend class segment_reader;
  friend class column_reader;

  doc_iterator(irs::doc_iterator::ptr it)
    : it_(it) {
  }

  irs::doc_iterator::ptr it_;
}; // doc_iterator

class column_meta {
 public:
  ~column_meta() { }

  const std::string& name() const { return meta_->name; }
  uint64_t id() const { return meta_->id; }

 private:
  friend class column_iterator;

  column_meta(const irs::column_meta* meta)
    : meta_(meta) {
  }

  const irs::column_meta* meta_;
};

class column_iterator {
 public:
  ~column_iterator() { }

  bool next() { return it_->next(); }
  bool seek(const char* name) { return it_->seek(name); }
  column_meta value() const { return &it_->value(); }

 private:
  friend class segment_reader;

  column_iterator(irs::column_iterator::ptr&& it)
    : it_(it.release(), std::move(it.get_deleter())) {
  }

  std::shared_ptr<irs::column_iterator> it_;
};

class column_values_reader {
 public:
  ~column_values_reader() { }

  bool value(uint64_t key, std::basic_string<uint8_t>& out);

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
};

class field_reader {
 public:
  ~field_reader() { }

  size_t docs_count() const { return field_->docs_count(); }
  std::vector<std::string> features() const;
  const std::string& name() const { return field_->meta().name; }
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
};

class field_iterator {
 public:
  ~field_iterator() { }

  bool seek(const char* field) { return it_->seek(field); }
  bool next() { return it_->next(); }
  field_reader value() const { return &it_->value(); }

 private:
  friend class segment_reader;

  field_iterator(irs::field_iterator::ptr&& it)
    : it_(it.release(), std::move(it.get_deleter())) {
  }

  std::shared_ptr<irs::field_iterator> it_;
};

class segment_reader {
 public:
  ~segment_reader() { }

  column_iterator columns() const { return reader_->columns(); }
  column_reader column(uint64_t id) const { return reader_->column_reader(id); }
  column_reader column(const char* field) const { return reader_->column_reader(field); }
  size_t docs_count() const { return reader_->docs_count(); }
  size_t docs_count(const char* field) const { return reader_->docs_count(field); }
  doc_iterator docs_iterator() const { return reader_->mask(reader_->docs_iterator()); }
  field_reader field(const char* name) const { return reader_->field(name); }
  field_iterator fields() const { return reader_->fields(); }
  size_t live_docs_count() const { return reader_->live_docs_count(); }
  doc_iterator live_docs_iterator() const { return reader_->docs_iterator(); }

 private:
  friend class index_reader;

  segment_reader(irs::sub_reader::ptr reader)
    : reader_(reader) {
  }

  irs::sub_reader::ptr reader_;
}; // segment

class index_reader {
 public:
  static index_reader open(const char* path);

  ~index_reader() { }

  segment_reader segment(size_t i) const;
  size_t docs_count() const { return reader_->docs_count(); }
  size_t docs_count(const char* field) const { return reader_->docs_count(field); }
  size_t live_docs_count() const { return reader_->live_docs_count(); }
  size_t size() const { return reader_->size(); }

  index_reader(irs::index_reader::ptr reader)
    : reader_(reader) {
  }

 private:
  iresearch::index_reader::ptr reader_;
}; // index

#endif
