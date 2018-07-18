#ifndef PYRESEARCH_H
#define PYRESEARCH_H

#include "index/index_reader.hpp"

class doc_iterator {
 public:
  ~doc_iterator() { }

  bool next() { return it_->next(); }

  irs::doc_id_t seek(irs::doc_id_t target) {
    return it->seek(target);
  }

  irs::doc_id_t value() const {
    return it_->value();
  }

 private:
  friend class segment_reader;

  doc_iterator(irs::doc_iterator::ptr it)
    : it_(it) {
  }

  irs::doc_iterator::ptr it_;
};

class segment_reader {
 public:
  ~segment_reader() { }

  doc_iterator docs_iterator() const { return reader_->docs_iterator(); }
  size_t docs_count() const { return reader_->docs_count(); }
  size_t docs_count(const char* field) const { return reader_->docs_count(field); }
  size_t live_docs_count() const { return reader_->live_docs_count(); }

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
