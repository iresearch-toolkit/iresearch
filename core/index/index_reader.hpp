//
// IResearch search engine 
// 
// Copyright © 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#ifndef IRESEARCH_INDEX_READER_H
#define IRESEARCH_INDEX_READER_H

#include "store/directory.hpp"
#include "store/directory_attributes.hpp"
#include "utils/string.hpp"
#include "formats/formats.hpp"
#include "utils/memory.hpp"
#include "utils/iterator.hpp"

#include <vector>
#include <numeric>
#include <functional>

NS_ROOT

/* -------------------------------------------------------------------
* index_reader
* ------------------------------------------------------------------*/

template<typename ReaderType>
struct context;

struct sub_reader;

struct IRESEARCH_API index_reader {
  DECLARE_SPTR(index_reader);
  DECLARE_FACTORY(index_reader);

  typedef std::function<bool(const field_meta&, data_input&)> document_visitor_f;
  typedef forward_iterator_impl<sub_reader> reader_iterator_impl;
  typedef forward_iterator<reader_iterator_impl> reader_iterator;

  virtual ~index_reader();

  // calls visitor by the specified document id
  virtual bool document(
    doc_id_t id,
    const document_visitor_f& visitor
  ) const = 0;

  // calls visitor by the specified document id
  virtual bool document(
    doc_id_t id,
    const stored_fields_reader::visitor_f& visitor
  ) const = 0;

  /* number of live documents */
  virtual uint64_t docs_count() const = 0;

  /* number of live documents for the specified field */
  virtual uint64_t docs_count(const string_ref& field) const = 0;

  /* maximum number of documents */
  virtual uint64_t docs_max() const = 0;

  /* first sub-segment */
  virtual reader_iterator begin() const = 0;

  /* after the last sub-segment */
  virtual reader_iterator end() const = 0;

  /* returns number of sub-segments in current reader */
  virtual size_t size() const = 0;
}; // index_reader

/* -------------------------------------------------------------------
* sub_reader
* ------------------------------------------------------------------*/

struct IRESEARCH_API sub_reader : index_reader {
  typedef iresearch::iterator<doc_id_t> docs_iterator_t;
  typedef std::function<bool(doc_id_t)> value_visitor_f;

  DECLARE_SPTR(sub_reader);
  DECLARE_FACTORY(sub_reader);

  using index_reader::docs_count;

  /* returns number of live documents by the specified field */
  virtual uint64_t docs_count(const string_ref& field) const {
    const term_reader* rdr = terms( field );
    return nullptr == rdr ? 0 : rdr->docs_count();
  }

  /* returns iterator over the live documents in current segment */
  virtual docs_iterator_t::ptr docs_iterator() const = 0;

  virtual const fields_meta& fields() const = 0;

  /* returns iterators to field names in segment */
  virtual iterator<const string_ref&>::ptr iterator() const = 0;

  /* returns corresponding term_reader by the specified field */
  virtual const term_reader* terms(const string_ref& field) const = 0;

  // returns corresponding column reader by the specified field
  virtual value_visitor_f values(
    const string_ref& name, 
    const columnstore_reader::value_reader_f& reader
  ) const = 0;
  
  // returns corresponding column reader by the specified field
  virtual value_visitor_f values(
    field_id id,
    const columnstore_reader::value_reader_f& reader
  ) const = 0;
};

/* context specialization for sub_reader */
template<>
struct context<sub_reader> {
  sub_reader::ptr reader;
  doc_id_t base = 0; // min document id
  doc_id_t max = 0; // max document id

  operator doc_id_t() const { return max; }
}; // reader_context

/* -------------------------------------------------------------------
* composite_reader
* ------------------------------------------------------------------*/

template<typename ReaderType>
class IRESEARCH_API_TEMPLATE composite_reader : public index_reader {
 public:
  typedef typename std::enable_if<
    std::is_base_of<index_reader, ReaderType>::value,
    ReaderType
  >::type reader_type;

  typedef context<reader_type> reader_context;
  typedef std::vector<reader_context> ctxs_t;

  composite_reader(
    index_meta&& meta,
    ctxs_t&& ctxs,
    uint64_t docs_count,
    uint64_t docs_max
  ):
    ctxs_(std::move(ctxs)),
    docs_count_(docs_count),
    docs_max_(docs_max),
    meta_(std::move(meta)) {
  }

  virtual bool document(
    doc_id_t doc,
    const document_visitor_f& visitor
  ) const override {
    return visit<decltype(visitor)>(doc, visitor);
  }
  
  virtual bool document(
    doc_id_t doc,
    const stored_fields_reader::visitor_f& visitor
  ) const override {
    return visit<decltype(visitor)>(doc, visitor);
  }

  // number of live documents
  virtual uint64_t docs_count() const override { return docs_count_; }

  // maximum number of documents
  virtual uint64_t docs_max() const override { return docs_max_; }

  // number of live documents for the specified field
  virtual uint64_t docs_count(const string_ref& field) const {
    return std::accumulate(ctxs_.begin(), ctxs_.end(), uint64_t(0),
      [&field](uint64_t total, const reader_context& ctx) {
        return total + ctx.reader->docs_count(field); }
    );
  }

  virtual reader_iterator begin() const {
    return reader_iterator(new iterator_impl(ctxs_.begin()));
  }

  virtual reader_iterator end() const {
    return reader_iterator(new iterator_impl(ctxs_.end()));
  }

  // returns total number of opened writers
  virtual size_t size() const override { return ctxs_.size(); }

  // returns base document id for the corresponded reader
  doc_id_t base(size_t i) const { return ctxs_[i].base; }

  // returns corresponded reader
  const sub_reader& operator[](size_t i) const {
    assert(i < ctxs_.size());
    return *ctxs_[i].reader;
  }

 protected:
  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  ctxs_t ctxs_;
  uint64_t docs_count_;
  uint64_t docs_max_;
  index_meta meta_;
  IRESEARCH_API_PRIVATE_VARIABLES_END

  // returns corresponded reader's context by the specified document id
  const reader_context& sub_ctx(doc_id_t doc) const {
    auto it = std::lower_bound(ctxs_.begin(), ctxs_.end(), doc);
    assert(ctxs_.end() != it);
    return *it;
  }

 private:
  class iterator_impl : public index_reader::reader_iterator_impl {
   public:
    explicit iterator_impl(
        const typename composite_reader::ctxs_t::const_iterator& pos)
      : pos_(pos) {
    }

    virtual void operator++() override { ++pos_; }
    virtual reference operator*() override {
      return const_cast<reference>(*pos_->reader);
    }
    virtual const_reference operator*() const override { return *pos_->reader; }
    virtual bool operator==(const reader_iterator_impl& rhs) override {
      return pos_ == static_cast<const iterator_impl&>(rhs).pos_;
    }

   private:
     typename composite_reader::ctxs_t::const_iterator pos_;
  };

  template<typename Visitor>
  bool visit(doc_id_t doc, const Visitor& visitor) const {
    const auto& ctx = sub_ctx(doc);
    return ctx.reader->document(doc - ctx.base, visitor);
  }
}; // composite_reader

MSVC_ONLY(template class IRESEARCH_API composite_reader<sub_reader>);

class IRESEARCH_API directory_reader : public composite_reader<sub_reader> {
 public:
  DECLARE_SPTR(directory_reader);
  DECLARE_FACTORY(directory_reader);

  // if codec == nullptr then use the latest file for all known codecs
  static directory_reader::ptr open(
    const directory& dir, const format::ptr& codec = format::ptr(nullptr)
  );

  // refresh internal state to match state of the latest index_meta
  virtual void refresh();

 private:
  typedef std::unordered_set<index_file_refs::ref_t> segment_file_refs_t;
  typedef std::vector<segment_file_refs_t> reader_file_refs_t;

  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  format::ptr codec_;
  const directory& dir_;
  reader_file_refs_t file_refs_;
  IRESEARCH_API_PRIVATE_VARIABLES_END

  directory_reader(
    const format::ptr& codec,
    const directory& dir,
    reader_file_refs_t&& file_refs,
    index_meta&& meta,
    ctxs_t&& ctxs,
    uint64_t docs_count,
    uint64_t docs_max
  );
}; // directory_reader

NS_END

#endif