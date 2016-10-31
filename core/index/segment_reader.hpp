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

#ifndef IRESEARCH_SEGMENT_READER_H
#define IRESEARCH_SEGMENT_READER_H

#include "index/index_reader.hpp"
#include "index/field_meta.hpp"
#include "formats/formats.hpp"

NS_ROOT

struct segment_meta;
class format;
struct directory;

class IRESEARCH_API segment_reader final : public sub_reader {
 public:
  DECLARE_SPTR(segment_reader);

  static segment_reader::ptr open(
    const directory& dir,
    const segment_meta& sm
  );

  // expects 1-based document id
  virtual bool document(
    doc_id_t id, 
    const document_visitor_f& visitor
  ) const override;
 
  // expects 1-based document id
  virtual bool document(
    doc_id_t id, 
    const stored_fields_reader::visitor_f& visitor
  ) const override;

  using sub_reader::docs_count;

  virtual uint64_t docs_count() const override { return docs_count_; }

  virtual docs_iterator_t::ptr docs_iterator() const override;

  virtual uint64_t docs_max() const override { return docs_count_; }

  void refresh(const segment_meta& meta); // update reader with any changes from meta

  virtual const term_reader* terms(const string_ref& field) const override {
    return fr_->terms(field);
  }
  
  virtual value_visitor_f values(
    field_id field,
    const columnstore_reader::value_reader_f& reader
  ) const override;

  virtual value_visitor_f values(
    const string_ref& field,
    const columnstore_reader::value_reader_f& reader
  ) const override;

  virtual size_t size() const override { return 1; }
 
  virtual const fields_meta& fields() const override { return fields_; }

  virtual index_reader::reader_iterator begin() const { 
    return index_reader::reader_iterator(new iterator_impl(this));
  }

  virtual index_reader::reader_iterator end() const { 
    return index_reader::reader_iterator(new iterator_impl());
  }

 private:
  class iterator_impl : public index_reader::reader_iterator_impl {
   public:
    explicit iterator_impl(const sub_reader* rdr = nullptr) NOEXCEPT
      : rdr_(rdr) {
    }

    virtual void operator++() override { rdr_ = nullptr; }
    virtual reference operator*() override {
      return const_cast<reference>(*rdr_);
    }
    virtual const_reference operator*() const override { return *rdr_; }
    virtual bool operator==(const reader_iterator_impl& rhs) override {
      return rdr_ == static_cast<const iterator_impl&>(rhs).rdr_;
    }

   private:
    const sub_reader* rdr_;
  };

  typedef std::pair<std::string, field_id> column_meta_t;

  segment_reader() = default;

  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  mutable std::vector<field_id> doc_header_; // buffered document header
  struct {
    const directory* dir;
    uint64_t version;
  } dir_state_;
  uint64_t docs_count_;
  document_mask docs_mask_;
  std::unordered_map<hashed_string_ref, column_meta_t> columns_;
  fields_meta fields_;
  field_reader::ptr fr_;
  stored_fields_reader::ptr sfr_;
  columnstore_reader::ptr csr_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
};

NS_END

#endif