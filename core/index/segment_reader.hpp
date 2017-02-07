//
// IResearch search engine 
// 
// Copyright (c) 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#ifndef IRESEARCH_SEGMENT_READER_H
#define IRESEARCH_SEGMENT_READER_H

#include "index_reader.hpp"

NS_ROOT

////////////////////////////////////////////////////////////////////////////////
/// @brief interface for a segment reader
////////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API segment_reader final: public sub_reader {
 public:
  typedef segment_reader ptr; // pointer to self
  segment_reader() = default; // required for context<segment_reader>
  explicit operator bool() const NOEXCEPT;
  segment_reader& operator*() NOEXCEPT;
  const segment_reader& operator*() const NOEXCEPT;
  segment_reader* operator->() NOEXCEPT;
  const segment_reader* operator->() const NOEXCEPT;
  virtual index_reader::reader_iterator begin() const override;
  virtual const column_meta* column(const string_ref& name) const override;
  virtual column_iterator::ptr columns() const override;
  using sub_reader::docs_count;
  virtual uint64_t docs_count() const override;
  virtual docs_iterator_t::ptr docs_iterator() const override;
  virtual index_reader::reader_iterator end() const override;
  virtual const term_reader* field(const string_ref& name) const override;
  virtual field_iterator::ptr fields() const override;
  virtual uint64_t live_docs_count() const override;
  static segment_reader open(const directory& dir, const segment_meta& meta);
  segment_reader reopen(const segment_meta& meta) const;
  void reset() NOEXCEPT;
  virtual size_t size() const override;
  using sub_reader::values;
  virtual value_visitor_f values(
    field_id field, const columnstore_reader::value_reader_f& reader
  ) const override;
  virtual bool visit(
    field_id field, const columnstore_reader::raw_reader_f& reader
  ) const override;
 private:
  class segment_reader_impl;
  typedef std::shared_ptr<segment_reader_impl> impl_ptr;

  impl_ptr impl_;

  segment_reader(const impl_ptr& impl);
};

NS_END

#endif