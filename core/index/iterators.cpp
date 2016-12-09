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

#include "iterators.hpp"
#include "search/cost.hpp"
#include "utils/type_limits.hpp"

NS_ROOT
NS_LOCAL

iresearch::attributes empty_doc_iterator_attributes() {
  iresearch::attributes attrs(1); // cost
  attrs.add<cost>()->value(0);
  return attrs;
}

//////////////////////////////////////////////////////////////////////////////
/// @class empty_doc_iterator
/// @brief represents an iterator with no documents 
//////////////////////////////////////////////////////////////////////////////
struct empty_doc_iterator : score_doc_iterator {
  virtual doc_id_t value() const override { return type_limits<type_t::doc_id_t>::eof(); }
  virtual bool next() override { return false; }
  virtual doc_id_t seek(doc_id_t) override { return type_limits<type_t::doc_id_t>::eof(); }
  virtual void score() override { }
  virtual const iresearch::attributes& attributes() const NOEXCEPT override {
    static iresearch::attributes empty = empty_doc_iterator_attributes();
    return empty;
  }
};

//////////////////////////////////////////////////////////////////////////////
/// @class empty_term_iterator
/// @brief represents an iterator without terms
//////////////////////////////////////////////////////////////////////////////
struct empty_term_iterator : term_iterator {
  virtual const bytes_ref& value() const override { return bytes_ref::nil; }
  virtual doc_iterator::ptr postings(const flags&) const {
    return score_doc_iterator::empty();
  }
  virtual void read() { }
  virtual bool next() override { return false; }
  virtual const iresearch::attributes& attributes() const NOEXCEPT override {
    return iresearch::attributes::empty_instance();
  }
};

NS_END // LOCAL

// ----------------------------------------------------------------------------
// --SECTION--                                              basic_term_iterator
// ----------------------------------------------------------------------------

term_iterator::ptr term_iterator::empty() {
  //TODO: make singletone
  return term_iterator::make<empty_term_iterator>();
}

// ----------------------------------------------------------------------------
// --SECTION--                                                seek_doc_iterator 
// ----------------------------------------------------------------------------

score_doc_iterator::ptr score_doc_iterator::empty() {
  //TODO: make singletone
  return score_doc_iterator::make<empty_doc_iterator>();
}

NS_END // ROOT 
