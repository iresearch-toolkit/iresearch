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

#ifndef IRESEARCH_FST_UTILS_H
#define IRESEARCH_FST_UTILS_H

#include "store/data_output.hpp"
#include "store/data_input.hpp"

#include "noncopyable.hpp"

#include "fst_decl.hpp"
#include <fst/string-weight.h>

NS_ROOT

NS_BEGIN( fst_utils )
NS_LOCAL

//////////////////////////////////////////////////////////////////////////////
/// @brief fast and effective helper to check if the specified weight equals
///        to special semiring member
/// @returns true - if the specified weight "w" equals to "val",
///          false - otherwise
//////////////////////////////////////////////////////////////////////////////
template<typename L, fst::StringType S = fst::STRING_LEFT>
inline bool equal_to(const fst::StringWeight<L,S>& w, int val) {
  if (w.Size() != 1) {
    return false;
  }

  fst::StringWeightIterator<L, S> iter(w);
  return iter.Value() == static_cast<L>(val);
}

NS_END

//////////////////////////////////////////////////////////////////////////////
/// @returns true - if the specified string weight "w" equals to
//           "StringWeight::Zero()", false - otherwise
//////////////////////////////////////////////////////////////////////////////
template<typename L, fst::StringType S = fst::STRING_LEFT>
inline bool is_zero(const fst::StringWeight<L,S>& w) {
  return equal_to(w, fst::kStringInfinity);
}

//////////////////////////////////////////////////////////////////////////////
/// @returns true - if the specified string weight "w" equals to
//           "StringWeight::NoWeight()", false - otherwise
//////////////////////////////////////////////////////////////////////////////
template<typename L, fst::StringType S = fst::STRING_LEFT>
inline bool is_no_weight(const fst::StringWeight<L,S>& w) {
  return equal_to(w, fst::kStringBad);
}

template<typename L, fst::StringType S = fst::STRING_LEFT>
inline void append( fst::StringWeight< L, S >& lhs,
                    const fst::StringWeight< L, S >& rhs ) {
  assert(!is_no_weight(rhs));

  if (fst::StringWeight<L,S>::One() == rhs || is_zero(rhs)) {
    return;
  }

  for ( fst::StringWeightIterator< L, S > it( rhs ); !it.Done(); it.Next() ) {
    lhs.PushBack( it.Value() );
  }
}

NS_END // fst_utils

struct weight_output : data_output, private util::noncopyable {
  virtual void close() override {}

  virtual void write_byte(byte_type b) override {
    weight.PushBack(b);
  }

  virtual void write_bytes(const byte_type* b, size_t len) override {
    for (size_t i = 0; i < len; ++i) {
      weight.PushBack(b[i]);
    }
  }

  byte_weight weight;
}; // weight_output 

struct weight_input : data_input, private util::noncopyable {
 public:
  weight_input() : it_(weight_) { }

  explicit weight_input(byte_weight&& weight)
    : weight_(std::move(weight)),
      it_(weight_) {
  }

  virtual byte_type read_byte() override {
    assert(!it_.Done());
    byte_type b = it_.Value();
    it_.Next();
    return b;
  }

  virtual size_t read_bytes(byte_type* b, size_t size) override {
    size_t read = 0;
    for (; read < size && !it_.Done();) {
      *b++ = it_.Value();
      it_.Next();
      ++read;
    }
    return read;
  }

  virtual bool eof() const override {
    return it_.Done();
  }

  virtual size_t length() const override {
    return weight_.Size();
  }

  virtual size_t file_pointer() const override {
    throw not_impl_error();
  }

  void reset() {
    it_.Reset();
  }

 private:
  byte_weight weight_;
  byte_weight_iterator it_;
}; // weight_input 

NS_END

#endif