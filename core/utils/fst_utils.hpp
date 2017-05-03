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

#ifndef IRESEARCH_FST_UTILS_H
#define IRESEARCH_FST_UTILS_H

#include "store/data_output.hpp"
#include "store/data_input.hpp"

#include "noncopyable.hpp"

#include "fst_decl.hpp"
#include "fst_string_weight.h"

NS_LOCAL

//////////////////////////////////////////////////////////////////////////////
/// @brief fast and effective helper to check if the specified weight equals
///        to special semiring member
/// @returns true - if the specified weight "w" equals to "val",
///          false - otherwise
//////////////////////////////////////////////////////////////////////////////
template<typename L>
inline bool equal_to(const fst::StringLeftWeight<L>& w, int val) {
  // it's important to cast 'val' to L here since
  // sizeof(L) maybe less than sizeof(int)
  return 1 == w.Size() && w[0] == static_cast<L>(val);
}

NS_END

NS_ROOT


NS_BEGIN(fst_utils)

//////////////////////////////////////////////////////////////////////////////
/// @returns true - if the specified string weight "w" equals to
//           "StringWeight::Zero()", false - otherwise
//////////////////////////////////////////////////////////////////////////////
template<typename L>
inline bool is_zero(const fst::StringLeftWeight<L>& w) {
  return equal_to(w, fst::kStringInfinity);
}

//////////////////////////////////////////////////////////////////////////////
/// @returns true - if the specified string weight "w" equals to
//           "StringWeight::NoWeight()", false - otherwise
//////////////////////////////////////////////////////////////////////////////
template<typename L>
inline bool is_no_weight(const fst::StringLeftWeight<L>& w) {
  return equal_to(w, fst::kStringBad);
}

template<typename L>
inline void append(
    fst::StringLeftWeight<L>& lhs,
    const fst::StringLeftWeight<L>& rhs) {
  assert(!is_no_weight(rhs));

  if (fst::StringLeftWeight<L>::One() == rhs || is_zero(rhs)) {
    return;
  }

  lhs.PushBack(rhs.begin(), rhs.end());
}

NS_END // fst_utils

struct byte_weight_output : data_output, private util::noncopyable {
  virtual void close() override {}

  virtual void write_byte(byte_type b) override final {
    weight.PushBack(b);
  }

  virtual void write_bytes(const byte_type* b, size_t len) override final {
    weight.PushBack(b, b + len);
  }

  byte_weight weight;
}; // byte_weight_output

struct byte_weight_input final : data_input, private util::noncopyable {
 public:
  byte_weight_input() = default;

  explicit byte_weight_input(byte_weight&& weight) NOEXCEPT
    : weight_(std::move(weight)),
      begin_(weight_.begin()),
      end_(weight_.end()) {
  }

  virtual byte_type read_byte() override {
    assert(!eof());
    const auto b = *begin_;
    ++begin_;
    return b;
  }

  virtual size_t read_bytes(byte_type* b, size_t size) override {
    const size_t pos = std::distance(weight_.begin(), begin_); // current position
    const size_t read = std::min(size, weight_.Size() - pos);  // number of elements to read

    std::memcpy(b, weight_.c_str() + pos, read*sizeof(byte_type));

    return read;
  }

  virtual bool eof() const override {
    return begin_ == end_;
  }

  virtual size_t length() const override {
    return weight_.Size();
  }

  virtual size_t file_pointer() const override {
    return std::distance(weight_.begin(), begin_);
  }

  void reset() {
    begin_ = weight_.begin();
  }

 private:
  byte_weight weight_;
  byte_weight::iterator begin_{ weight_.begin() };
  const byte_weight::iterator end_{ weight_.end() };
}; // byte_weight_input

NS_END // ROOT

#endif
