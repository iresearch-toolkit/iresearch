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

#ifndef IRESEARCH_FST_STRING_WEIGHT_H
#define IRESEARCH_FST_STRING_WEIGHT_H

#include "shared.hpp"
#include "utils/string.hpp"

#include <string>
#include <fst/string-weight.h>

NS_BEGIN(fst)

// String semiring: (longest_common_prefix/suffix, ., Infinity, Epsilon)
template <typename Label>
class StringLeftWeight {
 public:
  typedef StringLeftWeight<Label> ReverseWeight;
  typedef std::basic_string<Label> str_t;
  typedef typename str_t::const_iterator iterator;
  
  static const StringLeftWeight<Label>& Zero() {
    static const StringLeftWeight<Label> zero(kStringInfinity);
    return zero;
  }

  static const StringLeftWeight<Label>& One() {
    static const StringLeftWeight<Label> one;
    return one;
  }

  static const StringLeftWeight<Label>& NoWeight() {
    static const StringLeftWeight<Label> no_weight(kStringBad);
    return no_weight;
  }

  static const std::string& Type() {
    static const std::string type = "left_string";
    return type;
  }

  friend bool operator==(
      const StringLeftWeight& lhs, 
      const StringLeftWeight& rhs) NOEXCEPT {
    return lhs.str_ == rhs.str_;
  }
  
  StringLeftWeight()
    : StringLeftWeight(Label(0)) {
  }
  
  explicit StringLeftWeight(Label label) 
    : str_(1, label) {
  }

  template <typename Iterator>
  StringLeftWeight(Iterator begin, Iterator end)
    : str_(begin, end) {
  }

  StringLeftWeight(StringLeftWeight&& rhs) NOEXCEPT
    : str_(std::move(rhs.str_)) {
  }

  StringLeftWeight& operator=(StringLeftWeight&& rhs) NOEXCEPT {
    if (this != &rhs) {
      str_ = std::move(rhs.str_);
    }
    return *this;
  }

  bool Member() const NOEXCEPT {
    assert(!str_.empty());
    return str_[0] != kStringBad;
  }

  std::istream& Read(std::istream &strm);

  std::ostream& Write(std::ostream &strm) const;

  size_t Hash() const {
    return std::hash<str_t>()(str_);
  }

  StringLeftWeight Quantize(float delta = kDelta) const NOEXCEPT { 
    return *this; 
  }

  ReverseWeight Reverse() const {
    return ReverseWeight(str_.rbegin(), str_.rend());
  }

  static uint64 Properties() NOEXCEPT {
    static CONSTEXPR auto props = kLeftSemiring | kIdempotent;
    return props;
  }

  void Empty() const NOEXCEPT {
    return str_.empty();
  }

  void Clear() NOEXCEPT {
    str_.clear();
  }

  size_t Size() const NOEXCEPT { 
    return str_.size(); 
  }

  void PushBack(Label label) {
    str_.push_back(label);
  }

  template<typename Iterator>
  void PushBack(Iterator begin, Iterator end) {
    str_.append(begin, end);
  }

  void Reserve(size_t capacity) {
    str_.reserve(capacity);
  }

  iterator begin() const { return str_.begin(); }
  iterator end() const { return str_.end(); }

 private:
  str_t str_;
}; // StringLeftWeight 

// StringLeftWeight member functions follow that require

template <typename Label>
inline std::istream& StringLeftWeight<Label>::Read(std::istream& strm) {
  Clear();
  int32 size;
  ReadType(strm, &size);
  for (int32 i = 0; i < size; ++i) {
    Label label;
    ReadType(strm, &label);
    PushBack(label);
  }
  return strm;
}

template <typename Label>
inline std::ostream& StringLeftWeight<Label>::Write(std::ostream& strm) const {
  const int32 size = Size();
  WriteType(strm, size);
  for (auto it = str_.begin(), end = str_.end(); it != end; ++it) {
    WriteType(strm, *it);
  }
  return strm;
}

template <typename Label>
inline bool operator!=(
    const StringLeftWeight<Label>& w1,
    const StringLeftWeight<Label>& w2) {
  return !(w1 == w2);
}

template <typename Label>
inline std::ostream &operator<<(
    std::ostream& strm,
    const StringLeftWeight<Label>& weight) {
  if (weight.Empty()) {
    return strm << "Epsilon";
  }
  
  auto begin = weight.begin();
  const auto& first = *begin;

  if (first == kStringInfinity) {
    return strm << "Infinity";
  } else if (first == kStringBad) {
    return strm << "BadString";
  }

  const auto end = weight.end();
  if (begin != end) {
    strm << *begin;

    for (++begin; begin != end; ++begin) {
      strm << kStringSeparator << *begin;
    }
  }
  
  return strm;
}

template <typename Label>
inline std::istream& operator>>(
    std::istream& strm,
    StringLeftWeight<Label>& weight) {
  std::string str;
  strm >> str;
  if (str == "Infinity") {
    weight = StringLeftWeight<Label>::Zero();
  } else if (str == "Epsilon") {
    weight = StringLeftWeight<Label>::One();
  } else {
    weight.Clear();
    char *p = nullptr;
    for (const char *cs = str.c_str(); !p || *p != '\0'; cs = p + 1) {
      const Label label = strtoll(cs, &p, 10);
      if (p == cs || (*p != 0 && *p != kStringSeparator)) {
        strm.clear(std::ios::badbit);
        break;
      }
      weight.PushBack(label);
    }
  }
  return strm;
}

// Longest common prefix for left string semiring.
template <typename Label>
inline StringLeftWeight<Label> Plus(
    const StringLeftWeight<Label>& lhs,
    const StringLeftWeight<Label>& rhs) {
  typedef StringLeftWeight<Label> Weight;

  if (!lhs.Member() || !rhs.Member()) return Weight::NoWeight();
  if (lhs == Weight::Zero()) return rhs;
  if (rhs == Weight::Zero()) return lhs;

  const auto* plhs = &lhs;
  const auto* prhs = &rhs;

  if (rhs.Size() > lhs.size()) {
    // enusre that 'prhs' is shorter than 'plhs'
    // The behavior is undefined if the second range is shorter than the first range.
    // (http://en.cppreference.com/w/cpp/algorithm/mismatch)
    std::swap(plhs, prhs);
  }

  assert(prhs.Size() <= plhs.Size());
  
  return Weight(
    prhs->begin(), 
    std::mismatch(prhs->begin(), prhs->end(), plhs->begin()).first
  );
}

template <typename Label>
inline StringLeftWeight<Label> Times(
    const StringLeftWeight<Label>& lhs,
    const StringLeftWeight<Label>& rhs) {
  using Weight = StringLeftWeight<Label>;
 
  if (!lhs.Member() || !rhs.Member()) {
    return Weight::NoWeight();
  }

  if (lhs == Weight::Zero() || rhs == Weight::Zero()) {
    return Weight::Zero();
  }

  Weight product(lhs.end(), lhs.end()); // create empty weight
  product.Reserve(lhs.Size() + rhs.Size());
  product.PushBack(lhs.begin(), lhs.end());
  product.PushBack(rhs.begin(), rhs.end());
  return product;
}

// Left division in a left string semiring.
template <typename Label>
inline StringLeftWeight<Label> DivideLeft(
    const StringLeftWeight<Label>& lhs,
    const StringLeftWeight<Label>& rhs) {
  using Weight = StringLeftWeight<Label>;
  
  if (!lhs.Member() || !rhs.Member()) {
    return Weight::NoWeight();
  }

  if (rhs == Weight::Zero()) {
    return Weight(kStringBad);
  } else if (lhs == Weight::Zero()) {
    return Weight::Zero();
  }

  if (rhs.Size() > lhs.Size()) {
    return Weight(lhs.end(), lhs.end()); // return empty weight
  }

  return Weight(lhs.begin() + rhs.Size(), lhs.end());
}

NS_END // fst 

#endif  // IRESEARCH_FST_STRING_WEIGHT_H
