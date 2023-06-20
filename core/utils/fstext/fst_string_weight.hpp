////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <string>

#if defined(_MSC_VER)
#pragma warning(disable : 4267)  // conversion from 'size_t' to 'uint32_t',
                                 // possible loss of data
#endif

#include <fst/string-weight.h>

#if defined(_MSC_VER)
#pragma warning(default : 4267)
#endif

#include "shared.hpp"
#include "utils/bytes_utils.hpp"
#include "utils/std.hpp"
#include "utils/string.hpp"

#include <absl/strings/str_cat.h>

namespace fst {

template<typename Label>
class StringLeftWeight;

template<typename Label>
struct StringLeftWeightTraits {
  inline static const StringLeftWeight<Label>& Zero();

  inline static const StringLeftWeight<Label>& One();

  inline static const StringLeftWeight<Label>& NoWeight();

  inline static bool Member(const StringLeftWeight<Label>& weight);
};

// String semiring: (longest_common_prefix/suffix, ., Infinity, Epsilon)
template<typename Label>
class StringLeftWeight : public StringLeftWeightTraits<Label> {
 public:
  using ReverseWeight = StringLeftWeight<Label>;
  using str_t = irs::basic_string<Label>;
  using iterator = typename str_t::const_iterator;

  static const std::string& Type() {
    static const std::string type = "left_string";
    return type;
  }

  friend bool operator==(const StringLeftWeight& lhs,
                         const StringLeftWeight& rhs) noexcept {
    return lhs.str_ == rhs.str_;
  }

  StringLeftWeight() = default;

  explicit StringLeftWeight(Label label) : str_(1, label) {}

  template<typename Iterator>
  StringLeftWeight(Iterator begin, Iterator end) : str_(begin, end) {}

  StringLeftWeight(const StringLeftWeight&) = default;
  StringLeftWeight(StringLeftWeight&&) = default;

  explicit StringLeftWeight(irs::basic_string_view<Label> rhs)
    : str_(rhs.data(), rhs.size()) {}

  StringLeftWeight& operator=(StringLeftWeight&&) = default;
  StringLeftWeight& operator=(const StringLeftWeight&) = default;

  StringLeftWeight& operator=(irs::basic_string_view<Label> rhs) {
    str_.assign(rhs.data(), rhs.size());
    return *this;
  }

  bool Member() const noexcept {
    return StringLeftWeightTraits<Label>::Member(*this);
  }

  std::istream& Read(std::istream& strm) {
    // read size
    // use varlen encoding since weights are usually small
    std::uint32_t size;
    {
      auto it = irs::irstd::make_istream_iterator(strm);
      size = irs::vread<uint32_t>(it);
    }

    // read content
    str_.resize(size);
    strm.read(reinterpret_cast<char*>(str_.data()), size * sizeof(Label));

    return strm;
  }

  std::ostream& Write(std::ostream& strm) const {
    // write size
    // use varlen encoding since weights are usually small
    const std::uint32_t size = static_cast<uint32_t>(Size());
    {
      auto it = irs::irstd::make_ostream_iterator(strm);
      irs::vwrite(it, size);
    }

    // write content
    strm.write(reinterpret_cast<const char*>(str_.c_str()),
               size * sizeof(Label));

    return strm;
  }

  size_t Hash() const noexcept { return std::hash<str_t>()(str_); }

  StringLeftWeight Quantize(float delta = kDelta) const noexcept {
    return *this;
  }

  ReverseWeight Reverse() const {
    return ReverseWeight(str_.rbegin(), str_.rend());
  }

  static std::uint64_t Properties() noexcept {
    static constexpr auto props = kLeftSemiring | kIdempotent;
    return props;
  }

  Label& operator[](size_t i) noexcept { return str_[i]; }

  const Label& operator[](size_t i) const noexcept { return str_[i]; }

  const Label* c_str() const noexcept { return str_.c_str(); }

  void Resize(size_t size) noexcept { str_.resize(size); }

  bool Empty() const noexcept { return str_.empty(); }

  void Clear() noexcept { str_.clear(); }

  size_t Size() const noexcept { return str_.size(); }

  void PushBack(Label label) { str_.push_back(label); }

  template<typename Iterator>
  void PushBack(Iterator begin, Iterator end) {
    str_.append(begin, end);
  }

  void PushBack(const StringLeftWeight& w) { PushBack(w.begin(), w.end()); }

  void Reserve(size_t capacity) { str_.reserve(capacity); }

  iterator begin() const noexcept { return str_.begin(); }
  iterator end() const noexcept { return str_.end(); }

  // intentionally implicit
  operator irs::basic_string_view<Label>() const noexcept { return str_; }

  // intentionally implicit
  operator irs::basic_string<Label>() && noexcept { return std::move(str_); }

 private:
  str_t str_;
};

template<typename Label>
const StringLeftWeight<Label>& StringLeftWeightTraits<Label>::Zero() {
  static const StringLeftWeight<Label> zero(
    static_cast<Label>(kStringInfinity));  // cast same as in FST
  return zero;
}

template<typename Label>
const StringLeftWeight<Label>& StringLeftWeightTraits<Label>::One() {
  static const StringLeftWeight<Label> one;
  return one;
}

template<typename Label>
const StringLeftWeight<Label>& StringLeftWeightTraits<Label>::NoWeight() {
  static const StringLeftWeight<Label> no_weight(
    static_cast<Label>(kStringBad));  // cast same as in FST
  return no_weight;
}

template<typename Label>
bool StringLeftWeightTraits<Label>::Member(
  const StringLeftWeight<Label>& weight) {
  return weight != NoWeight();
}

template<typename Label>
inline bool operator!=(const StringLeftWeight<Label>& w1,
                       const StringLeftWeight<Label>& w2) {
  return !(w1 == w2);
}

template<typename Label>
inline std::ostream& operator<<(std::ostream& strm,
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

template<typename Label>
inline std::istream& operator>>(std::istream& strm,
                                StringLeftWeight<Label>& weight) {
  std::string str;
  strm >> str;
  if (str == "Infinity") {
    weight = StringLeftWeight<Label>::Zero();
  } else if (str == "Epsilon") {
    weight = StringLeftWeight<Label>::One();
  } else {
    weight.Clear();
    char* p = nullptr;
    for (const char* cs = str.c_str(); !p || *p != '\0'; cs = p + 1) {
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
template<typename Label>
inline StringLeftWeight<Label> Plus(const StringLeftWeight<Label>& lhs,
                                    const StringLeftWeight<Label>& rhs) {
  typedef StringLeftWeight<Label> Weight;

  if (!lhs.Member() || !rhs.Member()) {
    return Weight::NoWeight();
  }

  if (lhs == Weight::Zero()) {
    return rhs;
  }

  if (rhs == Weight::Zero()) {
    return lhs;
  }

  const auto* plhs = &lhs;
  const auto* prhs = &rhs;

  if (rhs.Size() > lhs.Size()) {
    // enusre that 'prhs' is shorter than 'plhs'
    // The behavior is undefined if the second range is shorter than the first
    // range. (http://en.cppreference.com/w/cpp/algorithm/mismatch)
    std::swap(plhs, prhs);
  }

  IRS_ASSERT(prhs->Size() <= plhs->Size());

  return Weight(prhs->begin(),
                std::mismatch(prhs->begin(), prhs->end(), plhs->begin()).first);
}

template<typename Label>
inline StringLeftWeight<Label> Times(const StringLeftWeight<Label>& lhs,
                                     const StringLeftWeight<Label>& rhs) {
  typedef StringLeftWeight<Label> Weight;

  if (!lhs.Member() || !rhs.Member()) {
    return Weight::NoWeight();
  }

  if (lhs == Weight::Zero() || rhs == Weight::Zero()) {
    return Weight::Zero();
  }

  Weight product;
  product.Reserve(lhs.Size() + rhs.Size());
  product.PushBack(lhs.begin(), lhs.end());
  product.PushBack(rhs.begin(), rhs.end());
  return product;
}

// Left division in a left string semiring.
template<typename Label>
inline StringLeftWeight<Label> DivideLeft(const StringLeftWeight<Label>& lhs,
                                          const StringLeftWeight<Label>& rhs) {
  typedef StringLeftWeight<Label> Weight;

  if (!lhs.Member() || !rhs.Member()) {
    return Weight::NoWeight();
  }

  if (rhs == Weight::Zero()) {
    return Weight::NoWeight();
  }
  if (lhs == Weight::Zero()) {
    return Weight::Zero();
  }

  if (rhs.Size() > lhs.Size()) {
    return Weight();
  }

  IRS_ASSERT(irs::basic_string_view<Label>(lhs).starts_with(rhs));

  return Weight(lhs.begin() + rhs.Size(), lhs.end());
}

template<typename Label>
inline StringLeftWeight<Label> Divide(const StringLeftWeight<Label>& lhs,
                                      const StringLeftWeight<Label>& rhs,
                                      DivideType typ) {
  IRS_ASSERT(DIVIDE_LEFT == typ);
  return DivideLeft(lhs, rhs);
}

// -----------------------------------------------------------------------------
// --SECTION--                                  StringLeftWeight<irs::byte_type>
// -----------------------------------------------------------------------------

template<>
struct StringLeftWeightTraits<irs::byte_type> {
  static const StringLeftWeight<irs::byte_type>& Zero() noexcept {
    static const StringLeftWeight<irs::byte_type> zero;
    return zero;
  }

  static const StringLeftWeight<irs::byte_type>& One() noexcept {
    return Zero();
  }

  static const StringLeftWeight<irs::byte_type>& NoWeight() noexcept {
    return Zero();
  }

  static bool Member(const StringLeftWeight<irs::byte_type>&) noexcept {
    // always member
    return true;
  }
};

inline std::ostream& operator<<(
  std::ostream& strm, const StringLeftWeight<irs::byte_type>& weight) {
  if (weight.Empty()) {
    return strm << "Epsilon";
  }

  auto begin = weight.begin();

  const auto end = weight.end();
  if (begin != end) {
    strm << *begin;

    for (++begin; begin != end; ++begin) {
      strm << kStringSeparator << *begin;
    }
  }

  return strm;
}

inline std::istream& operator>>(std::istream& strm,
                                StringLeftWeight<irs::byte_type>& weight) {
  std::string str;
  strm >> str;
  if (str == "Epsilon") {
    weight = StringLeftWeight<irs::byte_type>::One();
  } else {
    weight.Clear();
    char* p = nullptr;
    for (const char* cs = str.c_str(); !p || *p != '\0'; cs = p + 1) {
      const irs::byte_type label = strtoll(cs, &p, 10);
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
// For binary strings that's impossible to use
// Zero() or NoWeight() as they may interfere
// with real values
inline irs::bytes_view PlusImpl(irs::bytes_view lhs, irs::bytes_view rhs) {
  const auto* plhs = &lhs;
  const auto* prhs = &rhs;

  if (rhs.size() > lhs.size()) {
    // enusre that 'prhs' is shorter than 'plhs'
    // The behavior is undefined if the second range is shorter than the first
    // range. (http://en.cppreference.com/w/cpp/algorithm/mismatch)
    std::swap(plhs, prhs);
  }

  auto pair =
    std::mismatch(prhs->data(), prhs->data() + prhs->size(), plhs->data());
  return {prhs->data(), static_cast<size_t>(pair.first - prhs->data())};
}

inline irs::bytes_view Plus(const StringLeftWeight<irs::byte_type>& lhs,
                            const StringLeftWeight<irs::byte_type>& rhs) {
  return PlusImpl(lhs, rhs);
}

inline irs::bytes_view Plus(irs::bytes_view lhs,
                            const StringLeftWeight<irs::byte_type>& rhs) {
  return PlusImpl(lhs, rhs);
}

inline irs::bytes_view Plus(const StringLeftWeight<irs::byte_type>& lhs,
                            irs::bytes_view rhs) {
  return PlusImpl(lhs, rhs);
}

// For binary strings that's impossible to use
// Zero() or NoWeight() as they may interfere
// with real values
inline StringLeftWeight<irs::byte_type> TimesImpl(irs::bytes_view lhs,
                                                  irs::bytes_view rhs) {
  using Weight = StringLeftWeight<irs::byte_type>;

  Weight product;
  product.Reserve(lhs.size() + rhs.size());
  product.PushBack(lhs.begin(), lhs.end());
  product.PushBack(rhs.begin(), rhs.end());
  return product;
}

inline StringLeftWeight<irs::byte_type> Times(
  const StringLeftWeight<irs::byte_type>& lhs,
  const StringLeftWeight<irs::byte_type>& rhs) {
  return TimesImpl(lhs, rhs);
}

inline StringLeftWeight<irs::byte_type> Times(
  irs::bytes_view lhs, const StringLeftWeight<irs::byte_type>& rhs) {
  return TimesImpl(lhs, rhs);
}

inline StringLeftWeight<irs::byte_type> Times(
  const StringLeftWeight<irs::byte_type>& lhs, irs::bytes_view rhs) {
  return TimesImpl(lhs, rhs);
}

// Left division in a left string semiring.
// For binary strings that's impossible to use
// Zero() or NoWeight() as they may interfere
// with real values
inline irs::bytes_view DivideLeftImpl(irs::bytes_view lhs,
                                      irs::bytes_view rhs) {
  if (rhs.size() > lhs.size()) {
    return {};
  }
  IRS_ASSERT(lhs.starts_with(rhs),
             absl::StrCat(irs::ViewCast<char>(rhs), " is not prefix of ",
                          irs::ViewCast<char>(lhs)));
  return {lhs.data() + rhs.size(), lhs.size() - rhs.size()};
}

inline irs::bytes_view DivideLeft(const StringLeftWeight<irs::byte_type>& lhs,
                                  const StringLeftWeight<irs::byte_type>& rhs) {
  return DivideLeftImpl(lhs, rhs);
}

inline irs::bytes_view DivideLeft(irs::bytes_view lhs,
                                  const StringLeftWeight<irs::byte_type>& rhs) {
  return DivideLeftImpl(lhs, rhs);
}

inline irs::bytes_view DivideLeft(const StringLeftWeight<irs::byte_type>& lhs,
                                  irs::bytes_view rhs) {
  return DivideLeftImpl(lhs, rhs);
}

inline irs::bytes_view DivideLeft(irs::bytes_view lhs, irs::bytes_view rhs) {
  return DivideLeftImpl(lhs, rhs);
}

inline StringLeftWeight<irs::byte_type> Divide(
  const StringLeftWeight<irs::byte_type>& lhs,
  const StringLeftWeight<irs::byte_type>& rhs, DivideType typ) {
  IRS_ASSERT(DIVIDE_LEFT == typ);
  return StringLeftWeight<irs::byte_type>(DivideLeft(lhs, rhs));
}

}  // namespace fst
