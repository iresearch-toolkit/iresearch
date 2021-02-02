////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_AUTOMATON_H
#define IRESEARCH_AUTOMATON_H

#include "shared.hpp"

#if defined(_MSC_VER)
  // NOOP
#elif defined (__GNUC__)
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wsign-compare"
#endif

#ifndef FST_NO_DYNAMIC_LINKING
#define FST_NO_DYNAMIC_LINKING
#endif

#include <fst/fst.h>

#if defined(_MSC_VER)
  // NOOP
#elif defined (__GNUC__)
  #pragma GCC diagnostic pop
#endif

#include "utils/automaton_decl.hpp"
#include "utils/fstext/fst_utils.hpp"
#include "utils/string.hpp"

namespace fst {
namespace fsa {

class BooleanWeight {
 public:
  using ReverseWeight = BooleanWeight;
  using PayloadType = irs::byte_type;

  static const std::string& Type() {
    static const std::string type = "boolean";
    return type;
  }

  static constexpr BooleanWeight Zero() noexcept { return false; }
  static constexpr BooleanWeight One() noexcept { return true; }
  static constexpr BooleanWeight NoWeight() noexcept { return {}; }

  static constexpr uint64 Properties() noexcept {
    return kLeftSemiring | kRightSemiring |
           kCommutative | kIdempotent | kPath;
  }

  constexpr BooleanWeight() noexcept = default;
  constexpr BooleanWeight(bool v, PayloadType payload = 0) noexcept
    : v_(PayloadType(v)), p_(payload) {
  }

  constexpr bool Member() const noexcept { return Invalid != v_; }
  constexpr BooleanWeight Quantize([[maybe_unused]]float delta = kDelta) const noexcept { return {};  }
  std::istream& Read(std::istream& strm) noexcept {
    v_ = strm.get();
    if (strm.fail()) {
      v_ = Invalid;
    }
    return strm;
  }
  std::ostream& Write(std::ostream &strm) const noexcept {
    strm.put(v_);
    return strm;
  }
  constexpr size_t Hash() const noexcept { return size_t(v_); }
  constexpr ReverseWeight Reverse() const noexcept { return *this; }
  constexpr PayloadType Payload() const noexcept { return p_; }
  constexpr operator bool() const noexcept { return v_ == True; }

  friend constexpr bool operator==(const BooleanWeight& lhs, const BooleanWeight& rhs) noexcept {
    return lhs.Hash() == rhs.Hash();
  }
  friend constexpr bool operator!=(const BooleanWeight& lhs, const BooleanWeight& rhs) noexcept {
    return !(lhs == rhs);
  }
  friend constexpr BooleanWeight Plus(const BooleanWeight& lhs, const BooleanWeight& rhs) noexcept {
    return BooleanWeight(bool(lhs.Hash()) || bool(rhs.Hash()), lhs.Payload() | rhs.Payload());
  }
  friend constexpr BooleanWeight Times(const BooleanWeight& lhs, const BooleanWeight& rhs) noexcept {
    return BooleanWeight(bool(lhs.Hash()) && bool(rhs.Hash()), lhs.Payload() & rhs.Payload());
  }
  friend constexpr BooleanWeight Divide(BooleanWeight, BooleanWeight, DivideType) noexcept {
    return NoWeight();
  }
  friend constexpr BooleanWeight Divide(BooleanWeight, BooleanWeight) noexcept {
    return NoWeight();
  }
  friend std::ostream& operator<<(std::ostream& strm, const BooleanWeight& w) {
    if (w.Member()) {
      strm << "{" << char(bool(w) + 48) << "," << int(w.Payload()) << "}";
    }
    return strm;
  }
  friend constexpr bool ApproxEqual(const BooleanWeight& lhs, const BooleanWeight& rhs,
                                    [[maybe_unused]] float delta = kDelta) {
    return lhs == rhs;
  }

 private:
  static constexpr PayloadType False = 0;
  static constexpr PayloadType True = 1;     // "is true" mask
  static constexpr PayloadType Invalid = 2; // "not a member" value

  PayloadType v_{Invalid};
  PayloadType p_{};
};

// FIXME remove
constexpr std::pair<uint32_t, uint32_t> DecodeRange(uint64_t label) noexcept {
  return {
    static_cast<uint32_t>(label >> 32),
    static_cast<uint32_t>(label & UINT64_C(0xFFFFFFFF))
  };
}

struct RangeLabel {
  static constexpr RangeLabel fromRange(uint32_t min, uint32_t max) noexcept {
    return RangeLabel{min, max};
  }
  static constexpr RangeLabel fromRange(uint32_t min) noexcept {
    return fromRange(min, min);
  }
  static constexpr RangeLabel fromLabel(int64_t label) noexcept {
    return RangeLabel{label};
  }

  constexpr RangeLabel() noexcept
    : ilabel{fst::kNoLabel} {
  }

  constexpr RangeLabel(uint32_t min, uint32_t max) noexcept
    : max{max}, min{min} {
  }

  constexpr explicit RangeLabel(int64_t ilabel) noexcept
    : ilabel{ilabel} {
  }

  constexpr operator int64_t() const noexcept {
    return ilabel;
  }

  friend std::ostream& operator<<(std::ostream& strm, const RangeLabel& l) {
    strm << '[' << l.min << ".." << l.max << ']';
    return strm;
  }

  union {
    int64_t ilabel;
    struct {
      uint32_t max;
      uint32_t min;
    };
  };
}; // RangeLabel

template<typename W = BooleanWeight>
struct Transition : RangeLabel {
  using Weight = W;
  using Label = int64_t;
  using StateId = int32_t;

  static const std::string &Type() {
    static const std::string type("Transition");
    return type;
  }

  union {
    StateId nextstate{fst::kNoStateId};
    fstext::EmptyLabel<Label> olabel;
    fstext::EmptyWeight<Weight> weight; // all arcs are trivial
  };

  constexpr Transition() = default;

  constexpr Transition(RangeLabel ilabel, StateId nextstate)
    : RangeLabel{ilabel},
      nextstate(nextstate) {
  }

  constexpr Transition(Label ilabel, StateId nextstate)
    : RangeLabel{ilabel},
      nextstate{nextstate} {
  }

  // satisfy openfst API
  constexpr Transition(Label ilabel, Label, Weight, StateId nextstate)
    : RangeLabel{ilabel},
      nextstate{nextstate} {
  }

  // satisfy openfst API
  constexpr Transition(Label ilabel, Label, StateId nextstate)
    : RangeLabel{ilabel},
      nextstate{nextstate} {
  }
}; // Transition

} // fsa
} // fst

#if defined(_MSC_VER)
  // NOOP
#elif defined (__GNUC__)
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wsign-compare"
#endif

#include <fst/vector-fst.h>
#include <fst/matcher.h>

#if defined(_MSC_VER)
  // NOOP
#elif defined (__GNUC__)
  #pragma GCC diagnostic pop
#endif

#endif // IRESEARCH_AUTOMATON_H
