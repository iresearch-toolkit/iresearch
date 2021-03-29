////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_SIMD_UTILS_H
#define IRESEARCH_SIMD_UTILS_H

#include "shared.hpp"

#include <cassert>
#include <smmintrin.h>

extern "C" {
#include <simdcomp/include/simdcomputil.h>
}

#include <hwy/highway.h>

namespace iresearch {
namespace simd {

using namespace hwy::HWY_NAMESPACE;

using vu32_t = HWY_FULL(uint32_t);
static constexpr vu32_t vu32;

using vi32_t = HWY_FULL(int32_t);
static constexpr vi32_t vi32;

using vu64_t = HWY_FULL(uint64_t);
static constexpr vu64_t vu64;

using vi64_t = HWY_FULL(int64_t);
static constexpr vi64_t vi64;

template<size_t Length>
FORCE_INLINE std::pair<uint32_t, uint32_t> maxmin(
    const uint32_t* begin) noexcept {
  static_assert(0 == (Length % SIMDBlockSize));

  uint32_t accmin, accmax;
  simdmaxmin(begin, &accmin, &accmax);

  const uint32_t* end = begin + Length;
  for (begin += SIMDBlockSize ; begin != end; begin += SIMDBlockSize) {
    uint32_t min, max;
    simdmaxmin(begin, &min, &max);
    accmin = std::min(min, accmin);
    accmax = std::max(max, accmax);
  }

  return {accmin, accmax};
}

template<size_t Length>
inline void fill_n(uint32_t* begin, const uint32_t value) noexcept {
  constexpr size_t Step = MaxLanes(vu32);
  static_assert(0 == (Length % Step));

  const auto vvalue = Set(vu32, value);
  for (size_t i = 0; i < Length; i += Step) {
    Store(vvalue, vu32, begin + i);
  }
}

inline bool all_equal(
    const uint32_t* RESTRICT begin,
    const uint32_t* RESTRICT end) noexcept {
  constexpr size_t Step = MaxLanes(vu32);
  assert(0 == (std::distance(begin, end) % Step));

  if (begin == end) {
    return true;
  }

  const auto value = Set(vu32, *begin);
  for (; begin != end; begin += Step) {
    if (!AllTrue(value == LoadU(vu32, begin))) {
      return false;
    }
  }

  return true;
}

FORCE_INLINE Vec<vu32_t> zig_zag_encode32(Vec<vi32_t> v) noexcept {
  const auto uv = BitCast(vu32, v);
  return ((uv >> Set(vu32, 31)) ^ (uv << Set(vu32, 1)));
}

FORCE_INLINE Vec<vi32_t> zig_zag_decode32(Vec<vu32_t> uv) noexcept {
  const auto v = BitCast(vi32, uv);
  return ((v >> Set(vi32, 1)) ^ (Zero(vi32)-(v & Set(vi32, 1))));
}

FORCE_INLINE Vec<vu64_t> zig_zag_encode64(Vec<vi64_t> v) noexcept {
  const auto uv = BitCast(vu64, v);
  return ((uv >> Set(vu64, 63)) ^ (uv << Set(vu64, 1)));
}

FORCE_INLINE Vec<vi64_t> zig_zag_decode64(Vec<vu64_t> uv) noexcept {
  const auto v = BitCast(vi64, uv);
  return ((v >> Set(vi64, 1)) ^ (Zero(vi64)-(v & Set(vi64, 1))));
}

//FIXME simd for delta encoding

// Encodes block denoted by [begin;end) using average encoding algorithm
// Returns block std::pair{ base, average }
template<size_t Length>
inline std::pair<uint32_t, uint32_t> avg_encode32(uint32_t* begin) noexcept {
  static_assert(Length);
  constexpr size_t Step = MaxLanes(vu32);
  static_assert(0 == (Length % Step));
  assert(begin[Length-1] >= begin[0]);

  const uint32_t base = *begin;

  const int32_t avg = static_cast<int32_t>(
    static_cast<float_t>(begin[Length-1] - begin[0]) / std::max(size_t(1), Length - 1));

  auto vbase = Iota(vi32, 0) * Set(vi32, avg) + Set(vi32, base);
  const auto vavg = Set(vi32, avg) * Set(vi32, Step);

  for (size_t i = 0; i < Length; i += Step, vbase += vavg) {
    const auto v = Load(vi32, reinterpret_cast<int32_t*>(begin + i)) - vbase;
    Store(zig_zag_encode32(v), vu32, begin + i);
  }

  return std::make_pair(base, avg);
}

template<size_t Length>
inline void avg_decode32(
    const uint32_t* begin, uint32_t* out,
    uint32_t base, uint32_t avg) noexcept {
  static_assert(Length);
  constexpr size_t Step = MaxLanes(vu32);
  static_assert(0 == (Length % Step));
  assert(begin[Length-1] >= begin[0]);

  auto vbase = Iota(vi32, 0) * Set(vi32, avg) + Set(vi32, base);
  const auto vavg = Set(vi32, avg) * Set(vi32, Step);

  for (size_t i = 0; i < Length; i += Step, vbase += vavg) {
    const auto v = Load(vu32, begin + i);
    Store(zig_zag_decode32(v) + vbase, vi32, reinterpret_cast<int32_t*>(out + i));
  }
}

}
}

#endif // IRESEARCH_SIMD_UTILS_H
