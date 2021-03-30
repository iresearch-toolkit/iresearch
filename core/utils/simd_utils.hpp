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

#include <hwy/highway.h>

#include "utils/bit_packing.hpp"

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

template<size_t Length, typename Simd, typename T>
inline std::pair<T, T> maxmin(
    const Simd simd_tag,
    const T* begin) noexcept {
  constexpr size_t Step = MaxLanes(simd_tag);
  static_assert(0 == (Length % Step));

  auto minacc = Load(simd_tag, begin);
  auto maxacc = minacc;
  for (size_t i = Step; i < Length; i += Step) {
    const auto v = Load(simd_tag, begin + i);
    minacc = Min(minacc, v);
    maxacc = Max(maxacc, v);
  }
  return {
    GetLane(MinOfLanes(minacc)),
    GetLane(MaxOfLanes(maxacc))
  };
}

template<typename Simd, typename T>
inline uint32_t maxbits(
    const Simd simd_tag,
    const T* begin,
    size_t size) noexcept {
  constexpr size_t Step = MaxLanes(simd_tag);
  assert(0 == (size % Step));

  auto oracc = Load(simd_tag, begin);
  for (size_t i = Step; i < size; i += Step) {
    const auto v = Load(simd_tag, begin + i);
    oracc = Or(oracc, v);
  }

  // FIXME use OrOfLanes instead
  return math::math_traits<T>::bits_required(GetLane(MaxOfLanes(oracc)));
}

template<size_t Length, typename Simd, typename T>
FORCE_INLINE uint32_t maxbits(
    const Simd simd_tag,
    const T* begin) noexcept {
  static_assert(0 == (Length % MaxLanes(simd_tag)));
  return maxbits(simd_tag, begin, Length);
}

template<typename Simd, typename T>
inline void fill_n(const Simd simd_tag, T* begin, size_t size, const T value) noexcept {
  constexpr size_t Step = MaxLanes(simd_tag);
  assert(0 == (size % Step));

  const auto vvalue = Set(simd_tag, value);
  for (size_t i = 0; i < size; i += Step) {
    Store(vvalue, simd_tag, begin + i);
  }
}

template<size_t Length, typename Simd, typename T>
FORCE_INLINE void fill_n(const Simd simd_tag, T* begin, const T value) noexcept {
  static_assert(0 == (Length % MaxLanes(simd_tag)));
  fill_n(simd_tag, begin, Length, value);
}

template<typename Simd, typename T>
inline bool all_equal(
    const Simd simd_tag,
    const T* RESTRICT begin,
    const T* RESTRICT end) noexcept {
  constexpr size_t Step = MaxLanes(simd_tag);
  assert(0 == (std::distance(begin, end) % Step));

  if (begin == end) {
    return true;
  }

  const auto value = Set(simd_tag, *begin);
  for (; begin != end; begin += Step) {
    if (!AllTrue(value == LoadU(simd_tag, begin))) {
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

  // FIXME
  //  subtract min

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
