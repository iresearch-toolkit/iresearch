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


namespace iresearch {
namespace simd {

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
inline void fill_n(
    uint32_t* begin,
    const uint32_t value) noexcept {
  static_assert(0 == (Length % SIMDBlockSize));

  auto* mmbegin = reinterpret_cast<__m128i*>(begin);
  auto* mmend = reinterpret_cast<__m128i*>(begin + Length);
  const auto mmvalue = _mm_set1_epi32(value);

  for (; mmbegin != mmend; mmbegin += 4) {
    _mm_storeu_si128(mmbegin    , mmvalue);
    _mm_storeu_si128(mmbegin + 1, mmvalue);
    _mm_storeu_si128(mmbegin + 2, mmvalue);
    _mm_storeu_si128(mmbegin + 3, mmvalue);
  }
}

inline bool all_equal(
    const uint32_t* RESTRICT begin,
    const uint32_t* RESTRICT end) noexcept {
  assert(0 == (std::distance(begin, end) % sizeof(__m128i)));

  if (begin == end) {
    return true;
  }

  const __m128i* mmbegin = reinterpret_cast<const __m128i*>(begin);
  const __m128i* mmend = reinterpret_cast<const __m128i*>(end);
  const __m128i value = _mm_set1_epi32(*begin);

  while (mmbegin != mmend) {
    const __m128i neq = _mm_xor_si128(value, _mm_loadu_si128(mmbegin++));

    if (!_mm_test_all_zeros(neq,neq)) {
      return false;
    }
  }

  return true;
}

}
}

#endif // IRESEARCH_SIMD_UTILS_H
