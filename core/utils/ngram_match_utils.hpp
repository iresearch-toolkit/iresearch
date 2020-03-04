////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_NGRAM_MATCH_UTILS_H
#define IRESEARCH_NGRAM_MATCH_UTILS_H

#include "shared.hpp"
#include "utf8_utils.hpp"
#include <vector>

NS_ROOT


template<typename T, bool use_ngram_position_match>
float_t ngram_similarity(const T* lhs, size_t lhs_size,
                         const T* rhs, size_t rhs_size,
                         size_t ngram_size) {

  if (lhs_size < ngram_size || rhs_size < ngram_size) {
    return 0.f;
  }

  const size_t lhs_ngram_count = lhs_size - ngram_size + 1;
  const size_t rhs_ngram_count = rhs_size - ngram_size + 1;

  const T* lhs_ngram_start = lhs;
  
  const T* lhs_ngram_start_end  = lhs + lhs_size - ngram_size + 1; // <end> for ngram start

  float_t d = 0;
  std::vector<float_t> cache(std::max(lhs_ngram_count, rhs_ngram_count) + 1, 0);

  size_t lhs_ngram_idx = 1;
  for (; lhs_ngram_start != lhs_ngram_start_end; ++lhs_ngram_start, ++lhs_ngram_idx) {
    const T* lhs_ngram_end = lhs_ngram_start + ngram_size;
    const T* rhs_ngram_start = rhs;
    size_t rhs_ngram_idx = 1;
    const T* rhs_ngram_start_end  = rhs + rhs_size - ngram_size + 1; // <end> for ngram start

    for (; rhs_ngram_start != rhs_ngram_start_end; ++rhs_ngram_start, ++rhs_ngram_idx) {
      const T* rhs_ngram_end = rhs_ngram_start + ngram_size;
      // boolean similarity by now. Just match or not!
      float_t similarity = use_ngram_position_match ? 0 : 1;
      for (const T* l = lhs_ngram_start, *r = rhs_ngram_start; l != lhs_ngram_end && r != rhs_ngram_end; ++l, ++r) {
        if /*constexpr*/ (!use_ngram_position_match) {
          if (*l != *r) {
            similarity = 0;
            break;
          }
        } else {
          if (*l == *r) {
            ++similarity;
          }
        }
      }
      if /*constexpr*/ (use_ngram_position_match) {
        similarity = similarity / float_t(ngram_size);
      }

      auto tmp = cache[rhs_ngram_idx];
      cache[rhs_ngram_idx] =
          std::max(
            std::max(cache[rhs_ngram_idx - 1],
                     cache[lhs_ngram_idx]),
            d + similarity);
      d = tmp;
    }
  }

  return cache[rhs_ngram_count] / float_t(std::max(lhs_ngram_count, rhs_ngram_count));
}

NS_END


#endif IRESEARCH_NGRAM_MATCH_UTILS_H // IRESEARCH_NGRAM_MATCH_UTILS_H