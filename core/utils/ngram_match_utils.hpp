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
float_t ngram_similarity(const T* target, size_t target_size,
                         const T* src, size_t src_size,
                         size_t ngram_size) {
  if (ngram_size == 0) {
    return 0.f;
  }

  if /*consexpr*/ (use_ngram_position_match) {
    if (target_size > src_size) {
      std::swap(target_size, src_size);
      std::swap(target, src);
    }
  }

  if (target_size < ngram_size || src_size < ngram_size) {
    if /*constexpr*/ (use_ngram_position_match) {
      if (target_size == 0 && src_size == 0) {
        return 1; // consider two empty strings as matched
      }
      const T* r = src;
      size_t matched = 0;
      for (const T* it = target; it != target + target_size; ) {
        matched += size_t(*it == *r);
        ++r;
        ++it;
      }
      return float_t(matched) / float_t(src_size);
    } else {
      // search implies some ngrams should be found, but if intput is too short, no ngrams = no matches
      return 0;
    }
  }

  const size_t t_ngram_count = target_size - ngram_size + 1;
  const size_t s_ngram_count = src_size - ngram_size + 1;
  const T* t_ngram_start = target;
  const T* t_ngram_start_end  = target + target_size - ngram_size + 1; // end() analog for target ngram start

  float_t d = 0;
  std::vector<float_t> cache(s_ngram_count + 1, 0);

  size_t t_ngram_idx = 1;
  for (; t_ngram_start != t_ngram_start_end; ++t_ngram_start, ++t_ngram_idx) {
    const T* t_ngram_end = t_ngram_start + ngram_size;
    const T* s_ngram_start = src;
    size_t s_ngram_idx = 1;
    const T* s_ngram_start_end  = src + src_size - ngram_size + 1; // end() analog for src ngram start

    for (; s_ngram_start != s_ngram_start_end; ++s_ngram_start, ++s_ngram_idx) {
      const T* rhs_ngram_end = s_ngram_start + ngram_size;
      float_t similarity = use_ngram_position_match ? 0 : 1;
      for (const T* l = t_ngram_start, *r = s_ngram_start; l != t_ngram_end && r != rhs_ngram_end; ++l, ++r) {
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

      auto tmp = cache[s_ngram_idx];
      cache[s_ngram_idx] =
          std::max(
            std::max(cache[s_ngram_idx - 1],
                     cache[s_ngram_idx]),
            d + similarity);
      d = tmp;
    }
  }
  return cache[s_ngram_count] /
         float_t((use_ngram_position_match) ? s_ngram_count : t_ngram_count);
}

NS_END


#endif // IRESEARCH_NGRAM_MATCH_UTILS_H
