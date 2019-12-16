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

#ifndef IRESEARCH_LEVENSHTEIN_UTILS_H
#define IRESEARCH_LEVENSHTEIN_UTILS_H

#include "string.hpp"

#include <vector>
#include <numeric>

NS_ROOT

template<typename T, size_t SubstCost = 1>
inline size_t edit_distance(const T* lhs, size_t lhs_size,
                            const T* rhs, size_t rhs_size) {
  assert(lhs || !lhs_size);
  assert(rhs || !rhs_size);

  if (lhs_size > rhs_size) {
    std::swap(lhs, rhs);
    std::swap(lhs_size, rhs_size);
  }

  std::vector<size_t> cost(2*(lhs_size + 1));

  auto current = cost.begin();
  auto next = cost.begin() + cost.size()/2;
  std::iota(current, next, 0);

  for (size_t j = 1; j <= rhs_size; ++j) {
    next[0] = j;
    for (size_t i = 1; i <= lhs_size; ++i) {
      next[i] = std::min({
        next[i-1]    + 1,                                     // deletion
        current[i]   + 1,                                     // insertion
        current[i-1] + (lhs[i-1] == rhs[j-1] ? 0 : SubstCost) // substitution
      });
    }
    std::swap(next, current);
  }

  return current[lhs_size];
}

NS_END

#endif // IRESEARCH_LEVENSHTEIN_UTILS_H

