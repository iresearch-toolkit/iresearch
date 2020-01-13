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

#include <vector>
#include <numeric>

#include "string.hpp"
#include "automaton_decl.hpp"

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

////////////////////////////////////////////////////////////////////////////////
/// @brief evaluates edit distance between the specified words
/// @param lhs string to compare
/// @param rhs string to compare
/// @returns edit distance
////////////////////////////////////////////////////////////////////////////////
inline size_t edit_distance(const bytes_ref& lhs, const bytes_ref& rhs) {
  return edit_distance(lhs.begin(), lhs.size(), rhs.begin(), rhs.size());
}

// -----------------------------------------------------------------------------
// --SECTION--
//
// Implementation of the algorithm of building Levenshtein automaton
// by Klaus Schulz, Stoyan Mihov described in
//   http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.16.652
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief theoretically max possible distance we can evaluate, not really
///        feasible due to exponential growth of parametric description size
////////////////////////////////////////////////////////////////////////////////
constexpr byte_type MAX_DISTANCE = 31;

struct parametric_transition {
  parametric_transition(size_t to, uint32_t offset) noexcept
    : to(to), offset(offset) {
  }

  size_t to;
  uint32_t offset;
}; // parametric_transition

struct IRESEARCH_API parametric_description {
  std::vector<parametric_transition> transitions;
  std::vector<byte_type> distance;
  size_t num_states;                     // number of parametric states (transitions.size()/chi_max)
  uint64_t chi_size;                     // 2*max_distance+1
  uint64_t chi_max;                      // 1 << chi_size
  byte_type max_distance;                // max allowed distance
};

////////////////////////////////////////////////////////////////////////////////
/// @brief builds parametric description of Levenshtein automaton
/// @param max_distance maximum allowed distance
/// @param with_transposition count transpositions
/// @returns parametric description of Levenshtein automaton for supplied args
////////////////////////////////////////////////////////////////////////////////
IRESEARCH_API parametric_description make_parametric_description(
  byte_type max_distance,
  bool with_transposition);

////////////////////////////////////////////////////////////////////////////////
/// @brief instantiates DFA based on provided parametric description and target
/// @param description parametric description
/// @param target actual "string" (utf8 encoded)
/// @returns DFA
////////////////////////////////////////////////////////////////////////////////
IRESEARCH_API automaton make_levenshtein_automaton(
  const parametric_description& description,
  const bytes_ref& target);

////////////////////////////////////////////////////////////////////////////////
/// @brief evaluates edit distance between the specified words up to
///        specified in description.max_distance
/// @param description parametric description
/// @param lhs string to compare (utf8 encoded)
/// @param lhs_size size of the string to comprare
/// @param rhs string to compare (utf8 encoded)
/// @param rhs_size size of the string to comprare
/// @returns edit_distance up to specified in description.max_distance
////////////////////////////////////////////////////////////////////////////////
IRESEARCH_API size_t edit_distance(
  const parametric_description& description,
  const byte_type* lhs, size_t lhs_size,
  const byte_type* rhs, size_t rhs_size) noexcept;

////////////////////////////////////////////////////////////////////////////////
/// @brief evaluates edit distance between the specified words up to
///        specified in description.max_distance
/// @param description parametric description
/// @param lhs string to compare (utf8 encoded)
/// @param rhs string to compare (utf8 encoded)
/// @returns edit_distance up to specified in description.max_distance
////////////////////////////////////////////////////////////////////////////////
inline size_t edit_distance(
    const parametric_description& description,
    const bytes_ref& lhs,
    const bytes_ref& rhs) noexcept {
  return edit_distance(description, lhs.begin(), lhs.size(), rhs.begin(), rhs.size());
}

NS_END

#endif // IRESEARCH_LEVENSHTEIN_UTILS_H

