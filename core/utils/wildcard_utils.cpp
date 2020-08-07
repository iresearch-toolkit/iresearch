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

#include "wildcard_utils.hpp"

#include "fst/concat.h"
#include "fstext/determinize-star.h"
#include "draw-impl.h"

#include "automaton_utils.hpp"

NS_ROOT

WildcardType wildcard_type(const bytes_ref& expr) noexcept {
  if (expr.empty()) {
    return WildcardType::TERM;
  }

  bool escaped = false;
  bool seen_escaped = false;
  size_t num_match_any_string = 0;
  size_t num_adjacent_match_any_string = 0;

  const auto* char_begin = expr.begin();
  const auto* end = expr.end();

  for (size_t i = 0; char_begin < end; ++i) {
    const auto char_length = utf8_utils::cp_length(*char_begin);
    const auto char_end = char_begin + char_length;

    if (!char_length || char_end > end) {
      return WildcardType::INVALID;
    }

    switch (*char_begin) {
      case WildcardMatch::ANY_STRING:
        num_adjacent_match_any_string += size_t(!escaped);
        num_match_any_string += size_t(!escaped);
        seen_escaped |= escaped;
        escaped = false;
        break;
      case WildcardMatch::ANY_CHAR:
        if (!escaped) {
          return WildcardType::WILDCARD;
        }
        seen_escaped = true;
        num_adjacent_match_any_string = 0;
        escaped = false;
        break;
      case WildcardMatch::ESCAPE:
        num_adjacent_match_any_string = 0;
        seen_escaped |= escaped;
        escaped = !escaped;
        break;
      default:
        num_adjacent_match_any_string = 0;
        escaped = false;
        break;
    }

    char_begin = char_end;
  }

  if (0 == num_match_any_string) {
    return seen_escaped ? WildcardType::TERM_ESCAPED
                        : WildcardType::TERM;
  }

  if (expr.size() == num_match_any_string) {
    return WildcardType::MATCH_ALL;
  }

  if (num_match_any_string == num_adjacent_match_any_string) {
    return seen_escaped ? WildcardType::PREFIX_ESCAPED
                        : WildcardType::PREFIX;
  }

  return WildcardType::WILDCARD;
}

automaton from_wildcard(const bytes_ref& expr) {
  bool escaped = false;

  const auto* label_begin = expr.begin();
  const auto* end = expr.end();

  std::vector<automaton> parts;
  while (label_begin < end) {
    const auto label_length = utf8_utils::cp_length(*label_begin);
    const auto label_end = label_begin + label_length;

    if (!label_length || label_end > end) {
      // invalid UTF-8 sequence
      return {};
    }

    switch (*label_begin) {
      case WildcardMatch::ANY_STRING: {
        if (escaped) {
          parts.emplace_back(make_char({label_begin, label_length}));
          escaped = false;
        } else {
          parts.emplace_back(make_all());
        }
        break;
      }
      case WildcardMatch::ANY_CHAR: {
        if (escaped) {
          parts.emplace_back(make_char({label_begin, label_length}));
          escaped = false;
        } else {
          parts.emplace_back(make_any());
        }
        break;
      }
      case WildcardMatch::ESCAPE: {
        if (escaped) {
          parts.emplace_back(make_char({label_begin, label_length}));
          escaped = false;
        } else {
          escaped = !escaped;
        }
        break;
      }
      default: {
        parts.emplace_back(make_char({label_begin, label_length}));
        escaped = false;
        break;
      }
    }

    label_begin = label_end;
  }

  // need this variable to preserve valid address
  // for cases with match all and  terminal escape
  // character (%\\)
  const byte_type c = WildcardMatch::ESCAPE;

  if (escaped) {
    // non-terminated escape sequence
    parts.emplace_back(make_char({&c, 1}));
  }

  automaton nfa;
  nfa.SetStart(nfa.AddState());
  nfa.SetFinal(0, true);
  for (auto& part : parts) {
    fst::Concat(&nfa, part);
  }

   // {
   //   std::fstream f;
   //   f.open("/home/gnusi/1", std::fstream::out);
   //   fst::drawFst(nfa, f);
   // }

 automaton dfa;
 fst::DeterminizeStar(nfa, &dfa);
 fst::Minimize(&dfa);

#ifdef IRESEARCH_DEBUG
  // ensure resulting automaton is sorted and deterministic
  static constexpr auto EXPECTED_PROPERTIES =
    fst::kIDeterministic | fst::kODeterministic |
    fst::kILabelSorted | fst::kOLabelSorted |
    fst::kAcceptor | fst::kUnweighted;
  assert(EXPECTED_PROPERTIES == dfa.Properties(EXPECTED_PROPERTIES, true));
  UNUSED(EXPECTED_PROPERTIES);
#endif

  return dfa;
}

NS_END
