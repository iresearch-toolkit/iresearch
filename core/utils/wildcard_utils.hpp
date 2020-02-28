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

#ifndef IRESEARCH_WILDCARD_UTILS_H
#define IRESEARCH_WILDCARD_UTILS_H

#include "automaton.hpp"

NS_ROOT

template<typename Char>
struct wildcard_traits {
  using char_type = Char;

  // match any string or empty string
  static constexpr Char MATCH_ANY_STRING= Char('%');

  // match any char
  static constexpr Char MATCH_ANY_CHAR = Char('_');

  // denotes beginning of escape sequence
  static constexpr Char ESCAPE = Char('\\');
};

template<
  typename Char,
  typename Traits = wildcard_traits<Char>,
  // brackets over condition are for circumventing MSVC parser bug
  typename = typename std::enable_if<(sizeof(Char) < sizeof(fst::fsa::kMaxLabel))>::type, 
  typename = typename std::enable_if<Traits::MATCH_ANY_CHAR != Traits::MATCH_ANY_STRING>::type>
automaton from_wildcard(const irs::basic_string_ref<Char>& expr) {
  automaton a;

  automaton::StateId from = a.AddState();
  automaton::StateId to = from;
  a.SetStart(from);

  automaton::StateId match_all_state = fst::kNoStateId;
  bool escaped = false;
  auto appendChar = [&expr, &match_all_state, &escaped, &a, &to, &from](Char c, bool is_last) {
    to = a.AddState();
    a.EmplaceArc(from, c, to);
    from = to;
    escaped = false;
    if (match_all_state != fst::kNoStateId) {
      a.EmplaceArc(to, c, to);
      if (is_last) {
        a.EmplaceArc(to, fst::fsa::kRho, match_all_state);
      }
      a.EmplaceArc(match_all_state, fst::fsa::kRho, match_all_state);
      match_all_state = fst::kNoStateId;
    }
  };

  const auto* begin = expr.begin();
  const auto* end = expr.end();
  const auto* back = expr.end() - 1;

  for (; begin < end; ++begin) {
    const auto c = *begin;
    const auto is_last = (begin == back);

    switch (c) {
      case Traits::MATCH_ANY_STRING: {
        if (escaped) {
          appendChar(c, is_last);
        } else {
          match_all_state = from;
        }
        break;
      }
      case Traits::MATCH_ANY_CHAR: {
        if (escaped) {
          appendChar(c, is_last);
        } else {
          to = a.AddState();
          a.EmplaceArc(from, fst::fsa::kRho, to);
          from = to;

          if (match_all_state != fst::kNoStateId) {
            match_all_state = to;
          }
        }
        break;
      }
      case Traits::ESCAPE: {
        if (escaped) {
          appendChar(c, is_last);
        } else {
          escaped = !escaped;
        }
        break;
      }
      default: {
        appendChar(c, is_last);
        break;
      }
    }
  }

  // non-terminated escape sequence
  if (escaped) {
    appendChar(Traits::ESCAPE, true);
  }

  if (match_all_state != fst::kNoLabel) {
    a.DeleteArcs(to);
    a.EmplaceArc(to, fst::fsa::kRho, to);
  }

  a.SetFinal(to);

#ifdef IRESEARCH_DEBUG
  // ensure resulting automaton is sorted and deterministic
  static constexpr auto EXPECTED_PROPERTIES =
    fst::kIDeterministic | fst::kODeterministic |
    fst::kILabelSorted | fst::kOLabelSorted |
    fst::kAcceptor;
  assert(EXPECTED_PROPERTIES == a.Properties(EXPECTED_PROPERTIES, true));
#endif

  return a;
}

NS_END

#endif // IRESEARCH_WILDCARD_UTILS_H

