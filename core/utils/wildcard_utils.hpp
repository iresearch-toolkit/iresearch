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
#include "draw-impl.h"

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

  struct {
   automaton::StateId from;
   automaton::StateId to;
   automaton::StateId match_all_state{ fst::kNoStateId };
   Char match_all_label{};
   bool escaped{ false };
   bool match_all{ false };
  } state;

  state.from = a.AddState();
  state.to = state.from;
  a.SetStart(state.from);

  auto appendChar = [&state, &a](Char c) {
    state.to = a.AddState();
    if (!state.match_all) {
      a.EmplaceArc(state.from, c, state.to);
      state.match_all_state = fst::kNoStateId;
    } else {
      a.EmplaceArc(state.from, c, state.to);
      a.EmplaceArc(state.from, fst::fsa::kRho, state.from);

      state.match_all_state = state.from;
      state.match_all_label = c;
      state.match_all = false;
    }
    state.from = state.to;
    state.escaped = false;
  };

  const auto* begin = expr.begin();
  const auto* end = expr.end();

  for (; begin < end; ++begin) {
    const auto c = *begin;

    switch (c) {
      case Traits::MATCH_ANY_STRING: {
        if (state.escaped) {
          appendChar(c);
        } else {
          state.match_all = true;
        }
        break;
      }
      case Traits::MATCH_ANY_CHAR: {
        if (state.escaped) {
          appendChar(c);
        } else {
          state.to = a.AddState();
          a.EmplaceArc(state.from, fst::fsa::kRho, state.to);
          state.from = state.to;
        }
        break;
      }
      case Traits::ESCAPE: {
        if (state.escaped) {
          appendChar(c);
        } else {
          state.escaped = !state.escaped;
        }
        break;
      }
      default: {
        appendChar(c);
        break;
      }
    }
  }

  if (state.escaped) {
    // non-terminated escape sequence
    appendChar(Traits::ESCAPE);
  } if (state.match_all) {
    // terminal MATCH_ALL
    a.EmplaceArc(state.to, fst::fsa::kRho, state.to);
    state.match_all_state = fst::kNoStateId;
  }

  if (state.match_all_state != fst::kNoStateId) {
    // non-terminal MATCH_ALL
    a.EmplaceArc(state.to, state.match_all_label, state.to);
    a.EmplaceArc(state.to, fst::fsa::kRho, state.match_all_state);
  }

  a.SetFinal(state.to);

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

IRESEARCH_API automaton from_wildcard(const bytes_ref& expr);

inline automaton from_wildcard(const string_ref& expr) {
  return from_wildcard(ref_cast<byte_type>(expr));
}

NS_END

#endif // IRESEARCH_WILDCARD_UTILS_H

