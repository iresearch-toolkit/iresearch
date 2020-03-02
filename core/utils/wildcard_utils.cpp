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

#include "automaton_utils.hpp"
#include "draw-impl.h"

NS_ROOT

automaton from_wildcard(const bytes_ref& expr) {
  struct {
   automaton::StateId from;
   automaton::StateId to;
   automaton::StateId match_all_state{ fst::kNoStateId };
   bytes_ref match_all_label{};
   bool escaped{ false };
   bool match_all{ false };
  } state;

  automaton a;
  state.from = a.AddState();
  state.to = state.from;
  a.SetStart(state.from);

  auto appendChar = [&a, &state](const bytes_ref& c) {
    state.to = a.AddState();
    if (!state.match_all) {
      utf8_emplace_arc(a, state.from, c, state.to);
      state.match_all_state = fst::kNoStateId;
    } else {
      utf8_emplace_arc(a, state.from, state.from, c, state.to);

      state.match_all_state = state.from;
      state.match_all_label = c;
      state.match_all = false;
    }
    state.from = state.to;
    state.escaped = false;
  };

  const auto* label_begin = expr.begin();
  const auto* end = expr.end();

  while (label_begin < end) {
    const auto label_length = utf8_utils::cp_length(*label_begin);
    const auto label_end = label_begin + label_length;

    if (!label_length || label_end > end) {
      // invalid UTF-8 sequence
      a.DeleteStates();
      return a;
    }

    switch (*label_begin) {
      case WildcardMatch::ANY_STRING: {
        if (state.escaped) {
          appendChar({label_begin, label_length});
        } else {
          state.match_all = true;
        }
        break;
      }
      case WildcardMatch::ANY_CHAR: {
        if (state.escaped) {
          appendChar({label_begin, label_length});
        } else {
          state.to = a.AddState();
          a.EmplaceArc(state.from, fst::fsa::kRho, state.to);
          state.from = state.to;
        }
        break;
      }
      case WildcardMatch::ESCAPE: {
        if (state.escaped) {
          appendChar({label_begin, label_length});
        } else {
          state.escaped = !state.escaped;
        }
        break;
      }
      default: {
        appendChar({label_begin, label_length});
        break;
      }
    }

    label_begin = label_end;
  }

  if (state.escaped) {
    // non-terminated escape sequence
    const byte_type c = WildcardMatch::ESCAPE;
    appendChar({&c, 1});
  } if (state.match_all) {
    // terminal MATCH_ALL
    utf8_emplace_rho_arc(a, state.to, state.to);
    state.match_all_state = fst::kNoStateId;
  }

  if (state.match_all_state != fst::kNoStateId) {
    // non-terminal MATCH_ALL
    utf8_emplace_arc(a, state.to, state.match_all_state, state.match_all_label, state.to);
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

NS_END
