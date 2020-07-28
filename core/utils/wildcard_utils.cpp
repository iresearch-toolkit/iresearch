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
  // label + state
  using arc_type = std::pair<bytes_ref, automaton::StateId>;
  // offset + state
  using state_type = std::pair<size_t, automaton::StateId>;

  struct {
    automaton::StateId from;
    automaton::StateId to;
    automaton::StateId match_all_from{ fst::kNoStateId };
    size_t offset{};
    bool escaped{ false };
    bool match_all{ false };
  } state;

  std::vector<state_type> match_any_sequence;
  std::vector<arc_type> match_all_sequence;
  arc_type arcs[3];

  utf8_transitions_builder builder;

  automaton a;
  state.from = a.AddState();
  state.to = state.from;
  a.SetStart(state.from);

  auto add_arcs = [&](automaton::StateId from, automaton::StateId rho_state,
                      const auto* begin, const auto* end ) {
    assert(begin <= end);

    // use optimized version of sort as we know
    // that array contains at most 3 elements
    switch (std::distance(begin, end)) {
      case 1:
        assert(begin == arcs);
        utf8_emplace_arc(a, from, rho_state, arcs[0].first, arcs[0].second);
        return;
      case 2:
        assert(begin == arcs);
        assert(begin + 1 == arcs + 1);
        if (arcs[1].first < arcs[0].first) std::swap(arcs[0], arcs[1]);
        break;
      case 3:
        assert(begin == arcs);
        assert(begin + 1 == arcs + 1);
        assert(begin + 2 == arcs + 2);
        if (arcs[1].first < arcs[0].first) std::swap(arcs[0], arcs[1]);
        if (arcs[2].first < arcs[0].first) std::swap(arcs[0], arcs[2]);
        if (arcs[2].first < arcs[1].first) std::swap(arcs[1], arcs[2]);
        break;
      default:
        assert(false);
        break;
    }

    builder.insert(a, from, rho_state, begin, end);
  };

  auto append_char = [&](const bytes_ref& c) {
    state.to = a.AddState();
    if (!state.match_all) {
      if (match_all_sequence.empty()) {
        utf8_emplace_arc(a, state.from, c, state.to);
      } else {
        match_all_sequence.emplace_back(c, state.to);

        for (auto& match_any_state : match_any_sequence) {
          arcs[0] = { c, state.to };

          auto* end = arcs + 1;

          assert(!match_all_sequence.empty());
          if (auto& arc = match_all_sequence.front(); c != arc.first) {
            arcs[1] = arc;
            ++end;
          }

          if (const auto offset = (match_any_state.first + 1)% match_all_sequence.size(); offset) {
            assert(offset < match_all_sequence.size());
            if (auto& arc = match_all_sequence[offset]; c != arc.first) {
              *end++ = arc;
            }
          }

          const auto rho_state = match_any_state.first > match_any_sequence.size()
            ? state.match_all_from
            : state.from;

          add_arcs(match_any_state.second, rho_state, arcs, end);
        }

        match_any_sequence.clear();

        arcs[0] = { c, state.to };
        auto* end = arcs + 1;

        assert(state.offset < match_all_sequence.size());
        if (auto& arc = match_all_sequence[state.offset]; c != arc.first) {
          arcs[1] = arc;
          ++end;
          state.offset = 0;
        } else {
          ++state.offset;
        }

        if (state.offset) {
          assert(!match_all_sequence.empty());
          if (auto& arc = match_all_sequence.front(); c != arc.first) {
            *end++ = arc;
          }
        }

        add_arcs(state.from, state.match_all_from, arcs, end);
      }
    } else {
      utf8_emplace_arc(a, state.from, state.from, c, state.to);
      match_any_sequence.clear();
      match_all_sequence.clear();
      match_all_sequence.emplace_back(c, state.to);

      state.match_all_from = state.from;
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
          append_char({label_begin, label_length});
        } else {
          state.match_all = true;
        }
        break;
      }
      case WildcardMatch::ANY_CHAR: {
        if (state.escaped) {
          append_char({label_begin, label_length});
        } else {
          state.to = a.AddState();

          if (!state.match_all && !match_all_sequence.empty()) {
            for (auto& match_any_state : match_any_sequence) {
              const auto to = a.AddState();
              ++match_any_state.first;

              assert(match_any_state.first < match_all_sequence.size());

              utf8_emplace_arc(
                a, match_any_state.second, state.to,
                match_all_sequence[match_any_state.first].first, to);
              match_any_state.second = to;
            }

            const auto to = a.AddState();
            match_any_sequence.emplace_back(0, to);
            utf8_emplace_arc(a, state.from, match_all_sequence.front().first, to);
          }

          utf8_emplace_rho_arc(a, state.from, state.to);
          state.from = state.to;
        }
        break;
      }
      case WildcardMatch::ESCAPE: {
        if (state.escaped) {
          append_char({label_begin, label_length});
        } else {
          state.escaped = !state.escaped;
        }
        break;
      }
      default: {
        append_char({label_begin, label_length});
        break;
      } 
    }

    {
      std::fstream out;
      out.open("/home/gnusi/1", std::fstream::out);
      fst::drawFst(a, out);
    }

    label_begin = label_end;
  }

  // need this variable to preserve valid address
  // for cases with match all and  terminal escape
  // character (%\\)
  const byte_type c = WildcardMatch::ESCAPE;

  if (state.escaped) {
    // non-terminated escape sequence
    append_char({&c, 1});
  } if (state.match_all) {
    // terminal MATCH_ALL
    utf8_emplace_rho_arc(a, state.to, state.to);
    state.match_all_from = fst::kNoStateId;
  }

  if (state.match_all_from != fst::kNoStateId) {
    // non-terminal MATCH_ALL
    assert(state.offset < match_all_sequence.size());
    arcs[0] = match_all_sequence[state.offset];
    auto* end = arcs + 1;

    if (match_all_sequence[0].first != arcs[0].first) {
      arcs[1] = match_all_sequence[0];
      ++end;
    }

   // auto* end = arcs;
   // if (state.offset && state.offset < match_all_sequence.size()) {
   //   arcs[0] = match_all_sequence[state.offset];
   //   ++end;
   // }


   // if (auto& arc = match_all_sequence[0]; arcs == end || arcs[0].first != arc.first) {
   //   *end++ = arc;
   // }

    add_arcs(state.to, state.match_all_from, arcs, end);
  }

  a.SetFinal(state.to);

#ifdef IRESEARCH_DEBUG
  // ensure resulting automaton is sorted and deterministic
  static constexpr auto EXPECTED_PROPERTIES =
    fst::kIDeterministic | fst::kODeterministic |
    fst::kILabelSorted | fst::kOLabelSorted |
    fst::kAcceptor;
  assert(EXPECTED_PROPERTIES == a.Properties(EXPECTED_PROPERTIES, true));
  UNUSED(EXPECTED_PROPERTIES);
#endif

  return a;
}

NS_END
