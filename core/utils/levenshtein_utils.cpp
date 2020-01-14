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

#include "levenshtein_utils.hpp"

#include <unordered_map>
#include <unordered_set>
#include <set>
#include <cmath>

#include "shared.hpp"
#include "automaton.hpp"
#include "arena_allocator.hpp"
#include "bit_utils.hpp"
#include "bitset.hpp"
#include "map_utils.hpp"
#include "hash_utils.hpp"
#include "utf8_utils.hpp"
#include "draw-impl.h"

NS_LOCAL

using namespace irs;

// -----------------------------------------------------------------------------
// --SECTION--                    Helpers for parametric description computation
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief invalid parametric state
////////////////////////////////////////////////////////////////////////////////
constexpr size_t INVALID_STATE = 0;

////////////////////////////////////////////////////////////////////////////////
/// @struct position
/// @brief describes parametric transition related to a certain
///        parametric state
////////////////////////////////////////////////////////////////////////////////
struct position {
  explicit position(
      uint32_t offset = 0,
      byte_type distance = 0,
      bool transpose = false) noexcept
    : offset(offset),
      distance(distance),
      transpose(transpose) {
  }

  bool operator<(const position& rhs) const noexcept {
    if (offset == rhs.offset) {
      if (distance == rhs.distance) {
        return transpose < rhs.transpose;
      }

      return distance < rhs.distance;
    }

    return offset < rhs.offset;
  }

  bool operator==(const position& rhs) const noexcept {
    return offset == rhs.offset &&
        distance == rhs.distance &&
        transpose == rhs.transpose;
  }

  uint32_t offset{};     // parametric position offset
  byte_type distance{};  // parametric position distance
  bool transpose{false}; // position is introduced by transposition
};

FORCE_INLINE uint32_t abs_diff(uint32_t lhs, uint32_t rhs) noexcept {
  return lhs < rhs ? rhs - lhs : lhs - rhs;
}

////////////////////////////////////////////////////////////////////////////////
/// @returns true if position 'lhs' subsumes 'rhs',
///          i.e. |rhs.offset-lhs.offset| < rhs.distance - lhs.distance
////////////////////////////////////////////////////////////////////////////////
FORCE_INLINE bool subsumes(const position& lhs, const position& rhs) noexcept {
  return lhs.transpose | !rhs.transpose
      ? abs_diff(lhs.offset, rhs.offset) + lhs.distance <= rhs.distance
      : abs_diff(lhs.offset, rhs.offset) + lhs.distance <  rhs.distance;
}

////////////////////////////////////////////////////////////////////////////////
/// @class parametric_state
/// @brief describes parametric state of levenshtein automaton, basically a
///        set of positions.
////////////////////////////////////////////////////////////////////////////////
class parametric_state {
 public:
  parametric_state() = default;
  parametric_state(parametric_state&& rhs) = default;

  bool emplace(uint32_t offset, byte_type distance, bool transpose) {
    return emplace(position(offset, distance, transpose));
  }

  bool emplace(const position& new_pos) {
    for (auto& pos : positions_) {
      if (subsumes(pos, new_pos)) {
        // nothing to do
        return false;
      }
    }

    for (auto begin = positions_.begin(); begin != positions_.end(); ) {
      if (subsumes(new_pos, *begin)) {
        std::swap(*begin, positions_.back());
        positions_.pop_back(); // removed positions subsumed by new_pos
      } else {
        ++begin;
      }
    }

    positions_.emplace_back(new_pos);
    return true;
  }

  std::vector<position>::iterator begin() noexcept {
    return positions_.begin();
  }

  std::vector<position>::iterator end() noexcept {
    return positions_.end();
  }

  std::vector<position>::const_iterator begin() const noexcept {
    return positions_.begin();
  }

  std::vector<position>::const_iterator end() const noexcept {
    return positions_.end();
  }

  bool empty() const noexcept { return positions_.empty(); }
  void clear() noexcept { return positions_.clear(); }

  bool operator==(const parametric_state& rhs) const noexcept {
    return positions_ == rhs.positions_;
  }
  bool operator!=(const parametric_state& rhs) const noexcept {
    return !(*this == rhs);
  }

 private:
  parametric_state(const parametric_state& rhs) = delete;
  parametric_state& operator=(parametric_state&&) = delete;
  parametric_state& operator=(const parametric_state&) = delete;

  std::vector<position> positions_;
}; // parametric_state

////////////////////////////////////////////////////////////////////////////////
/// @class parametric_states
/// @brief container ensures uniquiness of 'parametric_state's
////////////////////////////////////////////////////////////////////////////////
class parametric_states {
 public:
  explicit parametric_states(size_t capacity = 0) {
    if (capacity) {
      states_.reserve(capacity);
      states_by_id_.reserve(capacity);
    }
  }

  size_t emplace(parametric_state&& state) {
    const auto res = irs::map_utils::try_emplace(
      states_, std::move(state), states_.size());

    if (res.second) {
      states_by_id_.emplace_back(&res.first->first);
    }

    assert(states_.size() == states_by_id_.size());

    return res.first->second;
  }

  const parametric_state& operator[](size_t i) const noexcept {
    assert(i < states_by_id_.size());
    return *states_by_id_[i];
  }

  size_t size() const noexcept {
    return states_.size();
  }

 private:
  struct parametric_state_hash {
    bool operator()(const parametric_state& state) const noexcept {
      size_t seed = 0;
      for (auto& pos: state) {
        seed = irs::hash_combine(seed, pos.offset);
        seed = irs::hash_combine(seed, pos.distance);
        seed = irs::hash_combine(seed, pos.transpose);
      }
      return seed;
    }
  };

  std::unordered_map<parametric_state, size_t, parametric_state_hash> states_;
  std::vector<const parametric_state*> states_by_id_;
}; // parametric_states

void add_elementary_transitions(
    parametric_state& state,
    const position& pos,
    const uint64_t chi,
    const byte_type max_distance,
    const bool with_transpositions) {
  if (irs::check_bit<0>(chi)) {
    // Situation 1: [i+1,e] subsumes { [i,e+1], [i+1,e+1], [i+1,e] }
    state.emplace(pos.offset + 1, pos.distance, false);

    if (pos.transpose) {
      state.emplace(pos.offset + 2, pos.distance, false);
    }
  }

  if (pos.distance < max_distance) {
    // Situation 2, 3 [i,e+1] - X is inserted before X[i+1]
    state.emplace(pos.offset, pos.distance + 1, false);

    // Situation 2, 3 [i+1,e+1] - X[i+1] is substituted by X
    state.emplace(pos.offset + 1, pos.distance + 1, false);

    // Situation 2, [i+j,e+j-1] - elements X[i+1:i+j-1] are deleted
    for (size_t j = 1, max = max_distance + 1 - pos.distance; j < max; ++j) {
      if (irs::check_bit(chi, j)) {
        state.emplace(pos.offset + 1 + j, pos.distance + j, false);
      }
    }

    if (with_transpositions && irs::check_bit<1>(chi)) {
      state.emplace(pos.offset, pos.distance + 1, true);
    }
  }
}

void add_transition(
    parametric_state& to,
    const parametric_state& from,
    const uint64_t cv,
    const byte_type max_ditance,
    const bool with_transpositions) {
  to.clear();
  for (const auto& pos : from) {
    assert(pos.offset < irs::bits_required<decltype(cv)>());
    const auto chi = cv >> pos.offset;
    add_elementary_transitions(to, pos, chi, max_ditance, with_transpositions);
  }

  std::sort(to.begin(), to.end());
}

FORCE_INLINE uint64_t chi_size(uint64_t max_distance) noexcept {
  return 2*max_distance + 1;
}

FORCE_INLINE uint64_t chi_max(uint64_t chi_size) noexcept {
  return UINT64_C(1) << chi_size;
}

uint32_t normalize(parametric_state& state) noexcept {
  const auto it = std::min_element(
    state.begin(), state.end(),
    [](const position& lhs, const position& rhs) noexcept {
      return lhs.offset < rhs.offset;
  });

  const auto min_offset = (it == state.end() ? 0 : it->offset);

  for (auto& pos : state) {
    pos.offset -= min_offset;
  }

  if (!std::is_sorted(state.begin(), state.end())) {
    std::sort(state.begin(), state.end());
  }

  return min_offset;
}

uint32_t distance(size_t max_distance,
                  const parametric_state& state,
                  uint32_t offset) noexcept {
  uint32_t min_dist = max_distance + 1;

  for (auto& pos : state) {
    const uint32_t dist = pos.distance + abs_diff(offset, pos.offset);

    if (dist < min_dist) {
      min_dist = dist;
    }
  }

  return min_dist;
}

// -----------------------------------------------------------------------------
// --SECTION--                                     Helpers for DFA instantiation
// -----------------------------------------------------------------------------

std::vector<std::pair<uint32_t, irs::bitset>> make_alphabet(const bytes_ref& word,
                                                            size_t& utf8_size) {
  memory::arena<uint32_t, 16> arena;
  memory::arena_vector<uint32_t, decltype(arena)> chars(arena);
  utf8_utils::to_utf8(word, std::back_inserter(chars));
  utf8_size = chars.size();

  std::sort(chars.begin(), chars.end());
  auto cbegin = chars.begin();
  auto cend = std::unique(cbegin, chars.end()); // no need to erase here

  std::vector<std::pair<uint32_t, irs::bitset>> alphabet(1 + size_t(std::distance(cbegin, cend))); // +1 for rho
  auto begin = alphabet.begin();

  // ensure we have enough capacity
  const auto capacity = utf8_size + bits_required<bitset::word_t>();

  begin->first = fst::fsa::kRho;
  begin->second.reset(capacity);
  ++begin;

  for (; cbegin != cend; ++cbegin, ++begin) {
    const auto c = *cbegin;

    // set char
    begin->first = c;

    // evaluate characteristic vector
    auto& bits = begin->second;
    bits.reset(capacity);
    auto utf8_begin = word.begin();
    for (size_t i = 0; i < utf8_size; ++i) {
      bits.reset(i, c == utf8_utils::next(utf8_begin));
    }
    IRS_ASSERT(utf8_begin == word.end());
  }

  return alphabet;
}

template<typename Iterator>
uint64_t chi(Iterator begin, Iterator end, uint32_t c) noexcept {
  uint64_t chi = 0;
  for (size_t i = 0; begin < end; ++begin, ++i) {
    chi |= uint64_t(c == *begin) << i;
  }
  return chi;
}

uint64_t chi(const bitset& bs, size_t offset, uint64_t mask) noexcept {
  auto word = bitset::word(offset);

  auto align = offset - bitset::bit_offset(word);
  if (!align) {
    return bs[word] & mask;
  }

  const auto lhs = bs[word] >> align;
  const auto rhs = bs[word+1] << (bits_required<bitset::word_t>() - align);
  return (lhs | rhs) & mask;
}

void print(const automaton& a) {
  fst::SymbolTable st;
  st.AddSymbol(std::string(1, '*'), fst::fsa::kRho);
  for (int i = 97; i < 97 + 28; ++i) {
    st.AddSymbol(std::string(1, char(i)), i);
  }

  std::fstream f;
  f.open("111", std::fstream::binary | std::fstream::out);
  fst::drawFst(a, f, "", &st, &st);
}

NS_END

NS_ROOT

parametric_description make_parametric_description(
    byte_type max_distance,
    bool with_transposition) {
  if (max_distance > parametric_description::MAX_DISTANCE) {
    // invalid parametric description
    return {};
  }

  // predict number of states for known cases
  size_t num_states = 0;
  switch (max_distance) {
    case 1: num_states = 6;    break;
    case 2: num_states = 31;   break;
    case 3: num_states = 197;  break;
    case 4: num_states = 1354; break;
  }

  const uint64_t chi_size = ::chi_size(max_distance);
  const uint64_t chi_max = ::chi_max(chi_size);

  parametric_states states(num_states);
  std::vector<parametric_description::transition_t> transitions;
  if (num_states) {
    transitions.reserve(states.size() * chi_max);
  }

  // empty state
  parametric_state to;
  size_t from_id = states.emplace(std::move(to));
  assert(to.empty());

  // initial state
  to.emplace(UINT32_C(0), UINT8_C(0), false);
  states.emplace(std::move(to));
  assert(to.empty());

  for (; from_id != states.size(); ++from_id) {
    for (uint64_t chi = 0; chi < chi_max; ++chi) {
      add_transition(to, states[from_id], chi, max_distance, with_transposition);

      const auto min_offset = normalize(to);
      const auto to_id = states.emplace(std::move(to));

      transitions.emplace_back(to_id, min_offset);
    }
  }

  std::vector<byte_type> distance(states.size() * chi_size);
  for (size_t i = 0, size = states.size(); i < size; ++i) {
    auto& state = states[i];
    for (uint64_t offset = 0; offset < chi_size; ++offset) {
      const auto idx = i*chi_size + offset;
      assert(idx < distance.size());
      distance[idx] = byte_type(::distance(max_distance, state, offset)); // FIXME cast
    }
  }

  return {
    std::move(transitions),
    std::move(distance),
    states.size(),
    chi_size,
    chi_max,
    max_distance
  };
}

automaton make_levenshtein_automaton(
    const parametric_description& description,
    const bytes_ref& target) {
  assert(description);

  struct state {
    state(size_t offset, size_t state, automaton::StateId from)
      : offset(offset), id(state), from(from) {
    }

    size_t offset;
    size_t id;
    automaton::StateId from;
  };

  size_t utf8_size;
  const auto alphabet = make_alphabet(target, utf8_size);
  const auto num_offsets = 1 + utf8_size;
  const uint64_t mask = (UINT64_C(1) << description.chi_size()) - 1;

  // transitions table of resulting automaton
  std::vector<automaton::StateId> transitions(description.size()*num_offsets,
                                              fst::kNoStateId);

  automaton a;
  a.ReserveStates(transitions.size());
  const auto invalid_state = a.AddState(); // state without outbound transitions
  a.SetStart(a.AddState());                // initial state

  std::deque<state> queue;             // state queue
  queue.emplace_back(0, 1, a.Start()); // 0 offset, 1st parametric state, initial automaton state

  while (!queue.empty()) {
    auto& state = queue.front();
    automaton::StateId default_state = fst::kNoStateId; // destination of rho transition if exist

    for (auto& entry : alphabet) {
      const auto chi = ::chi(entry.second, state.offset, mask);
      auto& transition = description.transition(state.id, chi);

      auto offset = transition.first ? transition.second + state.offset : 0;
      assert(transition.first*num_offsets + offset < transitions.size());
      auto& to_id = transitions[transition.first*num_offsets + offset];

      if (INVALID_STATE == transition.first) {
        to_id = invalid_state;
      } else if (fst::kNoStateId == to_id) {
        to_id = a.AddState();

        if (description.distance(transition.first, utf8_size - offset) <= description.max_distance()) {
          a.SetFinal(to_id);
        }

        queue.emplace_back(offset, transition.first, to_id);
      }

      if (chi && to_id != default_state) {
        a.EmplaceArc(state.from, entry.first, to_id);
      } else if (fst::kNoStateId == default_state) {
        default_state = to_id;
      }
    }

    if (fst::kNoStateId != default_state) {
      a.EmplaceArc(state.from, fst::fsa::kRho, default_state);
    }

    queue.pop_front();
  }

  // ensure resulting automaton is sorted and deterministic
  constexpr auto EXPECTED_PROPERTIES =
    fst::kIDeterministic | fst::kODeterministic |
    fst::kILabelSorted | fst::kOLabelSorted;
  assert(a.Properties(EXPECTED_PROPERTIES, true));

  // ensure invalid state has no outbound transitions
  assert(0 == a.NumArcs(invalid_state));

  return a;
}

size_t edit_distance(const parametric_description& description,
                     const byte_type* lhs, size_t lhs_size,
                     const byte_type* rhs, size_t rhs_size) {
  assert(description);

  memory::arena<uint32_t, 16> arena;
  memory::arena_vector<uint32_t, decltype(arena)> lhs_chars(arena);
  utf8_utils::to_utf8(lhs, lhs_size, std::back_inserter(lhs_chars));

  size_t state = 1; // current paramteric state
  size_t offset = 0; // current offset

  for (auto* rhs_end = rhs + rhs_size; rhs < rhs_end; ) {
    const auto c = utf8_utils::next(rhs);

    const auto begin = lhs_chars.begin() + offset;
    const auto end = std::min(begin + description.chi_size(), lhs_chars.end());
    const auto chi = ::chi(begin, end, c);
    const auto& transition = description.transition(state, chi);

    if (INVALID_STATE == transition.first) {
      return description.max_distance() + 1;
    }

    state = transition.first;
    offset += transition.second;
  }

  return description.distance(state, lhs_chars.size() - offset);
}

NS_END
