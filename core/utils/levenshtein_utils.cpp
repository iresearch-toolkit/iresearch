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
#include "bit_utils.hpp"
#include "bitset.hpp"
#include "map_utils.hpp"
#include "hash_utils.hpp"
#include "utf8_utils.hpp"
#include "draw-impl.h"

NS_LOCAL

using namespace irs;

struct parametric_description_args {
  irs::byte_type max_distance;
  bool with_transpositions;
};

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

  uint32_t offset{};
  byte_type distance{};
  bool transpose{false};
}; // position

FORCE_INLINE uint32_t abs_diff(uint32_t lhs, uint32_t rhs) noexcept {
  return lhs < rhs ? rhs - lhs : lhs - rhs;
}

/// @returns true if position 'lhs' subsumes 'rhs',
///          i.e. |rhs.offset-lhs.offset| < rhs.distance - lhs.distance
/*FORCE_INLINE*/ bool subsumes(const position& lhs, const position& rhs) noexcept {
  return lhs.transpose | !rhs.transpose
      ? abs_diff(lhs.offset, rhs.offset) + lhs.distance <= rhs.distance
      : abs_diff(lhs.offset, rhs.offset) + lhs.distance <  rhs.distance;
}

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
};

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

void add_elementary_transitions(const parametric_description_args& args,
                                const position& pos,
                                uint64_t chi,
                                parametric_state& state) {
  if (irs::check_bit<0>(chi)) {
    // Situation 1: [i+1,e] subsumes { [i,e+1], [i+1,e+1], [i+1,e] }
    state.emplace(pos.offset + 1, pos.distance, false);

    if (pos.transpose) {
      state.emplace(pos.offset + 2, pos.distance, false);
    }
  }

  if (pos.distance < args.max_distance) {
    // Situation 2, 3 [i,e+1] - X is inserted before X[i+1]
    state.emplace(pos.offset, pos.distance + 1, false);

    // Situation 2, 3 [i+1,e+1] - X[i+1] is substituted by X
    state.emplace(pos.offset + 1, pos.distance + 1, false);

    // Situation 2, [i+j,e+j-1] - elements X[i+1:i+j-1] are deleted
    for (size_t j = 1, max = args.max_distance + 1 - pos.distance; j < max; ++j) {
      if (irs::check_bit(chi, j)) {
        state.emplace(pos.offset + 1 + j, pos.distance + j, false);
      }
    }

    if (args.with_transpositions && irs::check_bit<1>(chi)) {
      state.emplace(pos.offset, pos.distance + 1, true);
    }
  }
}

void add_transition(const parametric_description_args& args,
                    const parametric_state& from,
                    parametric_state& to,
                    uint64_t cv) {
  to.clear();
  for (const auto& pos : from) {
    assert(pos.offset < irs::bits_required<decltype(cv)>());
    const auto chi = cv >> pos.offset;
    add_elementary_transitions(args, pos, chi, to);
  }

  std::sort(to.begin(), to.end());
}

inline uint64_t get_chi_size(uint64_t max_distance) noexcept {
  return 2*max_distance + 1;
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

uint32_t distance(
    const parametric_description_args& args,
    const parametric_state& state,
    uint32_t offset) {
  uint32_t min_dist = args.max_distance + 1;

  for (auto& pos : state) {
    const uint32_t dist = pos.distance + abs_diff(offset, pos.offset);

    if (dist < min_dist) {
      min_dist = dist;
    }
  }

  return min_dist;
}

std::vector<std::pair<uint32_t, irs::bitset>> make_alphabet(const bytes_ref& word, size_t& utf8_size) {
  std::basic_string<uint32_t> chars;
  utf8_utils::to_utf8(word, std::back_inserter(chars));
  utf8_size = chars.size();

  std::sort(chars.begin(), chars.end());
  chars.erase(std::unique(chars.begin(), chars.end()), chars.end());

  std::vector<std::pair<uint32_t, irs::bitset>> alphabet(chars.size());
  auto begin = alphabet.begin();

  for (uint32_t c : chars) {
    // set char
    begin->first = c;

    // evaluate characteristic vector
    auto& bits = begin->second;
    bits.reset(utf8_size);
    auto utf8_begin = word.begin();
    for (size_t i = 0; i < utf8_size; ++i) {
      bits.reset(i, c == utf8_utils::next(utf8_begin));
    }
    IRS_ASSERT(utf8_begin == word.end());

    ++begin;
  }

  return alphabet;
}

NS_END

NS_ROOT

parametric_description make_parametric_description(byte_type max_distance, bool with_transposition) {
  // predict number of states for known cases
  size_t num_states = 0;
  switch (max_distance) {
    case 1: num_states = 5;    break;
    case 2: num_states = 30;   break;
    case 3: num_states = 192;  break;
    case 4: num_states = 1352; break;
  }

  const parametric_description_args args{ max_distance, with_transposition };
  const uint64_t chi_size = get_chi_size(max_distance);
  const uint64_t chi_max = UINT64_C(1) << chi_size;

  parametric_states states(2 + num_states); // +1 for initial state, +1 for empty state
  parametric_description::parametric_transitions_t transitions;
  if (states.size()) {
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
      add_transition(args, states[from_id], to, chi);

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
      distance[idx] = byte_type(::distance(args, state, offset)); // FIXME cast
    }
  }

  return { std::move(transitions), std::move(distance),
           chi_size, chi_max, max_distance };
}

uint64_t get_chi(const bitset& bs, size_t offset, size_t size) {
  // FIXME optimize with mask (2n+1)
  uint64_t value = 0;
  size_t i = 0;
  for (const size_t end = std::min(bs.size(), offset + size); offset < end; ++offset, ++i) {
    value |= (uint64_t(bs.test(offset)) << i);
  }
  return value;
}

void print(const automaton& a) {
  fst::SymbolTable st;
  for (int i = 97; i < 97 + 28; ++i) {
  st.AddSymbol(std::string(1, char(i)), i);
  }

  std::fstream f;
  f.open("111", std::fstream::binary | std::fstream::out);
  fst::drawFst(a, f, "", &st, &st);
}

const parametric_transition& transition(const parametric_description& description,
                                        const bitset& cv,
                                        const size_t offset,
                                        const size_t state) noexcept {
  const auto chi = get_chi(cv, offset, description.chi_size);
  return description.transitions[state*description.chi_max + chi];
}

automaton make_levenshtein_automaton(
    const parametric_description& description,
    const bytes_ref& target) {
  size_t utf8_size;
  const auto alphabet = make_alphabet(target, utf8_size);
  const auto num_states = 1 + description.transitions.size() / description.chi_max; // FIXME +1???

  struct state_info {
    state_info(size_t offset, size_t state, automaton::StateId from)
      : offset(offset), state(state), from(from) {
    }

    size_t offset;
    size_t state;
    automaton::StateId from;
  };

  std::vector<automaton::StateId> automaton_states(num_states*(utf8_size+1), -1);

  std::deque<state_info> queue;
  queue.emplace_back(0, 1, 0);

  automaton a;
  a.ReserveStates(num_states * utf8_size);
  a.SetStart(a.AddState()); // set initial state

  while (!queue.empty()) {
    auto& state = queue.front();

    for (auto& entry : alphabet) {
      auto& transition = ::transition(description, entry.second, state.offset, state.state);

      auto offset = !transition.to ? 0 : transition.offset + state.offset;

      auto& to_id = automaton_states[transition.to*(utf8_size+1) + offset];
      if (-1 == to_id) {
        to_id = a.AddState();

        if (transition.to) {
          queue.emplace_back(offset, transition.to, to_id);
        }
      }

      a.EmplaceArc(state.from, entry.first, to_id);
      print(a);
    }

    queue.pop_front();
  }

  return a;
}

NS_END
