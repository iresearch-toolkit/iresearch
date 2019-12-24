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
#include "bit_utils.hpp"
#include "hash_utils.hpp"

NS_LOCAL

using namespace irs;

struct parametric_dfa_args {
  irs::byte_type max_distance;
  bool with_transpositions;
};

void add_elementary_transitions(const parametric_dfa_args& args,
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
        state.emplace(pos.offset + 1 + j, pos.distance + j, false); // FIXME why pos+1+j, but not pos+j???
      }
    }

    if (args.with_transpositions && irs::check_bit<1>(chi)) {
      state.emplace(pos.offset, pos.distance + 1, true);
    }
  }
}

void add_transition(const parametric_dfa_args& args,
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

size_t hash_value(const position& pos) noexcept {
  size_t seed = hash_combine(0, pos.offset);
  seed = hash_combine(seed, pos.distance);
  seed = hash_combine(seed, pos.transpose);
  return seed;
}

size_t hash_value(const parametric_state& state) noexcept {
  size_t seed = 0;
  for (auto& pos: state) {
    seed = irs::hash_combine(seed, hash_value(pos));
  }
  return seed;
}

/// @returns true if position 'lhs' subsumes 'rhs'
bool subsumes(const position& lhs, const position& rhs) noexcept {
  const auto abs_delta_offset = std::abs(ptrdiff_t(lhs.offset - rhs.offset));
  const auto delta_distance = rhs.distance - lhs.distance;

  return lhs.transpose | !rhs.transpose
      ? abs_delta_offset <= delta_distance
      : abs_delta_offset <  delta_distance;
}

NS_END

NS_BEGIN(std)

template<>
struct hash<irs::position> {
  size_t operator()(const irs::position& v) const noexcept {
    return hash_value(v);
  }
};

template<>
struct hash<irs::parametric_state> {
  size_t operator()(const irs::parametric_state& v) const noexcept {
    return hash_value(v);
  }
};

NS_END

NS_ROOT

bool parametric_state::emplace(const position& new_pos) {
  for (auto& pos : positions_) {
    if (subsumes(pos, new_pos)) {
      // nothing to do
      return false;
    }
  }

  for (size_t i = 0; i < positions_.size(); ) {
    if (subsumes(new_pos, positions_[i])) {
      std::swap(positions_[i], positions_.back());
      positions_.pop_back(); // removed positions subsumed by new_pos
    } else {
      ++i;
    }
  }

  positions_.emplace_back(new_pos);
  return true;
}

struct trans {
  trans(size_t from, size_t to, size_t chi)
    : from(from), to(to), chi(chi) {
  }

  bool operator<(const trans& rhs) const noexcept {
    if (from == rhs.from) {
      return to < to;
    }
    return from <rhs.from;
  }

  size_t from;
  size_t to;
  size_t chi;
};

void parametric_dfa(byte_type max_distance, bool with_transposition) {
  const parametric_dfa_args args{ max_distance, with_transposition };
  const size_t chi_size = get_chi_size(max_distance);

  std::unordered_map<parametric_state, size_t> states;
  std::vector<const parametric_state*> states_by_id;

//  std::set<trans> transitions;
  std::vector<trans> transitions;

  {
    parametric_state state;
    state.emplace(0,0,false);
    states.emplace(std::move(state), states.size());
    states_by_id.emplace_back(&(states.begin()->first));
  }

  parametric_state to;

  for (size_t from_id = 0; from_id != states.size(); ++from_id) {
    for (uint64_t chi = 0, chi_max = UINT64_C(1) << chi_size; chi < chi_max; ++chi) {
      add_transition(args, *states_by_id[from_id], to, chi);

      auto it = states.find(to);
      if (it == states.end()) {
        it = states.emplace(std::move(to), states_by_id.size()).first;
        states_by_id.emplace_back(&(it->first));
      }

      transitions.emplace_back(from_id, it->second, chi);
    }
  }

  int i = 5;
}

NS_END
