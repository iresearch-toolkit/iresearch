////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_FST_STATES_MAP_H
#define IRESEARCH_FST_STATES_MAP_H

#include "ebo.hpp"
#include "noncopyable.hpp"

NS_ROOT

template<typename Fst,
         typename State,
         typename Hash,
         typename StateEq,
         typename Fst::StateId NoStateId>
class fst_states_map : private compact<0, Hash>,
                       private compact<1, StateEq>,
                       private util::noncopyable {
 public:
  using fst_type = Fst;
  using state_type = State;
  using state_id = typename fst_type::StateId;
  using hasher = Hash;
  using state_equal = StateEq;

  explicit fst_states_map(
      size_t capacity = 16,
      const hasher& hash_function = {},
      const state_equal& state_eq = {})
    : compact<0, hasher>{ hash_function },
      compact<1, state_equal>{ state_eq },
      states_(capacity, NoStateId) {
  }

  state_id insert(const state_type& s, fst_type& fst) {
    const auto state_equal = state_eq();
    const auto hasher = hash_function();

    const size_t mask = states_.size() - 1;
    for (size_t pos = hasher(s, fst) % mask;;++pos, pos %= mask) {
      auto& bucket = states_[pos];

      if (NoStateId == bucket) {
        const state_id id = bucket = add_state(s, fst);
        assert(hasher(s, fst) == hasher(id, fst));
        ++count_;

        if (count_ > 2 * states_.size() / 3) {
          rehash(fst);
        }

        return id;
      }

      if (state_equal(s, bucket, fst)) {
        return bucket;
      }
    }
  }

  void reset() noexcept {
    count_ = 0;
    std::fill(states_.begin(), states_.end(), NoStateId);
  }

  hasher hash_function() const noexcept {
    return compact<0, hasher>::get();
  }

  state_equal state_eq() const noexcept {
    return compact<1, state_equal>::get();
  }

 private:
  void rehash(const fst_type& fst) {
    const auto hasher = hash_function();

    std::vector<state_id> states(states_.size() * 2, NoStateId);
    const size_t mask = states.size() - 1;
    for (auto id : states_) {
      if (NoStateId == id) {
        continue;
      }

      size_t pos = hasher(id, fst) % mask;
      for (;;++pos, pos %= mask) {
        if (NoStateId == states[pos] ) {
          states[pos] = id;
          break;
        }
      }
    }

    states_ = std::move(states);
  }

  state_id add_state(const state_type& s, fst_type& fst) {
    state_id id = s.id;

    if (id == NoStateId) {
      id = fst.AddState();
    }

    for (const auto& a : s.arcs) {
      fst.EmplaceArc(id, a.label, a.id);
    }

    if (s.rho_id != NoStateId) {
      fst.EmplaceArc(id, fst::fsa::kRho, s.rho_id);
    }

    return id;
  }

  std::vector<state_id> states_;
  size_t count_{};
}; // fst_states_map

NS_END

#endif
