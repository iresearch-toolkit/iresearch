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

#include "shared.hpp"
#include "utils/bit_utils.hpp"

#include <map>

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
    state.emplace_back(pos.offset + 1, pos.distance, false);

    if (pos.transpose) {
      state.emplace_back(pos.offset + 2, pos.distance, false);
    }
  }

  if (pos.distance < args.max_distance) {
    // Situation 2, 3 [i,e+1] - X is inserted before X[i+1]
    state.emplace_back(pos.offset, pos.distance + 1, false);

    // Situation 2, 3 [i+1,e+1] - X[i+1] is substituted by X
    state.emplace_back(pos.offset + 1, pos.distance + 1, false);

    // Situation 2, [i+j,e+j-1] - elements X[i+1:i+j-1] are deleted
    for (size_t j = 1, max = args.max_distance + 1 - pos.distance; j < max; ++j) {
      if (irs::check_bit(chi, j)) {
        state.emplace_back(pos.offset + 1 + j, pos.distance + j, false); // FIXME why pos+1+j, but not pos+j???
      }
    }

    if (args.with_transpositions && irs::check_bit<1>(chi)) {
      state.emplace_back(pos.offset, pos.distance + 1, true);
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
}

inline uint64_t get_chi_size(uint64_t max_distance) noexcept {
  return 2*max_distance + 1;
}

NS_END

NS_ROOT

void parametric_dfa(byte_type max_distance, bool with_transposition) {
  const parametric_dfa_args args{ max_distance, with_transposition };
  const size_t chi_size = get_chi_size(max_distance);

  std::vector<parametric_state> st;
  st.emplace_back();
  st.back().emplace_back(0,0,false);

  parametric_state to;
  for (uint64_t chi = 0, chi_max = UINT64_C(1) << chi_size; chi < chi_max; ++chi) {
    add_transition(args, st.front(), to, chi);
    st.emplace_back(std::move(to));
  }

  int i = 5;
}

NS_END
