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

#include "automaton_utils.hpp"

#include "index/index_reader.hpp"
#include "search/limited_sample_scorer.hpp"
#include "search/multiterm_query.hpp"
#include "utils/fst_table_matcher.hpp"

NS_LOCAL

using irs::automaton;

// table contains indexes of states in
// utf8_transitions_builder::rho_states_ table
const automaton::Arc::Label UTF8_RHO_STATE_TABLE[] {
  // 1 byte sequence (0-127)
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  // invalid sequence (128-191)
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  // 2 bytes sequence (192-223)
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  // 3 bytes sequence (224-239)
  2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
  // 4 bytes sequence (240-255)
  3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
};

NS_END

NS_ROOT

void utf8_transitions_builder::insert(
    automaton& a,
    const byte_type* label,
    const size_t size,
    const automaton::StateId to) {
  assert(label);

  add_states(size); // ensure we have enough states
  const size_t prefix = 1 + common_prefix_length(last_.c_str(), last_.size(), label, size);
  minimize(a, prefix); // minimize suffix

  // add current word suffix
  for (size_t i = prefix; i <= size; ++i) {
    auto& p = states_[i - 1];
    p.arcs.emplace_back(label[i - 1], &states_[i]);
    p.rho_id = rho_states_[size - i];
  }

  const bool is_final = last_.size() != size || prefix != (size + 1);

  if (is_final) {
    states_[size].id = to;
  }
}

void utf8_transitions_builder::finish(automaton& a, automaton::StateId from) {
#ifdef IRESEARCH_DEBUG
  auto ensure_empty = make_finally([this]() {
    // ensure everything is cleaned up
    assert(std::all_of(
      states_.begin(), states_.end(), [](const state& s) noexcept {
        return s.arcs.empty() &&
          s.id == fst::kNoStateId &&
          s.rho_id == fst::kNoStateId;
    }));
  });
#endif

  minimize(a, 1);

  if (fst::kNoStateId == rho_states_[0]) {
    if (!states_.empty()) {
      // no default state: just add transitions from the
      // root node to its successors
      auto& root = states_.front();

      for (const auto& arc : root.arcs) {
        a.EmplaceArc(from, arc.label, arc.id);
      }

      root.clear();
    }

    return;
  }

  // reserve enough memory to store all outbound transitions

  a.ReserveArcs(from, 256);

  // in presence of default state we have to add some extra
  // transitions from root to properly handle multi-bytes sequences
  // and preserve correctness of arcs order

  auto add_rho_arc = [&a, from, this](automaton::Arc::Label label) {
    const auto rho_state_idx = UTF8_RHO_STATE_TABLE[label];

    if (fst::kNoStateId == rho_state_idx) {
      return;
    }

    a.EmplaceArc(from, label, rho_states_[rho_state_idx]);
  };

  automaton::Arc::Label min = 0;

  if (!states_.empty()) {
    auto& root = states_.front();

    for (const auto& arc : root.arcs) {
      assert(arc.label < 256);
      assert(min <= arc.label); // ensure arcs are sorted

      for (; min < arc.label; ++min) {
        add_rho_arc(min);
      }

      assert(min == arc.label);
      a.EmplaceArc(from, min++, arc.id);
    }

    root.clear();
  }

  // add remaining rho transitions

  for (; min < 256; ++min) {
    add_rho_arc(min);
  }

  // connect intermediate states of default multi-bytes UTF8 sequence

  a.EmplaceArc(rho_states_[1], fst::fsa::kRho, rho_states_[0]);
  a.EmplaceArc(rho_states_[2], fst::fsa::kRho, rho_states_[1]);
  a.EmplaceArc(rho_states_[3], fst::fsa::kRho, rho_states_[2]);
}

filter::prepared::ptr prepare_automaton_filter(const string_ref& field,
                                               const automaton& acceptor,
                                               size_t scored_terms_limit,
                                               const index_reader& index,
                                               const order::prepared& order,
                                               boost_t boost) {
  automaton_table_matcher matcher(acceptor, fst::fsa::kRho);

  if (fst::kError == matcher.Properties(0)) {
    IR_FRMT_ERROR("Expected deterministic, epsilon-free acceptor, "
                  "got the following properties " IR_UINT64_T_SPECIFIER "",
                  acceptor.Properties(automaton_table_matcher::FST_PROPERTIES, false));

    return filter::prepared::empty();
  }

  limited_sample_scorer scorer(order.empty() ? 0 : scored_terms_limit); // object for collecting order stats
  multiterm_query::states_t states(index.size());

  for (const auto& segment : index) {
    // get term dictionary for field
    const term_reader* reader = segment.field(field);

    if (!reader) {
      continue;
    }

    auto it = reader->iterator(matcher);

    auto& meta = it->attributes().get<term_meta>(); // get term metadata
    const decltype(irs::term_meta::docs_count) NO_DOCS = 0;

    // NOTE: we can't use reference to 'docs_count' here, like
    // 'const auto& docs_count = meta ? meta->docs_count : NO_DOCS;'
    // since not gcc4.9 nor msvc2015-2019 can handle this correctly
    // probably due to broken optimization
    const auto* docs_count = meta ? &meta->docs_count : &NO_DOCS;

    if (it->next()) {
      auto& state = states.insert(segment);
      state.reader = reader;

      do {
        it->read(); // read term attributes

        state.estimation += *docs_count;
        scorer.collect(*docs_count, state.count++, state, segment, *it);
      } while (it->next());
    }
  }

  std::vector<bstring> stats;
  scorer.score(index, order, stats);

  return memory::make_shared<multiterm_query>(std::move(states), std::move(stats), boost);
}

NS_END
