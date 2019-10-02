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

#include "limited_sample_scorer.hpp"

#include "index/index_reader.hpp"
#include "utils/hash_utils.hpp"

NS_LOCAL

struct state_t {
  explicit state_t(const irs::order::prepared& order)
    : collectors(order.prepare_collectors(1)) { // 1 term per bstring because a range is treated as a disjunction
  }

  irs::order::prepared::collectors collectors;
  irs::bstring stats; // filter stats for a the current state/term
};

NS_END

NS_ROOT

void limited_sample_scorer::score(const index_reader& index, const order::prepared& order) {
  if (!scored_terms_limit_) {
    return; // nothing to score (optimization)
  }

  std::unordered_map<hashed_bytes_ref, state_t> term_stats; // stats for a specific term
  std::unordered_map<scored_term_state_t*, state_t*> state_stats; // stats for a specific state

  // iterate over all the states from which statistcis should be collected
  for (auto& entry: scored_states_) {
    auto& scored_state = entry.second;
    auto term_itr = scored_state.state.reader->iterator();

    // find the stats for the current term
    auto res = map_utils::try_emplace(
      term_stats,
      make_hashed_ref(bytes_ref(scored_state.term), std::hash<bytes_ref>()),
      order);
    auto inserted = res.second;
    auto& stats_entry = res.first->second;
    auto& collectors = stats_entry.collectors;
    auto& field = *scored_state.state.reader;
    auto& segment = scored_state.sub_reader;

    // once per every 'state_t' collect field statistics over the entire index
    if (inserted) {
      for (auto& sr: index) {
        collectors.collect(sr, field); // collect field statistics once per segment
      }
    }

    // find term attributes using cached state
    // use bytes_ref::blank here since we just "jump" to cached state,
    // and we are not interested in term value itself
    if (!term_itr
        || !scored_state.cookie
        || !term_itr->seek(bytes_ref::NIL, *(scored_state.cookie))) {
      continue; // some internal error that caused the term to disapear
    }

    collectors.collect(segment, field, 0, term_itr->attributes()); // collect statistics, 0 because only 1 term
    state_stats.emplace(&scored_state, &stats_entry); // associate states to a state
  }

  // iterate over all stats and apply/store order stats
  for (auto& entry: term_stats) {
    entry.second.stats.resize(order.stats_size());
    auto* stats_buf = const_cast<byte_type*>(entry.second.stats.data());

    order.prepare_stats(stats_buf);
    entry.second.collectors.finish(stats_buf, index);
  }

  // set filter attributes for each corresponding term
  for (auto& entry: scored_states_) {
    auto& scored_state = entry.second;
    auto itr = state_stats.find(&scored_state);
    assert(itr != state_stats.end() && itr->second); // values set just above

    // filter stats is copied since it's shared among multiple states
    scored_state.state.scored_states.emplace(
      scored_state.state_offset,
      itr->second->stats);
  }
}

void limited_sample_scorer::collect(
    size_t priority,
    size_t scored_state_id,
    limited_sample_state& scored_state,
    const sub_reader& reader,
    const seek_term_iterator& term_itr) {
  auto set_doc_ids = [](bitset& buf, const term_iterator& term) {
    auto itr = term.postings(flags::empty_instance());

    if (!itr) {
      return; // no doc_ids in iterator
    }

    while(itr->next()) {
      buf.set(itr->value());
    }
  };

  if (!scored_terms_limit_) {
    assert(scored_state.unscored_docs.size() >= (doc_limits::min)() + reader.docs_count()); // otherwise set will fail
    set_doc_ids(scored_state.unscored_docs, term_itr);

    return; // nothing to collect (optimization)
  }

  scored_states_.emplace(
    std::piecewise_construct,
    std::forward_as_tuple(priority),
    std::forward_as_tuple(reader, scored_state, scored_state_id, term_itr));

  if (scored_states_.size() <= scored_terms_limit_) {
    return; // have not reached the scored state limit yet
  }

  auto itr = scored_states_.begin(); // least significant state to be removed
  auto& entry = itr->second;
  auto state_term_itr = entry.state.reader->iterator();

  // add all doc_ids from the doc_iterator to the unscored_docs
  if (state_term_itr
      && entry.cookie
      && state_term_itr->seek(bytes_ref::NIL, *(entry.cookie))) {
    assert(entry.state.unscored_docs.size() >= (doc_limits::min)() + entry.sub_reader.docs_count()); // otherwise set will fail
    set_doc_ids(entry.state.unscored_docs, *state_term_itr);
  }

  scored_states_.erase(itr);
}

NS_END
