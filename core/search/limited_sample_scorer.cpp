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

#include "analysis/token_attributes.hpp"
#include "index/index_reader.hpp"
#include "utils/hash_utils.hpp"

NS_LOCAL

struct state_t {
  explicit state_t(const irs::order::prepared& order)
    : collectors(order.prepare_collectors(1)) { // 1 term per bstring because a range is treated as a disjunction
  }

  irs::order::prepared::collectors collectors;
  size_t stats_offset;
//  irs::bstring stats; // filter stats for a the current state/term
};

void set_doc_ids(irs::bitset& buf, const irs::term_iterator& term) {
  auto itr = term.postings(irs::flags::empty_instance());

  if (!itr) {
    return; // no doc_ids in iterator
  }

//FIXME use doc attribute
//  auto* doc = itr->attributes().get<irs::document>().get();
//
//  if (!doc) {
//    return; // no doc value
//  }

  while (itr->next()) {
    buf.set(itr->value());
  }
};

NS_END

NS_ROOT

limited_sample_scorer::limited_sample_scorer(size_t scored_terms_limit)
  : scored_terms_limit_(scored_terms_limit) {
  scored_states_.reserve(scored_terms_limit);
}

void limited_sample_scorer::score(const index_reader& index,
                                  const order::prepared& order,
                                  std::vector<bstring>& stats) {
  if (!scored_terms_limit_) {
    return; // nothing to score (optimization)
  }

  std::unordered_map<hashed_bytes_ref, state_t> term_stats; // stats for a specific term

  // iterate over all the states from which statistcis should be collected
  size_t stats_offset = 0;
  for (auto& entry: scored_states_) {
    auto& scored_state = entry.second;
    auto term_itr = scored_state.state->reader->iterator();

    // find the stats for the current term
    auto res = map_utils::try_emplace(
      term_stats,
      make_hashed_ref(bytes_ref(scored_state.term), std::hash<bytes_ref>()),
      order);
    auto inserted = res.second;
    auto& stats_entry = res.first->second;
    auto& collectors = stats_entry.collectors;
    auto& field = *scored_state.state->reader;
    auto& segment = *scored_state.segment;

    // once per every 'state_t' collect field statistics over the entire index
    if (inserted) {
      for (auto& sr: index) {
        collectors.collect(sr, field); // collect field statistics once per segment
      }

      stats_entry.stats_offset = stats_offset++;
    }

    // find term attributes using cached state
    // use bytes_ref::NIL here since we just "jump" to cached state,
    // and we are not interested in term value itself
    if (!term_itr
        || !scored_state.cookie
        || !term_itr->seek(bytes_ref::NIL, *(scored_state.cookie))) {
      continue; // some internal error that caused the term to disapear
    }

    // collect statistics, 0 because only 1 term
    collectors.collect(segment, field, 0, term_itr->attributes());

    // filter stats is copied since it's shared among multiple states
    scored_state.state->scored_states.emplace_back(
      std::move(scored_state.cookie),
      stats_entry.stats_offset);
  }

  // iterate over all stats and apply/store order stats
  stats.resize(stats_offset);
  for (auto& entry: term_stats) {
    auto& stats_entry = stats[entry.second.stats_offset];
    stats_entry.resize(order.stats_size());
    auto* stats_buf = const_cast<byte_type*>(stats_entry.data());

    order.prepare_stats(stats_buf);
    entry.second.collectors.finish(stats_buf, index);
  }
}

void limited_sample_scorer::collect(
    size_t priority,
    size_t scored_state_id,
    limited_sample_state& scored_state,
    const sub_reader& segment,
    const seek_term_iterator& term_itr) {
  if (!scored_terms_limit_) {
    assert(scored_state.unscored_docs.size() >= (doc_limits::min)() + segment.docs_count()); // otherwise set will fail
    set_doc_ids(scored_state.unscored_docs, term_itr);

    return; // nothing to collect (optimization)
  }

  auto less = [](const std::pair<size_t, scored_term_state_t>& lhs,
                 const std::pair<size_t, scored_term_state_t>& rhs) noexcept {
    if (rhs.first == lhs.first) {
      return lhs.second.state_offset < rhs.second.state_offset;
    }

    return rhs.first < lhs.first;
  };

  if (scored_states_.size() < scored_terms_limit_) {
    // have not reached the scored state limit yet
    scored_states_.emplace_back(std::piecewise_construct,
                                std::forward_as_tuple(priority),
                                std::forward_as_tuple(segment, scored_state, scored_state_id, term_itr.value(), term_itr.cookie()));

    std::push_heap(scored_states_.begin(), scored_states_.end(), less);
  } else if (scored_states_.front().first < priority || (scored_states_.front().first == priority && scored_states_.front().second.state_offset < scored_state_id)) {
    std::pop_heap(scored_states_.begin(), scored_states_.end(), less);

    auto& back = scored_states_.back();
    auto& state = back.second;

    auto state_term_it = state.state->reader->iterator(); // FIXME cache iterator???

    assert(state.cookie);
    if (state_term_it->seek(bytes_ref::NIL, *state.cookie)) {
      // state will not be scored
      // add all doc_ids from the doc_iterator to the unscored_docs
      assert(scored_state.unscored_docs.size() >= (doc_limits::min)() + segment.docs_count()); // otherwise set will fail
      set_doc_ids(state.state->unscored_docs, *state_term_it);
    }

    // update min state
    back.first = priority;
    state.state = &scored_state;
    state.cookie = term_itr.cookie();
    state.term = term_itr.value();
    state.segment = &segment;
    state.state_offset = scored_state_id;

    std::push_heap(scored_states_.begin(), scored_states_.end(), less);
  } else {
    // state will not be scored
    // add all doc_ids from the doc_iterator to the unscored_docs
    assert(scored_state.unscored_docs.size() >= (doc_limits::min)() + segment.docs_count()); // otherwise set will fail
    set_doc_ids(scored_state.unscored_docs, term_itr);
  }
}

NS_END
