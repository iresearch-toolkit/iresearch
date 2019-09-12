////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "bitset_doc_iterator.hpp"
#include "shared.hpp"
#include "range_query.hpp"
#include "range_filter.hpp"
#include "disjunction.hpp"
#include "score_doc_iterators.hpp"
#include "index/index_reader.hpp"
#include "utils/hash_utils.hpp"

NS_LOCAL

void set_doc_ids(irs::bitset& buf, const irs::term_iterator& term) {
  auto itr = term.postings(irs::flags::empty_instance());

  if (!itr) {
    return; // no doc_ids in iterator
  }

  while(itr->next()) {
    buf.set(itr->value());
  }
}

template<typename Comparer>
void collect_terms(
    const irs::sub_reader& segment,
    const irs::term_reader& field,
    irs::seek_term_iterator& terms,
    irs::range_query::states_t& states,
    irs::limited_sample_scorer& scorer,
    Comparer cmp) {
  auto& value = terms.value();

  if (cmp(value)) {
    // read attributes
    terms.read();

    // get state for current segment
    auto& state = states.insert(segment);
    state.reader = &field;
    state.min_term = value;
    state.min_cookie = terms.cookie();
    state.unscored_docs.reset(
      (irs::type_limits<irs::type_t::doc_id_t>::min)() + segment.docs_count()
    ); // highest valid doc_id in segment

    // get term metadata
    auto& meta = terms.attributes().get<irs::term_meta>();
    const decltype(irs::term_meta::docs_count) NO_DOCS = 0;
    const auto& docs_count = meta ? meta->docs_count : NO_DOCS;

    do {
      // fill scoring candidates
      scorer.collect(docs_count , state.count, state, segment, terms);
      ++state.count;
      state.estimation += docs_count;

      if (!terms.next()) {
        break;
      }

      // read attributes
      terms.read();
    } while (cmp(value));
  }
}

NS_END

NS_ROOT

limited_sample_scorer::limited_sample_scorer(size_t scored_terms_limit):
  scored_terms_limit_(scored_terms_limit) {
}

void limited_sample_scorer::collect(
  size_t priority, // priority of this entry, lowest priority removed first
  size_t scored_state_id, // state identifier used for querying of attributes
  range_state& scored_state, // state containing this scored term
  const sub_reader& reader, // segment reader for the current term
  const seek_term_iterator& term_itr // term-iterator positioned at the current term
) {
  if (!scored_terms_limit_) {
    assert(scored_state.unscored_docs.size() >= (doc_limits::min)() + reader.docs_count()); // otherwise set will fail
    set_doc_ids(scored_state.unscored_docs, term_itr);

    return; // nothing to collect (optimization)
  }

  scored_states_.emplace(
    std::piecewise_construct,
    std::forward_as_tuple(priority),
    std::forward_as_tuple(reader, scored_state, scored_state_id, term_itr)
  );

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

void limited_sample_scorer::score(
    const index_reader& index, const order::prepared& order
) {
  if (!scored_terms_limit_) {
    return; // nothing to score (optimization)
  }

  struct state_t {
    order::prepared::collectors collectors;
    bstring stats; // filter stats for a the current state/term
    state_t(const order::prepared& order)
      : collectors(order.prepare_collectors(1)) { // 1 term per bstring because a range is treated as a disjunction
    }
  };
  std::unordered_map<hashed_bytes_ref, state_t> term_stats; // stats for a specific term
  std::unordered_map<scored_term_state_t*, state_t*> state_stats; // stats for a specific state

  // iterate over all the states from which statistcis should be collected
  for (auto& entry: scored_states_) {
    auto& scored_state = entry.second;
    auto term_itr = scored_state.state.reader->iterator();

    // find the stats for the current term
    auto res = map_utils::try_emplace(
      term_stats,
      make_hashed_ref(bytes_ref(scored_state.term), std::hash<irs::bytes_ref>()),
      order
    );
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
      itr->second->stats
    );
  }
}

doc_iterator::ptr range_query::execute(
    const sub_reader& rdr,
    const order::prepared& ord,
    const attribute_view& /*ctx*/) const {
  typedef disjunction<doc_iterator::ptr> disjunction_t;

  // get term state for the specified reader
  auto state = states_.find(rdr);
  if (!state) {
    // invalid state
    return doc_iterator::empty();
  }

  // get terms iterator
  auto terms = state->reader->iterator();

  // find min term using cached state
  if (!terms->seek(state->min_term, *(state->min_cookie))) {
    return doc_iterator::empty();
  }

  // prepared disjunction
  const bool has_bit_set = state->unscored_docs.any();
  disjunction_t::doc_iterators_t itrs;
  itrs.reserve(state->count + size_t(has_bit_set)); // +1 for possible bitset_doc_iterator

  // get required features for order
  auto& features = ord.features();

  // add an iterator for the unscored docs
  if (has_bit_set) {
    itrs.emplace_back(doc_iterator::make<bitset_doc_iterator>(
      state->unscored_docs
    ));
  }

  size_t last_offset = 0;

  // add an iterator for each of the scored states
  for (auto& entry: state->scored_states) {
    auto offset = entry.first;
    auto* stats = entry.second.c_str();
    assert(offset >= last_offset);

    if (!skip(*terms, offset - last_offset)) {
      continue; // reached end of iterator
    }

    last_offset = offset;

    auto docs = terms->postings(features);
    auto& attrs = docs->attributes();

    // set score
    auto& score = attrs.get<irs::score>();
    if (score) {
      score->prepare(ord, ord.prepare_scorers(rdr, *state->reader, stats, attrs, boost()));
    }

    itrs.emplace_back(std::move(docs));
  }

  return make_disjunction<disjunction_t>(std::move(itrs), ord, state->estimation);
}

/*static*/ range_query::ptr range_query::make_from_range(
    const index_reader& index,
    const order::prepared& ord,
    boost_t boost,
    const string_ref& field,
    const range<bstring>& rng,
    size_t scored_terms_limit) {
  //TODO: optimize unordered case
  // - seek to min
  // - get ordinal position of the term
  // - seek to max
  // - get ordinal position of the term

  limited_sample_scorer scorer(ord.empty() ? 0 : scored_terms_limit); // object for collecting order stats
  range_query::states_t states(index.size());

  // iterate over the segments
  for (const auto& segment : index) {
    // get term dictionary for field
    const auto* reader = segment.field(field);

    if (!reader) {
      // can't find field with the specified name
      continue;
    }

    auto terms = reader->iterator();
    bool res = false;

    // seek to min
    switch (rng.min_type) {
      case BoundType::UNBOUNDED:
        res = terms->next();
        break;
      case BoundType::INCLUSIVE:
        res = seek_min<true>(*terms, rng.min);
        break;
      case BoundType::EXCLUSIVE:
        res = seek_min<false>(*terms, rng.min);
        break;
    }

    if (!res) {
      // reached the end, nothing to collect
      continue;
    }

    // now we are on the target or the next term
    const irs::bytes_ref max = rng.max;

    switch (rng.max_type) {
      case BoundType::UNBOUNDED:
        ::collect_terms(
          segment, *reader, *terms, states, scorer, [](const bytes_ref&) {
            return true;
        });
        break;
      case BoundType::INCLUSIVE:
        ::collect_terms(
          segment, *reader, *terms, states, scorer, [max](const bytes_ref& term) {
            return term <= max;
        });
        break;
      case BoundType::EXCLUSIVE:
        ::collect_terms(
          segment, *reader, *terms, states, scorer, [max](const bytes_ref& term) {
            return term < max;
        });
        break;
    }
  }

  scorer.score(index, ord);

  return memory::make_shared<range_query>(std::move(states), boost);
}

/*static*/ range_query::ptr range_query::make_from_prefix(
    const index_reader& index,
    const order::prepared& ord,
    boost_t boost,
    const string_ref& field,
    const bytes_ref& prefix,
    size_t scored_terms_limit) {
  limited_sample_scorer scorer(ord.empty() ? 0 : scored_terms_limit); // object for collecting order stats
  range_query::states_t states(index.size());

  // iterate over the segments
  for (const auto& segment: index) {
    // get term dictionary for field
    const term_reader* reader = segment.field(field);

    if (!reader) {
      continue;
    }

    seek_term_iterator::ptr terms = reader->iterator();

    // seek to prefix
    if (SeekResult::END == terms->seek_ge(prefix)) {
      continue;
    }

    auto& value = terms->value();

    // get term metadata
    auto& meta = terms->attributes().get<term_meta>();
    const decltype(irs::term_meta::docs_count) NO_DOCS = 0;
    const auto& docs_count = meta ? meta->docs_count : NO_DOCS;

    if (starts_with(value, prefix)) {
      terms->read();

      // get state for current segment
      auto& state = states.insert(segment);
      state.reader = reader;
      state.min_term = value;
      state.min_cookie = terms->cookie();
      state.unscored_docs.reset((type_limits<type_t::doc_id_t>::min)() + segment.docs_count()); // highest valid doc_id in reader

      do {
        // fill scoring candidates
        scorer.collect(docs_count, state.count, state, segment, *terms);
        ++state.count;
        state.estimation += docs_count; // collect cost

        if (!terms->next()) {
          break;
        }

        terms->read();
      } while (starts_with(value, prefix));
    }
  }

  scorer.score(index, ord);

  return memory::make_shared<range_query>(std::move(states), boost);
}

NS_END // ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
