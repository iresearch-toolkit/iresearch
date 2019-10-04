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

#include "shared.hpp"

#include "range_query.hpp"
#include "limited_sample_scorer.hpp"
#include "bitset_doc_iterator.hpp"
#include "range_filter.hpp"
#include "disjunction.hpp"
#include "score_doc_iterators.hpp"

NS_LOCAL

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

    // get term metadata
    auto& meta = terms.attributes().get<irs::term_meta>();
    const decltype(irs::term_meta::docs_count) NO_DOCS = 0;
    const auto& docs_count = meta ? meta->docs_count : NO_DOCS;

    do {
      // fill scoring candidates
      scorer.collect(docs_count, state.count++, state, segment, terms);
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

  // add an iterator for each of the scored states
  for (auto& entry: state->scored_states) {
    assert(entry.first);
    if (!terms->seek(bytes_ref::NIL, *entry.first)) {
      return doc_iterator::empty(); // internal error
    }

    auto* stats = stats_[entry.second].c_str();
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

  std::vector<bstring> stats;
  scorer.score(index, ord, stats);

  return memory::make_shared<range_query>(std::move(states), std::move(stats), boost);
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

      do {
        // fill scoring candidates
        scorer.collect(docs_count, state.count++, state, segment, *terms);
        state.estimation += docs_count; // collect cost

        if (!terms->next()) {
          break;
        }

        terms->read();
      } while (starts_with(value, prefix));
    }
  }

  std::vector<bstring> stats;
  scorer.score(index, ord, stats);

  return memory::make_shared<range_query>(std::move(states), std::move(stats), boost);
}

NS_END // ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
