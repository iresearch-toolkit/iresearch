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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#include "ngram_similarity_filter.hpp"

#include "index/index_reader.hpp"
#include "search/collectors.hpp"
#include "search/ngram_similarity_query.hpp"
#include "search/terms_filter.hpp"
#include "shared.hpp"

namespace irs {

filter::prepared::ptr by_ngram_similarity::prepare(
  const IndexReader& rdr, const Order& ord, score_t boost,
  const attribute_provider* ctx) const {
  const auto threshold = std::max(0.f, std::min(1.f, options().threshold));
  const auto& ngrams = options().ngrams;

  if (ngrams.empty() || field().empty()) {
    // empty field or terms or invalid threshold
    return filter::prepared::empty();
  }

  const size_t min_match_count =
    std::max(static_cast<size_t>(
               std::ceil(static_cast<double>(ngrams.size()) * threshold)),
             size_t{1});

  if (ord.empty() && 1 == min_match_count) {
    irs::by_terms disj;
    for (auto& terms = disj.mutable_options()->terms;
         auto& term : options().ngrams) {
      terms.emplace(term, irs::kNoBoost);
    }
    *disj.mutable_field() = this->field();
    disj.boost(this->boost());
    return disj.prepare(rdr, irs::Order::kUnordered, boost, ctx);
  }

  NGramStates query_states{rdr.size()};

  // per segment terms states
  const auto terms_count = ngrams.size();
  std::vector<seek_cookie::ptr> term_states;
  term_states.reserve(terms_count);

  // prepare ngrams stats
  field_collectors field_stats{ord};
  term_collectors term_stats{ord, terms_count};

  const std::string_view field_name = this->field();

  for (const auto& segment : rdr) {
    // get term dictionary for field
    const term_reader* field = segment.field(field_name);

    if (!field) {
      continue;
    }

    // check required features
    if (NGramSimilarityQuery::kRequiredFeatures !=
        (field->meta().index_features &
         NGramSimilarityQuery::kRequiredFeatures)) {
      continue;
    }

    // collect field statistics once per segment
    field_stats.collect(segment, *field);
    size_t term_idx = 0;
    size_t count_terms = 0;
    seek_term_iterator::ptr term = field->iterator(SeekMode::NORMAL);
    for (const auto& ngram : ngrams) {
      term_states.emplace_back();
      auto& state = term_states.back();
      if (term->seek(ngram)) {
        // read term attributes
        term->read();
        // collect statistics
        term_stats.collect(segment, *field, term_idx, *term);
        state = term->cookie();
        ++count_terms;
      }

      ++term_idx;
    }

    if (count_terms < min_match_count) {
      // we have not found enough terms
      term_states.clear();
      continue;
    }

    auto& state = query_states.insert(segment);
    state.terms = std::move(term_states);
    state.field = field;

    term_states.reserve(terms_count);
  }

  bstring stats(ord.stats_size(), 0);
  auto* stats_buf = const_cast<byte_type*>(stats.data());

  for (size_t term_idx = 0; term_idx < terms_count; ++term_idx) {
    term_stats.finish(stats_buf, term_idx, field_stats, rdr);
  }

  return memory::make_managed<NGramSimilarityQuery>(
    min_match_count, std::move(query_states), std::move(stats),
    this->boost() * boost);
}

}  // namespace irs
