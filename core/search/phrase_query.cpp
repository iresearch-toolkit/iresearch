////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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

#include "phrase_query.hpp"

#include "search/phrase_filter.hpp"
#include "search/phrase_iterator.hpp"

namespace iresearch {

doc_iterator::ptr FixedPhraseQuery::execute(const ExecutionContext& ctx) const {
  auto& rdr = ctx.segment;
  auto& ord = ctx.scorers;

  // get phrase state for the specified reader
  auto phrase_state = states_.find(rdr);

  if (!phrase_state) {
    // invalid state
    return doc_iterator::empty();
  }

  // get index features required for query & order
  const IndexFeatures features = ord.features() | by_phrase::kRequiredFeatures;

  std::vector<score_iterator_adapter<doc_iterator::ptr>> itrs;
  itrs.reserve(phrase_state->terms.size());

  std::vector<FixedPhraseFrequency::TermPosition> positions;
  positions.reserve(phrase_state->terms.size());

  auto* reader = phrase_state->reader;
  assert(reader);

  auto position = std::begin(positions_);

  for (const auto& term_state : phrase_state->terms) {
    assert(term_state.first);

    // get postings using cached state
    auto docs = reader->postings(*term_state.first, features);
    assert(docs);

    auto* pos = irs::get_mutable<irs::position>(docs.get());

    if (!pos) {
      // positions not found
      return doc_iterator::empty();
    }

    positions.emplace_back(std::ref(*pos), *position);

    // add base iterator
    itrs.emplace_back(std::move(docs));

    ++position;
  }

  using phrase_iterator_t =
      PhraseIterator<conjunction<doc_iterator::ptr, NoopAggregator>,
                     FixedPhraseFrequency>;

  return memory::make_managed<phrase_iterator_t>(
      std::move(itrs), std::move(positions), rdr, *phrase_state->reader,
      stats_.c_str(), ord, boost());
}

// FIXME add proper handling of overlapped case
template<bool VolatileBoost>
using phrase_iterator_t =
    PhraseIterator<conjunction<doc_iterator::ptr, NoopAggregator>,
                   VariadicPhraseFrequency<VolatileBoost>>;

doc_iterator::ptr VariadicPhraseQuery::execute(
    const ExecutionContext& ctx) const {
  using adapter_t = VariadicPhraseAdapter;
  using compound_doc_iterator_t = irs::compound_doc_iterator<adapter_t>;
  using disjunction_t =
      disjunction<doc_iterator::ptr, NoopAggregator, adapter_t, true>;

  auto& rdr = ctx.segment;
  auto& ord = ctx.scorers;

  // get phrase state for the specified reader
  auto phrase_state = states_.find(rdr);

  if (!phrase_state) {
    // invalid state
    return doc_iterator::empty();
  }

  // get features required for query & order
  const IndexFeatures features = ord.features() | by_phrase::kRequiredFeatures;

  std::vector<score_iterator_adapter<doc_iterator::ptr>> conj_itrs;
  conj_itrs.reserve(phrase_state->terms.size());

  const auto phrase_size = phrase_state->num_terms.size();

  std::vector<VariadicTermPosition> positions;
  positions.resize(phrase_size);

  // find term using cached state
  auto* reader = phrase_state->reader;
  assert(reader);

  auto position = std::begin(positions_);

  auto term_state = std::begin(phrase_state->terms);
  for (size_t i = 0; i < phrase_size; ++i) {
    const auto num_terms = phrase_state->num_terms[i];
    auto& pos = positions[i];
    pos.second = *position;

    std::vector<adapter_t> disj_itrs;
    disj_itrs.reserve(num_terms);
    for (const auto end = term_state + num_terms; term_state != end;
         ++term_state) {
      assert(term_state->first);

      adapter_t docs{reader->postings(*term_state->first, features),
                     term_state->second};

      if (!docs.position) {
        // positions not found
        continue;
      }

      disj_itrs.emplace_back(std::move(docs));
    }

    if (disj_itrs.empty()) {
      return doc_iterator::empty();
    }

    auto disj =
        MakeDisjunction<disjunction_t>(std::move(disj_itrs), NoopAggregator{});
    pos.first = down_cast<compound_doc_iterator_t>(disj.get());
    conj_itrs.emplace_back(std::move(disj));
    ++position;
  }
  assert(term_state == std::end(phrase_state->terms));

  if (phrase_state->volatile_boost) {
    return memory::make_managed<phrase_iterator_t<true>>(
        std::move(conj_itrs), std::move(positions), rdr, *phrase_state->reader,
        stats_.c_str(), ord, boost());
  }

  return memory::make_managed<phrase_iterator_t<false>>(
      std::move(conj_itrs), std::move(positions), rdr, *phrase_state->reader,
      stats_.c_str(), ord, boost());
}

}  // namespace iresearch
