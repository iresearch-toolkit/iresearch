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

#include "terms_filter.hpp"

#include "index/index_reader.hpp"
#include "search/all_filter.hpp"
#include "search/all_terms_collector.hpp"
#include "search/boolean_filter.hpp"
#include "search/collectors.hpp"
#include "search/filter_visitor.hpp"
#include "search/multiterm_query.hpp"
#include "search/term_filter.hpp"

namespace {

using namespace irs;

template<typename Visitor>
void visit(const SubReader& segment, const term_reader& field,
           const by_terms_options::search_terms& search_terms,
           Visitor& visitor) {
  auto terms = field.iterator(SeekMode::NORMAL);

  if (IRS_UNLIKELY(!terms)) {
    return;
  }

  visitor.prepare(segment, field, *terms);

  for (auto& term : search_terms) {
    if (!terms->seek(term.term)) {
      continue;
    }

    terms->read();

    visitor.visit(term.boost);
  }
}

template<typename Collector>
class terms_visitor {
 public:
  explicit terms_visitor(Collector& collector) noexcept
    : collector_(collector) {}

  void prepare(const SubReader& segment, const term_reader& field,
               const seek_term_iterator& terms) {
    collector_.prepare(segment, field, terms);
    collector_.stat_index(0);
  }

  void visit(score_t boost) {
    size_t stat_index = collector_.stat_index();
    collector_.visit(boost);
    collector_.stat_index(++stat_index);
  }

 private:
  Collector& collector_;
};

template<typename Collector>
void collect_terms(const IndexReader& index, std::string_view field,
                   const by_terms_options::search_terms& terms,
                   Collector& collector) {
  terms_visitor<Collector> visitor(collector);

  for (auto& segment : index) {
    auto* reader = segment.field(field);

    if (!reader) {
      continue;
    }

    visit(segment, *reader, terms, visitor);
  }
}

}  // namespace

namespace irs {

/*static*/ void by_terms::visit(const SubReader& segment,
                                const term_reader& field,
                                const by_terms_options::search_terms& terms,
                                filter_visitor& visitor) {
  ::visit(segment, field, terms, visitor);
}

filter::prepared::ptr by_terms::prepare(const IndexReader& index,
                                        const Scorers& order, score_t boost,
                                        const attribute_provider* ctx) const {
  const auto& [terms, min_match, merge_type] = options();
  const size_t size = terms.size();
  boost *= this->boost();

  if (0 == size || min_match > size) {
    // Empty or unreachable search criteria
    return prepared::empty();
  }

  if (0 == min_match) {
    if (order.empty()) {
      return MakeAllDocsFilter(kNoBoost)->prepare(index);
    } else {
      Or disj;
      // Don't contribute to the score
      disj.add(MakeAllDocsFilter(0.));
      // Reset min_match to 1
      disj.add<by_terms>(*this).mutable_options()->min_match = 1;
      return disj.prepare(index, order, kNoBoost, ctx);
    }
  }

  if (1 == size) {
    const auto term = std::begin(terms);
    return by_term::prepare(index, order, boost * term->boost, field(),
                            term->term);
  }

  field_collectors field_stats{order};
  term_collectors term_stats{order, size};
  MultiTermQuery::States states{index.size()};
  all_terms_collector collector{states, field_stats, term_stats};
  collect_terms(index, field(), terms, collector);

  // FIXME(gnusi): Filter out unmatched states during collection
  if (min_match > 1) {
    states.erase_if([min = min_match](const auto& state) noexcept {
      return state.scored_states.size() < min;
    });
  }

  if (states.empty()) {
    return prepared::empty();
  }

  MultiTermQuery::Stats stats{size};
  for (size_t term_idx = 0; auto& stat : stats) {
    stat.resize(order.stats_size(), 0);
    auto* stats_buf = stat.data();
    term_stats.finish(stats_buf, term_idx++, field_stats, index);
  }

  return memory::make_managed<MultiTermQuery>(
    std::move(states), std::move(stats), boost, merge_type, min_match);
}

}  // namespace irs
