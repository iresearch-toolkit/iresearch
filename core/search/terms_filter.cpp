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
#include "search/all_terms_collector.hpp"
#include "search/collectors.hpp"
#include "search/term_query.hpp"
#include "search/filter_visitor.hpp"
#include "search/multiterm_query.hpp"

NS_LOCAL

using namespace irs;

template<typename Visitor>
void visit(
    const sub_reader& segment,
    const term_reader& field,
    const std::vector<std::pair<bstring, boost_t>>& search_terms,
    Visitor& visitor) {
  // find term
  auto terms = field.iterator();

  if (IRS_UNLIKELY(!terms)) {
    return;
  }

  visitor.prepare(segment, field, *terms);

  for (auto& term : search_terms) {
    if (!terms->seek(term.first)) {
      continue;
    }

    terms->read();

    visitor.visit();
  }
}

template<typename Collector>
class terms_visitor final : public filter_visitor {
 public:
  terms_visitor(
      Collector& collector,
      const std::vector<std::pair<bstring, boost_t>>& terms)
    : collector_(collector),
      terms_(terms) {
  }

  virtual void prepare(
      const sub_reader& segment,
      const term_reader& field,
      const seek_term_iterator& terms) override {
    term_ = &terms.value();
    collector_.prepare(segment, field, terms);
    collector_.stat_index(0);
  }

  virtual void visit() override {
    size_t stat_index = collector_.stat_index();
    assert(stat_index < terms_.size());
    collector_.collect(terms_[stat_index].second);
    collector_.stat_index(++stat_index);
  }

 private:
  Collector& collector_;
  const std::vector<std::pair<bstring, boost_t>>& terms_;
  const sub_reader* segment_{};
  const term_reader* field_{};
  const bytes_ref* term_{};
}; // terms_visitor

template<typename Collector>
void collect_terms(
    const index_reader& index,
    const string_ref& field,
    const std::vector<std::pair<bstring, boost_t>>& terms,
    Collector& collector) {
  terms_visitor<Collector> visitor(collector, terms);

  for (auto& segment : index) {
    auto* reader = segment.field(field);

    if (!reader) {
      continue;
    }

    visit(segment, *reader, terms, visitor);
  }
}

NS_END

NS_ROOT

field_visitor visitor(const by_terms_options::filter_options& options) {
  // FIXME
  return [terms = options.terms](
      const sub_reader& segment,
      const term_reader& field,
      filter_visitor& visitor) {
    visit(segment, field, terms, visitor);
  };
}


DEFINE_FILTER_TYPE(by_terms)
DEFINE_FACTORY_DEFAULT(by_terms)

filter::prepared::ptr by_terms::prepare(
    const index_reader& index,
    const order::prepared& order,
    boost_t boost,
    const attribute_view& /*ctx*/) const {
  boost *= this->boost();
  const size_t num_match = options().num_match;
  const auto& terms = options().terms;
  const size_t size = terms.size();

  if (!num_match || num_match > size) {
    return prepared::empty();
  }

  if (1 == size) {
    auto& term = terms.front();
    return term_query::make(index, order, boost*term.second, field(), term.first);
  }

  field_collectors field_stats(order);
  term_collectors term_stats(order, size);
  multiterm_query::states_t states(index.size());

  all_terms_collector<decltype(states)> collector(states, field_stats, term_stats);
  collect_terms(index, field(), terms, collector);

  std::vector<bstring> stats(size);
  for (auto& stat : stats) {
    stat.resize(order.stats_size(), 0);
    auto* stats_buf = const_cast<byte_type*>(stat.data());
    term_stats.finish(stats_buf, field_stats, index);
  }

  return memory::make_shared<multiterm_query>(
    std::move(states), std::move(stats),
    boost, sort::MergeType::AGGREGATE);
}

NS_END
