////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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

#include "column_existence_filter.hpp"

#include "formats/empty_term_reader.hpp"
#include "search/disjunction.hpp"

namespace {

using namespace irs;

class column_existence_query : public irs::filter::prepared {
 public:
  explicit column_existence_query(std::string_view field, bstring&& stats,
                                  boost_t boost)
      : filter::prepared(boost), field_{field}, stats_(std::move(stats)) {}

  virtual doc_iterator::ptr execute(
      const sub_reader& segment, const Order& ord, ExecutionMode /*mode*/,
      const attribute_provider* /*ctx*/) const override {
    const auto* column = segment.column(field_);

    if (!column) {
      return doc_iterator::empty();
    }

    return iterator(segment, *column, ord);
  }

 protected:
  doc_iterator::ptr iterator(const sub_reader& segment,
                             const column_reader& column,
                             const Order& ord) const {
    auto it = column.iterator(false);

    if (IRS_UNLIKELY(!it)) {
      return doc_iterator::empty();
    }

    if (!ord.empty()) {
      if (auto* score = irs::get_mutable<irs::score>(it.get()); score) {
        auto scorers = PrepareScorers(ord.buckets(), segment,
                                      empty_term_reader(column.size()),
                                      stats_.c_str(), *it, boost());

        *score = CompileScorers(std::move(scorers));
      }
    }

    return it;
  }

  std::string field_;
  bstring stats_;
};  // column_existence_query

class column_prefix_existence_query final : public column_existence_query {
 public:
  explicit column_prefix_existence_query(std::string_view prefix,
                                         bstring&& stats, boost_t boost)
      : column_existence_query(prefix, std::move(stats), boost) {}

  virtual irs::doc_iterator::ptr execute(
      const irs::sub_reader& segment, const irs::Order& ord,
      ExecutionMode /*mode*/,
      const irs::attribute_provider* /*ctx*/) const override {
    using adapter_t = irs::score_iterator_adapter<irs::doc_iterator::ptr>;

    const string_ref prefix = field_;

    auto it = segment.columns();

    if (!it->seek(prefix)) {
      // reached the end
      return irs::doc_iterator::empty();
    }

    std::vector<adapter_t> itrs;

    while (irs::starts_with(it->value().name(), prefix)) {
      const auto* column = segment.column(it->value().id());

      if (!column) {
        continue;
      }

      itrs.emplace_back(iterator(segment, *column, ord));

      if (!it->next()) {
        break;
      }
    }

    return ResoveMergeType(
        sort::MergeType::AGGREGATE, ord.buckets().size(),
        [&]<typename A>(A&& aggregator) -> irs::doc_iterator::ptr {
          using disjunction_t = std::conditional_t<
              std::is_same_v<A, irs::NoopAggregator>,
              irs::disjunction_iterator<irs::doc_iterator::ptr, A>,
              irs::scored_disjunction_iterator<irs::doc_iterator::ptr, A>>;

          return irs::make_disjunction<disjunction_t>(std::move(itrs),
                                                      std::move(aggregator));
        });
  }
};

}  // namespace

namespace iresearch {

DEFINE_FACTORY_DEFAULT(by_column_existence)  // cppcheck-suppress unknownMacro

filter::prepared::ptr by_column_existence::prepare(
    const index_reader& reader, const Order& order, boost_t filter_boost,
    const attribute_provider* /*ctx*/) const {
  // skip field-level/term-level statistics because there are no explicit
  // fields/terms, but still collect index-level statistics
  // i.e. all fields and terms implicitly match
  bstring stats(order.stats_size(), 0);
  auto* stats_buf = const_cast<byte_type*>(stats.data());

  PrepareCollectors(order.buckets(), stats_buf, reader);

  filter_boost *= boost();

  return options().prefix_match
             ? memory::make_managed<column_prefix_existence_query>(
                   field(), std::move(stats), filter_boost)
             : memory::make_managed<column_existence_query>(
                   field(), std::move(stats), filter_boost);
}

}  // namespace iresearch
