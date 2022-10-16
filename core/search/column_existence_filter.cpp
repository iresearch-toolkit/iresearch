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
  column_existence_query(std::string_view field, bstring&& stats, score_t boost)
    : filter::prepared(boost), field_{field}, stats_(std::move(stats)) {}

  doc_iterator::ptr execute(const ExecutionContext& ctx) const override {
    const auto& segment = ctx.segment;
    const auto* column = segment.column(field_);

    if (!column) {
      return doc_iterator::empty();
    }

    return iterator(segment, *column, ctx.scorers);
  }

  void visit(const sub_reader&, PreparedStateVisitor&, score_t) const override {
    // No terms to visit
  }

 protected:
  doc_iterator::ptr iterator(const sub_reader& segment,
                             const column_reader& column,
                             const Order& ord) const {
    auto it = column.iterator(ColumnHint::kMask);

    if (IRS_UNLIKELY(!it)) {
      return doc_iterator::empty();
    }

    if (!ord.empty()) {
      if (auto* score = irs::get_mutable<irs::score>(it.get()); score) {
        *score =
          CompileScore(ord.buckets(), segment, empty_term_reader(column.size()),
                       stats_.c_str(), *it, boost());
      }
    }

    return it;
  }

  std::string field_;
  bstring stats_;
};

class column_prefix_existence_query final : public column_existence_query {
 public:
  column_prefix_existence_query(std::string_view prefix, bstring&& stats,
                                const ColumnAcceptor& acceptor, score_t boost)
    : column_existence_query{prefix, std::move(stats), boost},
      acceptor_{acceptor} {
    assert(acceptor_);
  }

  irs::doc_iterator::ptr execute(const ExecutionContext& ctx) const override {
    using adapter_t = irs::score_iterator_adapter<irs::doc_iterator::ptr>;

    assert(acceptor_);

    auto& segment = ctx.segment;
    auto& ord = ctx.scorers;
    const string_ref prefix = field_;

    auto it = segment.columns();

    if (!it->seek(prefix)) {
      // reached the end
      return irs::doc_iterator::empty();
    }

    const auto* column = &it->value();

    std::vector<adapter_t> itrs;
    for (; column->name().starts_with(prefix); column = &it->value()) {
      if (acceptor_(column->name(), prefix)) {
        itrs.emplace_back(iterator(segment, *column, ord));
      }

      if (!it->next()) {
        break;
      }
    }

    return ResoveMergeType(
      sort::MergeType::kSum, ord.buckets().size(),
      [&]<typename A>(A&& aggregator) -> irs::doc_iterator::ptr {
        using disjunction_t =
          irs::disjunction_iterator<irs::doc_iterator::ptr, A>;

        return irs::MakeDisjunction<disjunction_t>(std::move(itrs),
                                                   std::move(aggregator));
      });
  }

 private:
  ColumnAcceptor acceptor_;
};

}  // namespace

namespace iresearch {

filter::prepared::ptr by_column_existence::prepare(
  const index_reader& reader, const Order& order, score_t filter_boost,
  const attribute_provider* /*ctx*/) const {
  // skip field-level/term-level statistics because there are no explicit
  // fields/terms, but still collect index-level statistics
  // i.e. all fields and terms implicitly match
  bstring stats(order.stats_size(), 0);
  auto* stats_buf = const_cast<byte_type*>(stats.data());

  PrepareCollectors(order.buckets(), stats_buf, reader);

  filter_boost *= boost();

  auto& acceptor = options().acceptor;

  return acceptor ? memory::make_managed<column_prefix_existence_query>(
                      field(), std::move(stats), acceptor, filter_boost)
                  : memory::make_managed<column_existence_query>(
                      field(), std::move(stats), filter_boost);
}

}  // namespace iresearch
