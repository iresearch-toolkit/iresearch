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
////////////////////////////////////////////////////////////////////////////////

#include "boolean_filter.hpp"

#include <boost/functional/hash.hpp>

#include "conjunction.hpp"
#include "disjunction.hpp"
#include "exclusion.hpp"
#include "min_match_disjunction.hpp"
#include "prepared_state_visitor.hpp"

namespace {

// first - pointer to the innermost not "not" node
// second - collapsed negation mark
std::pair<const irs::filter*, bool> optimize_not(const irs::Not& node) {
  bool neg = true;
  const irs::filter* inner = node.filter();
  while (inner && inner->type() == irs::type<irs::Not>::id()) {
    neg = !neg;
    inner = static_cast<const irs::Not*>(inner)->filter();
  }

  return std::make_pair(inner, neg);
}

// Returns disjunction iterator created from the specified queries
template<typename QueryIterator, typename... Args>
irs::doc_iterator::ptr make_disjunction(const irs::ExecutionContext& ctx,
                                        irs::sort::MergeType merge_type,
                                        QueryIterator begin, QueryIterator end,
                                        Args&&... args) {
  assert(std::distance(begin, end) >= 0);
  const size_t size = size_t(std::distance(begin, end));

  // check the size before the execution
  if (0 == size) {
    // empty or unreachable search criteria
    return irs::doc_iterator::empty();
  }

  std::vector<iresearch::score_iterator_adapter<irs::doc_iterator::ptr>> itrs;
  itrs.reserve(size);

  for (; begin != end; ++begin) {
    // execute query - get doc iterator
    auto docs = begin->execute(ctx);

    // filter out empty iterators
    if (!irs::doc_limits::eof(docs->value())) {
      itrs.emplace_back(std::move(docs));
    }
  }

  return irs::ResoveMergeType(
    merge_type, ctx.scorers.buckets().size(),
    [&]<typename A>(A&& aggregator) -> irs::doc_iterator::ptr {
      using disjunction_t =
        irs::disjunction_iterator<irs::doc_iterator::ptr, A>;

      return irs::MakeDisjunction<disjunction_t>(
        std::move(itrs), std::move(aggregator), std::forward<Args>(args)...);
    });
}

// Returns conjunction iterator created from the specified queries
template<typename QueryIterator, typename... Args>
irs::doc_iterator::ptr make_conjunction(const irs::ExecutionContext& ctx,
                                        irs::sort::MergeType merge_type,
                                        QueryIterator begin, QueryIterator end,
                                        Args&&... args) {
  assert(std::distance(begin, end) >= 0);
  const size_t size = std::distance(begin, end);

  // check size before the execution
  switch (size) {
    case 0:
      return irs::doc_iterator::empty();
    case 1:
      return begin->execute(ctx);
  }

  std::vector<irs::score_iterator_adapter<irs::doc_iterator::ptr>> itrs;
  itrs.reserve(size);

  for (; begin != end; ++begin) {
    auto docs = begin->execute(ctx);

    // filter out empty iterators
    if (irs::doc_limits::eof(docs->value())) {
      return irs::doc_iterator::empty();
    }

    itrs.emplace_back(std::move(docs));
  }

  return irs::ResoveMergeType(
    merge_type, ctx.scorers.buckets().size(),
    [&]<typename A>(A&& aggregator) -> irs::doc_iterator::ptr {
      using conjunction_t = irs::conjunction<irs::doc_iterator::ptr, A>;

      return irs::MakeConjunction<conjunction_t>(
        std::move(itrs), std::move(aggregator), std::forward<Args>(args)...);
    });
}

}  // namespace

namespace iresearch {

// Base class for boolean queries
class BooleanQuery : public filter::prepared {
 public:
  typedef std::vector<filter::prepared::ptr> queries_t;
  typedef ptr_iterator<queries_t::const_iterator> iterator;

  BooleanQuery() noexcept : excl_{0} {}

  doc_iterator::ptr execute(const ExecutionContext& ctx) const override {
    if (empty()) {
      return doc_iterator::empty();
    }

    assert(excl_);
    auto incl = execute(ctx, begin(), begin() + excl_);

    // exclusion part does not affect scoring at all
    auto excl = ::make_disjunction(
      {.segment = ctx.segment, .scorers = Order::kUnordered, .ctx = ctx.ctx},
      irs::sort::MergeType::kSum, begin() + excl_, end());

    // got empty iterator for excluded
    if (doc_limits::eof(excl->value())) {
      // pure conjunction/disjunction
      return incl;
    }

    return memory::make_managed<exclusion>(std::move(incl), std::move(excl));
  }

  void visit(const irs::sub_reader& segment, irs::PreparedStateVisitor& visitor,
             score_t boost) const override {
    boost *= this->boost();

    if (!visitor.Visit(*this, boost)) {
      return;
    }

    // FIXME(gnusi): visit exclude group?
    for (auto it = begin(), end = excl_begin(); it != end; ++it) {
      it->visit(segment, visitor, boost);
    }
  }

  virtual void prepare(const index_reader& rdr, const Order& ord, score_t boost,
                       sort::MergeType merge_type,
                       const attribute_provider* ctx,
                       std::span<const filter* const> incl,
                       std::span<const filter* const> excl) {
    BooleanQuery::queries_t queries;
    queries.reserve(incl.size() + excl.size());

    // apply boost to the current node
    this->boost(boost);

    // prepare included
    for (const auto* filter : incl) {
      queries.emplace_back(filter->prepare(rdr, ord, boost, ctx));
    }

    // prepare excluded
    for (const auto* filter : excl) {
      // exclusion part does not affect scoring at all
      queries.emplace_back(
        filter->prepare(rdr, Order::kUnordered, irs::kNoBoost, ctx));
    }

    // nothrow block
    queries_ = std::move(queries);
    excl_ = incl.size();
    merge_type_ = merge_type;
  }

  iterator begin() const { return iterator(std::begin(queries_)); }
  iterator excl_begin() const { return iterator(std::begin(queries_) + excl_); }
  iterator end() const { return iterator(std::end(queries_)); }

  bool empty() const { return queries_.empty(); }
  size_t size() const { return queries_.size(); }

 protected:
  virtual doc_iterator::ptr execute(const ExecutionContext& ctx, iterator begin,
                                    iterator end) const = 0;

  sort::MergeType merge_type() const noexcept { return merge_type_; }

 private:
  // 0..excl_-1 - included queries
  // excl_..queries.end() - excluded queries
  queries_t queries_;
  // index of the first excluded query
  size_t excl_;
  sort::MergeType merge_type_{sort::MergeType::kSum};
};

// Represent a set of queries joint by "And"
class AndQuery final : public BooleanQuery {
 public:
  doc_iterator::ptr execute(const ExecutionContext& ctx, iterator begin,
                            iterator end) const override {
    return ::make_conjunction(ctx, merge_type(), begin, end);
  }
};

// Represent a set of queries joint by "Or"
class OrQuery final : public BooleanQuery {
 public:
  doc_iterator::ptr execute(const ExecutionContext& ctx, iterator begin,
                            iterator end) const override {
    return ::make_disjunction(ctx, merge_type(), begin, end);
  }
};  // or_query

// Represent a set of queries joint by "Or" with the specified
// minimum number of clauses that should satisfy criteria
class MinMatchQuery final : public BooleanQuery {
 public:
  explicit MinMatchQuery(size_t min_match_count) noexcept
    : min_match_count_{min_match_count} {
    assert(min_match_count_ > 1);
  }

  virtual doc_iterator::ptr execute(const ExecutionContext& ctx, iterator begin,
                                    iterator end) const override {
    assert(std::distance(begin, end) >= 0);
    const size_t size = size_t(std::distance(begin, end));

    // 1 <= min_match_count
    size_t min_match_count = std::max(size_t(1), min_match_count_);

    // check the size before the execution
    if (0 == size || min_match_count > size) {
      // empty or unreachable search criteria
      return doc_iterator::empty();
    } else if (min_match_count == size) {
      // pure conjunction
      return ::make_conjunction(ctx, merge_type(), begin, end);
    }

    // min_match_count <= size
    min_match_count = std::min(size, min_match_count);

    std::vector<score_iterator_adapter<doc_iterator::ptr>> itrs(size);
    auto it = std::begin(itrs);
    for (; begin != end; ++begin) {
      // execute query - get doc iterator
      *it = begin->execute(ctx);

      // filter out empty iterators
      if (IRS_LIKELY(*it && !doc_limits::eof(it->value()))) {
        ++it;
      }
    }
    itrs.erase(it, std::end(itrs));

    return ResoveMergeType(
      merge_type(), ctx.scorers.buckets().size(),
      [&]<typename A>(A&& aggregator) -> doc_iterator::ptr {
        // FIXME(gnusi): use FAST version
        using disjunction_t = min_match_iterator<doc_iterator::ptr, A>;

        return MakeWeakDisjunction<disjunction_t, A>(
          std::move(itrs), min_match_count, std::move(aggregator));
      });
  }

 private:
  size_t min_match_count_;
};

boolean_filter::boolean_filter(const type_info& type) noexcept : filter{type} {}

size_t boolean_filter::hash() const noexcept {
  size_t seed = 0;

  ::boost::hash_combine(seed, filter::hash());
  std::for_each(
    filters_.begin(), filters_.end(),
    [&seed](const filter::ptr& f) { ::boost::hash_combine(seed, *f); });

  return seed;
}

bool boolean_filter::equals(const filter& rhs) const noexcept {
  const boolean_filter& typed_rhs = static_cast<const boolean_filter&>(rhs);

  return filter::equals(rhs) && filters_.size() == typed_rhs.size() &&
         std::equal(begin(), end(), typed_rhs.begin());
}

filter::prepared::ptr boolean_filter::prepare(
  const index_reader& rdr, const Order& ord, score_t boost,
  const attribute_provider* ctx) const {
  // determine incl/excl parts
  std::vector<const filter*> incl;
  std::vector<const filter*> excl;

  const auto all_docs_zero_boost = MakeAllDocsFilter(0.f);
  group_filters(*all_docs_zero_boost, incl, excl);

  const auto all_docs_no_boost = MakeAllDocsFilter(kNoBoost);
  if (incl.empty() && !excl.empty()) {
    // single negative query case
    incl.push_back(all_docs_no_boost.get());
  }

  return prepare(incl, excl, rdr, ord, boost, ctx);
}

void boolean_filter::group_filters(const filter& all_docs_no_boost,
                                   std::vector<const filter*>& incl,
                                   std::vector<const filter*>& excl) const {
  incl.reserve(size() / 2);
  excl.reserve(incl.capacity());

  const irs::filter* empty_filter{nullptr};
  const auto is_or = type() == irs::type<Or>::id();
  for (auto begin = this->begin(), end = this->end(); begin != end; ++begin) {
    if (irs::type<irs::empty>::id() == begin->type()) {
      empty_filter = &*begin;
      continue;
    }
    if (irs::type<Not>::id() == begin->type()) {
      const auto res = optimize_not(down_cast<Not>(*begin));

      if (!res.first) {
        continue;
      }

      if (res.second) {
        if (all_docs_no_boost == *res.first) {
          // not all -> empty result
          incl.clear();
          return;
        }
        excl.push_back(res.first);
        if (is_or) {
          // FIXME: this should have same boost as Not filter.
          // But for now we do not boost negation.
          incl.push_back(&all_docs_no_boost);
        }
      } else {
        incl.push_back(res.first);
      }
    } else {
      incl.push_back(&*begin);
    }
  }
  if (empty_filter != nullptr) {
    incl.push_back(empty_filter);
  }
}

And::And() noexcept : boolean_filter{irs::type<And>::get()} {}

filter::prepared::ptr And::prepare(std::vector<const filter*>& incl,
                                   std::vector<const filter*>& excl,
                                   const index_reader& rdr, const Order& ord,
                                   score_t boost,
                                   const attribute_provider* ctx) const {
  // optimization step
  //  if include group empty itself or has 'empty' -> this whole conjunction is
  //  empty
  if (incl.empty() || incl.back()->type() == irs::type<irs::empty>::id()) {
    return prepared::empty();
  }

  auto cumulative_all = MakeAllDocsFilter(kNoBoost);
  score_t all_boost{0};
  size_t all_count{0};
  for (auto filter : incl) {
    if (*filter == *cumulative_all) {
      all_count++;
      all_boost += filter->boost();
    }
  }
  if (all_count != 0) {
    const auto non_all_count = incl.size() - all_count;
    auto it = std::remove_if(incl.begin(), incl.end(),
                             [&cumulative_all](const irs::filter* filter) {
                               return *cumulative_all == *filter;
                             });
    incl.erase(it, incl.end());
    // Here And differs from Or. Last 'All' should be left in include group only
    // if there is more than one filter of other type. Otherwise this another
    // filter could be container for boost from 'all' filters
    if (1 == non_all_count) {
      // let this last filter hold boost from all removed ones
      // so we aggregate in external boost values from removed all filters
      // If we will not optimize resulting boost will be:
      //   boost * OR_BOOST * ALL_BOOST + boost * OR_BOOST * LEFT_BOOST
      // We could adjust only 'boost' so we recalculate it as
      // new_boost =  ( boost * OR_BOOST * ALL_BOOST + boost * OR_BOOST *
      // LEFT_BOOST) / (OR_BOOST * LEFT_BOOST) so when filter will be executed
      // resulting boost will be: new_boost * OR_BOOST * LEFT_BOOST. If we
      // substitute new_boost back we will get ( boost * OR_BOOST * ALL_BOOST +
      // boost * OR_BOOST * LEFT_BOOST) - original non-optimized boost value
      auto left_boost = (*incl.begin())->boost();
      if (this->boost() != 0 && left_boost != 0 && !ord.empty()) {
        boost = (boost * this->boost() * all_boost +
                 boost * this->boost() * left_boost) /
                (left_boost * this->boost());
      } else {
        boost = 0;
      }
    } else {
      // create new 'all' with boost from all removed
      cumulative_all->boost(all_boost);
      incl.push_back(cumulative_all.get());
    }
  }
  boost *= this->boost();
  if (1 == incl.size() && excl.empty()) {
    // single node case
    return incl.front()->prepare(rdr, ord, boost, ctx);
  }
  auto q = memory::make_managed<AndQuery>();
  q->prepare(rdr, ord, boost, merge_type(), ctx, incl, excl);
  return q;
}

Or::Or() noexcept : boolean_filter(irs::type<Or>::get()), min_match_count_(1) {}

filter::prepared::ptr Or::prepare(std::vector<const filter*>& incl,
                                  std::vector<const filter*>& excl,
                                  const index_reader& rdr, const Order& ord,
                                  score_t boost,
                                  const attribute_provider* ctx) const {
  // preparing
  boost *= this->boost();

  if (0 == min_match_count_) {  // only explicit 0 min match counts!
    // all conditions are satisfied
    return MakeAllDocsFilter(kNoBoost)->prepare(rdr, ord, boost, ctx);
  }

  if (!incl.empty() && incl.back()->type() == irs::type<irs::empty>::id()) {
    incl.pop_back();
  }

  if (incl.empty()) {
    return prepared::empty();
  }

  auto cumulative_all = MakeAllDocsFilter(kNoBoost);
  size_t optimized_match_count = 0;
  // Optimization steps

  score_t all_boost{0};
  size_t all_count{0};
  const irs::filter* incl_all{nullptr};
  for (auto filter : incl) {
    if (*filter == *cumulative_all) {
      all_count++;
      all_boost += filter->boost();
      incl_all = filter;
    }
  }
  if (all_count != 0) {
    if (ord.empty() && incl.size() > 1 && min_match_count_ <= all_count) {
      // if we have at least one all in include group - all other filters are
      // not necessary in case there is no scoring and 'all' count satisfies
      // min_match
      assert(incl_all != nullptr);
      incl.resize(1);
      incl.front() = incl_all;
      optimized_match_count = all_count - 1;
    } else {
      // Here Or differs from And. Last All should be left in include group
      auto it = std::remove_if(incl.begin(), incl.end(),
                               [&cumulative_all](const irs::filter* filter) {
                                 return *cumulative_all == *filter;
                               });
      incl.erase(it, incl.end());
      // create new 'all' with boost from all removed
      cumulative_all->boost(all_boost);
      incl.push_back(cumulative_all.get());
      optimized_match_count = all_count - 1;
    }
  }
  // check strictly less to not roll back to 0 min_match (we`ve handled this
  // case above!) single 'all' left -> it could contain boost we want to
  // preserve
  const auto adjusted_min_match_count =
    (optimized_match_count < min_match_count_)
      ? min_match_count_ - optimized_match_count
      : 1;

  if (adjusted_min_match_count > incl.size()) {
    // can't satisfy 'min_match_count' conditions
    // having only 'incl.size()' queries
    return prepared::empty();
  }

  if (1 == incl.size() && excl.empty()) {
    // single node case
    return incl.front()->prepare(rdr, ord, boost, ctx);
  }

  assert(adjusted_min_match_count > 0 &&
         adjusted_min_match_count <= incl.size());

  memory::managed_ptr<BooleanQuery> q;
  if (adjusted_min_match_count == incl.size()) {
    q = memory::make_managed<AndQuery>();
  } else if (1 == adjusted_min_match_count) {
    q = memory::make_managed<OrQuery>();
  } else {  // min_match_count > 1 && min_match_count < incl.size()
    q = memory::make_managed<MinMatchQuery>(adjusted_min_match_count);
  }

  q->prepare(rdr, ord, boost, merge_type(), ctx, incl, excl);
  return q;
}

Not::Not() noexcept : irs::filter{irs::type<Not>::get()} {}

filter::prepared::ptr Not::prepare(const index_reader& rdr, const Order& ord,
                                   score_t boost,
                                   const attribute_provider* ctx) const {
  const auto res = optimize_not(*this);

  if (!res.first) {
    return prepared::empty();
  }

  boost *= this->boost();

  if (res.second) {
    auto all_docs = MakeAllDocsFilter(kNoBoost);
    const std::array<const irs::filter*, 1> incl{all_docs.get()};
    const std::array<const irs::filter*, 1> excl{res.first};

    auto q = memory::make_managed<AndQuery>();
    q->prepare(rdr, ord, boost, sort::MergeType::kSum, ctx, incl, excl);
    return q;
  }

  // negation has been optimized out
  return res.first->prepare(rdr, ord, boost, ctx);
}

size_t Not::hash() const noexcept {
  size_t seed = 0;
  ::boost::hash_combine(seed, filter::hash());
  if (filter_) {
    ::boost::hash_combine<const irs::filter&>(seed, *filter_);
  }
  return seed;
}

bool Not::equals(const irs::filter& rhs) const noexcept {
  const Not& typed_rhs = static_cast<const Not&>(rhs);
  return filter::equals(rhs) &&
         ((!empty() && !typed_rhs.empty() && *filter_ == *typed_rhs.filter_) ||
          (empty() && typed_rhs.empty()));
}

}  // namespace iresearch
