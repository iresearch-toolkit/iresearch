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

#include "nested_filter.hpp"

#include <tuple>

#include "analysis/token_attributes.hpp"
#include "search/cost.hpp"
#include "search/score.hpp"
#include "search/sort.hpp"
#include "utils/frozen_attributes.hpp"
#include "utils/type_limits.hpp"

namespace {

// 1 2 3 4 5 6 7
//
// c c c p c c p
// const auto prev = parent->seek_previous(target - 1);
// const auto firstChild = child->seek(prev + 1); // first target's child
// return parent->seek(firstChild + 1)
//
//
// p p c c c
// p c c c p c c
//

// do {
//   const auto parent = parent->seek(target)
//   const auto firstChild = child->seek(parent + 1);
//   return parent->seek_prev(firstChild - 1)
//
//   while (parent->next() && parent->value() < firstChild);
//
//   const auto next = parent->value();
//   child->seek(parent + 1);
//   if (child < next) {
//     break;
//   }
//   target = next;
// while (true);

// FIXME(gnusi): need to figure out the previous parent before target

// FIXME(gnusi):
// - tests for MinMatcher
// - add AvgMatcher
// - implement backwards seek for columnstore

using namespace irs;

class NoneMatcher;

template<typename Matcher>
class ChildToParentJoin final : public doc_iterator, private Matcher {
 public:
  ChildToParentJoin(doc_iterator::ptr&& parent, doc_iterator::ptr&& child,
                    Matcher&& matcher) noexcept
      : Matcher{std::move(matcher)},
        parent_{std::move(parent)},
        child_{std::move(child)} {
    assert(parent_);
    assert(child_);

    parent_doc_ = irs::get<irs::document>(*parent_);
    child_doc_ = irs::get<irs::document>(*child_);

    std::get<attribute_ptr<cost>>(attrs_) =
        irs::get_mutable<cost>(child_.get());

    if constexpr (HasScore_v<Matcher>) {
      PrepareScore();
    }
  }

  doc_id_t value() const noexcept override {
    return std::get<document>(attrs_).value;
  }

  attribute* get_mutable(irs::type_info::type_id id) override {
    return irs::get_mutable(attrs_, id);
  }

  doc_id_t seek(doc_id_t target) override {
    auto& doc = std::get<document>(attrs_);

    if (IRS_UNLIKELY(target <= doc.value)) {
      return doc.value;
    }

    auto parent = parent_->seek(target);

    if (doc_limits::eof(parent)) {
      doc.value = doc_limits::eof();
      return doc_limits::eof();
    }

    return SeekInternal(parent);
  }

  bool next() override {
    if (IRS_LIKELY(!doc_limits::eof(parent_doc_->value))) {
      return !doc_limits::eof(SeekInternal(parent_doc_->value));
    }

    std::get<document>(attrs_).value = doc_limits::eof();
    return false;
  }

 private:
  friend Matcher;

  using Attributes = std::tuple<document, attribute_ptr<cost>, score>;

  doc_id_t SeekInternal(doc_id_t parent) {
    assert(!doc_limits::eof(parent));
    auto& doc = std::get<document>(attrs_);

    // First valid child
    child_->seek(parent + 1);

    do {
      const auto child = child_doc_->value;
      assert(child > parent_doc_->value);

      if constexpr (std::is_same_v<Matcher, NoneMatcher>) {
        doc.value = parent_doc_->value;
        parent_->next();
      } else {
        if (doc_limits::eof(child)) {
          doc.value = doc_limits::eof();
          return doc_limits::eof();
        }

        do {
          doc.value = parent_doc_->value;
          parent_->next();
        } while (parent_doc_->value < child);
      }

    } while (!Matcher::Accept());

    return doc.value;
  }

  void PrepareScore();

  doc_iterator::ptr parent_;
  doc_iterator::ptr child_;
  Attributes attrs_;
  const score* child_score_;
  const document* child_doc_;
  const document* parent_doc_;
};

template<typename Matcher>
void ChildToParentJoin<Matcher>::PrepareScore() {
  auto& score = std::get<irs::score>(attrs_);
  child_score_ = irs::get<irs::score>(*child_);
  child_doc_ = irs::get<document>(*child_);

  if (!std::is_same_v<Matcher, NoneMatcher> &&
      (!child_doc_ || !child_score_ ||
       *child_score_ == ScoreFunction::kDefault)) {
    assert(Matcher::size());
    score = ScoreFunction::Default(Matcher::size());
  } else {
    static_assert(HasScore_v<Matcher>);
    score = static_cast<Matcher&>(*this).PrepareScore();
  }
}

template<typename Merger>
struct ScoreBuffer;

template<>
struct ScoreBuffer<NoopAggregator> {
  explicit ScoreBuffer(const NoopAggregator&) {}
};

template<typename Merger, size_t Size>
struct ScoreBuffer<Aggregator<Merger, Size>> {
  static constexpr bool IsDynamic = Size == std::numeric_limits<size_t>::max();

  using BufferType =
      std::conditional_t<IsDynamic, bstring, std::array<score_t, Size>>;

  explicit ScoreBuffer(const auto& merger) noexcept(!IsDynamic) {
    if constexpr (IsDynamic) {
      buf.resize(merger.byte_size());
    }
  }

  score_t* data() noexcept { return reinterpret_cast<score_t*>(buf.data()); }

  BufferType buf{};
};

class NoneMatcher : public NoopAggregator {
 public:
  using JoinType = ChildToParentJoin<NoneMatcher>;

  template<typename Merger>
  NoneMatcher(Merger&& merger, score_t none_boost) noexcept
      : boost_{none_boost}, size_{merger.size()} {}

  bool Accept() {
    auto& self = static_cast<JoinType&>(*this);

    if (self.parent_doc_->value <= self.child_doc_->value &&
        doc_limits::valid(self.value())) {
      return true;
    }

    self.child_->seek(self.parent_doc_->value);
    return false;
  }

  ScoreFunction PrepareScore() const {
    return ScoreFunction::Constant(boost_, size_);
  }

 private:
  score_t boost_;
  size_t size_;
};

template<typename Merger>
class AnyMatcher : public Merger, private score_ctx {
 public:
  using JoinType = ChildToParentJoin<AnyMatcher<Merger>>;

  explicit AnyMatcher(Merger&& merger) noexcept : Merger{std::move(merger)} {}

  constexpr bool Accept() const { return true; }

  ScoreFunction PrepareScore() {
    static_assert(HasScore_v<Merger>);

    return {this, [](score_ctx* ctx, score_t* res) {
              assert(ctx);
              assert(res);
              auto& self = static_cast<JoinType&>(*ctx);
              auto& merger = static_cast<Merger&>(self);

              auto& child = *self.child_;
              const auto parent_doc = self.parent_doc_->value;
              const auto* child_doc = self.child_doc_;
              const auto& child_score = *self.child_score_;

              child_score(res);
              while (child.next() && child_doc->value < parent_doc) {
                child_score(merger.temp());
                merger(res, merger.temp());
              }
            }};
  }
};

template<typename Merger>
class AllMatcher : public Merger,
                   private ScoreBuffer<Merger>,
                   private score_ctx {
 public:
  using BufferType = ScoreBuffer<Merger>;
  using JoinType = ChildToParentJoin<AllMatcher<Merger>>;

  explicit AllMatcher(Merger&& merger) noexcept
      : Merger{std::move(merger)},
        BufferType{static_cast<const Merger&>(*this)} {}

  bool Accept() {
    auto& self = static_cast<JoinType&>(*this);
    auto& merger = static_cast<Merger&>(*this);
    auto& buf = static_cast<BufferType&>(*this);

    auto& child = *self.child_;
    const auto parent_doc = self.parent_doc_->value;
    const auto* child_doc = self.child_doc_;
    const auto& child_score = *self.child_score_;

    auto prev = child_doc->value;

    if constexpr (HasScore_v<Merger>) {
      child_score(buf.data());
    }
    for (; child.next() && prev < parent_doc; prev = child_doc->value) {
      if (child_doc->value - prev != 1) {
        child.seek(parent_doc);
        return false;
      }

      if constexpr (HasScore_v<Merger>) {
        child_score(merger.temp());
        merger(buf.data(), merger.temp());
      }
    }

    return doc_limits::eof(parent_doc) ||
           (prev < parent_doc && parent_doc - prev == 1);
  }

  ScoreFunction PrepareScore() noexcept {
    static_assert(HasScore_v<Merger>);

    return {this, [](score_ctx* ctx, score_t* res) {
              assert(ctx);
              assert(res);
              auto& self = static_cast<AllMatcher&>(*ctx);
              auto& merger = static_cast<Merger&>(self);
              auto& buf = static_cast<ScoreBuffer<Merger>&>(self);
              std::memcpy(res, buf.data(), merger.byte_size());
            }};
  }
};

template<typename Merger>
class RangeMatcher : public Merger,
                     private ScoreBuffer<Merger>,
                     private score_ctx {
 public:
  using BufferType = ScoreBuffer<Merger>;
  using JoinType = ChildToParentJoin<RangeMatcher<Merger>>;

  RangeMatcher(Match match, Merger&& merger) noexcept
      : Merger{std::move(merger)},
        BufferType{static_cast<const Merger&>(*this)},
        match_{match} {}

  bool Accept() {
    auto& self = static_cast<JoinType&>(*this);
    auto& merger = static_cast<Merger&>(*this);
    auto& buf = static_cast<BufferType&>(*this);

    auto& child = *self.child_;
    const auto parent_doc = self.parent_doc_->value;
    const auto* child_doc = self.child_doc_;
    const auto& child_score = *self.child_score_;

    const auto [min, max] = match_;
    assert(min <= max);

    doc_id_t count = 0;

    if constexpr (HasScore_v<Merger>) {
      child_score(buf.data());
    }
    while (child.next() && child_doc->value < parent_doc) {
      if (++count > max) {
        child.seek(parent_doc);
        return false;
      }

      if constexpr (HasScore_v<Merger>) {
        child_score(merger.temp());
        merger(buf.data(), merger.temp());
      }
    }

    return min <= count;
  }

  ScoreFunction PrepareScore() noexcept {
    static_assert(HasScore_v<Merger>);

    return {this, [](score_ctx* ctx, score_t* res) {
              assert(ctx);
              assert(res);
              auto& self = static_cast<RangeMatcher&>(*ctx);
              auto& merger = static_cast<Merger&>(self);
              auto& buf = static_cast<ScoreBuffer<Merger>&>(self);
              std::memcpy(res, buf.data(), merger.byte_size());
            }};
  }

 private:
  const Match match_;
};

template<typename Merger>
class MinMatcher : public Merger,
                   private ScoreBuffer<Merger>,
                   private score_ctx {
 public:
  using BufferType = ScoreBuffer<Merger>;
  using JoinType = ChildToParentJoin<MinMatcher<Merger>>;

  MinMatcher(doc_id_t min, Merger&& merger) noexcept
      : Merger{std::move(merger)},
        BufferType{static_cast<const Merger&>(*this)},
        min_{min} {}

  bool Accept() {
    auto& self = static_cast<JoinType&>(*this);
    auto& merger = static_cast<Merger&>(*this);
    auto& buf = static_cast<BufferType&>(*this);

    auto& child = *self.child_;
    const auto parent_doc = self.parent_doc_->value;
    const auto* child_doc = self.child_doc_;
    const auto& child_score = *self.child_score_;

    doc_id_t count = min_;

    if constexpr (HasScore_v<Merger>) {
      child_score(buf.data());
    }
    while (child.next() && child_doc->value < parent_doc) {
      if constexpr (HasScore_v<Merger>) {
        child_score(merger.temp());
        merger(buf.data(), merger.temp());
      }

      if (!count) {
        return true;
      }

      --count;
    }

    return 0 == count;
  }

  ScoreFunction PrepareScore() noexcept {
    static_assert(HasScore_v<Merger>);

    return {this, [](score_ctx* ctx, score_t* res) {
              assert(ctx);
              assert(res);
              auto& self = static_cast<JoinType&>(*ctx);
              auto& merger = static_cast<Merger&>(self);
              auto& buf = static_cast<BufferType&>(self);

              auto& child = *self.child_;
              const auto parent_doc = self.parent_doc_->value;
              const auto* child_doc = self.child_doc_;
              const auto& child_score = *self.child_score_;

              while (child.next() && child_doc->value < parent_doc) {
                child_score(merger.temp());
                merger(buf.data(), merger.temp());
              }

              std::memcpy(res, buf.data(), merger.byte_size());
            }};
  }

 private:
  const doc_id_t min_;
};

template<typename A, typename Visitor>
auto ResolveMatchType(Match match, score_t none_boost, A&& aggregator,
                      Visitor&& visitor) {
  if (match == kMatchNone) {
    return visitor(NoneMatcher{std::forward<A>(aggregator), none_boost});
  } else if (match == kMatchAny) {
    return visitor(AnyMatcher<A>{std::forward<A>(aggregator)});
  } else if (match == kMatchAll) {
    return visitor(AllMatcher<A>{std::forward<A>(aggregator)});
  } else if (match.IsMinMatch()) {
    assert(doc_limits::eof(match.Max));
    return visitor(MinMatcher<A>{match.Min, std::forward<A>(aggregator)});
  } else {
    return visitor(RangeMatcher<A>{match, std::forward<A>(aggregator)});
  }
}

class ByNesterQuery final : public filter::prepared {
 public:
  ByNesterQuery(prepared::ptr&& parent, prepared::ptr&& child,
                sort::MergeType merge_type, Match match,
                score_t none_boost) noexcept
      : parent_{std::move(parent)},
        child_{std::move(child)},
        match_{match},
        merge_type_{merge_type},
        none_boost_{none_boost} {
    assert(parent_);
    assert(child_);
    assert(match_.Min <= match_.Max);
  }

  doc_iterator::ptr execute(const sub_reader& rdr, const Order& ord,
                            ExecutionMode mode,
                            const attribute_provider* ctx) const override;

 private:
  prepared::ptr parent_;
  prepared::ptr child_;
  Match match_;
  sort::MergeType merge_type_;
  score_t none_boost_;
};

doc_iterator::ptr ByNesterQuery::execute(const sub_reader& rdr,
                                         const Order& ord,
                                         ExecutionMode /*mode*/,
                                         const attribute_provider* ctx) const {
  auto parent =
      parent_->execute(rdr, Order::kUnordered, ExecutionMode::kAll, ctx);

  if (IRS_UNLIKELY(!parent || doc_limits::eof(parent->value()))) {
    return doc_iterator::empty();
  }

  const auto& order = kMatchNone == match_ ? Order::kUnordered : ord;
  auto child = child_->execute(rdr, order, ExecutionMode::kAll, ctx);

  if (IRS_UNLIKELY(!child || doc_limits::eof(child->value()))) {
    return doc_iterator::empty();
  }

  return ResoveMergeType(
      merge_type_, ord.buckets().size(),
      [&]<typename A>(A&& aggregator) -> irs::doc_iterator::ptr {
        return ResolveMatchType(
            match_, none_boost_, std::forward<A>(aggregator),
            [&]<typename M>(M&& matcher) -> irs::doc_iterator::ptr {
              return memory::make_managed<ChildToParentJoin<M>>(
                  std::move(parent), std::move(child), std::move(matcher));
            });
      });
}

}  // namespace

namespace iresearch {

template<template<typename> typename M>
struct HasScoreHelper<M<NoopAggregator>> : std::false_type {};

/*static*/ filter::ptr ByNestedFilter::make() {
  return memory::make_unique<ByNestedFilter>();
}

filter::prepared::ptr ByNestedFilter::prepare(
    const index_reader& rdr, const Order& ord, score_t boost,
    const attribute_provider* ctx) const {
  auto& [parent, child, match, merge_type] = options();

  if (match.Max < match.Min || !parent || !child) {
    return prepared::empty();
  }

  boost *= this->boost();
  const auto& order = kMatchNone == match ? ord : Order::kUnordered;

  auto prepared_parent = parent->prepare(rdr, Order::kUnordered, kNoBoost, ctx);
  auto prepared_child = child->prepare(rdr, order, boost, ctx);

  if (!prepared_parent || !prepared_child) {
    return prepared::empty();
  }

  return memory::make_managed<ByNesterQuery>(
      std::move(prepared_parent), std::move(prepared_child), merge_type, match,
      /*none_boost*/ boost);
}

}  // namespace iresearch
