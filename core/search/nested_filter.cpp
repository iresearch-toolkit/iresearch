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
#include "search/seek_prev.hpp"
#include "search/sort.hpp"
#include "utils/attribute_helper.hpp"
#include "utils/type_limits.hpp"

namespace iresearch {
template<template<typename> typename M>
struct HasScoreHelper<M<NoopAggregator>> : std::false_type {};
}  // namespace iresearch

namespace {

using namespace irs;

const Order& GetOrder(const auto& match, const Order& ord) noexcept {
  return std::visit(
      [&]<typename T>(const T& v) noexcept -> const Order& {
        static_assert(std::is_same_v<T, Match> ||
                      std::is_same_v<T, DocIteratorProvider>);

        if constexpr (std::is_same_v<T, Match>) {
          return kMatchNone == v ? Order::kUnordered : ord;
        }
        return ord;
      },
      match);
}

bool IsValid(const auto& match) noexcept {
  return std::visit(
      []<typename T>(const T& v) {
        static_assert(std::is_same_v<T, Match> ||
                      std::is_same_v<T, DocIteratorProvider>);

        if constexpr (std::is_same_v<T, Match>) {
          return v.Min <= v.Max;
        } else if constexpr (std::is_same_v<T, DocIteratorProvider>) {
          return nullptr != v;
        } else {
          return false;
        }
      },
      match);
}

class NoneMatcher;

template<typename Matcher>
class ChildToParentJoin final : public doc_iterator, private Matcher {
 public:
  ChildToParentJoin(doc_iterator::ptr&& parent, const seek_prev& prev_parent,
                    doc_iterator::ptr&& child, Matcher&& matcher) noexcept
      : Matcher{std::move(matcher)},
        parent_{std::move(parent)},
        child_{std::move(child)},
        prev_parent_{&prev_parent} {
    assert(parent_);
    assert(prev_parent);
    assert(child_);

    std::get<attribute_ptr<document>>(attrs_) =
        irs::get_mutable<irs::document>(parent_.get());
    assert(std::get<attribute_ptr<document>>(attrs_).ptr);

    child_doc_ = irs::get<irs::document>(*child_);

    std::get<attribute_ptr<cost>>(attrs_) =
        irs::get_mutable<cost>(child_.get());

    if constexpr (HasScore_v<Matcher>) {
      PrepareScore();
    }
  }

  doc_id_t value() const noexcept override {
    return std::get<attribute_ptr<document>>(attrs_).ptr->value;
  }

  attribute* get_mutable(irs::type_info::type_id id) override {
    return irs::get_mutable(attrs_, id);
  }

  doc_id_t seek(doc_id_t target) override {
    const auto& doc = *std::get<attribute_ptr<document>>(attrs_).ptr;

    if (IRS_UNLIKELY(target <= doc.value)) {
      return doc.value;
    }

    auto parent = parent_->seek(target);

    if (doc_limits::eof(parent)) {
      return doc_limits::eof();
    }

    return SeekInternal(parent);
  }

  bool next() override {
    if (IRS_LIKELY(parent_->next())) {
      return !doc_limits::eof(SeekInternal(value()));
    }

    return false;
  }

 private:
  friend Matcher;

  using Attributes =
      std::tuple<attribute_ptr<document>, attribute_ptr<cost>, score>;

  doc_id_t FirstChild() const {
    assert(!doc_limits::eof((*prev_parent_)()));
    return (*prev_parent_)() + 1;
  }

  doc_id_t SeekInternal(doc_id_t parent) {
    assert(!doc_limits::eof(parent));

    for (doc_id_t first_child = child_->seek(FirstChild());
         !Matcher::Accept(first_child, parent);
         first_child = child_->seek(FirstChild())) {
      parent = parent_->seek(first_child);

      if (doc_limits::eof(parent)) {
        return doc_limits::eof();
      }
    }

    return value();
  }

  void PrepareScore();

  doc_iterator::ptr parent_;
  doc_iterator::ptr child_;
  Attributes attrs_;
  const seek_prev* prev_parent_;
  const score* child_score_;
  const document* child_doc_;
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

  explicit ScoreBuffer(const Aggregator<Merger, Size>& merger) noexcept(
      !IsDynamic) {
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

  bool Accept(doc_id_t& child, doc_id_t parent) const noexcept {
    assert(!doc_limits::eof(parent));
    if (child < parent) {
      child = parent + 1;
      return false;
    }

    return true;
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

  constexpr bool Accept(doc_id_t child, doc_id_t parent) const noexcept {
    return child < parent;
  }

  ScoreFunction PrepareScore() {
    static_assert(HasScore_v<Merger>);

    return {this, [](score_ctx* ctx, score_t* res) {
              assert(ctx);
              assert(res);
              auto& self = static_cast<JoinType&>(*ctx);
              auto& merger = static_cast<Merger&>(self);

              auto& child = *self.child_;
              const auto parent_doc = self.value();
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
class PredMatcher : public Merger,
                    private ScoreBuffer<Merger>,
                    private score_ctx {
 public:
  using BufferType = ScoreBuffer<Merger>;
  using JoinType = ChildToParentJoin<PredMatcher<Merger>>;

  PredMatcher(Merger&& merger, doc_iterator::ptr&& pred) noexcept
      : Merger{std::move(merger)},
        BufferType{static_cast<const Merger&>(*this)},
        pred_{std::move(pred)} {
    if (IRS_UNLIKELY(!pred_)) {
      pred_ = doc_iterator::empty();
    }

    pred_doc_ = irs::get<document>(*pred_);
    assert(pred_doc_);
  }

  bool Accept(doc_id_t& first_child, doc_id_t parent_doc) {
    assert(!doc_limits::eof(parent_doc));

    auto& self = static_cast<JoinType&>(*this);
    auto& child = *self.child_;

    if (first_child >= parent_doc ||
        first_child != pred_->seek(self.FirstChild())) {
      first_child = parent_doc + 1;
      return false;
    }

    auto& merger = static_cast<Merger&>(*this);
    auto& buf = static_cast<BufferType&>(*this);

    const auto* child_doc = self.child_doc_;
    const auto& child_score = *self.child_score_;

    if constexpr (HasScore_v<Merger>) {
      child_score(buf.data());
    }
    while (child.next() && child_doc->value < parent_doc) {
      if (!pred_->next() || pred_doc_->value != child_doc->value) {
        first_child = parent_doc + 1;
        return false;
      }

      if constexpr (HasScore_v<Merger>) {
        child_score(merger.temp());
        merger(buf.data(), merger.temp());
      }
    }

    return true;
  }

  ScoreFunction PrepareScore() noexcept {
    static_assert(HasScore_v<Merger>);

    return {this, [](score_ctx* ctx, score_t* res) {
              assert(ctx);
              assert(res);
              auto& self = static_cast<PredMatcher&>(*ctx);
              auto& merger = static_cast<Merger&>(self);
              auto& buf = static_cast<ScoreBuffer<Merger>&>(self);
              std::memcpy(res, buf.data(), merger.byte_size());
            }};
  }

 private:
  doc_iterator::ptr pred_;
  const document* pred_doc_;
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

  bool Accept(doc_id_t& first_child, doc_id_t parent_doc) {
    assert(!doc_limits::eof(parent_doc));

    if (first_child >= parent_doc) {
      return false;
    }

    auto& self = static_cast<JoinType&>(*this);
    auto& merger = static_cast<Merger&>(*this);
    auto& buf = static_cast<BufferType&>(*this);

    auto& child = *self.child_;
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
        first_child = parent_doc + 1;
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

  bool Accept(doc_id_t first_child, doc_id_t parent_doc) {
    if (first_child >= parent_doc) {
      return false;
    }

    auto& self = static_cast<JoinType&>(*this);
    auto& merger = static_cast<Merger&>(*this);
    auto& buf = static_cast<BufferType&>(*this);

    auto& child = *self.child_;
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
              const auto parent_doc = self.value();
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
auto ResolveMatchType(const sub_reader& segment,
                      const std::variant<Match, DocIteratorProvider>& match,
                      score_t none_boost, A&& aggregator, Visitor&& visitor) {
  return std::visit(
      [&]<typename T>(const T& v) {
        static_assert(std::is_same_v<T, Match> ||
                      std::is_same_v<T, DocIteratorProvider>);

        if constexpr (std::is_same_v<T, Match>) {
          if (v == kMatchNone) {
            return visitor(
                NoneMatcher{std::forward<A>(aggregator), none_boost});
          } else if (v == kMatchAny) {
            return visitor(AnyMatcher<A>{std::forward<A>(aggregator)});
          } else if (v.IsMinMatch()) {
            assert(doc_limits::eof(v.Max));
            return visitor(MinMatcher<A>{v.Min, std::forward<A>(aggregator)});
          } else {
            return visitor(RangeMatcher<A>{v, std::forward<A>(aggregator)});
          }
        } else if constexpr (std::is_same_v<T, DocIteratorProvider>) {
          return visitor(
              PredMatcher<A>{std::forward<A>(aggregator), v(segment)});
        }
      },
      match);
}

class ByNesterQuery final : public filter::prepared {
 public:
  ByNesterQuery(DocIteratorProvider parent, prepared::ptr&& child,
                sort::MergeType merge_type,
                std::variant<Match, DocIteratorProvider> match,
                score_t none_boost) noexcept
      : parent_{std::move(parent)},
        child_{std::move(child)},
        match_{match},
        merge_type_{merge_type},
        none_boost_{none_boost} {
    assert(parent_);
    assert(child_);
    assert(IsValid(match_));
  }

  doc_iterator::ptr execute(const sub_reader& rdr, const Order& ord,
                            ExecutionMode mode,
                            const attribute_provider* ctx) const override;

 private:
  DocIteratorProvider parent_;
  prepared::ptr child_;
  std::variant<Match, DocIteratorProvider> match_;
  sort::MergeType merge_type_;
  score_t none_boost_;
};

doc_iterator::ptr ByNesterQuery::execute(const sub_reader& rdr,
                                         const Order& ord,
                                         ExecutionMode /*mode*/,
                                         const attribute_provider* ctx) const {
  auto parent = parent_(rdr);

  if (IRS_UNLIKELY(!parent || doc_limits::eof(parent->value()))) {
    return doc_iterator::empty();
  }

  const auto* prev = irs::get<irs::seek_prev>(*parent);

  if (IRS_UNLIKELY(!prev || !*prev)) {
    return doc_iterator::empty();
  }

  auto child =
      child_->execute(rdr, GetOrder(match_, ord), ExecutionMode::kAll, ctx);

  if (IRS_UNLIKELY(!child || doc_limits::eof(child->value()))) {
    return doc_iterator::empty();
  }

  return ResoveMergeType(
      merge_type_, ord.buckets().size(),
      [&]<typename A>(A&& aggregator) -> irs::doc_iterator::ptr {
        return ResolveMatchType(
            rdr, match_, none_boost_, std::forward<A>(aggregator),
            [&]<typename M>(M&& matcher) -> irs::doc_iterator::ptr {
              return memory::make_managed<ChildToParentJoin<M>>(
                  std::move(parent), *prev, std::move(child),
                  std::move(matcher));
            });
      });
}

}  // namespace

namespace iresearch {

filter::prepared::ptr ByNestedFilter::prepare(
    const index_reader& rdr, const Order& ord, score_t boost,
    const attribute_provider* ctx) const {
  auto& [parent, child, match, merge_type] = options();

  if (!parent || !child) {
    return prepared::empty();
  }

  if (!IsValid(match)) {
    return prepared::empty();
  }

  boost *= this->boost();

  auto prepared_child = child->prepare(rdr, GetOrder(match, ord), boost, ctx);

  if (!prepared_child) {
    return prepared::empty();
  }

  return memory::make_managed<ByNesterQuery>(parent, std::move(prepared_child),
                                             merge_type, match,
                                             /*none_boost*/ boost);
}

}  // namespace iresearch
