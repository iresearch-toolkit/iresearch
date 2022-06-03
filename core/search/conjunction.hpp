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

#ifndef IRESEARCH_CONJUNCTION_H
#define IRESEARCH_CONJUNCTION_H

#include "analysis/token_attributes.hpp"
#include "search/cost.hpp"
#include "search/score.hpp"
#include "utils/attribute_helper.hpp"
#include "utils/type_limits.hpp"

namespace iresearch {

// Adapter to use doc_iterator with conjunction and disjunction.
template<typename DocIterator>
struct score_iterator_adapter {
  typedef DocIterator doc_iterator_t;

  score_iterator_adapter() = default;
  score_iterator_adapter(doc_iterator_t&& it) noexcept
      : it(std::move(it)),
        doc(irs::get<irs::document>(*this->it)),
        score(&irs::score::get(*this->it)) {
    assert(doc);
  }

  score_iterator_adapter(score_iterator_adapter&&) = default;
  score_iterator_adapter& operator=(score_iterator_adapter&&) = default;

  typename doc_iterator_t::element_type* operator->() const noexcept {
    return it.get();
  }

  const attribute* get(irs::type_info::type_id type) const noexcept {
    return it->get(type);
  }

  attribute* get_mutable(irs::type_info::type_id type) noexcept {
    return it->get_mutable(type);
  }

  operator doc_iterator_t&&() noexcept { return std::move(it); }

  // access iterator value without virtual call
  doc_id_t value() const noexcept { return doc->value; }

  doc_iterator_t it;
  const irs::document* doc{};
  const irs::score* score{};
};

// Conjunction of N iterators
// -----------------------------------------------------------------------------
// c |  [0] <-- lead (the least cost iterator)
// o |  [1]    |
// s |  [2]    | tail (other iterators)
// t |  ...    |
//   V  [n] <-- end
// -----------------------------------------------------------------------------
template<typename DocIterator, typename Merger>
class conjunction : public doc_iterator, private Merger, private score_ctx {
 public:
  using merger_type = Merger;
  using doc_iterator_t = score_iterator_adapter<DocIterator>;
  using doc_iterators_t = std::vector<doc_iterator_t>;

  static_assert(std::is_nothrow_move_constructible_v<doc_iterator_t>,
                "default move constructor expected");

  explicit conjunction(doc_iterators_t&& itrs, Merger&& merger = Merger{})
      : Merger{std::move(merger)},
        itrs_{[](doc_iterators_t&& itrs) {
          assert(!itrs.empty());

          // sort subnodes in ascending order by their cost
          std::sort(std::begin(itrs), std::end(itrs),
                    [](const auto& lhs, const auto& rhs) {
                      return cost::extract(lhs, cost::kMax) <
                             cost::extract(rhs, cost::kMax);
                    });
#if defined(__GNUC__) && (__GNUC__ < 11)
          // Circumvent GCC10 compilation issue.
          return std::move(itrs);
#else
          return itrs;
#endif
        }(std::move(itrs))},
        front_{itrs_.front().it.get()},
        front_doc_{irs::get_mutable<document>(front_)} {
    assert(!itrs_.empty());
    assert(front_);
    assert(front_doc_);
    std::get<attribute_ptr<document>>(attrs_) =
        const_cast<document*>(front_doc_);
    std::get<attribute_ptr<cost>>(attrs_) = irs::get_mutable<cost>(front_);

    if constexpr (HasScore_v<Merger>) {
      prepare_score();
    }
  }

  auto begin() const noexcept { return std::begin(itrs_); }
  auto end() const noexcept { return std::end(itrs_); }

  // size of conjunction
  size_t size() const noexcept { return itrs_.size(); }

  virtual attribute* get_mutable(
      irs::type_info::type_id type) noexcept override final {
    return irs::get_mutable(attrs_, type);
  }

  virtual doc_id_t value() const override final { return front_doc_->value; }

  virtual bool next() override {
    if (!front_->next()) {
      return false;
    }

    return !doc_limits::eof(converge(front_doc_->value));
  }

  virtual doc_id_t seek(doc_id_t target) override {
    if (doc_limits::eof(target = front_->seek(target))) {
      return doc_limits::eof();
    }

    return converge(target);
  }

 private:
  using attributes =
      std::tuple<attribute_ptr<document>, attribute_ptr<cost>, score>;

  void prepare_score() {
    assert(Merger::size());

    auto& score = std::get<irs::score>(attrs_);

    // copy scores into separate container
    // to avoid extra checks
    scores_.reserve(itrs_.size());
    for (auto& it : itrs_) {
      // FIXME(gnus): remove const cast
      auto* score = const_cast<irs::score*>(it.score);
      assert(score);  // ensured by score_iterator_adapter
      if (*score != ScoreFunction::kDefault) {
        scores_.emplace_back(score);
      }
    }

    // prepare score
    switch (scores_.size()) {
      case 0:
        assert(score == ScoreFunction::kDefault);
        score = ScoreFunction::Default(Merger::size());
        break;
      case 1:
        score = std::move(*scores_.front());
        break;
      case 2:
        score.Reset(this, [](score_ctx* ctx, score_t* res) noexcept {
          // FIXME(gnusi)
          auto& self = *static_cast<conjunction*>(ctx);
          auto& merger = static_cast<Merger&>(self);
          (*self.scores_.front())(res);
          (*self.scores_.back())(merger.temp());
          merger(res, merger.temp());
        });
        break;
      case 3:
        score.Reset(this, [](score_ctx* ctx, score_t* res) noexcept {
          // FIXME(gnusi)
          auto& self = *static_cast<conjunction*>(ctx);
          auto& merger = static_cast<Merger&>(self);
          (*self.scores_.front())(res);
          (*self.scores_[1])(merger.temp());
          merger(res, merger.temp());
          (*self.scores_.back())(merger.temp());
          merger(res, merger.temp());
        });
        break;
      default:
        score.Reset(this, [](score_ctx* ctx, score_t* res) noexcept {
          // FIXME(gnusi)
          auto& self = *static_cast<conjunction*>(ctx);
          auto& merger = static_cast<Merger&>(self);
          auto begin = std::begin(self.scores_);
          auto end = std::end(self.scores_);

          std::memset(res, 0, merger.byte_size());
          (**begin)(res);
          for (++begin; begin != end; ++begin) {
            (**begin)(merger.temp());
            merger(res, merger.temp());
          }
        });
        break;
    }
  }

  // tries to converge front_ and other iterators to the specified target.
  // if it impossible tries to find first convergence place
  doc_id_t converge(doc_id_t target) {
    assert(!doc_limits::eof(target));

    for (auto rest = seek_rest(target); target != rest;
         rest = seek_rest(target)) {
      target = front_->seek(rest);
      if (doc_limits::eof(target)) {
        break;
      }
    }

    return target;
  }

  // seeks all iterators except the
  // first to the specified target
  doc_id_t seek_rest(doc_id_t target) {
    assert(!doc_limits::eof(target));

    for (auto it = itrs_.begin() + 1, end = itrs_.end(); it != end; ++it) {
      const auto doc = (*it)->seek(target);

      if (target < doc) {
        return doc;
      }
    }

    return target;
  }

  attributes attrs_;
  doc_iterators_t itrs_;
  std::vector<score*> scores_;  // valid sub-scores
  irs::doc_iterator* front_;
  const irs::document* front_doc_{};
};

// Returns conjunction iterator created from the specified sub iterators
template<typename Conjunction, typename... Args>
doc_iterator::ptr make_conjunction(typename Conjunction::doc_iterators_t&& itrs,
                                   Args&&... args) {
  switch (itrs.size()) {
    case 0:
      // empty or unreachable search criteria
      return doc_iterator::empty();
    case 1:
      // single sub-query
      return std::move(itrs.front());
  }

  // conjunction
  return memory::make_managed<Conjunction>(std::move(itrs),
                                           std::forward<Args>(args)...);
}

}  // namespace iresearch

#endif  // IRESEARCH_CONJUNCTION_H
