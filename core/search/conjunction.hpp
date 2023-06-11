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

#pragma once

#include "analysis/token_attributes.hpp"
#include "search/cost.hpp"
#include "search/score.hpp"
#include "utils/attribute_helper.hpp"
#include "utils/type_limits.hpp"

namespace irs {

// Adapter to use doc_iterator with conjunction and disjunction.
template<typename DocIterator>
struct score_iterator_adapter {
  using doc_iterator_t = DocIterator;

  score_iterator_adapter() noexcept = default;
  score_iterator_adapter(doc_iterator_t&& it) noexcept
    : it{std::move(it)},
      doc{irs::get<irs::document>(*this->it)},
      score{&irs::score::get(*this->it)} {
    IRS_ASSERT(doc);
  }

  score_iterator_adapter(score_iterator_adapter&&) noexcept = default;
  score_iterator_adapter& operator=(score_iterator_adapter&&) noexcept =
    default;

  typename doc_iterator_t::element_type* operator->() const noexcept {
    return it.get();
  }

  const attribute* get(irs::type_info::type_id type) const noexcept {
    return it->get(type);
  }

  attribute* get_mutable(irs::type_info::type_id type) noexcept {
    return it->get_mutable(type);
  }

  operator doc_iterator_t&&() && noexcept { return std::move(it); }

  explicit operator bool() const noexcept { return it != nullptr; }

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
    : Merger{std::move(merger)}, itrs_{[](doc_iterators_t&& itrs) {
        IRS_ASSERT(!itrs.empty());

        // sort subnodes in ascending order by their cost
        std::sort(std::begin(itrs), std::end(itrs),
                  [](const auto& lhs, const auto& rhs) noexcept {
                    return cost::extract(lhs, cost::kMax) <
                           cost::extract(rhs, cost::kMax);
                  });
        // NRVO doesn't work for function parameters
        return std::move(itrs);
      }(std::move(itrs))} {
    IRS_ASSERT(!itrs_.empty());
    std::get<attribute_ptr<cost>>(attrs_) =
      irs::get_mutable<cost>(itrs_.front().it.get());

    if constexpr (HasScore_v<Merger>) {
      prepare_score();
    }
  }

  auto begin() const noexcept { return std::begin(itrs_); }
  auto end() const noexcept { return std::end(itrs_); }

  // size of conjunction
  size_t size() const noexcept { return itrs_.size(); }

  attribute* get_mutable(irs::type_info::type_id type) noexcept final {
    return irs::get_mutable(attrs_, type);
  }

  auto& doc() { return std::get<document>(attrs_); }
  auto& score() { return std::get<irs::score>(attrs_); }

  doc_id_t value() const final { return std::get<document>(attrs_).value; }

  bool next() override { return !doc_limits::eof(seek(value() + 1)); }

  IRS_NO_INLINE doc_id_t Seal() {
    leafs_doc_ = doc_limits::eof();
    doc().value = doc_limits::eof();
    score().leaf_max = {};
    score().list_max = {};
    return doc_limits::eof();
  }

  doc_id_t ShallowSeekImpl(doc_id_t target) {
    auto& doc_value = doc().value;
    auto& merger = static_cast<Merger&>(*this);
    if (target <= leafs_doc_) {
    score_check:
      if constexpr (HasScore_v<Merger>) {
        if (min_competitive_score_ < score().leaf_max) {
          return target;
        }
      } else {
        return target;
      }
      target = leafs_doc_ + !doc_limits::eof(leafs_doc_);
    }
    IRS_ASSERT(doc_value <= leafs_doc_);
  eof_check:
    if (IRS_UNLIKELY(doc_limits::eof(target))) {
      return target;
    }

    auto max_leafs = doc_limits::eof();
    auto min_leafs = doc_value;
    score_t max_leafs_score = {};

    for (auto& it : itrs_) {
      auto max_leaf = it->shallow_seek(target);
      auto min_leaf = it.doc->value;
      IRS_ASSERT(min_leaf <= max_leaf);
      if (target < min_leaf) {
        target = min_leaf;
      }
      if (max_leafs < min_leaf) {
        goto eof_check;
      }
      if (min_leafs < min_leaf) {
        min_leafs = min_leaf;
      }
      if (max_leafs > max_leaf) {
        max_leafs = max_leaf;
      }
      IRS_ASSERT(min_leafs <= max_leafs);
      if constexpr (HasScore_v<Merger>) {
        merger(&max_leafs_score, &it.score->leaf_max);
      }
    }

    leafs_doc_ = max_leafs;
    doc_value = min_leafs;
    score().leaf_max = max_leafs_score;
    IRS_ASSERT(doc_value <= target);
    IRS_ASSERT(target <= leafs_doc_);
    goto score_check;
  }

  doc_id_t shallow_seek(doc_id_t target) final {
    target = ShallowSeekImpl(target);
    if (IRS_UNLIKELY(doc_limits::eof(target))) {
      return Seal();
    }
    return leafs_doc_;
  }

  doc_id_t seek(doc_id_t target) override {
    auto& doc_value = doc().value;
    if (IRS_UNLIKELY(target <= doc_value)) {
      if (min_competitive_score_ < score_) {
        return doc_value;
      }
      target = doc_value + !doc_limits::eof(doc_value);
    }
    auto& merger = static_cast<Merger&>(*this);
  align_leafs:
    target = ShallowSeekImpl(target);
  align_docs:
    if (IRS_UNLIKELY(doc_limits::eof(target))) {
      return Seal();
    }
    auto it = itrs_.begin();
    const auto seek_target = (*it)->seek(target);
    if (seek_target > leafs_doc_) {
      target = seek_target;
      goto align_leafs;
    }
    if (IRS_UNLIKELY(doc_limits::eof(seek_target))) {
      return Seal();
    }
    ++it;
    const auto end = itrs_.end();
    do {
      target = (*it)->seek(seek_target);
      if (target != seek_target) {
        if (target > leafs_doc_) {
          goto align_leafs;
        }
        goto align_docs;
      }
      ++it;
    } while (it != end);
    doc_value = seek_target;

    if constexpr (HasScore_v<Merger>) {
      auto begin = std::begin(scores_);
      auto end = std::end(scores_);

      (**begin)(&score_);
      for (++begin; begin != end; ++begin) {
        (**begin)(merger.temp());
        merger(&score_, merger.temp());
      }
      if (min_competitive_score_ < score_) {
        return target;
      }
    } else {
      return target;
    }

    ++target;
    if (target > leafs_doc_) {
      goto align_leafs;
    }
    goto align_docs;
  }

 private:
  using attributes = std::tuple<document, attribute_ptr<cost>, irs::score>;

  void prepare_score() {
    IRS_ASSERT(Merger::size());

    auto& score = std::get<irs::score>(attrs_);

    // copy scores into separate container
    // to avoid extra checks
    scores_.reserve(itrs_.size());
    sumScores_ = {};
    minScores_ = std::numeric_limits<score_t>::max();
    for (auto& it : itrs_) {
      // FIXME(gnus): remove const cast
      auto* sub_score = const_cast<irs::score*>(it.score);
      IRS_ASSERT(score);  // ensured by score_iterator_adapter
      if (!sub_score->IsDefault()) {
        scores_.emplace_back(sub_score);
      }
      sumScores_ += sub_score->list_max;
      minScores_ = std::min(sub_score->list_max, minScores_);
    }
    score.Reset(*this, [](score_ctx* ctx, score_t* res) noexcept {
      auto& self = *static_cast<conjunction*>(ctx);
      *res = self.score_;
    });
    score.min_ = [](score_ctx* ctx, score_t min) noexcept {
      auto& self = *static_cast<conjunction*>(ctx);
      self.min_competitive_score_ = min;
      if (min <= self.minScores_) {
        return;
      }
      for (auto* score : self.scores_) {
        auto others = self.sumScores_ - score->list_max;
        if (min > others) {
          score->SetMin(min - others);
        }
      }
    };
  }

  attributes attrs_;
  doc_iterators_t itrs_;
  doc_id_t leafs_doc_{doc_limits::invalid()};
  score_t min_competitive_score_{};
  score_t score_{};
  score_t sumScores_{};
  score_t minScores_{std::numeric_limits<score_t>::max()};
  std::vector<irs::score*> scores_;  // valid sub-scores
};

// Returns conjunction iterator created from the specified sub iterators
template<typename Conjunction, typename Merger, typename... Args>
doc_iterator::ptr MakeConjunction(typename Conjunction::doc_iterators_t&& itrs,
                                  Merger&& merger, Args&&... args) {
  if (const auto size = itrs.size(); 0 == size) {
    // empty or unreachable search criteria
    return doc_iterator::empty();
  } else if (1 == size) {
    // single sub-query
    return std::move(itrs.front());
  }

  // conjunction
  return memory::make_managed<Conjunction>(
    std::move(itrs), std::forward<Merger>(merger), std::forward<Args>(args)...);
}

}  // namespace irs
