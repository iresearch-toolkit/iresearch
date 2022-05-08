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

#ifndef IRESEARCH_MIN_MATCH_DISJUNCTION_H
#define IRESEARCH_MIN_MATCH_DISJUNCTION_H

#include "disjunction.hpp"

namespace iresearch {

////////////////////////////////////////////////////////////////////////////////
/// @class min_match_disjunction
///-----------------------------------------------------------------------------
///      [0] <-- begin
///      [1]      |
///      [2]      | head (min doc_id, cost heap)
///      [3]      |
///      [4] <-- lead_
/// c ^  [5]      |
/// o |  [6]      | lead (list of accepted iterators)
/// s |  ...      |
/// t |  [n] <-- end
///-----------------------------------------------------------------------------
////////////////////////////////////////////////////////////////////////////////
template<typename DocIterator, typename Merger>
class min_match_disjunction
    : public doc_iterator,
      private Merger,
      private score_ctx {
 public:
  struct cost_iterator_adapter : score_iterator_adapter<DocIterator> {
    cost_iterator_adapter(irs::doc_iterator::ptr&& it) noexcept
      : score_iterator_adapter<DocIterator>(std::move(it)) {
      est = cost::extract(*this->it, cost::MAX);
    }

    cost_iterator_adapter(cost_iterator_adapter&&) = default;
    cost_iterator_adapter& operator=(cost_iterator_adapter&&) = default;

    cost::cost_t est;
  }; // cost_iterator_adapter

  static_assert(std::is_nothrow_move_constructible<cost_iterator_adapter>::value,
                "default move constructor expected");

  typedef cost_iterator_adapter doc_iterator_t;
  typedef std::vector<doc_iterator_t> doc_iterators_t;

  min_match_disjunction(
      doc_iterators_t&& itrs,
      size_t min_match_count,
      Merger&& merger = Merger{})
    : Merger{std::move(merger)},
      itrs_(std::move(itrs)),
      min_match_count_(
        std::min(itrs_.size(), std::max(size_t(1), min_match_count))),
      lead_(itrs_.size()) {
    assert(!itrs_.empty());
    assert(min_match_count_ >= 1 && min_match_count_ <= itrs_.size());

    // sort subnodes in ascending order by their cost
    std::sort(
      itrs_.begin(), itrs_.end(),
      [](const doc_iterator_t& lhs, const doc_iterator_t& rhs) {
        return cost::extract(lhs, 0) < cost::extract(rhs, 0);
    });

    std::get<cost>(attrs_).reset([this](){
      return std::accumulate(
        itrs_.begin(), itrs_.end(), cost::cost_t(0),
        [](cost::cost_t lhs, const doc_iterator_t& rhs) {
          return lhs + cost::extract(rhs, 0);
        });
    });

    // prepare external heap
    heap_.resize(itrs_.size());
    std::iota(heap_.begin(), heap_.end(), size_t(0));

    prepare_score();
  }

  virtual attribute* get_mutable(type_info::type_id id) noexcept override {
    return irs::get_mutable(attrs_, id);
  }

  virtual doc_id_t value() const override {
    return std::get<document>(attrs_).value;
  }

  virtual bool next() override {
    auto& doc_ = std::get<document>(attrs_);

    if (doc_limits::eof(doc_.value)) {
      return false;
    }

    while (check_size()) {
      // start next iteration. execute next for all lead iterators
      // and move them to head
      if (!pop_lead()) {
        doc_.value = doc_limits::eof();
        return false;
      }

      // make step for all head iterators less or equal current doc (doc_)
      while (top().value() <= doc_.value) {
        const bool exhausted = top().value() == doc_.value
          ? !top()->next()
          : doc_limits::eof(top()->seek(doc_.value + 1));

        if (exhausted && !remove_top()) {
          doc_.value = doc_limits::eof();
          return false;
        } else {
          refresh_top();
        }
      }

      // count equal iterators
      const auto top = this->top().value();

      do {
        add_lead();
        if (lead_ >= min_match_count_) {
          return !doc_limits::eof(doc_.value = top);
        }
      } while (top == this->top().value());
    }

    doc_.value = doc_limits::eof();
    return false;
  }

  virtual doc_id_t seek(doc_id_t target) override {
    auto& doc_ = std::get<document>(attrs_);

    if (target <= doc_.value) {
      return doc_.value;
    }

    if (doc_limits::eof(doc_.value)) {
      return doc_.value;
    }

    /* execute seek for all lead iterators and
     * move one to head if it doesn't hit the target */
    for (auto it = lead(), end = heap_.end();it != end;) {
      assert(*it < itrs_.size());
      const auto doc = itrs_[*it]->seek(target);

      if (doc == target) {
        // got hit here
        ++it;
        continue;
      }

      if (doc_limits::eof(doc)) {
        --lead_;

        // iterator exhausted
        if (!remove_lead(it)) {
          return (doc_.value = doc_limits::eof());
        }

#ifdef _MSC_VER
        // Microsoft invalidates iterator
        it = lead();
#endif

        // update end
        end = heap_.end();
      } else { // doc != target
        // move back to head
        push_head(it);
        --lead_;
        ++it;
      }
    }

    // check if we still satisfy search criteria
    if (lead_ >= min_match_count_) {
      return doc_.value = target;
    }

    // main search loop
    for(;;target = top().value()) {
      while (top().value() <= target) {
        const auto doc = top()->seek(target);

        if (doc_limits::eof(doc)) {
          // iterator exhausted
          if (!remove_top()) {
            return (doc_.value = doc_limits::eof());
          }
        } else if (doc == target) {
          // valid iterator, doc == target
          add_lead();
          if (lead_ >= min_match_count_) {
            return (doc_.value = target);
          }
        } else {
          // invalid iterator, doc != target
          refresh_top();
        }
      }

      // can't find enough iterators equal to target here.
      // start next iteration. execute next for all lead iterators
      // and move them to head
      if (!pop_lead()) {
        return doc_.value = doc_limits::eof();
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief calculates total count of matched iterators. This value could be
  ///        greater than required min_match. All matched iterators points
  ///        to current matched document after this call.
  /// @returns total matched iterators count
  //////////////////////////////////////////////////////////////////////////////
  size_t match_count() {
    push_valid_to_lead();
    return lead_;
  }

 private:
  using attributes = std::tuple<document, cost, score>;

  void prepare_score() {
    if (!Merger::size()) {
      return;
    }

    auto& score = std::get<irs::score>(attrs_);
    score_buf_.resize(Merger::size()*sizeof(score_t));

    scores_vals_.resize(itrs_.size());
    score.Reset(this, [](score_ctx* ctx, score_t* res) noexcept {
      auto evaluate_score_iter = [](irs::score_t* res, auto& src) {
        auto* score = src.score;
        assert(score); // must be ensure by the adapter
        if (score->Func() != ScoreFunction::kDefaultScoreFunc) {
          (*score)(res);
        }
      };

      auto& self = *static_cast<min_match_disjunction*>(ctx);
      auto& merger = static_cast<Merger&>(self);
      auto* score_buf = reinterpret_cast<score_t*>(self.score_buf_.data());
      assert(!self.heap_.empty());

      self.push_valid_to_lead();

      // score lead iterators
      std::memset(res, 0, merger.size()*sizeof(score_t));
      std::for_each(
        self.lead(), self.heap_.end(),
        [&self, &evaluate_score_iter, &merger, res, score_buf](size_t it) {
          assert(it < self.itrs_.size());
          evaluate_score_iter(score_buf, self.itrs_[it]);
          merger(res, score_buf);
      });
    });
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief push all valid iterators to lead
  //////////////////////////////////////////////////////////////////////////////
  inline void push_valid_to_lead() {
    auto& doc_ = std::get<document>(attrs_);

    for(auto lead = this->lead(), begin = heap_.begin();
      lead != begin && top().value() <= doc_.value;) {
      // hitch head
      if (top().value() == doc_.value) {
        // got hit here
        add_lead();
        --lead;
      } else {
        if (doc_limits::eof(top()->seek(doc_.value))) {
          // iterator exhausted
          remove_top();
        } else {
          refresh_top();
        }
      }
    }
  }

  template<typename Iterator>
  inline void push(Iterator begin, Iterator end) {
    // lambda here gives ~20% speedup on GCC
    std::push_heap(begin, end, [this](const size_t lhs, const size_t rhs) noexcept {
      assert(lhs < itrs_.size());
      assert(rhs < itrs_.size());
      const auto& lhs_it = itrs_[lhs];
      const auto& rhs_it = itrs_[rhs];
      const auto lhs_doc = lhs_it.value();
      const auto rhs_doc = rhs_it.value();
      return (lhs_doc > rhs_doc || (lhs_doc == rhs_doc && lhs_it.est > rhs_it.est));
    });
  }

  template<typename Iterator>
  inline void pop(Iterator begin, Iterator end) {
    // lambda here gives ~20% speedup on GCC
    detail::pop_heap(begin, end, [this](const size_t lhs, const size_t rhs) noexcept {
      assert(lhs < itrs_.size());
      assert(rhs < itrs_.size());
      const auto& lhs_it = itrs_[lhs];
      const auto& rhs_it = itrs_[rhs];
      const auto lhs_doc = lhs_it.value();
      const auto rhs_doc = rhs_it.value();
      return (lhs_doc > rhs_doc || (lhs_doc == rhs_doc && lhs_it.est > rhs_it.est));
    });
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief perform step for each iterator in lead group and push it to head
  /// @returns true - if the min_match_count_ condition still can be satisfied,
  ///          false - otherwise
  //////////////////////////////////////////////////////////////////////////////
  bool pop_lead() {
    for (auto it = lead(), end = heap_.end(); it != end;) {
      assert(*it < itrs_.size());
      if (!itrs_[*it]->next()) {
        --lead_;

        // remove iterator
        if (!remove_lead(it)) {
          return false;
        }

#ifdef _MSC_VER
        // Microsoft invalidates iterator
        it = lead();
#endif

        // update end
        end = heap_.end();
      } else {
        // push back to head
        push(heap_.begin(), ++it);
        --lead_;
      }
    }

    return true;
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief removes iterator from the specified position in lead group
  ///        without moving iterators after the specified iterator
  /// @returns true - if the min_match_count_ condition still can be satisfied,
  ///          false - otherwise
  //////////////////////////////////////////////////////////////////////////////
  template<typename Iterator>
  inline bool remove_lead(Iterator it) {
    if (&*it != &heap_.back()) {
      std::swap(*it, heap_.back());
    }
    heap_.pop_back();
    return check_size();
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief removes iterator from the top of the head without moving
  ///        iterators after the specified iterator
  /// @returns true - if the min_match_count_ condition still can be satisfied,
  ///          false - otherwise
  //////////////////////////////////////////////////////////////////////////////
  inline bool remove_top() {
    auto lead = this->lead();
    pop(heap_.begin(), lead);
    return remove_lead(--lead);
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief refresh the value of the top of the head
  //////////////////////////////////////////////////////////////////////////////
  inline void refresh_top() {
    auto lead = this->lead();
    pop(heap_.begin(), lead);
    push(heap_.begin(), lead);
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief push the specified iterator from lead group to the head
  //         without movinh iterators after the specified iterator
  //////////////////////////////////////////////////////////////////////////////
  template<typename Iterator>
  inline void push_head(Iterator it) {
    Iterator lead = this->lead();
    if (it != lead) {
      std::swap(*it, *lead);
    }
    ++lead;
    push(heap_.begin(), lead);
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @returns true - if the min_match_count_ condition still can be satisfied,
  ///          false - otherwise
  //////////////////////////////////////////////////////////////////////////////
  inline bool check_size() const {
    return heap_.size() >= min_match_count_;
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @returns reference to the top of the head
  //////////////////////////////////////////////////////////////////////////////
  inline doc_iterator_t& top() {
    assert(!heap_.empty());
    assert(heap_.front() < itrs_.size());
    return itrs_[heap_.front()];
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @returns the first iterator in the lead group
  //////////////////////////////////////////////////////////////////////////////
  inline auto lead() {
    return heap_.end() - lead_;
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief adds iterator to the lead group
  //////////////////////////////////////////////////////////////////////////////
  inline void add_lead() {
    pop(heap_.begin(), lead());
    ++lead_;
  }

  doc_iterators_t itrs_; // sub iterators
  std::vector<size_t> heap_;
  mutable std::vector<const irs::score_t*> scores_vals_;
  size_t min_match_count_; // minimum number of hits
  size_t lead_; // number of iterators in lead group
  attributes attrs_;
  bstring score_buf_; // FIXME(gnusi): compile time size
};

} // ROOT

#endif // IRESEARCH_MIN_MATCH_DISJUNCTION_H
