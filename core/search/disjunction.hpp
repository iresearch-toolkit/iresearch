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

#ifndef IRESEARCH_DISJUNCTION_H
#define IRESEARCH_DISJUNCTION_H

#include <queue>

#include "conjunction.hpp"
#include "utils/std.hpp"
#include "utils/type_limits.hpp"
#include "index/iterators.hpp"
#include "utils/attribute_range.hpp"

NS_ROOT
NS_BEGIN(detail)

// Need this proxy since Microsoft has heap validity check in std::pop_heap.
// Our approach is to refresh top iterator (next or seek) and then remove it
// or move to lead. So we don't need this check.
// It is quite difficult to disable check since it managed by _ITERATOR_DEBUG_LEVEL
// macros which affects ABI (it must be the same for all libs and objs).
template<typename Iterator, typename Pred>
inline void pop_heap(Iterator first, Iterator last, Pred comp) {
  assert(first != last); // pop requires non-empty range

  #ifndef _MSC_VER
    std::pop_heap(first, last, comp);
  #elif _MSC_FULL_VER < 190024000 // < MSVC2015.3
    std::_Pop_heap(std::_Unchecked(first), std::_Unchecked(last), comp);
  #elif _MSC_FULL_VER < 191526726 // < MSVC2017.8
    std::_Pop_heap_unchecked(std::_Unchecked(first), std::_Unchecked(last), comp);
  #else
    std::_Pop_heap_unchecked(first._Unwrapped(), last._Unwrapped(), comp);
  #endif
}

template<typename DocIterator>
void evaluate_score_iter(const irs::byte_type**& pVal,  DocIterator& src) {
  const auto* score = src.score;
  assert(score);
  if (&irs::score::no_score() != score) {
    score->evaluate();
    *pVal++ = score->c_str();
  }
}


NS_END // detail

////////////////////////////////////////////////////////////////////////////////
/// @class position_score_iterator_adapter
/// @brief adapter to use doc_iterator with positions for disjunction
////////////////////////////////////////////////////////////////////////////////
template<typename DocIterator>
struct position_score_iterator_adapter : score_iterator_adapter<DocIterator> {

  typename attribute_view::ref<irs::position> attribute;

  position_score_iterator_adapter(typename position_score_iterator_adapter<DocIterator>::doc_iterator_t&& it) noexcept
    : score_iterator_adapter<DocIterator>(std::move(it)) {
    auto& attrs = this->it->attributes();
    position = irs::position::extract(attrs);
  }

  position_score_iterator_adapter(const position_score_iterator_adapter&) = default;
  position_score_iterator_adapter& operator=(const position_score_iterator_adapter&) = default;

  position_score_iterator_adapter(position_score_iterator_adapter&& rhs) noexcept
    : score_iterator_adapter<DocIterator>(std::move(rhs)),
      position(std::move(rhs.position)) {
  }

  position_score_iterator_adapter& operator=(position_score_iterator_adapter&& rhs) noexcept {
    if (this != &rhs) {
      score_iterator_adapter<DocIterator>::operator=(std::move(rhs));
      position = rhs.position;
    }
    return *this;
  }

  irs::position* position;
}; // position_score_iterator_adapter

template<typename Adapter>
class attribute_range_adapter {
 public:
  attribute_range_adapter(typename attribute_view::ref<attribute_range<Adapter>>::type& map_attribute_range) {
    map_attribute_range = &attribute_range_;
  }

 protected:
  attribute_range<Adapter> attribute_range_;
};

template<typename Adapter>
class unary_disjunction_attribute_range_adapter : public attribute_range_adapter<Adapter> {
 public:
  unary_disjunction_attribute_range_adapter(typename attribute_view::ref<attribute_range<Adapter>>::type& map_attribute_range)
    : attribute_range_adapter<Adapter>(map_attribute_range) {}

 protected:
  template<typename Iterator>
  void update_attribute_range(Iterator& /*it*/) {
    // see specializations
  }
};

template<typename Adapter>
class basic_disjunction_attribute_range_adapter : public attribute_range_adapter<Adapter> {
 public:
  basic_disjunction_attribute_range_adapter(typename attribute_view::ref<attribute_range<Adapter>>::type& map_attribute_range)
    : attribute_range_adapter<Adapter>(map_attribute_range) {}

 protected:
  template<typename Value, typename Iterator>
  void update_attribute_range(const Value /*value*/, Iterator& /*lhs*/, Iterator& /*rhs*/) {
    // see specializations
  }
};

template<typename Adapter>
class small_disjunction_attribute_range_adapter : public attribute_range_adapter<Adapter> {
 public:
  small_disjunction_attribute_range_adapter(typename attribute_view::ref<attribute_range<Adapter>>::type& map_attribute_range)
    : attribute_range_adapter<Adapter>(map_attribute_range) {}

 protected:
  template<typename Value, typename IteratorsContainer>
  void update_attribute_range(const Value /*value*/, IteratorsContainer& /*iterators_container*/) {
    // see specializations
  }
};

template<typename Adapter>
class disjunction_attribute_range_adapter : public attribute_range_adapter<Adapter> {
 public:
  disjunction_attribute_range_adapter(typename attribute_view::ref<attribute_range<Adapter>>::type& map_attribute_range)
    : attribute_range_adapter<Adapter>(map_attribute_range) {}

 protected:
  template<typename IndexesContainer, typename IteratorsContainer>
  void update_attribute_range(const IndexesContainer& /*indexes_container*/, IteratorsContainer& /*iterators_container*/) {
    // see specializations
  }
};

////////////////////////////////////////////////////////////////////////////////
/// @class unary_disjunction
////////////////////////////////////////////////////////////////////////////////
template<typename DocIterator, typename Adapter = score_iterator_adapter<DocIterator>>
class unary_disjunction final : public doc_iterator_base, unary_disjunction_attribute_range_adapter<Adapter> {
 public:
  typedef Adapter doc_iterator_t;

  unary_disjunction(doc_iterator_t&& it)
    : unary_disjunction_attribute_range_adapter<Adapter>(attrs_.emplace<irs::attribute_range<Adapter>>()),
      doc_(doc_limits::invalid()),
      it_(std::move(it)) {
    attrs_.emplace(doc_);
    if /*constexpr*/ (std::is_same<Adapter, position_score_iterator_adapter<DocIterator>>::value) {
      this->update_attribute_range(it_);
    }
  }

  virtual doc_id_t value() const noexcept override {
    return it_.value();
  }

  virtual bool next() override {
    return it_->next();
  }

  virtual doc_id_t seek(doc_id_t target) override {
    return it_->seek(target);
  }

 private:
  document doc_;
  doc_iterator_t it_;
}; // unary_disjunction

template<>
template<>
inline void unary_disjunction_attribute_range_adapter<position_score_iterator_adapter<doc_iterator::ptr>>::update_attribute_range(
    unary_disjunction<doc_iterator::ptr, position_score_iterator_adapter<doc_iterator::ptr>>::doc_iterator_t& it) {
  attribute_range_.set({&it});
}

////////////////////////////////////////////////////////////////////////////////
/// @class basic_disjunction
/// @brief use for special adapters only
////////////////////////////////////////////////////////////////////////////////
template<typename DocIterator, typename Adapter = score_iterator_adapter<DocIterator>>
class basic_disjunction final : public doc_iterator_base, score_ctx, basic_disjunction_attribute_range_adapter<Adapter> {
 public:
  typedef Adapter doc_iterator_t;

  basic_disjunction(
      doc_iterator_t&& lhs,
      doc_iterator_t&& rhs,
      const order::prepared& ord = order::prepared::unordered())
    : basic_disjunction(std::move(lhs), std::move(rhs), ord, resolve_overload_tag()) {
    // estimate disjunction
    estimate([this](){
      cost::cost_t est = 0;
      est += cost::extract(lhs_->attributes(), 0);
      est += cost::extract(rhs_->attributes(), 0);
      return est;
    });
  }

  basic_disjunction(
      doc_iterator_t&& lhs,
      doc_iterator_t&& rhs,
      const order::prepared& ord,
      cost::cost_t est)
    : basic_disjunction(std::move(lhs), std::move(rhs), ord, resolve_overload_tag()) {
    // estimate disjunction
    estimate(est);
  }

  virtual doc_id_t value() const noexcept override {
    return doc_.value;
  }

  virtual bool next() override {
    next_iterator_impl(lhs_);
    next_iterator_impl(rhs_);
    doc_.value = std::min(lhs_.value(), rhs_.value());
    if (doc_limits::eof(doc_.value)) {
      return false;
    }
    if /*constexpr*/ (std::is_same<Adapter, position_score_iterator_adapter<DocIterator>>::value) {
      this->update_attribute_range(doc_.value, lhs_, rhs_);
    }
    return true;
  }

  virtual doc_id_t seek(doc_id_t target) override {
    if (target <= doc_.value) {
      return doc_.value;
    }

    auto found = false;
    auto seek_l = seek_iterator_impl(lhs_, target);
    if /*constexpr*/ (std::is_same<Adapter, position_score_iterator_adapter<DocIterator>>::value) {
      if (seek_iterator_impl(rhs_, target) || seek_l) {
        doc_.value = target;
        found = true;
      }
    } else if (seek_l || seek_iterator_impl(rhs_, target)) {
      doc_.value = target;
      found = true;
    }
    if (!found) {
      doc_.value = std::min(lhs_.value(), rhs_.value());
    }
    if /*constexpr*/ (std::is_same<Adapter, position_score_iterator_adapter<DocIterator>>::value) {
      this->update_attribute_range(doc_.value, lhs_, rhs_);
    }
    return doc_.value;
  }

 private:
  struct resolve_overload_tag { };

  basic_disjunction(
      doc_iterator_t&& lhs,
      doc_iterator_t&& rhs,
      const order::prepared& ord,
      resolve_overload_tag)
    : basic_disjunction_attribute_range_adapter<Adapter>(attrs_.emplace<irs::attribute_range<Adapter>>()),
      lhs_(std::move(lhs)),
      rhs_(std::move(rhs)),
      doc_(doc_limits::invalid()),
      ord_(&ord) {
    // make 'document' attribute accessible from outside
    attrs_.emplace(doc_);
    // prepare score
    if (lhs_.score != &irs::score::no_score()
        && rhs_.score != &irs::score::no_score()) {
      // both sub-iterators has score
      scores_vals_[0] = lhs.score->c_str();
      scores_vals_[1] = rhs.score->c_str();
      prepare_score(ord, this, [](const score_ctx* ctx, byte_type* score) {
        auto& self = *static_cast<const basic_disjunction*>(ctx);

        const irs::byte_type** pVal = self.scores_vals_;
        size_t matched_iterators = (size_t)self.score_iterator_impl(self.lhs_);
        pVal += !matched_iterators;
        matched_iterators += (size_t)self.score_iterator_impl(self.rhs_);
        // always call merge. even if zero matched - we need to reset last accumulated score at least.
        self.ord_->merge(score, pVal, matched_iterators);
      });
    } else if (lhs_.score != &irs::score::no_score()) {
      // only left sub-iterator has score
      assert(rhs_.score == &irs::score::no_score());
      scores_vals_[0] = lhs.score->c_str();
      prepare_score(ord, this, [](const score_ctx* ctx, byte_type* score) {
        auto& self = *static_cast<const basic_disjunction*>(ctx);
        self.ord_->merge(score, self.scores_vals_, (size_t)self.score_iterator_impl(self.lhs_));
      });
    } else if (rhs_.score != &irs::score::no_score()) {
      // only right sub-iterator has score
      scores_vals_[0] = rhs.score->c_str();
      assert(lhs_.score == &irs::score::no_score());
      prepare_score(ord, this, [](const score_ctx* ctx, byte_type* score) {
        auto& self = *static_cast<const basic_disjunction*>(ctx);
        self.ord_->merge(score, self.scores_vals_, (size_t)self.score_iterator_impl(self.rhs_));
      });
    } else {
      assert(lhs_.score == &irs::score::no_score());
      assert(rhs_.score == &irs::score::no_score());
      prepare_score(ord, nullptr, [](const score_ctx*, byte_type*) {/*NOOP*/});
    }
  }

  bool seek_iterator_impl(doc_iterator_t& it, doc_id_t target) {
    return it.value() < target && target == it->seek(target);
  }

  void next_iterator_impl(doc_iterator_t& it) {
    const auto doc = it.value();

    if (doc_ == doc) {
      it->next();
    } else if (doc < doc_.value) {
      assert(!doc_limits::eof(doc_.value));
      it->seek(doc_.value + 1);
    }
  }

  bool score_iterator_impl(doc_iterator_t& it) const {
    auto doc = it.value();

    if (doc < doc_.value) {
      doc = it->seek(doc_.value);
    }

    if (doc == doc_.value) {
      const auto* rhs = it.score;
      rhs->evaluate();
      return true;
    }
    return false;
  }

  mutable doc_iterator_t lhs_;
  mutable doc_iterator_t rhs_;
  mutable const irs::byte_type* scores_vals_[2];
  document doc_;
  const order::prepared* ord_;
}; // basic_disjunction

template<>
template<>
inline void basic_disjunction_attribute_range_adapter<position_score_iterator_adapter<doc_iterator::ptr>>::update_attribute_range(
    const doc_id_t value,
    basic_disjunction<doc_iterator::ptr, position_score_iterator_adapter<doc_iterator::ptr>>::doc_iterator_t& lhs,
    basic_disjunction<doc_iterator::ptr, position_score_iterator_adapter<doc_iterator::ptr>>::doc_iterator_t& rhs) {
  attribute_range<position_score_iterator_adapter<doc_iterator::ptr>>::iterators_t iters;
  if (lhs->value() == value) {
    iters.emplace_back(&lhs);
  }
  if (rhs->value() == value) {
    iters.emplace_back(&rhs);
  }
  attribute_range_.set(std::move(iters));
}

////////////////////////////////////////////////////////////////////////////////
/// @class small_disjunction
/// @brief linear search based disjunction
////////////////////////////////////////////////////////////////////////////////
template<typename DocIterator, typename Adapter = score_iterator_adapter<DocIterator>>
class small_disjunction : public doc_iterator_base, score_ctx, small_disjunction_attribute_range_adapter<Adapter> {
 public:
  typedef Adapter doc_iterator_t;
  typedef std::vector<doc_iterator_t> doc_iterators_t;

  small_disjunction(
      doc_iterators_t&& itrs,
      const order::prepared& ord,
      cost::cost_t est)
    : small_disjunction(std::move(itrs), ord, resolve_overload_tag()) {
    // estimate disjunction
    estimate(est);
  }

  explicit small_disjunction(
      doc_iterators_t&& itrs,
      const order::prepared& ord = order::prepared::unordered())
    : small_disjunction(std::move(itrs), ord, resolve_overload_tag()) {
    // estimate disjunction
    estimate([this](){
      return std::accumulate(
        itrs_.begin(), itrs_.end(), cost::cost_t(0),
        [](cost::cost_t lhs, const doc_iterator_t& rhs) {
          return lhs + cost::extract(rhs->attributes(), 0);
      });
    });
  }

  virtual doc_id_t value() const noexcept override {
    return doc_.value;
  }

  bool next_iterator_impl(doc_iterator_t& it) {
    const auto doc = it.value();

    if (doc == doc_.value) {
      return it->next();
    } else if (doc < doc_.value) {
      return !doc_limits::eof(it->seek(doc_.value+1));
    }

    return true;
  }

  virtual bool next() override {
    if (doc_limits::eof(doc_.value)) {
      return false;
    }

    doc_id_t min = doc_limits::eof();

    for (auto begin = itrs_.begin(); begin != itrs_.end(); ) {
      auto& it = *begin;
      if (!next_iterator_impl(it)) {
        if (!remove_iterator(it)) {
          doc_ = doc_limits::eof();
          return false;
        }
#if defined(_MSC_VER) && defined(IRESEARCH_DEBUG)
        // workaround for Microsoft checked iterators
        begin = itrs_.begin() + std::distance(itrs_.data(), &it);
#endif
      } else {
        min = std::min(min, it.value());
        ++begin;
      }
    }

    doc_ = min;

    if /*constexpr*/ (std::is_same<Adapter, position_score_iterator_adapter<DocIterator>>::value) {
      this->update_attribute_range(doc_.value, itrs_);
    }

    return true;
  }

  virtual doc_id_t seek(doc_id_t target) override {
    if (doc_limits::eof(doc_.value)) {
      return doc_.value;
    }

    doc_id_t min = doc_limits::eof();

    auto begin = itrs_.begin();
    while (begin != itrs_.end()) {
      auto& it = *begin;

      if (it.value() < target) {
        const auto doc = it->seek(target);

        if (doc == target) {
          min = doc;
          break;
        } else if (doc_limits::eof(doc)) {
          if (!remove_iterator(it)) {
            // exhausted
            return doc_.value = doc_limits::eof();
          }
#if defined(_MSC_VER) && defined(IRESEARCH_DEBUG)
          // workaround for Microsoft checked iterators
          begin = itrs_.begin() + std::distance(itrs_.data(), &it);
#endif
          continue; // don't need to increment 'begin' here
        }
      }

      min = std::min(min, it.value());
      ++begin;
    }

    doc_.value = min;

    if /*constexpr*/ (std::is_same<Adapter, position_score_iterator_adapter<DocIterator>>::value) {
      hitch_remaining_iterators(target, begin);
      this->update_attribute_range(doc_.value, itrs_);
    }

    return doc_.value;
  }

 private:
  struct resolve_overload_tag{};

  small_disjunction(
      doc_iterators_t&& itrs,
      const order::prepared& ord,
      resolve_overload_tag)
    : small_disjunction_attribute_range_adapter<Adapter>(attrs_.emplace<irs::attribute_range<Adapter>>()),
      itrs_(std::move(itrs)),
      doc_(itrs_.empty()
        ? doc_limits::eof()
        : doc_limits::invalid()),
      ord_(&ord) {
    // copy iterators with scores into separate container
    // to avoid extra checks
    scored_itrs_.reserve(itrs_.size());
    for (auto& it : itrs_) {
      if (&irs::score::no_score() != it.score) {
        scored_itrs_.emplace_back(it);
      }
    }
    scores_vals_.resize(scored_itrs_.size());
    // make 'document' attribute accessible from outside
    attrs_.emplace(doc_);

    // prepare score
    if (scored_itrs_.empty()) {
      prepare_score(ord, nullptr, [](const irs::score_ctx*, byte_type*){ /*NOOP*/ });
    } else {
      prepare_score(ord, this, [](const irs::score_ctx* ctx, byte_type* score) {
        auto& self = *static_cast<const small_disjunction*>(ctx);
        const irs::byte_type** pVal = self.scores_vals_.data();
        for (auto& it : self.scored_itrs_) {
          auto doc = it.value();

          if (doc < self.doc_.value) {
            doc = it->seek(self.doc_.value);
          }

          if (doc == self.doc_.value) {
            it.score->evaluate();
            *pVal++ = it.score->c_str();
          }
        }
        self.ord_->merge(score, self.scores_vals_.data(), std::distance(self.scores_vals_.data(), pVal));
      });
    }
  }

  bool remove_iterator(doc_iterator_t& it) {
    std::swap(it, itrs_.back());
    itrs_.pop_back();
    return !itrs_.empty();
  }

  void hitch_remaining_iterators(doc_id_t target, typename doc_iterators_t::iterator begin) {
    while (begin != itrs_.end()) {
      auto& it = *begin;
      if (it.value() < target) {
        const auto doc = it->seek(target);

        if (doc_limits::eof(doc)) {
          #ifdef IRESEARCH_DEBUG
            assert(remove_iterator(it));
          #else
            remove_iterator(it);
          #endif
#if defined(_MSC_VER) && defined(IRESEARCH_DEBUG)
          // workaround for Microsoft checked iterators
          begin = itrs_.begin() + std::distance(itrs_.data(), &it);
#endif
          continue; // don't need to increment 'begin' here
        }
      }
      ++begin;
    }
  }

  doc_iterators_t itrs_;
  doc_iterators_t scored_itrs_; // iterators with scores
  document doc_;
  mutable std::vector<const irs::byte_type*> scores_vals_;
  const order::prepared* ord_;
}; // small_disjunction

template<>
template<>
inline void small_disjunction_attribute_range_adapter<position_score_iterator_adapter<doc_iterator::ptr>>::update_attribute_range(
    const doc_id_t value,
    small_disjunction<doc_iterator::ptr, position_score_iterator_adapter<doc_iterator::ptr>>::doc_iterators_t& itrs) {
  attribute_range<position_score_iterator_adapter<doc_iterator::ptr>>::iterators_t iters;
  for (auto& it : itrs) {
    if (it->value() == value) {
      iters.emplace_back(&it);
    }
  }
  attribute_range_.set(std::move(iters));
}

////////////////////////////////////////////////////////////////////////////////
/// @class disjunction
/// @brief heap sort based disjunction
/// ----------------------------------------------------------------------------
///   [0]   <-- begin
///   [1]      |
///   [2]      | head (min doc_id heap)
///   ...      |
///   [n-1] <-- end
///   [n]   <-- lead (accepted iterator)
/// ----------------------------------------------------------------------------
////////////////////////////////////////////////////////////////////////////////
template<typename DocIterator, typename Adapter = score_iterator_adapter<DocIterator>>
class disjunction : public doc_iterator_base, score_ctx, disjunction_attribute_range_adapter<Adapter> {
 public:
  typedef small_disjunction<DocIterator, Adapter> small_disjunction_t;
  typedef basic_disjunction<DocIterator, Adapter> basic_disjunction_t;
  typedef unary_disjunction<DocIterator, Adapter> unary_disjunction_t;
  typedef Adapter doc_iterator_t;
  typedef std::vector<doc_iterator_t> doc_iterators_t;

  typedef std::vector<size_t> heap_container;
  typedef heap_container::iterator heap_iterator;

  disjunction(
      doc_iterators_t&& itrs,
      const order::prepared& ord,
      cost::cost_t est)
    : disjunction(std::move(itrs), ord, resolve_overload_tag()) {
    // estimate disjunction
    estimate(est);
  }

  explicit disjunction(
      doc_iterators_t&& itrs,
      const order::prepared& ord = order::prepared::unordered())
    : disjunction(std::move(itrs), ord, resolve_overload_tag()) {
    // estimate disjunction
    estimate([this](){
      return std::accumulate(
        itrs_.begin(), itrs_.end(), cost::cost_t(0),
        [](cost::cost_t lhs, const doc_iterator_t& rhs) {
          return lhs + cost::extract(rhs->attributes(), 0);
      });
    });
  }

  virtual doc_id_t value() const noexcept override {
    return doc_.value;
  }

  virtual bool next() override {
    if (doc_limits::eof(doc_.value)) {
      return false;
    }

    while (lead().value() <= doc_.value) {
      bool const exhausted = lead().value() == doc_.value
        ? !lead()->next()
        : doc_limits::eof(lead()->seek(doc_.value + 1));

      if (exhausted && !remove_lead()) {
        doc_.value = doc_limits::eof();
        return false;
      } else {
        refresh_lead();
      }
    }

    doc_.value = lead().value();

    if /*constexpr*/ (std::is_same<Adapter, position_score_iterator_adapter<DocIterator>>::value) {
      hitch_all_iterators();
      this->update_attribute_range(heap_, itrs_);
    }
    return true;
  }

  virtual doc_id_t seek(doc_id_t target) override {
    if (doc_limits::eof(doc_.value)) {
      return doc_.value;
    }

    while (lead().value() < target) {
      const auto doc = lead()->seek(target);

      if (doc_limits::eof(doc) && !remove_lead()) {
        return doc_.value = doc_limits::eof();
      } else if (doc != target) {
        refresh_lead();
      }
    }

    doc_.value = lead().value();

    if /*constexpr*/ (std::is_same<Adapter, position_score_iterator_adapter<DocIterator>>::value) {
      hitch_all_iterators();
      this->update_attribute_range(heap_, itrs_);
    }

    return doc_.value;
  }

 private:
  struct resolve_overload_tag{};

  disjunction(
      doc_iterators_t&& itrs,
      const order::prepared& ord,
      resolve_overload_tag)
    : disjunction_attribute_range_adapter<Adapter>(attrs_.emplace<irs::attribute_range<Adapter>>()),
      itrs_(std::move(itrs)),
      doc_(itrs_.empty()
        ? doc_limits::eof()
        : doc_limits::invalid()),
      ord_(&ord) {
    // since we are using heap in order to determine next document,
    // in order to avoid useless make_heap call we expect that all
    // iterators are equal here */
    //assert(irstd::all_equal(itrs_.begin(), itrs_.end()));

    // make 'document' attribute accessible from outside
    attrs_.emplace(doc_);

    // prepare external heap
    heap_.resize(itrs_.size());
    std::iota(heap_.begin(), heap_.end(), size_t(0));
    scores_vals_.resize(itrs_.size(), nullptr);

    // prepare score
    prepare_score(ord, this, [](const score_ctx* ctx, byte_type* score) {
      auto& self = const_cast<disjunction&>(*static_cast<const disjunction*>(ctx));
      self.score_impl(score);
    });
  }

  template<typename Iterator>
  inline void push(Iterator begin, Iterator end) {
    // lambda here gives ~20% speedup on GCC
    std::push_heap(begin, end, [this](const size_t lhs, const size_t rhs) noexcept {
      assert(lhs < itrs_.size());
      assert(rhs < itrs_.size());
      return itrs_[lhs].value() > itrs_[rhs].value();
    });
  }

  template<typename Iterator>
  inline void pop(Iterator begin, Iterator end) {
    // lambda here gives ~20% speedup on GCC
    detail::pop_heap(begin, end, [this](const size_t lhs, const size_t rhs) noexcept {
      assert(lhs < itrs_.size());
      assert(rhs < itrs_.size());
      return itrs_[lhs].value() > itrs_[rhs].value();
    });
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief removes lead iterator
  /// @returns true - if the disjunction condition still can be satisfied,
  ///          false - otherwise
  //////////////////////////////////////////////////////////////////////////////
  inline bool remove_lead() {
    heap_.pop_back();

    if (!heap_.empty()) {
      pop(heap_.begin(), heap_.end());
      return true;
    }

    return false;
  }

  inline void refresh_lead() {
    auto begin = heap_.begin(), end = heap_.end();
    push(begin, end);
    pop(begin, end);
  }

  inline doc_iterator_t& lead() noexcept {
    assert(!heap_.empty());
    assert(heap_.back() < itrs_.size());
    return itrs_[heap_.back()];
  }

  inline doc_iterator_t& top() noexcept {
    assert(!heap_.empty());
    assert(heap_.front() < itrs_.size());
    return itrs_[heap_.front()];
  }

  std::pair<heap_iterator, heap_iterator> hitch_all_iterators() {
    // hitch all iterators in head to the lead (current doc_)
    auto begin = heap_.begin(), end = heap_.end()-1;

    while (begin != end && top().value() < doc_.value) {
      const auto doc = top()->seek(doc_.value);

      if (doc_limits::eof(doc)) {
        // remove top
        pop(begin,end);
        std::swap(*--end, heap_.back());
        heap_.pop_back();
      } else {
        // refresh top
        pop(begin,end);
        push(begin,end);
      }
    }
    return {begin, end};
  }

  void score_impl(byte_type* lhs) {
    assert(!heap_.empty());

    auto its = hitch_all_iterators();
    const irs::byte_type** pVal = scores_vals_.data();
    detail::evaluate_score_iter(pVal, lead());
    if (top().value() == doc_.value) {
      irstd::heap::for_each_if(
        its.first, its.second,
        [this](const size_t it) {
          assert(it < itrs_.size());
          return itrs_[it].value() == doc_.value;
        },
        [this, lhs, &pVal](size_t it) {
          assert(it < itrs_.size());
          detail::evaluate_score_iter(pVal, itrs_[it]);
      });
    }
    ord_->merge(lhs, scores_vals_.data(), std::distance(scores_vals_.data(), pVal));
  }

  doc_iterators_t itrs_;
  heap_container heap_;
  mutable std::vector<const irs::byte_type*> scores_vals_;
  document doc_;
  const order::prepared* ord_;
}; // disjunction

template<>
template<>
inline void disjunction_attribute_range_adapter<position_score_iterator_adapter<doc_iterator::ptr>>::update_attribute_range(
    const disjunction<doc_iterator::ptr, position_score_iterator_adapter<doc_iterator::ptr>>::heap_container& heap,
    disjunction<doc_iterator::ptr, position_score_iterator_adapter<doc_iterator::ptr>>::doc_iterators_t& itrs) {
  attribute_range<position_score_iterator_adapter<doc_iterator::ptr>>::iterators_t iters;
  auto& lead = itrs[heap.back()];
  iters.emplace_back(&lead);
  if (heap.size() > 1) {
    auto value = lead.value();
    irstd::heap::for_each_if(
      heap.cbegin(),
      heap.cend()-1,
      [&itrs, value](const size_t it) {
        assert(it < itrs.size());
        return itrs[it].value() == value;
      },
      [&itrs, &iters](const size_t it) {
        assert(it < itrs.size());
        iters.emplace_back(&itrs[it]);
      });
  }
  attribute_range_.set(std::move(iters));
}

//////////////////////////////////////////////////////////////////////////////
/// @returns disjunction iterator created from the specified sub iterators
//////////////////////////////////////////////////////////////////////////////
template<typename Disjunction, typename... Args>
doc_iterator::ptr make_disjunction(
    typename Disjunction::doc_iterators_t&& itrs,
    Args&&... args) {
  const auto size = itrs.size();

  switch (size) {
    case 0:
      // empty or unreachable search criteria
      return doc_iterator::empty();
    case 1:
      if /*constexpr*/ (std::is_same<typename Disjunction::doc_iterators_t::value_type,
          position_score_iterator_adapter<typename Disjunction::doc_iterators_t::value_type::doc_iterator_t>>::value) {
        typedef typename Disjunction::unary_disjunction_t unary_disjunction_t;
        return doc_iterator::make<unary_disjunction_t>(
          std::move(itrs.front())
        );
      }
      // single sub-query
      return std::move(itrs.front());
    case 2: {
      typedef typename Disjunction::basic_disjunction_t basic_disjunction_t;

      // simple disjunction
      auto first = itrs.begin();
      auto second = first;
      std::advance(second, 1);

      return doc_iterator::make<basic_disjunction_t>(
        std::move(*first),
        std::move(*second),
        std::forward<Args>(args)...
      );
    }
  }

  const size_t LINEAR_MERGE_UPPER_BOUND = 5;
  if (size <= LINEAR_MERGE_UPPER_BOUND) {
    typedef typename Disjunction::small_disjunction_t small_disjunction_t;

    // small disjunction
    return doc_iterator::make<small_disjunction_t>(
      std::move(itrs), std::forward<Args>(args)...
    );
  }

  // disjunction
  return doc_iterator::make<Disjunction>(
    std::move(itrs), std::forward<Args>(args)...
  );
}

NS_END // ROOT

#endif // IRESEARCH_DISJUNCTION_H
