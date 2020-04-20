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

#ifndef IRESEARCH_PHRASE_ITERATOR_H
#define IRESEARCH_PHRASE_ITERATOR_H

#include "disjunction.hpp"

NS_ROOT

class fixed_phrase_frequency {
 public:
  using term_position_t = std::pair<
    position::ref, // position attribute
    position::value_t>; // desired offset in the phrase

  fixed_phrase_frequency(
      std::vector<term_position_t>&& pos,
      attribute_view& attrs,
      const order::prepared& ord)
    : pos_(std::move(pos)), order_(&ord) {
    assert(!pos_.empty()); // must not be empty
    assert(0 == pos_.front().second); // lead offset is always 0

    // set attributes
    if (!ord.empty()) {
      attrs.emplace(phrase_freq_);
    }
  }

  // returns frequency of the phrase
  frequency::value_t operator()() {
    phrase_freq_.value = 0;
    bool match;

    position& lead = pos_.front().first;
    lead.next();

    for (auto end = pos_.end(); !pos_limits::eof(lead.value());) {
      const position::value_t base_position = lead.value();

      match = true;

      for (auto it = pos_.begin() + 1; it != end; ++it) {
        position& pos = it->first;
        const auto term_position = base_position + it->second;
        if (!pos_limits::valid(term_position)) {
          return phrase_freq_.value;
        }
        const auto sought = pos.seek(term_position);

        if (pos_limits::eof(sought)) {
          // exhausted
          return phrase_freq_.value;
        } else if (sought!= term_position) {
          // sought too far from the lead
          match = false;

          lead.seek(sought- it->second);
          break;
        }
      }

      if (match) {
        if (order_->empty()) {
          return (phrase_freq_.value = 1);
        }

        ++phrase_freq_.value;
        lead.next();
      }
    }

    return phrase_freq_.value;
  }

 private:
  std::vector<term_position_t> pos_; // list of desired positions along with corresponding attributes
  const order::prepared* order_;
  frequency phrase_freq_; // freqency of the phrase in a document
}; // fixed_phrase_frequency

////////////////////////////////////////////////////////////////////////////////
/// @class doc_iterator_adapter
/// @brief adapter to use doc_iterator with positions for disjunction
////////////////////////////////////////////////////////////////////////////////
struct variadic_phrase_adapter: score_iterator_adapter<doc_iterator::ptr> {
  variadic_phrase_adapter(doc_iterator::ptr&& it, boost_t boost) noexcept
    : score_iterator_adapter<doc_iterator::ptr>(std::move(it)),
      position(irs::position::extract(this->it->attributes())),
      boost(boost) {
  }

  irs::position* position;
  boost_t boost;
}; // variadic_phrase_adapter

using variadic_term_position = std::pair<
  compound_doc_iterator<variadic_phrase_adapter>*,
  position::value_t>; // desired offset in the phrase

template<bool VolatileBoost>
class variadic_phrase_frequency {
 public:
  using term_position_t = variadic_term_position;

  variadic_phrase_frequency(
      std::vector<term_position_t>&& pos,
      attribute_view& attrs,
      const order::prepared& ord)
    : pos_(std::move(pos)), order_empty_(ord.empty()) {
    assert(!pos_.empty()); // must not be empty
    assert(0 == pos_.front().second); // lead offset is always 0

    if (!ord.empty()) {
      attrs.emplace(phrase_freq_);
      if /*constexpr*/ (VolatileBoost) {
        attrs.emplace(phrase_boost_);
      }
    }
  }

  // returns frequency of the phrase
  frequency::value_t operator()() {
    if /*constexpr*/ (VolatileBoost) {
      phrase_boost_.value = no_boost();
    }
    phrase_freq_.value = 0;

    pos_.front().first->visit(this, visit_lead);

    return phrase_freq_.value;
  }

 private:
  static bool visit(void* ctx, variadic_phrase_adapter& it_adapter) {
    assert(ctx);
    auto& self = *reinterpret_cast<variadic_phrase_frequency*>(ctx);
    auto* p = it_adapter.position;
    p->reset();
    const auto sought = p->seek(self.term_position_);
    if (pos_limits::eof(sought)) {
      return true;
    } else if (sought != self.term_position_) {
      if (sought < self.min_sought_) {
        self.min_sought_ = sought;
      }
      return true;
    }
    self.match = true;
    if /*constexpr*/ (VolatileBoost) {
      self.match_boost_ *= it_adapter.boost;
    }
    return false;
  }

  static bool visit_lead(void* ctx, variadic_phrase_adapter& lead_adapter) {
    assert(ctx);
    auto& self = *reinterpret_cast<variadic_phrase_frequency*>(ctx);
    const auto end = self.pos_.end();
    auto* lead = lead_adapter.position;
    lead->next();
    for (position::value_t base_position; !pos_limits::eof(base_position = lead->value()); ) {
      if /*constexpr*/ (VolatileBoost) {
        self.match_boost_ = lead_adapter.boost;
      }
      self.match = true;
      for (auto it = self.pos_.begin() + 1; it != end; ++it) {
        self.match = false;
        self.term_position_ = base_position + it->second;
        if (!pos_limits::valid(self.term_position_)) {
          return false; // invalid for all
        }
        self.min_sought_ = pos_limits::eof();
        it->first->visit(&self, visit);
        if (!self.match) {
          if (!pos_limits::eof(self.min_sought_)) {
            lead->seek(self.min_sought_ - it->second);
            break;
          }
          return true; // eof for all
        }
      }
      if (self.match) {
        ++self.phrase_freq_.value;
        if (self.order_empty_) {
          return false;
        }
        if /*constexpr*/ (VolatileBoost) {
          self.phrase_boost_.value *= self.match_boost_;
        }
        lead->next();
      }
    }
    return true;
  }

  std::vector<term_position_t> pos_; // list of desired positions along with corresponding attributes
  frequency phrase_freq_; // freqency of the phrase in a document
  filter_boost phrase_boost_;
  boost_t match_boost_{ no_boost() };
  position::value_t term_position_{ pos_limits::eof() };
  position::value_t min_sought_{ pos_limits::eof() };
  bool match{ false };
  const bool order_empty_;
}; // variadic_phrase_frequency

// implementation is optimized for frequency based similarity measures
// for generic implementation see a03025accd8b84a5f8ecaaba7412fc92a1636be3
template<typename Conjunction, typename Frequency>
class phrase_iterator : public doc_iterator_base<doc_iterator> {
 public:
  phrase_iterator(
      typename Conjunction::doc_iterators_t&& itrs,
      std::vector<typename Frequency::term_position_t>&& pos,
      const sub_reader& segment,
      const term_reader& field,
      const byte_type* stats,
      const order::prepared& ord,
      boost_t boost)
    : approx_(std::move(itrs)),
      freq_(std::move(pos), attrs_, ord) {

    // FIXME find a better estimation
    // estimate iterator
    estimate([this](){ return irs::cost::extract(approx_.attributes()); });

    // set attributes
    doc_ = (attrs_.emplace<irs::document>()
             = approx_.attributes().template get<irs::document>()).get(); // document (required by scorers)
    assert(doc_);

    // set scorers
    prepare_score(
      ord,
      ord.prepare_scorers(segment, field, stats, attributes(), boost)
    );
  }

  virtual doc_id_t value() const override final {
    return doc_->value;
  }

  virtual bool next() override {
    bool next = false;
    while ((next = approx_.next()) && !freq_()) {}

    return next;
  }

  virtual doc_id_t seek(doc_id_t target) override {
    // important to call freq_() in order
    // to set attribute values
    const auto prev = doc_->value;
    const auto doc = approx_.seek(target);

    if (prev == doc || freq_()) {
      return doc;
    }

    next();

    return doc_->value;
  }

 private:
  Conjunction approx_; // first approximation (conjunction over all words in a phrase)
  Frequency freq_;
  const document* doc_{}; // document itself
}; // phrase_iterator

NS_END // ROOT

#endif
