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
#include "score_doc_iterators.hpp"
#include "shared.hpp"

NS_ROOT

class fixed_phrase_frequency {
 public:
  typedef std::pair<
    position::ref, // position attribute
    position::value_t // desired offset in the phrase
  > position_t;
  typedef std::vector<position_t> positions_t;

  fixed_phrase_frequency(
      positions_t&& pos,
      const order::prepared& ord
  ) : pos_(std::move(pos)), order_(&ord) {
    assert(!pos_.empty()); // must not be empty
    assert(0 == pos_.front().second); // lead offset is always 0
  }

 protected:
  // returns frequency of the phrase
  frequency::value_t phrase_freq() {
    frequency::value_t freq = 0;
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
          return freq;
        }
        const auto seeked = pos.seek(term_position);

        if (pos_limits::eof(seeked)) {
          // exhausted
          return freq;
        } else if (seeked != term_position) {
          // seeked too far from the lead
          match = false;

          lead.seek(seeked - it->second);
          break;
        }
      }

      if (match) {
        if (order_->empty()) {
          return 1;
        }

        ++freq;
        lead.next();
      }
    }

    return freq;
  }

 private:
  positions_t pos_; // list of desired positions along with corresponding attributes
  const order::prepared* order_;
}; // fixed_phrase_frequency

class variadic_phrase_frequency {
 public:
  typedef std::pair<
    disjunction_visitor<position_score_iterator_adapter<doc_iterator::ptr>>*,
    position::value_t // desired offset in the phrase
  > position_t;
  typedef std::vector<position_t> positions_t;

  variadic_phrase_frequency(
      positions_t&& pos,
      const order::prepared& ord)
    : pos_(std::move(pos)), order_(&ord) {
    assert(!pos_.empty()); // must not be empty
    assert(0 == pos_.front().second); // lead offset is always 0
    is_iterators_hitched_.resize(pos_.size());
  }

 protected:
  // returns frequency of the phrase
  frequency::value_t phrase_freq() {
    frequency::value_t freq = 0;
    position::value_t term_position = pos_limits::eof();
    uint32_t min_seeked = pos_limits::eof();
    auto match = false;
    // reset is_hitched
    is_iterators_hitched_.assign(is_iterators_hitched_.size(), 0);

    auto innerVisitor = [&term_position, &min_seeked, &match](
        position_score_iterator_adapter<doc_iterator::ptr>& it_adapter) {
      auto* p = it_adapter.position;
      p->reset();
      const auto seeked = p->seek(term_position);
      if (pos_limits::eof(seeked)) {
        return true;
      } else if (seeked != term_position) {
        if (seeked < min_seeked) {
          min_seeked = seeked;
        }
        return true;
      }
      match = true;
      return false;
    };

    auto visitor = [this, &freq, &term_position, &min_seeked, &match, &innerVisitor](
        position_score_iterator_adapter<doc_iterator::ptr>& lead_adapter) {
      const auto size = pos_.size();
      auto* lead = lead_adapter.position;
      lead->next();
      position::value_t base_position = pos_limits::eof();
      while (!pos_limits::eof(base_position = lead->value())) {
        match = true;
        for (size_t i = 1; i < size; ++i) {
          auto& p = pos_[i];
          match = false;
          term_position = base_position + p.second;
          if (!pos_limits::valid(term_position)) {
            return false; // invalid for all
          }
          min_seeked = pos_limits::eof();
          auto& h = is_iterators_hitched_[i];
          p.first->visit(innerVisitor, 0 == h);
          h = 1;
          if (!match) {
            if (!pos_limits::eof(min_seeked)) {
              lead->seek(min_seeked - p.second);
              break;
            }
            return true; // eof for all
          }
        }
        if (match) {
          ++freq;
          if (order_->empty()) {
            return false;
          }
          lead->next();
        }
      }
      return true;
    };
    pos_.front().first->visit(visitor, true);
    return freq;
  }

 private:
  positions_t pos_; // list of desired positions along with corresponding attributes
  std::vector<int> is_iterators_hitched_; // hitch iterators one time
  const order::prepared* order_;
}; // variadic_phrase_frequency

// implementation is optimized for frequency based similarity measures
// for generic implementation see a03025accd8b84a5f8ecaaba7412fc92a1636be3
template<typename Conjunction, typename Frequency>
class phrase_iterator : public doc_iterator_base, Frequency {
 public:
  typedef typename Frequency::positions_t positions_t;

  phrase_iterator(
      typename Conjunction::doc_iterators_t&& itrs,
      typename Frequency::positions_t&& pos,
      const sub_reader& segment,
      const term_reader& field,
      const byte_type* stats,
      const order::prepared& ord,
      boost_t boost
  ) : Frequency(std::move(pos), ord), approx_(std::move(itrs)) {

    // FIXME find a better estimation
    // estimate iterator
    estimate([this](){ return irs::cost::extract(approx_.attributes()); });

    // set attributes
    attrs_.emplace(phrase_freq_); // phrase frequency
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
    while ((next = approx_.next()) && !(phrase_freq_.value = this->phrase_freq())) {}

    return next;
  }

  virtual doc_id_t seek(doc_id_t target) override {
    if (approx_.seek(target) == target) {
      return target;
    }

    if (doc_limits::eof(value()) || (phrase_freq_.value = this->phrase_freq())) {
      return value();
    }

    next();

    return value();
  }

 private:
  Conjunction approx_; // first approximation (conjunction over all words in a phrase)
  const document* doc_{}; // document itself
  frequency phrase_freq_; // freqency of the phrase in a document
}; // phrase_iterator

NS_END // ROOT

#endif
