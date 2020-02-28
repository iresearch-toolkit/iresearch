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
#include "utils/attribute_range.hpp"

NS_ROOT

// implementation is optimized for frequency based similarity measures
// for generic implementation see a03025accd8b84a5f8ecaaba7412fc92a1636be3
template<typename Conjunction>
class phrase_iterator : public doc_iterator_base {
 public:
  phrase_iterator(
      typename Conjunction::doc_iterators_t&& itrs,
      const sub_reader& segment,
      const term_reader& field,
      const byte_type* stats,
      const order::prepared& ord,
      boost_t boost
  ) : approx_(std::move(itrs)),
      order_(&ord) {

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
    while ((next = approx_.next()) && !(phrase_freq_.value = phrase_freq())) {}

    return next;
  }

  virtual doc_id_t seek(doc_id_t target) override {
    if (approx_.seek(target) == target) {
      return target;
    }

    if (doc_limits::eof(value()) || (phrase_freq_.value = phrase_freq())) {
      return value();
    }

    next();

    return value();
  }

 protected:
  // returns frequency of the phrase
  virtual frequency::value_t phrase_freq() = 0;

  Conjunction approx_; // first approximation (conjunction over all words in a phrase)
  const document* doc_{}; // document itself
  frequency phrase_freq_; // freqency of the phrase in a document
  const order::prepared* order_;
}; // phrase_iterator

template<typename Conjunction>
class fixed_phrase_iterator final : public phrase_iterator<Conjunction> {
 public:
  typedef std::pair<
    position::ref, // position attribute
    position::value_t // desired offset in the phrase
  > position_t;
  typedef std::vector<position_t> positions_t;

  fixed_phrase_iterator(
      typename Conjunction::doc_iterators_t&& itrs,
      positions_t&& pos,
      const sub_reader& segment,
      const term_reader& field,
      const byte_type* stats,
      const order::prepared& ord,
      boost_t boost
  ) : phrase_iterator<Conjunction>(std::move(itrs), segment, field, stats, ord, boost),
      pos_(std::move(pos)) {
    assert(!pos_.empty()); // must not be empty
    assert(0 == pos_.front().second); // lead offset is always 0
  }

 private:
  // returns frequency of the phrase
  virtual frequency::value_t phrase_freq() override {
    frequency::value_t freq = 0;
    bool match;

    position& lead = pos_.front().first;
    lead.next();

    for (auto end = pos_.end(); !pos_limits::eof(lead.value());) {
      const position::value_t base_offset = lead.value();

      match = true;

      for (auto it = pos_.begin() + 1; it != end; ++it) {
        position& pos = it->first;
        const auto term_offset = base_offset + it->second;
        const auto seeked = pos.seek(term_offset);

        if (pos_limits::eof(seeked)) {
          // exhausted
          return freq;
        } else if (seeked != term_offset) {
          // seeked too far from the lead
          match = false;

          lead.seek(seeked - it->second);
          break;
        }
      }

      if (match) {
        if (this->order_->empty()) {
          return 1;
        }

        ++freq;
        lead.next();
      }
    }

    return freq;
  }

  positions_t pos_; // list of desired positions along with corresponding attributes
}; // fixed_phrase_iterator

template<typename Conjunction>
class variadic_phrase_iterator final : public phrase_iterator<Conjunction> {
 public:
  typedef std::pair<
    const attribute_view::ref<attribute_range<position_score_iterator_adapter<doc_iterator::ptr>>>::type*, // position attribute
    position::value_t // desired offset in the phrase
  > position_t;
  typedef std::vector<position_t> positions_t;

  variadic_phrase_iterator(
      typename Conjunction::doc_iterators_t&& itrs,
      positions_t&& pos,
      const sub_reader& segment,
      const term_reader& field,
      const byte_type* stats,
      const order::prepared& ord,
      boost_t boost
  ) : phrase_iterator<Conjunction>(std::move(itrs), segment, field, stats, ord, boost),
    pos_(std::move(pos)) {
    assert(!pos_.empty()); // must not be empty
  }

 private:
  // returns frequency of the phrase
  virtual frequency::value_t phrase_freq() override {
    frequency::value_t freq = 0;
    auto end = pos_.end();
    auto* posa = pos_.front().first->get();
    assert(posa);
    posa->reset();
    while (posa->next()) {
      auto* lead_adapter = posa->value();
      auto* lead = lead_adapter->position;
      auto global_match = true;
      // lead->reset(); // Do not need here. There is the first time always.
      lead->next();

      position::value_t base_offset = pos_limits::eof();
      while (!pos_limits::eof(base_offset = lead->value())) {
        auto match = true;
        for (auto it = pos_.begin() + 1; it != end; ++it) {
          const auto term_offset = base_offset + it->second;
          match = false;
          auto min_seeked = std::numeric_limits<position::value_t>::max();
          auto* ita = it->first->get();
          assert(ita);
          ita->reset();
          while (ita->next()) {
            auto* it_adapter = ita->value();
            auto* p = it_adapter->position;
            p->reset();
            const auto seeked = p->seek(term_offset);

            if (pos_limits::eof(seeked)) {
              continue;
            } else if (seeked != term_offset) {
              if (seeked < min_seeked) {
                min_seeked = seeked;
              }
              continue;
            }
            match = true;
            break;
          }
          if (!match) {
            if (min_seeked < std::numeric_limits<position::value_t>::max()) {
              lead->seek(min_seeked - it->second);
              break;
            }
            global_match = false; // eof for all
            break;
          }
        }
        if (!global_match) {
          break;
        }
        if (match) {
          if (this->order_->empty()) {
            return 1;
          }
          ++freq;
          lead->next();
        }
      }
    }
    return freq;
  }

  positions_t pos_; // list of desired positions along with corresponding attributes
}; // variadic_phrase_iterator

NS_END // ROOT

#endif
