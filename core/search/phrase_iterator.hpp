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

namespace iresearch {

// position attribute + desired offset in the phrase
using FixedTermPosition = std::pair<position::ref, position::value_t>;

template<typename CallbackType>
class CallbackRep : private compact<0, CallbackType> {
 private:
  using Rep = compact<0, CallbackType>;

 protected:
  explicit CallbackRep(CallbackType&& callback) : Rep{std::move(callback)} {}

  bool stop() noexcept { return !Rep::get()(); }
};

template<typename CallbackType>
class FixedPhraseFrequency : private CallbackRep<CallbackType> {
 public:
  using TermPosition = FixedTermPosition;
  using Callback = CallbackType;

  FixedPhraseFrequency(std::vector<TermPosition>&& pos, Callback&& callback)
      : CallbackRep<Callback>{std::move(callback)}, pos_{std::move(pos)} {
    assert(!pos_.empty());             // must not be empty
    assert(0 == pos_.front().second);  // lead offset is always 0
  }

  frequency& freq() noexcept { return phrase_freq_; }

  filter_boost* boost() const noexcept { return nullptr; }

  // returns frequency of the phrase
  uint32_t operator()() {
    phrase_freq_.value = 0;

    position& lead = pos_.front().first;
    lead.next();

    for (auto end = std::end(pos_); !pos_limits::eof(lead.value());) {
      const position::value_t base_position = lead.value();

      bool match = true;

      for (auto it = std::begin(pos_) + 1; it != end; ++it) {
        position& pos = it->first;
        const auto term_position = base_position + it->second;
        if (!pos_limits::valid(term_position)) {
          return phrase_freq_.value;
        }
        const auto sought = pos.seek(term_position);

        if (pos_limits::eof(sought)) {
          // exhausted
          return phrase_freq_.value;
        } else if (sought != term_position) {
          // sought too far from the lead
          match = false;

          lead.seek(sought - it->second);
          break;
        }
      }

      if (match) {
        ++phrase_freq_.value;

        if (this->stop()) {
          return phrase_freq_.value;
        }

        lead.next();
      }
    }

    return phrase_freq_.value;
  }

 private:
  // list of desired positions along with corresponding attributes
  std::vector<TermPosition> pos_;
  // freqency of the phrase in a document
  frequency phrase_freq_;
};

// Adapter to use doc_iterator with positions for disjunction
struct VariadicPhraseAdapter final : score_iterator_adapter<doc_iterator::ptr> {
  VariadicPhraseAdapter() = default;
  VariadicPhraseAdapter(doc_iterator::ptr&& it, score_t boost) noexcept
      : score_iterator_adapter<doc_iterator::ptr>(std::move(it)),
        position(irs::get_mutable<irs::position>(this->it.get())),
        boost(boost) {}

  irs::position* position{};
  score_t boost{kNoBoost};
};

static_assert(std::is_nothrow_move_constructible_v<VariadicPhraseAdapter>);
static_assert(std::is_nothrow_move_assignable_v<VariadicPhraseAdapter>);

using VariadicTermPosition =
    std::pair<compound_doc_iterator<VariadicPhraseAdapter>*,
              position::value_t>;  // desired offset in the phrase

// Helper for variadic phrase frequency evaluation for cases when
// only one term may be at a single position in a phrase (e.g. synonyms)
template<bool VolatileBoost, typename CallbackType>
class VariadicPhraseFrequency : private CallbackRep<CallbackType> {
 public:
  using TermPosition = VariadicTermPosition;
  using Callback = CallbackType;

  VariadicPhraseFrequency(std::vector<TermPosition>&& pos, Callback&& callback)
      : CallbackRep<Callback>{std::move(callback)},
        pos_{std::move(pos)},
        phrase_size_{pos_.size()} {
    assert(!pos_.empty() && phrase_size_);  // must not be empty
    assert(0 == pos_.front().second);       // lead offset is always 0
  }

  frequency& freq() noexcept { return phrase_freq_; }

  filter_boost* boost() noexcept {
    if constexpr (VolatileBoost) {
      return &phrase_boost_;
    } else {
      return nullptr;
    }
  }

  // returns frequency of the phrase
  uint32_t operator()() {
    if constexpr (VolatileBoost) {
      phrase_boost_.value = {};
    }
    phrase_freq_.value = 0;
    pos_.front().first->visit(this, visit_lead);

    if constexpr (VolatileBoost) {
      if (phrase_freq_.value) {
        phrase_boost_.value =
            phrase_boost_.value / (phrase_size_ * phrase_freq_.value);
      }
    }

    return phrase_freq_.value;
  }

 private:
  struct SubMatchContext {
    position::value_t term_position{pos_limits::eof()};
    position::value_t min_sought{pos_limits::eof()};
    score_t boost{};
    bool match{false};
  };

  static bool visit(void* ctx, VariadicPhraseAdapter& it_adapter) {
    assert(ctx);
    auto& match = *reinterpret_cast<SubMatchContext*>(ctx);
    auto* p = it_adapter.position;
    p->reset();
    const auto sought = p->seek(match.term_position);
    if (pos_limits::eof(sought)) {
      return true;
    } else if (sought != match.term_position) {
      if (sought < match.min_sought) {
        match.min_sought = sought;
      }
      return true;
    }

    if constexpr (VolatileBoost) {
      match.boost += it_adapter.boost;
    }

    match.match = true;
    return false;
  }

  static bool visit_lead(void* ctx, VariadicPhraseAdapter& lead_adapter) {
    assert(ctx);
    auto& self = *reinterpret_cast<VariadicPhraseFrequency*>(ctx);
    const auto end = std::end(self.pos_);
    auto* lead = lead_adapter.position;
    lead->next();

    SubMatchContext match;

    for (position::value_t base_position;
         !pos_limits::eof(base_position = lead->value());) {
      match.match = true;
      if constexpr (VolatileBoost) {
        match.boost = lead_adapter.boost;
      }

      for (auto it = std::begin(self.pos_) + 1; it != end; ++it) {
        match.term_position = base_position + it->second;
        if (!pos_limits::valid(match.term_position)) {
          return false;  // invalid for all
        }

        match.match = false;
        match.min_sought = pos_limits::eof();

        it->first->visit(&match, visit);

        if (!match.match) {
          if (!pos_limits::eof(match.min_sought)) {
            lead->seek(match.min_sought - it->second);
            break;
          }

          return true;  // eof for all
        }
      }

      if (match.match) {
        ++self.phrase_freq_.value;
        if (self.stop()) {
          return false;
        }
        if constexpr (VolatileBoost) {
          self.phrase_boost_.value += match.boost;
        }
        lead->next();
      }
    }

    return true;
  }

  // list of desired positions along with corresponding attributes
  std::vector<VariadicTermPosition> pos_;
  // size of the phrase (speedup phrase boost evaluation)
  const size_t phrase_size_;
  frequency phrase_freq_;      // freqency of the phrase in a document
  filter_boost phrase_boost_;  // boost of the phrase in a document
};

// Helper for variadic phrase frequency evaluation for cases when
// different terms may be at the same position in a phrase (e.g.
// synonyms)
template<bool VolatileBoost, typename CallbackType>
class VariadicPhraseFrequencyOverlapped : private CallbackRep<CallbackType> {
 public:
  using TermPosition = VariadicTermPosition;
  using Callback = CallbackType;

  VariadicPhraseFrequencyOverlapped(std::vector<TermPosition>&& pos,
                                    Callback&& callback)
      : CallbackRep<Callback>{std::move(callback)},
        pos_(std::move(pos)),
        phrase_size_(pos_.size()) {
    assert(!pos_.empty() && phrase_size_);  // must not be empty
    assert(0 == pos_.front().second);       // lead offset is always 0
  }

  frequency& freq() noexcept { return phrase_freq_; }

  filter_boost* boost() noexcept {
    if constexpr (VolatileBoost) {
      return &phrase_boost_;
    } else {
      return nullptr;
    }
  }

  // returns frequency of the phrase
  uint32_t operator()() {
    if constexpr (VolatileBoost) {
      lead_freq_ = 0;
      lead_boost_ = {};
      phrase_boost_.value = {};
    }

    phrase_freq_.value = 0;
    pos_.front().first->visit(this, visit_lead);

    if constexpr (VolatileBoost) {
      if (lead_freq_) {
        phrase_boost_.value =
            (phrase_boost_.value + (lead_boost_ / lead_freq_)) / phrase_size_;
      }
    }

    return phrase_freq_.value;
  }

 private:
  struct SubMatchContext {
    position::value_t term_position{pos_limits::eof()};
    position::value_t min_sought{pos_limits::eof()};
    score_t boost{};
    uint32_t freq{};
  };

  static bool visit(void* ctx, VariadicPhraseAdapter& it_adapter) {
    assert(ctx);
    auto& match = *reinterpret_cast<SubMatchContext*>(ctx);
    auto* p = it_adapter.position;
    p->reset();
    const auto sought = p->seek(match.term_position);
    if (pos_limits::eof(sought)) {
      return true;
    } else if (sought != match.term_position) {
      if (sought < match.min_sought) {
        match.min_sought = sought;
      }
      return true;
    }

    ++match.freq;
    if constexpr (VolatileBoost) {
      match.boost += it_adapter.boost;
    }

    return true;  // continue iteration in overlapped case
  }

  static bool visit_lead(void* ctx, VariadicPhraseAdapter& lead_adapter) {
    assert(ctx);
    auto& self = *reinterpret_cast<VariadicPhraseFrequencyOverlapped*>(ctx);
    const auto end = std::end(self.pos_);
    auto* lead = lead_adapter.position;
    lead->next();

    SubMatchContext match;     // sub-match
    uint32_t phrase_freq = 0;  // phrase frequency for current lead_iterator
    // accumulated match frequency for current lead_iterator
    uint32_t match_freq;
    score_t phrase_boost = {};  // phrase boost for current lead_iterator
    score_t match_boost;  // accumulated match boost for current lead_iterator
    for (position::value_t base_position;
         !pos_limits::eof(base_position = lead->value());) {
      match_freq = 1;
      if constexpr (VolatileBoost) {
        match_boost = 0.f;
      }

      for (auto it = std::begin(self.pos_) + 1; it != end; ++it) {
        match.term_position = base_position + it->second;
        if (!pos_limits::valid(match.term_position)) {
          return false;  // invalid for all
        }

        match.freq = 0;
        if constexpr (VolatileBoost) {
          match.boost = 0.f;
        }
        match.min_sought = pos_limits::eof();

        it->first->visit(&match, visit);

        if (!match.freq) {
          match_freq = 0;

          if (!pos_limits::eof(match.min_sought)) {
            lead->seek(match.min_sought - it->second);
            break;
          }

          if constexpr (VolatileBoost) {
            if (phrase_freq) {
              ++self.lead_freq_;
              self.lead_boost_ += lead_adapter.boost;
              self.phrase_boost_.value += phrase_boost / phrase_freq;
            }
          }

          return true;  // eof for all
        }

        match_freq *= match.freq;
        if constexpr (VolatileBoost) {
          match_boost += match.boost / match.freq;
        }
      }

      if (match_freq) {
        self.phrase_freq_.value += match_freq;
        if (self.stop()) {
          return false;
        }
        ++phrase_freq;
        if constexpr (VolatileBoost) {
          phrase_boost += match_boost;
        }
        lead->next();
      }
    }

    if constexpr (VolatileBoost) {
      if (phrase_freq) {
        ++self.lead_freq_;
        self.lead_boost_ += lead_adapter.boost;
        self.phrase_boost_.value += phrase_boost / phrase_freq;
      }
    }

    return true;
  }
  // list of desired positions along with corresponding attributes
  std::vector<TermPosition> pos_;
  // size of the phrase (speedup phrase boost evaluation)
  const size_t phrase_size_;
  frequency phrase_freq_;      // freqency of the phrase in a document
  filter_boost phrase_boost_;  // boost of the phrase in a document
  score_t lead_boost_{0.f};    // boost from all matched lead iterators
  uint32_t lead_freq_{0};      // number of matched lead iterators
};

// implementation is optimized for frequency based similarity measures
// for generic implementation see a03025accd8b84a5f8ecaaba7412fc92a1636be3
template<typename Conjunction, typename Frequency>
class PhraseIterator final : public doc_iterator {
 public:
  PhraseIterator(typename Conjunction::doc_iterators_t&& itrs,
                 std::vector<typename Frequency::TermPosition>&& pos,
                 typename Frequency::Callback&& callback)
      : approx_{std::move(itrs), NoopAggregator{}},
        freq_{std::move(pos), std::move(callback)} {
    std::get<attribute_ptr<document>>(attrs_) =
        irs::get_mutable<document>(&approx_);

    // FIXME find a better estimation
    std::get<irs::cost>(attrs_).reset(
        [this]() { return cost::extract(approx_); });
  }

  PhraseIterator(typename Conjunction::doc_iterators_t&& itrs,
                 std::vector<typename Frequency::TermPosition>&& pos,
                 typename Frequency::Callback&& callback,
                 const sub_reader& segment, const term_reader& field,
                 const byte_type* stats, const Order& ord, score_t boost)
      : PhraseIterator{std::move(itrs), std::move(pos), std::move(callback)} {
    if (!ord.empty()) {
      std::get<attribute_ptr<frequency>>(attrs_) = &freq_.freq();
      std::get<attribute_ptr<filter_boost>>(attrs_) = freq_.boost();

      auto& score = std::get<irs::score>(attrs_);
      score = CompileScore(ord.buckets(), segment, field, stats, *this, boost);
    }
  }

  virtual attribute* get_mutable(type_info::type_id type) noexcept override {
    return irs::get_mutable(attrs_, type);
  }

  virtual doc_id_t value() const override final {
    return std::get<attribute_ptr<document>>(attrs_).ptr->value;
  }

  virtual bool next() override {
    bool next = false;
    while ((next = approx_.next()) && !freq_()) {
    }

    return next;
  }

  virtual doc_id_t seek(doc_id_t target) override {
    auto* pdoc = std::get<attribute_ptr<document>>(attrs_).ptr;

    // important to call freq_() in order
    // to set attribute values
    const auto prev = pdoc->value;
    const auto doc = approx_.seek(target);

    if (prev == doc || freq_()) {
      return doc;
    }

    next();

    return pdoc->value;
  }

 private:
  // FIXME can store only 4 attrbiutes for non-volatile boost case
  using attributes =
      std::tuple<attribute_ptr<document>, cost, score, attribute_ptr<frequency>,
                 attribute_ptr<filter_boost>>;

  // first approximation (conjunction over all words in a phrase)
  Conjunction approx_;
  Frequency freq_;
  attributes attrs_;
};

}  // namespace iresearch

#endif
