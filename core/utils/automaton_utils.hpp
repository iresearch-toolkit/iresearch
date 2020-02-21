////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_AUTOMATON_UTILS_H
#define IRESEARCH_AUTOMATON_UTILS_H

#include "automaton.hpp"
#include "formats/formats.hpp"
#include "search/filter.hpp"
#include "utils/hash_utils.hpp"
#include "draw-impl.h"

NS_ROOT

inline automaton match_any_char() {
 automaton a;
 const auto start = a.AddState();
 const auto finish = a.AddState();
 a.SetStart(start);
 a.EmplaceArc(start, fst::fsa::kRho, finish);
 a.SetFinal(finish);

 return a;
}

inline automaton match_any() {
 automaton a;
 const auto start = a.AddState();
 a.SetStart(start);
 a.EmplaceArc(start, fst::fsa::kRho, start);
 a.SetFinal(start);

 return a;
}

inline automaton match_char(automaton::Arc::Label c) {
 automaton a;
 const auto start = a.AddState();
 a.SetStart(start);
 a.EmplaceArc(start, c, start);
 a.SetFinal(start);

 return a;
}

template<typename Char, typename Matcher>
automaton::Weight accept(const automaton& a, Matcher& matcher, const basic_string_ref<Char>& target) {
  auto state = a.Start();
  matcher.SetState(state);

  auto begin = target.begin();
  const auto end = target.end();

  for (; begin < end && matcher.Find(*begin); ++begin) {
    state = matcher.Value().nextstate;
    matcher.SetState(state);
  }

  return begin == end ? a.Final(state)
                      : automaton::Weight::Zero();
}

template<typename Char>
automaton::Weight accept(const automaton& a, const basic_string_ref<Char>& target) {
  typedef fst::RhoMatcher<fst::fsa::AutomatonMatcher> matcher_t;

  // FIXME optimize rho label lookup (just check last arc)
  matcher_t matcher(a, fst::MatchType::MATCH_INPUT, fst::fsa::kRho);
  return accept(a, matcher, target);
}

class automaton_term_iterator final : public seek_term_iterator {
 public:
  automaton_term_iterator(const automaton& a, seek_term_iterator::ptr&& it)
    : a_(&a), matcher_(a_, fst::MatchType::MATCH_INPUT, fst::fsa::kRho), it_(std::move(it)) {
    assert(it_);
    value_ = &it_->value();
  }

  virtual const bytes_ref& value() const noexcept override {
    return *value_;
  }

  virtual doc_iterator::ptr postings(const flags& features) const override {
    return it_->postings(features);
  }

  virtual void read() override {
    it_->read();
  }

  virtual bool next() override {
    bool next = it_->next();

    while (next && !accept()) {
      next = it_->next();
    }

    return next;
  }

  virtual const attribute_view& attributes() const noexcept override {
    return it_->attributes();
  }

  virtual SeekResult seek_ge(const bytes_ref& target) override {
    it_->seek_ge(target);

    if (accept()) {
      return SeekResult::FOUND;
    }

    return next() ? SeekResult::NOT_FOUND : SeekResult::END;
  }

  virtual bool seek(const bytes_ref& target) override {
    return SeekResult::FOUND == seek_ge(target);
  }

  virtual bool seek(const bytes_ref& target, const seek_cookie& cookie) override {
    return it_->seek(target, cookie);
  }

  virtual seek_cookie::ptr cookie() const override {
    return it_->cookie();
  }

 private:
  typedef fst::RhoMatcher<fst::fsa::AutomatonMatcher> matcher_t;

  bool accept() { return irs::accept(*a_, matcher_, *value_); }

  const automaton* a_;
  matcher_t matcher_;
  seek_term_iterator::ptr it_;
  const bytes_ref* value_;
}; // automaton_term_iterator

class utf8_transitions_builder {
 public:
  explicit utf8_transitions_builder(automaton& a) noexcept
    : a_(&a) {
  }

  template<typename Iterator>
  void insert(automaton::StateId from, automaton::StateId rho_state, Iterator begin, Iterator end) {
    last_ = bytes_ref::NIL;
    states_map_.reset();

    std::fill(std::begin(rho_states_), std::end(rho_states_), rho_state);

    if (fst::kNoStateId != rho_state) {
      rho_states_[1] = a_->AddState();
      rho_states_[2] = a_->AddState();
      rho_states_[3] = a_->AddState();
    }

    for (; begin != end; ++begin) {
      // we expect sorted input
      assert(last_.empty() || last_ <= begin->first);

      const auto& label = std::get<0>(*begin);
      insert(label, std::get<1>(*begin));
      last_ = label;
    }

    finish(from);
  }

 private:
  struct state;

  struct arc : private util::noncopyable {
    arc(automaton::Arc::Label label, state* target)
      : target(target),
        label(label) {
    }

    arc(arc&& rhs) noexcept
      : target(rhs.target),
        label(rhs.label) {
    }

    bool operator==(const automaton::Arc& rhs) const {
      return label == rhs.ilabel
        && id == rhs.nextstate;
    }

    bool operator!=(const automaton::Arc& rhs) const {
      return !(*this == rhs);
    }

    friend size_t hash_value(const arc& a) {
      size_t hash = 0;
      hash = hash_combine(hash, a.label);
      hash = hash_combine(hash, a.id);
      return hash;
    }

    union {
      state* target;
      automaton::StateId id;
    };
    automaton::Arc::Label label;
  }; // arc

  struct state : private util::noncopyable {
    state() = default;

    state(state&& rhs) noexcept
      : rho_id(rhs.rho_id),
        id(rhs.id),
        arcs(std::move(rhs.arcs)) {
      rhs.id = fst::kNoStateId;
      rhs.rho_id = fst::kNoStateId;
    }

    void clear() {
      rho_id = fst::kNoStateId;
      id = fst::kNoStateId;
      arcs.clear();
    }

    friend size_t hash_value(const state& s) {
      size_t seed = 0;

      seed = hash_combine(seed, s.rho_id);
      seed = hash_combine(seed, s.id);
      for (auto& arc: s.arcs) {
        seed = hash_combine(seed, hash_value(arc));
      }

      return seed;
    }

    automaton::StateId rho_id{fst::kNoStateId};
    automaton::StateId id{fst::kNoStateId};
    std::vector<arc> arcs;
  }; // state

  class state_map : private util::noncopyable {
   public:
    static const size_t InitialSize = 16;

    state_map(): states_(InitialSize, fst::kNoStateId) {}

    automaton::StateId insert(const state& s, automaton& fst) {

      automaton::StateId id;
      const size_t mask = states_.size() - 1;
      size_t pos = hash_value(s) % mask;
      for ( ;; ++pos, pos %= mask ) {
        if (fst::kNoStateId == states_[pos]) {
          states_[pos] = id = add_state(s, fst);
          ++count_;

          if (count_ > 2 * states_.size() / 3) {
            rehash(fst);
          }
          break;
        } else if (equals(s, states_[pos], fst)) {
          id = states_[pos];
          break;
        }
      }

      return id;
    }

    void reset() noexcept {
      count_ = 0;
      std::fill(states_.begin(), states_.end(), fst::kNoStateId);
    }

   private:
    static bool equals(const state& lhs, automaton::StateId rhs, const automaton& fst) {
      if (fst.NumArcs(rhs) != lhs.arcs.size()) {
        return false;
      }

      for (fst::ArcIterator<automaton>it(fst, rhs); !it.Done(); it.Next()) {
        if (lhs.arcs[it.Position()] != it.Value()) {
          return false;
        }
      }
      return true;
    }

    static size_t hash(automaton::StateId id, const automaton& fst) {
      size_t hash = 0;
      for (fst::ArcIterator<automaton> it(fst, id); !it.Done(); it.Next()) {
        const auto& a = it.Value();
        hash = hash_combine(hash, a.ilabel);
        hash = hash_combine(hash, a.nextstate);
        hash = hash_combine(hash, a.weight.Hash());
      }
      return hash;
    }

    void rehash(const automaton& fst) {
      std::vector<automaton::StateId> states(states_.size() * 2, fst::kNoStateId);
      const size_t mask = states.size() - 1;
      for (auto id : states_) {

        if (fst::kNoStateId == id) {
          continue;
        }

        size_t pos = hash(id, fst) % mask;
        for (;;++pos, pos %= mask) {
          if (fst::kNoStateId == states[pos] ) {
            states[pos] = id;
            break;
          }
        }
      }

      states_ = std::move(states);
    }

    automaton::StateId add_state(const state& s, automaton& fst) {
      automaton::StateId id = s.id;

      if (id == fst::kNoStateId) {
        id = fst.AddState();
      }

      for (const arc& a : s.arcs) {
        fst.EmplaceArc(id, a.label, a.id);
      }

      if (s.rho_id != fst::kNoStateId) {
        fst.EmplaceArc(id, fst::fsa::kRho, s.rho_id);
      }

      return id;
    }

    std::vector<automaton::StateId> states_;
    size_t count_{};
  }; // state_map

  void add_states(size_t size) {
    // reserve size + 1 for root state
    if (states_.size() < ++size ) {
      states_.resize(size);
    }
  }

  void minimize(size_t prefix) {
    assert(prefix > 0);

    for (size_t i = last_.size(); i >= prefix; --i) {
      state& s = states_[i];
      state& p = states_[i - 1];
      assert(!p.arcs.empty());

      p.arcs.back().id = states_map_.insert(s, *a_);
      s.clear();
    }
  }

  void insert(const bytes_ref& label, automaton::StateId target);

  void finish(automaton::StateId from);

  automaton::StateId rho_states_[4];
  std::vector<state> states_;
  state_map states_map_;
  bytes_ref last_;
  automaton* a_;
}; // utf8_automaton_builder

IRESEARCH_API filter::prepared::ptr prepare_automaton_filter(
  const string_ref& field,
  const automaton& acceptor,
  size_t scored_terms_limit,
  const index_reader& index,
  const order::prepared& order,
  boost_t boost);

NS_END

#endif
