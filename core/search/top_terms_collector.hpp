////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_TOP_TERMS_COLLECTOR_H
#define IRESEARCH_TOP_TERMS_COLLECTOR_H

#include <unordered_map>
#include <vector>

#include "shared.hpp"
#include "search/limited_sample_collector.hpp" // FIXME remove
#include "search/sort.hpp"
#include "utils/noncopyable.hpp"

NS_ROOT

struct segment_state {
  segment_state(
      const sub_reader& segment,
      const term_reader& field,
      uint32_t docs_count) noexcept
    : segment(&segment),
      field(&field),
      docs_count(docs_count) {
  }

  const sub_reader* segment;
  const term_reader* field;
  size_t terms_count{1}; // number of terms in a segment
  uint32_t docs_count;
};

template<typename T>
struct top_term_state {
  template<typename U = T>
  top_term_state(const bytes_ref& term, U&& key)
    : term(term.c_str(), term.size()),
      key(std::forward<U>(key)) {
  }

  void emplace(const sub_reader& segment, const term_reader& field,
               seek_term_iterator::cookie_ptr&& cookie, uint32_t docs_count) {
    if (segments.empty() || segments.back().segment != &segment) {
      segments.emplace_back(segment, field, docs_count);
    } else {
      auto& segment = segments.back();
      ++segment.terms_count;
      ++segment.docs_count += docs_count;
    }
    terms.emplace_back(std::move(cookie));
  }

  std::vector<segment_state> segments;
  std::vector<seek_term_iterator::cookie_ptr> terms;
  bstring term;
  T key;
};

// FIXME use C++14 with transparent comparison???
template<typename T>
struct top_term_comparer {
  bool operator()(const top_term_state<T>& lhs,
                  const top_term_state<T>& rhs) const noexcept {
    return operator()(lhs, rhs.key, rhs.term);
  }

  bool operator()(const top_term_state<T>& lhs,
                  const T& rhs_key,
                  const bytes_ref& rhs_term) const noexcept {
    return lhs.key < rhs_key ||
        (!(rhs_key < lhs.key) && lhs.term < rhs_term);
  }
};

template<typename Key, 
         typename Comparer = top_term_comparer<Key>
> class top_terms_collector : private compact<0, Comparer>,
                              private util::noncopyable {
 private:
  using comparer_rep = compact<0, Comparer>;

 public:
  using key_type = Key;
  using comparer_type = Comparer;
  using state_type = top_term_state<key_type>;

  explicit top_terms_collector(
      size_t size,
      const Comparer& comp = {})
    : comparer_rep(comp),
      size_(size) {
    heap_.reserve(size);
    terms_.reserve(size); // ensure all iterators remain valid
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief prepare scorer for terms collecting
  /// @param segment segment reader for the current term
  /// @param state state containing this scored term
  /// @param terms segment term-iterator positioned at the current term
  //////////////////////////////////////////////////////////////////////////////
  void prepare(const sub_reader& segment,
               const term_reader& field,
               const seek_term_iterator& terms) noexcept {
    state_.segment = &segment;
    state_.field = &field;
    state_.terms = &terms;
    state_.term = &terms.value();
    state_.attrs = &terms.attributes();

    // get term metadata
    auto& meta = state_.attrs->template get<term_meta>();
    state_.docs_count = meta ? &meta->docs_count : &no_docs_;
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief collect current term
  //////////////////////////////////////////////////////////////////////////////
  void collect(const Key& key) {
    const auto& term = *state_.term;

    if (terms_.size() < size_) {
      const auto res = emplace(make_hashed_ref(term), key);

      if (res.second) {
        heap_.emplace_back(res.first);
        push();

      }

      collect_state(res.first->second);

      return;
    }

    const auto min = heap_.front();
    const auto& min_state = min->second;

    if (!comparer()(min_state, key, term)) {
      // nothing to do
      return;
    }

    const auto hashed_term = make_hashed_ref(term);
    const auto it = terms_.find(hashed_term);

    if (it == terms_.end()) {
      // update min entry
      pop();
      terms_.erase(min);
      const auto res = emplace(hashed_term, key);
      assert(res.second);

      heap_.back() = res.first;
      push();

      collect_state(res.first->second);
    } else {
      assert(it->second.key == key);
      // update existing entry
      collect_state(it->second);
    }
  }

  template<typename Visitor>
  void visit(Visitor& visitor) {
    for (auto& entry : terms_) {
      auto& state = entry.second;
      visitor(state.key, bytes_ref(state.term));
      auto cookie = state.terms.begin();
      for (auto& segment : state.segments) {
        visitor(*segment.segment, *segment.field, segment.docs_count);
        for (size_t i = 0, size = segment.terms_count; i < size; ++i, ++cookie) {
          visitor(*cookie);
        }
      }
    }
  }

 private:
  typedef std::unordered_map<hashed_bytes_ref, state_type> states_map_t;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief a representation of state of the collector
  //////////////////////////////////////////////////////////////////////////////
  struct collector_state {
    const sub_reader* segment{};
    const term_reader* field{};
    const seek_term_iterator* terms{};
    const bytes_ref* term{};
    const attribute_view* attrs{};
    const uint32_t* docs_count{};
  };

  void collect_state(state_type& state) {
    state.emplace(*state_.segment, *state_.field,
                  state_.terms->cookie(), *state_.docs_count);
  }

  void push() noexcept {
    std::push_heap(
      heap_.begin(),
      heap_.end(),
      [this](const typename states_map_t::iterator lhs,
             const typename states_map_t::iterator rhs) noexcept {
       return comparer()(rhs->second, lhs->second);
    });
  }

  void pop() noexcept {
    std::pop_heap(
      heap_.begin(),
      heap_.end(),
      [this](const typename states_map_t::iterator lhs,
             const typename states_map_t::iterator rhs) noexcept {
       return comparer()(rhs->second, lhs->second);
    });
  }

  std::pair<typename states_map_t::iterator, bool>
  emplace(const hashed_bytes_ref& term, const Key& key) {
    // replace original reference to 'name' provided by the caller
    // with a reference to the cached copy in 'value'
    return map_utils::try_emplace_update_key(
      terms_,
      [](const hashed_bytes_ref& key,
         const state_type& value) noexcept {
          // reuse hash but point ref at value
          return hashed_bytes_ref(key.hash(), value.term);
      },
      term, term, key);
  }

  const comparer_type& comparer() const noexcept {
    return comparer_rep::get();
  }

  collector_state state_;
  std::vector<typename states_map_t::iterator> heap_;
  states_map_t terms_;
  size_t size_;
  const decltype(term_meta::docs_count) no_docs_{0};
};

NS_END

#endif // IRESEARCH_TOP_TERMS_COLLECTOR_H

