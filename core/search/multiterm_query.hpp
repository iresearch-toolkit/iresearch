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

#ifndef IRESEARCH_MULTITERM_QUERY_H
#define IRESEARCH_MULTITERM_QUERY_H

#include "search/cost.hpp"
#include "search/filter.hpp"
#include "search/states_cache.hpp"

namespace iresearch {

// Cached per reader state.
struct multiterm_state {
  struct term_state {
    term_state(seek_cookie::ptr&& cookie, uint32_t stat_offset,
               boost_t boost = kNoBoost) noexcept
        : cookie(std::move(cookie)), stat_offset(stat_offset), boost(boost) {}

    seek_cookie::ptr cookie;
    uint32_t stat_offset{};
    float_t boost{kNoBoost};
  };

  using unscored_term_state = seek_cookie::ptr;

  // Return true if state is empty
  bool empty() const noexcept {
    return scored_states.empty() && unscored_terms.empty();
  }

  // Return total cost of execution
  cost::cost_t estimation() const noexcept {
    return scored_states_estimation + unscored_states_estimation;
  }

  // Reader using for iterate over the terms
  const term_reader* reader{};

  // Scored term states
  std::vector<term_state> scored_states;

  // Matching terms that may have been skipped
  // while collecting statistics and should not be
  // scored by the disjunction.
  std::vector<unscored_term_state> unscored_terms;

  // Estimated cost of scored states
  cost::cost_t scored_states_estimation{};

  // Estimated cost of unscored states
  cost::cost_t unscored_states_estimation{};
};

// Compiled query suitable for filters with non adjacent set of terms.
class multiterm_query : public filter::prepared {
 public:
  typedef states_cache<multiterm_state> states_t;
  typedef std::vector<bstring> stats_t;

  explicit multiterm_query(states_t&& states,
                           std::shared_ptr<stats_t> const& stats, boost_t boost,
                           sort::MergeType merge_type)

      : prepared(boost),
        states_(std::move(states)),
        stats_ptr_(stats),
        merge_type_(merge_type) {
    assert(stats_ptr_);
  }

  // multiterm_query will own stats
  explicit multiterm_query(states_t&& states, stats_t&& stats, boost_t boost,
                           sort::MergeType merge_type)
      : prepared(boost),
        states_(std::move(states)),
        stats_(std::move(stats)),
        stats_ptr_(std::shared_ptr<stats_t>(), &stats_),
        merge_type_(merge_type) {
    assert(stats_ptr_);
  }

  virtual doc_iterator::ptr execute(
      const sub_reader& rdr, const Order& ord, ExecutionMode mode,
      const attribute_provider* ctx) const override;

 private:
  const stats_t& stats() const noexcept {
    assert(stats_ptr_);
    return *stats_ptr_;
  }

  states_t states_;
  stats_t stats_;
  std::shared_ptr<stats_t> stats_ptr_;
  sort::MergeType merge_type_;
};

}  // namespace iresearch

#endif  // IRESEARCH_MULTITERM_QUERY_H
