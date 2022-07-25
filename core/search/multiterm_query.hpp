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
#include "search/multiterm_state.hpp"
#include "search/states_cache.hpp"

namespace iresearch {

// Compiled query suitable for filters with non adjacent set of terms.
class multiterm_query final : public filter::prepared {
 public:
  using states_t = states_cache<MultiTermState>;
  using stats_t = std::vector<bstring>;

  explicit multiterm_query(states_t&& states, stats_t&& stats, score_t boost,
                           sort::MergeType merge_type, size_t min_match)
      : prepared{boost},
        states_{std::move(states)},
        stats_{std::move(stats)},
        merge_type_{merge_type},
        min_match_{min_match} {}

  doc_iterator::ptr execute(const ExecutionContext& ctx) const override;

  void visit(const sub_reader& segment,
             PreparedStateVisitor& visitor) const override;

 private:
  states_t states_;
  stats_t stats_;
  sort::MergeType merge_type_;
  size_t min_match_;
};

}  // namespace iresearch

#endif  // IRESEARCH_MULTITERM_QUERY_H
