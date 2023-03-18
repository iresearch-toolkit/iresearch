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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "sort.hpp"

#include "analysis/token_attributes.hpp"
#include "index/index_reader.hpp"
#include "shared.hpp"
#include "utils/memory_pool.hpp"

namespace irs {

REGISTER_ATTRIBUTE(filter_boost);

Scorers Scorers::Prepare(std::span<const Scorer*> scorers) {
  ScorerBuckets buckets;
  buckets.reserve(scorers.size());

  IndexFeatures features{};
  size_t stats_size{};
  size_t stats_align{};

  for (const auto* scorer : scorers) {
    if (IRS_UNLIKELY(!scorer)) {
      continue;
    }

    // cppcheck-suppress shadowFunction
    const auto [bucket_stats_size, bucket_stats_align] = scorer->stats_size();
    IRS_ASSERT(bucket_stats_align <= alignof(std::max_align_t));
    IRS_ASSERT(
      math::is_power2(bucket_stats_align));  // math::is_power2(0) returns true

    stats_align = std::max(stats_align, bucket_stats_align);

    stats_size = memory::align_up(stats_size, bucket_stats_align);
    features |= scorer->index_features();

    buckets.emplace_back(*scorer, stats_size);

    stats_size += memory::align_up(bucket_stats_size, bucket_stats_align);
  }

  stats_size = memory::align_up(stats_size, stats_align);

  return {std::move(buckets), stats_size, features};
}

const Scorers Scorers::kUnordered;

}  // namespace irs
