////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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

#include "score.hpp"

#include "formats/formats.hpp"
#include "index/field_meta.hpp"
#include "shared.hpp"

namespace irs {

const score score::kNoScore;

ScoreFunctions PrepareScorers(std::span<const ScorerBucket> buckets,
                              const ColumnProvider& segment,
                              const term_reader& field,
                              const byte_type* stats_buf,
                              const attribute_provider& doc, score_t boost) {
  ScoreFunctions scorers;
  scorers.reserve(buckets.size());

  for (const auto& entry : buckets) {
    const auto& bucket = *entry.bucket;

    if (IRS_UNLIKELY(!entry.bucket)) {
      continue;
    }

    auto scorer =
      bucket.prepare_scorer(segment, field.meta().features,
                            stats_buf + entry.stats_offset, doc, boost);

    if (IRS_LIKELY(scorer)) {
      scorers.emplace_back(std::move(scorer));
    } else {
      scorers.emplace_back(ScoreFunction::Default(1));
    }
  }

  return scorers;
}

ScoreFunction CompileScorers(ScoreFunctions&& scorers) {
  switch (scorers.size()) {
    case 0: {
      return ScoreFunction{};
    }
    case 1: {
      // The most important and frequent case when only
      // one scorer is provided.
      return std::move(scorers.front());
    }
    case 2: {
      struct Ctx final : score_ctx {
        Ctx(ScoreFunction&& func0, ScoreFunction&& func1) noexcept
          : func0{std::move(func0)}, func1{std::move(func1)} {}

        ScoreFunction func0;
        ScoreFunction func1;
      };

      return ScoreFunction::Make<Ctx>(
        [](score_ctx* ctx, score_t* res) noexcept {
          IRS_ASSERT(res != nullptr);
          auto* scorers_ctx = static_cast<Ctx*>(ctx);
          scorers_ctx->func0(res);
          scorers_ctx->func1(res + 1);
        },
        std::move(scorers.front()), std::move(scorers.back()));
    }
    default: {
      struct Ctx final : score_ctx {
        explicit Ctx(ScoreFunctions&& scorers) noexcept
          : scorers{std::move(scorers)} {}

        ScoreFunctions scorers;
      };

      return ScoreFunction::Make<Ctx>(
        [](score_ctx* ctx, score_t* res) noexcept {
          auto& scorers = static_cast<Ctx*>(ctx)->scorers;
          for (auto& scorer : scorers) {
            scorer(res++);
          }
        },
        std::move(scorers));
    }
  }
}

void PrepareCollectors(std::span<const ScorerBucket> order,
                       byte_type* stats_buf) {
  for (const auto& entry : order) {
    if (IRS_LIKELY(entry.bucket)) {
      entry.bucket->collect(stats_buf + entry.stats_offset, nullptr, nullptr);
    }
  }
}

}  // namespace irs
