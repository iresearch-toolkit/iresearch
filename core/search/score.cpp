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

#include "shared.hpp"

namespace iresearch {

/*static*/ const score score::kNoScore;

void reset(irs::score& score, Scorers&& scorers) {
  switch (scorers.size()) {
    case 0: {
      score = ScoreFunction{};
    } break;
    case 1: {
      // The most important and frequent case when only
      // one scorer is provided.
      score = std::move(scorers.front());
    } break;
    case 2: {
      struct ctx : score_ctx {
        ctx(ScoreFunction&& func0, ScoreFunction&& func1) noexcept
            : func0{std::move(func0)}, func1{std::move(func1)} {}

        ScoreFunction func0;
        ScoreFunction func1;
      };

      score.Reset(memory::make_unique<ctx>(std::move(scorers.front()),
                                           std::move(scorers.back())),
                  [](score_ctx* ctx, score_t* res) noexcept {
                    auto* scorers = static_cast<struct ctx*>(ctx);
                    scorers->func0(res);
                    scorers->func1(res + 1);
                  });
    } break;
    default: {
      struct ctx : score_ctx {
        explicit ctx(Scorers&& scorers) noexcept
            : scorers{std::move(scorers)} {}

        Scorers scorers;
      };

      score.Reset(memory::make_unique<ctx>(std::move(scorers)),
                  [](score_ctx* ctx, score_t* res) noexcept {
                    auto& scorers = static_cast<struct ctx*>(ctx)->scorers;
                    for (auto& scorer : scorers) {
                      scorer(res++);
                    }
                  });
    } break;
  }
}

}  // namespace iresearch
