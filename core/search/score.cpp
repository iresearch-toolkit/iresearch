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

void reset(irs::score& score, std::vector<Scorer>&& scorers) {
  // FIXME(gnusi): revisit, we either have to
  // disallow creation of nullptr scorers
  // or correctly track score offsets

  switch (scorers.size()) {
    case 0: {
      score.reset();
    } break;
    case 1: {
      auto& scorer = scorers.front();
      if (0 == scorer.bucket->score_index) {
        // The most important and frequent case when only
        // one scorer is provided.
        score.reset(std::move(scorer.func));
      } else {
        struct ctx : score_ctx {
          explicit ctx(Scorer&& scorer) noexcept
            : scorer{std::move(scorer)} {
          }

          Scorer scorer;
        };

        // FIXME(gnusi): revisit
        score.reset(
          memory::make_unique<ctx>(std::move(scorer)),
          [](score_ctx* ctx, score_t* res) noexcept {
            auto& scorer = static_cast<struct ctx*>(ctx)->scorer;
            scorer.func(res + scorer.bucket->score_index);
        });
      }
    } break;
    case 2: {
      struct ctx : score_ctx {
        explicit ctx(std::vector<Scorer>&& scorers) noexcept
          : scorers{std::move(scorers)} {
        }

        std::vector<Scorer> scorers;
      };

      score.reset(
        memory::make_unique<ctx>(std::move(scorers)),
        [](score_ctx* ctx, score_t* res)  noexcept  {
          auto& scorers = static_cast<struct ctx*>(ctx)->scorers;
          scorers.front().func(res);
          scorers.back().func(res + 1);
      });
    } break;
    default: {
      struct ctx : score_ctx {
        explicit ctx(std::vector<Scorer>&& scorers) noexcept
          : scorers{std::move(scorers)} {
        }

        std::vector<Scorer> scorers;
      };

      score.reset(
        memory::make_unique<ctx>(std::move(scorers)),
        [](score_ctx* ctx, score_t* res) noexcept {
          auto& scorers = static_cast<struct ctx*>(ctx)->scorers;
          for (auto& scorer : scorers) {
            scorer.func(res++);
          }
      });
    } break;
  }
}

} // ROOT
