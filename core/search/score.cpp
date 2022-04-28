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

namespace {

using namespace irs;

const score_t* default_score(score_ctx* ctx) noexcept {
  return reinterpret_cast<score_t*>(ctx);
}

}

namespace iresearch {

/*static*/ const score score::kNoScore;
/*static*/ const score_f score::kDefaultScoreFunc{&::default_score};

score::score() noexcept
  : func_(reinterpret_cast<score_ctx*>(data()), &::default_score) {
}

score::score(const Order& ord)
  : buf_(ord.score_size, 0),
    func_(reinterpret_cast<score_ctx*>(data()), &::default_score) {
}

void score::reset() noexcept {
  func_.reset(reinterpret_cast<score_ctx*>(data()),
              &::default_score);
}

void reset(irs::score& score, Order::Scorers&& scorers) {
  switch (scorers.scorers.size()) {
    case 0: {
      score.reset();
    } break;
    case 1: {
      auto& scorer = scorers.scorers.front();
      if (!scorer.bucket->score_index) {
        score.reset(std::move(scorer.func));
      } else {
        struct ctx : score_ctx {
          explicit ctx(Order::Scorer&& scorer,
                       const score_t* score_buf) noexcept
            : scorer{std::move(scorer)},
              score_buf{score_buf} {
          }

          Order::Scorer scorer;
          const score_t* score_buf;
        };

        score.reset(
          memory::make_unique<ctx>(std::move(scorer), scorers.score_buf),
          [](score_ctx* ctx) {
            auto& state = *static_cast<struct ctx*>(ctx);
            state.scorer.func();
            return state.score_buf;
        });
      }
    } break;
    case 2: {
      struct ctx : score_ctx {
        explicit ctx(Order::Scorers&& scorers) noexcept
          : scorers(std::move(scorers)) {
        }

        Order::Scorers scorers;
      };

      score.reset(
        memory::make_unique<ctx>(std::move(scorers)),
        [](score_ctx* ctx) {
          auto& scorers = static_cast<struct ctx*>(ctx)->scorers;
          scorers.scorers.front().func();
          scorers.scorers.back().func();
          return scorers.score_buf;
      });
    } break;
    default: {
      struct ctx : score_ctx {
        explicit ctx(Order::Scorers&& scorers) noexcept
          : scorers(std::move(scorers)) {
        }

        Order::Scorers scorers;
      };

      score.reset(
        memory::make_unique<ctx>(std::move(scorers)),
        [](score_ctx* ctx) {
          auto& scorers = static_cast<struct ctx*>(ctx)->scorers;
          for (auto& scorer : scorers.scorers) {
            scorer.func();
          }
          return scorers.score_buf;
      });
    } break;
  }
}

} // ROOT
