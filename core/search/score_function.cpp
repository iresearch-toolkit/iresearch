////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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

#include "search/score_function.hpp"

namespace {

void DefaultScore(irs::score_ctx* ctx, irs::score_t* res) noexcept {
  assert(res);
  std::memset(res, 0, irs::bit_cast<size_t>(ctx));
}

}  // namespace

namespace iresearch {

/*static*/ const score_f ScoreFunction::kDefault{&::DefaultScore};

ScoreFunction::ScoreFunction() noexcept : func_{kDefault} {}

ScoreFunction::ScoreFunction(ScoreFunction&& rhs) noexcept
    : ctx_(std::move(rhs.ctx_)), func_(std::exchange(rhs.func_, kDefault)) {}

ScoreFunction& ScoreFunction::operator=(ScoreFunction&& rhs) noexcept {
  if (this != &rhs) {
    ctx_ = std::move(rhs.ctx_);
    func_ = std::exchange(rhs.func_, kDefault);
  }
  return *this;
}

}  // namespace iresearch
