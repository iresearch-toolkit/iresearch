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

#include "boost_sort.hpp"

namespace irs {
namespace {

Sort::ptr make_json(std::string_view /*args*/) {
  return std::make_unique<boost_sort>();
}

struct volatile_boost_score_ctx final : score_ctx {
  volatile_boost_score_ctx(const filter_boost* volatile_boost,
                           score_t boost) noexcept
    : boost{boost}, volatile_boost{volatile_boost} {
    IRS_ASSERT(volatile_boost);
  }

  score_t boost;
  const filter_boost* volatile_boost;
};

struct Prepared final : PreparedSortBase<void> {
  IndexFeatures features() const noexcept final { return IndexFeatures::NONE; }

  ScoreFunction prepare_scorer(const SubReader& /*segment*/,
                               const term_reader& /*field*/,
                               const byte_type* /*stats*/,
                               const irs::attribute_provider& attrs,
                               irs::score_t boost) const final {
    const auto* volatile_boost = irs::get<irs::filter_boost>(attrs);

    if (volatile_boost == nullptr) {
      return ScoreFunction::Constant(boost);
    }

    return ScoreFunction::Make<volatile_boost_score_ctx>(
      [](score_ctx* ctx, score_t* res) noexcept {
        auto& state = *static_cast<volatile_boost_score_ctx*>(ctx);
        *res = state.volatile_boost->value * state.boost;
      },
      volatile_boost, boost);
  }
};

}  // namespace

void boost_sort::init() { REGISTER_SCORER_JSON(boost_sort, make_json); }

boost_sort::boost_sort() noexcept : Sort{irs::type<boost_sort>::get()} {}

PreparedSort::ptr boost_sort::prepare() const {
  // FIXME can avoid allocation
  // TODO(MBkkt) managed_ptr? What exactly do you want?
  return std::make_unique<Prepared>();
}

}  // namespace irs
