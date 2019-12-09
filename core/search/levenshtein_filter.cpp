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

#include "levenshtein_filter.hpp"

#include "shared.hpp"
#include "limited_sample_scorer.hpp"
#include "index/index_reader.hpp"
#include "utils/automaton_utils.hpp"
#include "utils/hash_utils.hpp"

NS_ROOT

DEFINE_FILTER_TYPE(by_edit_distance)
DEFINE_FACTORY_DEFAULT(by_edit_distance)

filter::prepared::ptr by_edit_distance::prepare(
    const index_reader& index,
    const order::prepared& order,
    boost_t boost,
    const attribute_view& ctx) const {
  if (0 == max_distance_) {
    return by_term::prepare(index, order, boost, ctx);
  }

  boost *= this->boost();
  const string_ref field = this->field();

  //return prepare_automaton_filter(field, from_wildcard<byte_type, wildcard_traits_t>(term()),
  //                                scored_terms_limit(), index, order, boost);

  assert(false);
  return prepared::empty();
}

by_edit_distance::by_edit_distance() noexcept
  : by_prefix(by_edit_distance::type()) {
}

size_t by_edit_distance::hash() const noexcept {
  size_t seed = 0;
  seed = hash_combine(0, by_prefix::hash());
  seed = hash_combine(seed, max_distance_);
  return seed;
}

bool by_edit_distance::equals(const filter& rhs) const noexcept {
  const auto& impl = static_cast<const by_edit_distance&>(rhs);
  return by_prefix::equals(rhs) && max_distance_ == impl.max_distance_;
}

NS_END
