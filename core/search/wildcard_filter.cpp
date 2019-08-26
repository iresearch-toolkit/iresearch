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

#include "shared.hpp"
#include "wildcard_filter.hpp"
#include "all_filter.hpp"

#include "utils/automaton.hpp"
#include "utils/hash_utils.hpp"

NS_LOCAL

using wildcard_traits_t = fst::fsa::WildcardTraits<irs::byte_type>;

NS_END

NS_ROOT

DEFINE_FILTER_TYPE(by_wildcard)
DEFINE_FACTORY_DEFAULT(by_wildcard)

filter::prepared::ptr by_wildcard::prepare(
    const index_reader& index,
    const order::prepared& ord,
    boost_t boost,
    const attribute_view& ctx) const {
  auto& pattern = term();

  if (pattern.empty()) {
    return by_term::prepare(index, ord, boost, ctx);
  }

  if (wildcard_traits_t::MatchAnyString == pattern.back()) {
    if (1 == pattern.size()) {
      return all().prepare(index, ord, this->boost()*boost, ctx);
    }

    if (0 == std::count(pattern.begin(), pattern.end() - 1, wildcard_traits_t::MatchAnyChar)) {
      return by_prefix::prepare(index, ord, boost, ctx);
    }
  }

  auto a = fst::fsa::fromWildcard<byte_type, wildcard_traits_t>(pattern);

  // build automaton

  return nullptr;
}

by_wildcard::by_wildcard() noexcept
  : by_prefix(by_wildcard::type()) {
}

NS_END
