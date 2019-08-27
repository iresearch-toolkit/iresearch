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
#include "range_query.hpp"
#include "term_query.hpp"
#include "index/index_reader.hpp"

#include "utils/automaton.hpp"
#include "utils/hash_utils.hpp"

NS_LOCAL

using wildcard_traits_t = fst::fsa::WildcardTraits<irs::byte_type>;

enum class WildcardType {
  MATCH_ALL = 0,
  TERM,
  PREFIX,
  WILDCARD
};

WildcardType type(const irs::bstring& expr) noexcept {
  if (expr.empty()) {
    return WildcardType::TERM;
  }

  bool escaped = false;
  size_t num_match_any_string = 0;
  for (const auto c : expr) {
    switch (c) {
      case wildcard_traits_t::MatchAnyString:
        num_match_any_string = size_t(!escaped);
        break;
      case wildcard_traits_t::MatchAnyChar:
        if (!escaped) {
          return WildcardType::WILDCARD;
        }
        break;
      case wildcard_traits_t::Escape:
       escaped = !escaped;
       break;
    }
  }

  if (0 == num_match_any_string) {
    return WildcardType::TERM;
  }

  if (1 == num_match_any_string) {
    if (1 == expr.size()) {
      return WildcardType::MATCH_ALL;
    }

    if (wildcard_traits_t::MatchAnyString == expr.back()) {
      return WildcardType::PREFIX;
    }
  }

  return WildcardType::WILDCARD;
}

NS_END

NS_ROOT

struct state {
  const term_reader* field;
  cost::cost_t estimation;
  std::vector<seek_term_iterator::seek_cookie::ptr> cookies;
};

DEFINE_FILTER_TYPE(by_wildcard)
DEFINE_FACTORY_DEFAULT(by_wildcard)

filter::prepared::ptr by_wildcard::prepare(
    const index_reader& index,
    const order::prepared& ord,
    boost_t boost,
    const attribute_view& ctx) const {
  const auto wildcard_type = ::type(term());

  switch (wildcard_type) {
    case WildcardType::MATCH_ALL:
      return all().prepare(index, ord, this->boost()*boost, ctx);
    case WildcardType::TERM:
      return term_query::make(index, ord, this->boost()*boost, field(), term());
    case WildcardType::PREFIX:
      return by_prefix::prepare(index, ord, boost, ctx);
    default:
      break;
  }

  assert(WildcardType::WILDCARD == wildcard_type);

  states_cache<state> states(index.size());
  limited_sample_scorer scorer(ord.empty() ? 0 : scored_terms_limit()); // object for collecting order stats
  auto acceptor = fst::fsa::fromWildcard<byte_type, wildcard_traits_t>(term());

  const string_ref field = this->field();

  for (const auto& segment : index) {
    // get term dictionary for field
    const term_reader* terms = segment.field(field);

    if (!terms) {
      continue;
    }

    seek_term_iterator::ptr it = terms->iterator();

    if (!it->next()) {
      continue;
    }

    auto& value = it->value();

    if (fst::fsa::accept(acceptor, value)) {
      auto& meta = it->attributes().get<term_meta>(); // get term metadata
      const decltype(irs::term_meta::docs_count) NO_DOCS = 0;
      const auto& docs_count = meta ? meta->docs_count : NO_DOCS;

      auto& state = states.insert(segment);
      state.field = terms;

      do {
        // read term attributes
        it->read();

        // get state for current segment
        state.cookies.emplace_back(it->cookie());
        state.estimation += docs_count;
      } while (it->next() && fst::fsa::accept(acceptor, value));
    }
  }

  return nullptr;
}

by_wildcard::by_wildcard() noexcept
  : by_prefix(by_wildcard::type()) {
}

NS_END
