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

#include "wildcard_filter.hpp"
#include "phrase_filter.hpp"

#include "shared.hpp"
#include "limited_sample_scorer.hpp"
#include "multiterm_query.hpp"
#include "term_query.hpp"
#include "index/index_reader.hpp"
#include "utils/wildcard_utils.hpp"
#include "utils/automaton_utils.hpp"
#include "utils/hash_utils.hpp"

NS_ROOT

NS_LOCAL

inline bytes_ref unescape(const bytes_ref& in, bstring& out) {
  out.reserve(in.size());

  bool copy = true;
  std::copy_if(in.begin(), in.end(), std::back_inserter(out),
               [&copy](byte_type c) {
    if (c == WildcardMatch::ESCAPE) {
      copy = !copy;
    } else {
      copy = true;
    }
    return copy;
  });

  return out;
}

template<typename Invalid, typename Term, typename Prefix, typename WildCard>
inline void executeWildcard(
    bstring& buf, bytes_ref& term, Invalid inv, Term t, Prefix p, WildCard w) {
  switch (wildcard_type(term)) {
    case WildcardType::INVALID:
      inv();
      break;
    case WildcardType::TERM_ESCAPED:
      term = unescape(term, buf);
#if IRESEARCH_CXX > IRESEARCH_CXX_14
      [[fallthrough]];
#endif
    case WildcardType::TERM:
      t(term);
      break;
    case WildcardType::MATCH_ALL:
      term = bytes_ref::EMPTY;
      p(term);
      break;
    case WildcardType::PREFIX_ESCAPED:
      term = unescape(term, buf);
#if IRESEARCH_CXX > IRESEARCH_CXX_14
      [[fallthrough]];
#endif
    case WildcardType::PREFIX: {
      assert(!term.empty());
      const auto* begin = term.c_str();
      const auto* end = begin + term.size();

      // term is already checked to be a valid UTF-8 sequence
      const auto* pos = utf8_utils::find<false>(begin, end, WildcardMatch::ANY_STRING);
      assert(pos != end);

      term = bytes_ref(begin, size_t(pos - begin)); // remove trailing '%'
      p(term);
      break;
    }
    case WildcardType::WILDCARD:
      w(term);
      break;
    default:
      assert(false);
      inv();
  }
}

NS_END

DEFINE_FILTER_TYPE(by_wildcard)
DEFINE_FACTORY_DEFAULT(by_wildcard)

/*static*/ filter::prepared::ptr by_wildcard::prepare(
    const index_reader& index,
    const order::prepared& order,
    boost_t boost,
    const string_ref& field,
    bytes_ref term,
    size_t scored_terms_limit) {
  bstring buf;
  filter::prepared::ptr res;
  executeWildcard(
    buf, term,
    [&res]() {
      res = prepared::empty(); },
    [&res, &index, &order, boost, &field](const bytes_ref& term) {
      res = term_query::make(index, order, boost, field, term);},
    [&res, &index, &order, boost, &field, scored_terms_limit](const bytes_ref& term) {
      res = by_prefix::prepare(index, order, boost, field, term, scored_terms_limit);},
    [&res, &index, &order, boost, &field, scored_terms_limit](const bytes_ref& term) {
      res = prepare_automaton_filter(field, from_wildcard(term), scored_terms_limit, index, order, boost);}
  );
  return res;
}

by_wildcard::by_wildcard() noexcept
  : by_prefix(by_wildcard::type()) {
}

void wildcard_phrase_helper(
    bytes_ref term,
    const sub_reader& segment,
    const term_reader& reader,
    const order::prepared::variadic_terms_collectors& collectors,
    size_t term_offset,
    void (*term_visitor)(void* ctx, const seek_term_iterator::ptr& terms),
    void* ctx,
    void (*previsitor)(void* ctx, const seek_term_iterator::ptr& terms),
    void (*if_visitor)(void* ctx),
    void (*loop_visitor)(void* ctx, const seek_term_iterator::ptr& terms)) {
  bstring buf;
  executeWildcard(
    buf, term,
    []() {},
    [&segment, &reader, &collectors, term_offset, ctx, term_visitor](const bytes_ref& term) {
      term_query::visit(segment, reader, term, collectors, term_offset, ctx, term_visitor);
    },
    [&reader, ctx, previsitor, if_visitor, loop_visitor](const bytes_ref& term) {
      by_prefix::visit(reader, term, ctx, previsitor, if_visitor, loop_visitor);
    },
    [&reader, ctx, previsitor, if_visitor, loop_visitor](const bytes_ref& term) {
      automaton_visit(from_wildcard(term), reader, ctx, previsitor, if_visitor, loop_visitor);
    }
  );
}

NS_END
