////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "prefix_filter.hpp"

#include <boost/functional/hash.hpp>

#include "shared.hpp"
#include "multiterm_query.hpp"
#include "analysis/token_attributes.hpp"
#include "index/index_reader.hpp"
#include "index/iterators.hpp"

NS_ROOT

NS_LOCAL

struct visitor_ctx {
  limited_sample_scorer& scorer;
  multiterm_query::states_t& states;
  const sub_reader& segment;
  const term_reader& reader;
  multiterm_state* state;
  const decltype(irs::term_meta::docs_count) NO_DOCS;
  const decltype(irs::term_meta::docs_count)* docs_count;
};

void previsitor(void* ctx, const seek_term_iterator::ptr& terms) {
  assert(ctx);
  auto& vis_ctx = *reinterpret_cast<visitor_ctx*>(ctx);
  // get term metadata
  auto& meta = terms->attributes().get<term_meta>();

  // NOTE: we can't use reference to 'docs_count' here, like
  // 'const auto& docs_count = meta ? meta->docs_count : NO_DOCS;'
  // since not gcc4.9 nor msvc2015-2019 can handle this correctly
  // probably due to broken optimization
  vis_ctx.docs_count = meta ? &meta->docs_count : &vis_ctx.NO_DOCS;
}

void if_visitor(void* ctx) {
  assert(ctx);
  auto& vis_ctx = *reinterpret_cast<visitor_ctx*>(ctx);
  // get state for current segment
  vis_ctx.state = &vis_ctx.states.insert(vis_ctx.segment);
  vis_ctx.state->reader = &vis_ctx.reader;
}

void loop_visitor(void* ctx, const seek_term_iterator::ptr& terms) {
  assert(ctx);
  auto& vis_ctx = *reinterpret_cast<visitor_ctx*>(ctx);

  // fill scoring candidates
  assert(vis_ctx.docs_count);
  assert(vis_ctx.state);
  vis_ctx.scorer.collect(*vis_ctx.docs_count, vis_ctx.state->count++, *vis_ctx.state, vis_ctx.segment, *terms);
  vis_ctx.state->estimation += *vis_ctx.docs_count; // collect cost
}

NS_END

/*static*/ bool by_prefix::visit(
    const term_reader& reader,
    const bytes_ref& prefix,
    void* ctx,
    void (*previsitor)(void* ctx, const seek_term_iterator::ptr& terms),
    void (*if_visitor)(void* ctx),
    void (*loop_visitor)(void* ctx, const seek_term_iterator::ptr& terms)) {
  // find term
  auto terms = reader.iterator();

  // seek to prefix
  if (IRS_UNLIKELY(!terms) || SeekResult::END == terms->seek_ge(prefix)) {
    return false;
  }

  previsitor(ctx, terms);

  const auto& value = terms->value();
  if (starts_with(value, prefix)) {
    terms->read();

    if_visitor(ctx);

    do {
      loop_visitor(ctx, terms);

      if (!terms->next()) {
        break;
      }

      terms->read();
    } while (starts_with(value, prefix));
  }

  return true;
}

DEFINE_FILTER_TYPE(by_prefix)
DEFINE_FACTORY_DEFAULT(by_prefix)

/*static*/ filter::prepared::ptr by_prefix::prepare(
    const index_reader& index,
    const order::prepared& ord,
    boost_t boost,
    const string_ref& field,
    const bytes_ref& prefix,
    size_t scored_terms_limit) {
  limited_sample_scorer scorer(ord.empty() ? 0 : scored_terms_limit); // object for collecting order stats
  multiterm_query::states_t states(index.size());

  // iterate over the segments
  for (const auto& segment: index) {
    // get term dictionary for field
    const term_reader* reader = segment.field(field);

    if (!reader) {
      continue;
    }

    visitor_ctx vis_ctx{scorer, states, segment, *reader, nullptr, 0, nullptr};

    visit(*reader, prefix, &vis_ctx, previsitor, if_visitor, loop_visitor);
  }

  std::vector<bstring> stats;
  scorer.score(index, ord, stats);

  return memory::make_shared<multiterm_query>(std::move(states), std::move(stats), boost);
}

by_prefix::by_prefix() noexcept : by_prefix(by_prefix::type()) { }

size_t by_prefix::hash() const noexcept {
  size_t seed = 0;
  ::boost::hash_combine(seed, by_term::hash());
  ::boost::hash_combine(seed, scored_terms_limit_);
  return seed;
}

bool by_prefix::equals(const filter& rhs) const noexcept {
  const auto& impl = static_cast<const by_prefix&>(rhs);
  return by_term::equals(rhs) && scored_terms_limit_ == impl.scored_terms_limit_;
}

NS_END // ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
