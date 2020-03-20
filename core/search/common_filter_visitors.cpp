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
/// @author Yuriy Popov
////////////////////////////////////////////////////////////////////////////////

#include "common_filter_visitors.hpp"

NS_ROOT

void filter_if_visitor(void* ctx, const seek_term_iterator::ptr& terms) {
  assert(ctx);
  auto& vis_ctx = *reinterpret_cast<filter_visitor_ctx*>(ctx);
  // get term metadata
  auto& meta = terms->attributes().get<term_meta>();

  // NOTE: we can't use reference to 'docs_count' here, like
  // 'const auto& docs_count = meta ? meta->docs_count : NO_DOCS;'
  // since not gcc4.9 nor msvc2015-2019 can handle this correctly
  // probably due to broken optimization
  vis_ctx.docs_count = meta ? &meta->docs_count : &vis_ctx.NO_DOCS;

  // get state for current segment
  vis_ctx.state = &vis_ctx.states.insert(vis_ctx.segment);
  vis_ctx.state->reader = &vis_ctx.reader;
}

void filter_loop_visitor(void* ctx, const seek_term_iterator::ptr& terms) {
  assert(ctx);
  auto& vis_ctx = *reinterpret_cast<filter_visitor_ctx*>(ctx);

  // fill scoring candidates
  assert(vis_ctx.docs_count);
  assert(vis_ctx.state);
  vis_ctx.scorer.collect(*vis_ctx.docs_count, vis_ctx.state->count++, *vis_ctx.state, vis_ctx.segment, *terms);
  vis_ctx.state->estimation += *vis_ctx.docs_count; // collect cost
}

NS_END
