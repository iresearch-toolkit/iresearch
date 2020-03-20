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

#ifndef IRESEARCH_COMMON_FILTER_VISITORS_H
#define IRESEARCH_COMMON_FILTER_VISITORS_H

#include "multiterm_query.hpp"
#include "formats/formats.hpp"

NS_ROOT

struct filter_visitor_ctx {
  limited_sample_scorer& scorer;
  multiterm_query::states_t& states;
  const sub_reader& segment;
  const term_reader& reader;
  multiterm_state* state;
  const decltype(term_meta::docs_count) NO_DOCS;
  const decltype(term_meta::docs_count)* docs_count;
};

void filter_if_visitor(void* ctx, const seek_term_iterator::ptr& terms);

void filter_loop_visitor(void* ctx, const seek_term_iterator::ptr& terms);

NS_END

#endif // IRESEARCH_COMMON_FILTER_VISITORS_H
