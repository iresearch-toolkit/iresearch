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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "multiterm_filter.hpp"

NS_ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                                 all_term_selector
// -----------------------------------------------------------------------------
all_term_selector::all_term_selector(
    const index_reader& index, const order::prepared& order
): term_selector(index, order) {
}

filter::prepared::ptr all_term_selector::build(filter::boost_t boost) const {
  return nullptr; // FIXME TODO implement
}

void all_term_selector::insert(
    const sub_reader& segment,
    const term_reader& field,
    const term_iterator& term
) {
  // FIXME TODO implement
}

// -----------------------------------------------------------------------------
// --SECTION--                               limited_term_selector_by_field_size
// -----------------------------------------------------------------------------
limited_term_selector_by_field_size::limited_term_selector_by_field_size(
    const index_reader& index, const order::prepared& order
): term_selector(index, order) {
}

filter::prepared::ptr limited_term_selector_by_field_size::build(
    filter::boost_t boost
) const {
  return nullptr; // FIXME TODO implement
}

void limited_term_selector_by_field_size::insert(
    const sub_reader& segment,
    const term_reader& field,
    const term_iterator& term
) {
  // FIXME TODO implement
}

// -----------------------------------------------------------------------------
// --SECTION--                            limited_term_selector_by_postings_size
// -----------------------------------------------------------------------------
limited_term_selector_by_postings_size::limited_term_selector_by_postings_size(
    const index_reader& index, const order::prepared& order
): term_selector(index, order) {
}

filter::prepared::ptr limited_term_selector_by_postings_size::build(
    filter::boost_t boost
) const {
  return nullptr; // FIXME TODO implement
}

void limited_term_selector_by_postings_size::insert(
    const sub_reader& segment,
    const term_reader& field,
    const term_iterator& term
) {
  // FIXME TODO implement
}

// -----------------------------------------------------------------------------
// --SECTION--                             limited_term_selector_by_segment_size
// -----------------------------------------------------------------------------
limited_term_selector_by_segment_size::limited_term_selector_by_segment_size(
    const index_reader& index, const order::prepared& order
): term_selector(index, order) {
}

filter::prepared::ptr limited_term_selector_by_segment_size::build(
    filter::boost_t boost
) const {
  return nullptr; // FIXME TODO implement
}

void limited_term_selector_by_segment_size::insert(
    const sub_reader& segment,
    const term_reader& field,
    const term_iterator& term
) {
  // FIXME TODO implement
}

NS_END // ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------