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

#ifndef IRESEARCH_PREFIX_FILTER_H
#define IRESEARCH_PREFIX_FILTER_H

#include "search/filter.hpp"
#include "utils/string.hpp"

NS_ROOT

class by_prefix;
struct filter_visitor;

////////////////////////////////////////////////////////////////////////////////
/// @struct by_prefix_options
/// @brief options for prefix filter
////////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API by_prefix_options {
  using filter_type = by_prefix;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief search prefix
  //////////////////////////////////////////////////////////////////////////////
  bstring term;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief the maximum number of most frequent terms to consider for scoring
  //////////////////////////////////////////////////////////////////////////////
  size_t scored_terms_limit{1024};

  bool operator==(const by_prefix_options& rhs) const noexcept {
    return term == rhs.term && scored_terms_limit == rhs.scored_terms_limit;
  }

  size_t hash() const noexcept {
    return hash_combine(std::hash<size_t>()(scored_terms_limit),
                        hash_utils::hash(term));
  }
}; // by_prefix_options

////////////////////////////////////////////////////////////////////////////////
/// @class by_prefix
/// @brief user-side prefix filter
////////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API by_prefix : public filter_with_field<by_prefix_options> {
 public:
  DECLARE_FILTER_TYPE();
  DECLARE_FACTORY();

  static prepared::ptr prepare(
    const index_reader& index,
    const order::prepared& ord,
    boost_t boost,
    const string_ref& field,
    const bytes_ref& prefix,
    size_t scored_terms_limit);

  static void visit(
    const term_reader& reader,
    const bytes_ref& prefix,
    filter_visitor& visitor);

  by_prefix() = default;

  using filter::prepare;

  virtual filter::prepared::ptr prepare(
      const index_reader& index,
      const order::prepared& ord,
      boost_t boost,
      const attribute_view& /*ctx*/) const override {
    return prepare(index, ord, this->boost()*boost,
                   field(), options().term,
                   options().scored_terms_limit);
  }
}; // by_prefix

NS_END

#endif
