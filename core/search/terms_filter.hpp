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

#ifndef IRESEARCH_TERMS_FILTER_H
#define IRESEARCH_TERMS_FILTER_H

#include "search/filter.hpp"
#include "utils/string.hpp"

NS_ROOT

class by_terms;
struct filter_visitor;

////////////////////////////////////////////////////////////////////////////////
/// @struct by_terms_options
/// @brief options for terms filter
////////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API by_terms_options {
  using filter_type = by_terms;
  using filter_options = by_terms_options;

  using search_term = std::pair<bstring, boost_t>;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief search terms
  //////////////////////////////////////////////////////////////////////////////
  std::vector<search_term> terms;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief min number of terms to match
  //////////////////////////////////////////////////////////////////////////////
  size_t num_match{0};

  bool operator==(const by_terms_options& rhs) const noexcept {
    return terms == rhs.terms && num_match == rhs.num_match;
  }

  size_t hash() const noexcept {
    size_t hash = std::hash<decltype(num_match)>()(num_match);
    for (auto& term : terms) {
      hash = hash_combine(hash, std::hash<decltype(term.first)>()(term.first));
      hash = hash_combine(hash, std::hash<decltype(term.second)>()(term.second));
    }
    return hash;
  }
}; // by_terms_options

IRESEARCH_API field_visitor visitor(const by_terms_options::filter_options& options);

////////////////////////////////////////////////////////////////////////////////
/// @class by_terms
/// @brief user-side filter by a set of terms
////////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API by_terms final
    : public filter_base<by_terms_options> {
 public:
  DECLARE_FILTER_TYPE();
  DECLARE_FACTORY();

  using filter::prepare;

  virtual filter::prepared::ptr prepare(
    const index_reader& index,
    const order::prepared& order,
    boost_t boost,
    const attribute_view& /*ctx*/) const override;
}; // by_terms

NS_END

#endif // IRESEARCH_TERMS_FILTER_H

