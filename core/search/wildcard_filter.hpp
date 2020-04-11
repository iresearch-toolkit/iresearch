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

#ifndef IRESEARCH_WILDCARD_FILTER_H
#define IRESEARCH_WILDCARD_FILTER_H

#include "search/filter.hpp"
#include "utils/string.hpp"

NS_ROOT

class by_wildcard;
struct filter_visitor;

////////////////////////////////////////////////////////////////////////////////
/// @struct by_wildcard_options
/// @brief options for wildcard filter
////////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API by_wildcard_options : single_term_options<by_wildcard> { };

//////////////////////////////////////////////////////////////////////////////
/// @class by_wildcard
/// @brief user-side wildcard filter
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API by_wildcard final : public filter_with_field<by_wildcard_options> {
 public:
  DECLARE_FILTER_TYPE();
  DECLARE_FACTORY();

  static prepared::ptr prepare(
    const index_reader& index,
    const order::prepared& order,
    boost_t boost,
    const string_ref& field,
    bytes_ref term,
    size_t scored_terms_limit);

  static void visit(
    const term_reader& reader,
    bytes_ref term,
    filter_visitor& fv);

  by_wildcard() = default;

  using filter::prepare;

  virtual filter::prepared::ptr prepare(
      const index_reader& index,
      const order::prepared& order,
      boost_t boost,
      const attribute_view& /*ctx*/) const override {
    return prepare(index, order, this->boost()*boost,
                   field(), options().term, scored_terms_limit());
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief the maximum number of most frequent terms to consider for scoring
  //////////////////////////////////////////////////////////////////////////////
  by_wildcard& scored_terms_limit(size_t limit) noexcept {
    scored_terms_limit_ = limit;
    return *this;
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief the maximum number of most frequent terms to consider for scoring
  //////////////////////////////////////////////////////////////////////////////
  size_t scored_terms_limit() const noexcept {
    return scored_terms_limit_;
  }

  virtual size_t hash() const noexcept override;

 protected:
  virtual bool equals(const filter& rhs) const noexcept override;

 private:
  size_t scored_terms_limit_{1024};
}; // by_wildcard

#endif // IRESEARCH_WILDCARD_FILTER_H

NS_END
