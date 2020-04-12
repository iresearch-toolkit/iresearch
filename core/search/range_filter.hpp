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

#ifndef IRESEARCH_RANGE_FILTER_H
#define IRESEARCH_RANGE_FILTER_H

#include "search/filter.hpp"
#include "utils/string.hpp"

NS_ROOT

class by_range;
struct filter_visitor;

enum class Bound {
  MIN, MAX
};

enum class BoundType {
  UNBOUNDED, INCLUSIVE, EXCLUSIVE
};

template<typename T>
struct generic_range {
  T min{};
  T max{};
  BoundType min_type = BoundType::UNBOUNDED;
  BoundType max_type = BoundType::UNBOUNDED;

  bool operator==(const generic_range& rhs) const {
    return min == rhs.min && min_type == rhs.min_type
      && max == rhs.max && max_type == rhs.max_type;
  }

  bool operator!=(const generic_range& rhs) const {
    return !(*this == rhs); 
  }
}; // range

////////////////////////////////////////////////////////////////////////////////
/// @struct by_prefix_options
/// @brief options for prefix filter
////////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API by_range_options {
  using filter_type = by_range;

  using range_type = generic_range<bstring>;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief search range
  //////////////////////////////////////////////////////////////////////////////
  range_type range;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief the maximum number of most frequent terms to consider for scoring
  //////////////////////////////////////////////////////////////////////////////
  size_t scored_terms_limit{1024};

  bool operator==(const by_range_options& rhs) const noexcept {
    return range == rhs.range && scored_terms_limit == rhs.scored_terms_limit;
  }

  size_t hash() const noexcept {
    const auto hash0 = hash_combine(hash_utils::hash(range.min),
                                    hash_utils::hash(range.max));
    const auto hash1 = hash_combine(std::hash<decltype(range.min_type)>()(range.min_type),
                                    std::hash<decltype(range.max_type)>()(range.max_type));
    return hash_combine(std::hash<decltype(scored_terms_limit)>()(scored_terms_limit),
                        hash_combine(hash0, hash1));
  }
}; // by_prefix_options

//////////////////////////////////////////////////////////////////////////////
/// @class by_range
/// @brief user-side term range filter
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API by_range
    : public filter_with_field<by_range_options> {
 public:
  DECLARE_FILTER_TYPE();
  DECLARE_FACTORY();

  static prepared::ptr prepare(
    const index_reader& index,
    const order::prepared& ord,
    boost_t boost,
    const string_ref& field,
    const options_type::range_type& rng,
    size_t scored_terms_limit);

  static void visit(
    const term_reader& reader,
    const options_type::range_type& rng,
    filter_visitor& visitor);

  by_range() = default;

  using filter::prepare;

  virtual filter::prepared::ptr prepare(
    const index_reader& index,
    const order::prepared& ord,
    boost_t boost,
    const attribute_view& /*ctx*/
  ) const override {
    return prepare(index, ord, this->boost()*boost,
                   field(), options().range,
                   options().scored_terms_limit);
  }

//  template<Bound B>
//  by_range& term(bstring&& term) {
//    get<B>::term(rng_) = std::move(term);
//
//    if (BoundType::UNBOUNDED == get<B>::type(rng_)) {
//      get<B>::type(rng_) = BoundType::EXCLUSIVE;
//    }
//
//    return *this;
//  }
//
//  template<Bound B>
//  by_range& term(const bytes_ref& term) {
//    get<B>::term(rng_) = term;
//
//    if (term.null()) {
//      get<B>::type(rng_) = BoundType::UNBOUNDED;
//    } else if (BoundType::UNBOUNDED == get<B>::type(rng_)) {
//      get<B>::type(rng_) = BoundType::EXCLUSIVE;
//    }
//
//    return *this;
//  }

//  template<Bound B>
//  by_range& include(bool incl) {
//    get<B>::type(rng_) = incl ? BoundType::INCLUSIVE : BoundType::EXCLUSIVE;
//    return *this;
//  }
}; // by_range 


NS_END // ROOT

#endif // IRESEARCH_RANGE_FILTER_H
