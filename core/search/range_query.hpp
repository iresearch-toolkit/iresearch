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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_RANGE_QUERY_H
#define IRESEARCH_RANGE_QUERY_H

#include <map>
#include <unordered_map>

#include "filter.hpp"
#include "cost.hpp"
#include "limited_sample_scorer.hpp"
#include "utils/bitset.hpp"
#include "utils/string.hpp"

NS_ROOT

struct term_reader;

template<typename T>
struct range;

//////////////////////////////////////////////////////////////////////////////
/// @class range_state
/// @brief cached per reader range state
//////////////////////////////////////////////////////////////////////////////
struct range_state : public limited_sample_state,
                     private util::noncopyable {
  range_state() = default;

  range_state(range_state&& rhs) noexcept
    : limited_sample_state(std::move(rhs)),
      min_term(std::move(rhs.min_term)),
      min_cookie(std::move(rhs.min_cookie)),
      estimation(rhs.estimation),
      count(rhs.count) {
    rhs.reader = nullptr;
    rhs.count = 0;
    rhs.estimation = 0;
  }

  range_state& operator=(range_state&& rhs) noexcept {
    if (this != &rhs) {
      limited_sample_state::operator=(std::move(rhs));
      min_term = std::move(rhs.min_term);
      min_cookie = std::move(rhs.min_cookie);
      estimation = std::move(rhs.estimation);
      rhs.estimation = 0;
      count = std::move(rhs.count);
      rhs.count = 0;
    }

    return *this;
  }

  bstring min_term; // minimum term to start from
  seek_term_iterator::seek_cookie::ptr min_cookie; // cookie corresponding to the start term
  cost::cost_t estimation{}; // per-segment query estimation
  size_t count{}; // number of terms to process from start term
}; // reader_state

//////////////////////////////////////////////////////////////////////////////
/// @class range_query
/// @brief compiled query suitable for filters with continious range of terms
///        like "by_range" or "by_prefix". 
//////////////////////////////////////////////////////////////////////////////
class range_query : public filter::prepared {
 public:
  typedef states_cache<range_state> states_t;

  DECLARE_SHARED_PTR(range_query);

  static ptr make_from_prefix(
    const index_reader& index,
    const order::prepared& ord,
    boost_t boost,
    const string_ref& field,
    const bytes_ref& prefix,
    size_t scored_terms_limit);

  static ptr make_from_range(
    const index_reader& index,
    const order::prepared& ord,
    boost_t boost,
    const string_ref& field,
    const range<bstring>& range,
    size_t scored_terms_limit);

  explicit range_query(states_t&& states, boost_t boost)
    : prepared(boost), states_(std::move(states)) {
  }

  virtual doc_iterator::ptr execute(
      const sub_reader& rdr,
      const order::prepared& ord,
      const attribute_view& /*ctx*/) const override;

 private:
  states_t states_;
}; // range_query 

NS_END // ROOT

#endif // IRESEARCH_RANGE_QUERY_H 
