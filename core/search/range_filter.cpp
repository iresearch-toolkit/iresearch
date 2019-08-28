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

#include "shared.hpp"
#include "range_filter.hpp"
#include "range_query.hpp"
#include "term_query.hpp"
#include "index/index_reader.hpp"
#include "analysis/token_attributes.hpp"

#include <boost/functional/hash.hpp>

NS_ROOT

DEFINE_FILTER_TYPE(by_range)
DEFINE_FACTORY_DEFAULT(by_range)

by_range::by_range() noexcept
  : filter(by_range::type()) {
}

bool by_range::equals(const filter& rhs) const noexcept {
  const by_range& trhs = static_cast<const by_range&>(rhs);
  return filter::equals(rhs) && fld_ == trhs.fld_ && rng_ == trhs.rng_;
}

size_t by_range::hash() const noexcept {
  size_t seed = 0;
  ::boost::hash_combine(seed, filter::hash());
  ::boost::hash_combine(seed, fld_);
  ::boost::hash_combine(seed, rng_.min);
  ::boost::hash_combine(seed, rng_.min_type);
  ::boost::hash_combine(seed, rng_.max);
  ::boost::hash_combine(seed, rng_.max_type);
  return seed;
}

filter::prepared::ptr by_range::prepare(
    const index_reader& index,
    const order::prepared& ord,
    boost_t boost,
    const attribute_view& /*ctx*/) const {
  //TODO: optimize unordered case
  // - seek to min
  // - get ordinal position of the term
  // - seek to max
  // - get ordinal position of the term

  if (rng_.min_type != BoundType::UNBOUNDED
      && rng_.max_type != BoundType::UNBOUNDED
      && rng_.min == rng_.max) {

    if (rng_.min_type == rng_.max_type && rng_.min_type == BoundType::INCLUSIVE) {
      // degenerated case
      return term_query::make(index, ord, boost*this->boost(), fld_, rng_.min);
    }

    // can't satisfy conditon
    return prepared::empty();
  }

  return range_query::make_from_range(index, ord, this->boost()*boost, fld_, rng_, scored_terms_limit_);
}

NS_END // ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
