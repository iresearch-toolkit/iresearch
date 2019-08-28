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
#include "prefix_filter.hpp"
#include "range_query.hpp"
#include "analysis/token_attributes.hpp"
#include "index/index_reader.hpp"
#include "index/iterators.hpp"

#include <boost/functional/hash.hpp>

NS_ROOT

filter::prepared::ptr by_prefix::prepare(
    const index_reader& index,
    const order::prepared& ord,
    boost_t boost,
    const attribute_view& /*ctx*/) const {
  return range_query::make_from_prefix(index, ord, this->boost()*boost,
                                       field(), term(), scored_terms_limit_);
}

DEFINE_FILTER_TYPE(by_prefix)
DEFINE_FACTORY_DEFAULT(by_prefix)

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
