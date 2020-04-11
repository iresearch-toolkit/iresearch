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

#include "term_filter.hpp"

#include <boost/functional/hash.hpp>

#include "term_query.hpp"
#include "cost.hpp"
#include "analysis/token_attributes.hpp"
#include "index/index_reader.hpp"

NS_ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                            by_term implementation
// -----------------------------------------------------------------------------

DEFINE_FILTER_TYPE(by_term)
DEFINE_FACTORY_DEFAULT(by_term)

filter::prepared::ptr by_term::prepare(
    const index_reader& rdr,
    const order::prepared& ord,
    boost_t boost,
    const attribute_view& /*ctx*/) const {
  return term_query::make(rdr, ord, boost*this->boost(), fld_, term_);
}

NS_END // ROOT
