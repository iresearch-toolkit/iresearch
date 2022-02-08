////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/container/flat_hash_map.h>

#include "analysis/token_attributes.hpp"
#include "filter.hpp"
#include "types.hpp"
#include "utils/type_id.hpp"

namespace iresearch {

class proxy_query_cache;

class proxy_filter final : public filter {
 public:
  static ptr make();

  using cache_ptr = std::shared_ptr<proxy_query_cache>;

  static cache_ptr make_cache();

  proxy_filter() noexcept : filter(irs::type<proxy_filter>::get()) {}

  filter::prepared::ptr prepare(const index_reader& rdr,
                                const order::prepared& ord, boost_t boost,
    const attribute_provider* ctx) const override;

  proxy_filter& add(filter::ptr&& real_filter) {
    real_filter_ = std::move(real_filter);
    return *this;
  }

  proxy_filter& set_cache(cache_ptr cache) {
    cache_ = cache;
    return *this;
  }

 private:
  mutable filter::ptr real_filter_{nullptr};
  mutable cache_ptr cache_;
};
}  // namespace iresearch