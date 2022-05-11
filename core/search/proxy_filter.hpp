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

struct proxy_query_cache;

// Proxy filter designed to cache results of underlying real filter and
// provide fast replaying same filter on consequent calls.
// It is up to caller to control validity of the supplied cache. If the index
// was changed caller must discard cache and request a new one via make_cache.
// Scoring cache is not supported yet.
class proxy_filter final : public filter {
 public:
  static ptr make();

  using cache_ptr = std::shared_ptr<proxy_query_cache>;

  proxy_filter() noexcept;

  using filter::prepare;

  filter::prepared::ptr prepare(const index_reader& rdr, const Order&,
                                score_t boost,
                                const attribute_provider*) const override;

  template<typename T>
  std::pair<T&, cache_ptr> set_filter() {
    using type = typename std::enable_if_t<std::is_base_of_v<filter, T>, T>;
    auto& ptr = cache_filter(type::make());
    return {static_cast<type&>(ptr), cache_};
  }

  proxy_filter& set_cache(const cache_ptr& cache) {
    cache_ = cache;
    return *this;
  }

 private:
  filter& cache_filter(filter::ptr&& ptr);

  mutable cache_ptr cache_;
};
}  // namespace iresearch
