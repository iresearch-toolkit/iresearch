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

#ifndef IRESEARCH_GEO_FILTER_H
#define IRESEARCH_GEO_FILTER_H

#include <s2/s2region_term_indexer.h>
#include <s2/util/math/vector3_hash.h>

#include "filter.hpp"

NS_ROOT

////////////////////////////////////////////////////////////////////////////////
/// @struct by_term_options
/// @brief options for term filter
////////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API by_geo_distance_options {
  using filter_type = by_geo_distance_options;

  S2Point point;
  double_t distance{0.};

  bool operator==(const by_geo_distance_options& rhs) const noexcept {
    return point == rhs.point && distance == rhs.distance;
  }

  size_t hash() const noexcept {
    return hash_combine(GoodFastHash<S2Point>()(point),
                        std::hash<decltype(distance)>()(distance));
  }
}; // by_term_options

//////////////////////////////////////////////////////////////////////////////
/// @class by_geo_distance
/// @brief user-side geo distance filter
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API by_geo_distance final
  : public filter_base<by_geo_distance_options>{
 public:
  static constexpr string_ref type_name() noexcept {
    return "iresearch::by_geo_distance";
  }

  DECLARE_FACTORY();

  using filter::prepare;

  virtual prepared::ptr prepare(
      const index_reader& rdr,
      const order::prepared& ord,
      boost_t boost,
      const attribute_provider* /*ctx*/) const;

 private:
  mutable S2RegionTermIndexer indexer_;
}; // by_geo_distance

NS_END

#endif // IRESEARCH_GEO_FILTER_H
