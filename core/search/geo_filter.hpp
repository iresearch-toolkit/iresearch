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

#include "filter.hpp"

NS_ROOT

//////////////////////////////////////////////////////////////////////////////
/// @class geo_filter
/// @brief base class for all geo filters
//////////////////////////////////////////////////////////////////////////////
class geo_filter : public filter {
 public:
  geo_filter& field(std::string field) {
    field_ = std::move(field);
    return *this;
  }

  const std::string& field() const noexcept {
    return field_;
  }

  using filter::prepare;

  virtual filter::prepared::ptr prepare(
    const index_reader& rdr,
    const order::prepared& ord,
    boost_t boost,
    const attribute_view& ctx) const override;

  virtual size_t hash() const noexcept override;

  const S2RegionTermIndexer::Options& options() const noexcept {
    return indexer_.options();
  }

  geo_filter& options(const S2RegionTermIndexer::Options& options) {
    *indexer_.mutable_options() = options;
    return *this;
  }

 protected:
  geo_filter(const type_id& type);

  virtual bool equals(const filter &rhs) const noexcept override;

  virtual std::vector<std::string>
  get_geo_terms(S2RegionTermIndexer& indexer) const = 0;

 private:
  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  std::string field_;
  mutable S2RegionTermIndexer indexer_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
}; // geo_filter

//////////////////////////////////////////////////////////////////////////////
/// @class by_geo_distance
/// @brief user-side geo distance filter
//////////////////////////////////////////////////////////////////////////////
class by_geo_distance final : public geo_filter {
 public:
  DECLARE_FILTER_TYPE();
  DECLARE_FACTORY();

  by_geo_distance();

  virtual size_t hash() const noexcept override;

 protected:
  virtual bool equals(const filter &rhs) const noexcept override;

  virtual std::vector<std::string>
  get_geo_terms(S2RegionTermIndexer& indexer) const override;

 private:
  S2Point point_;
  double_t distance_{0.};
}; // by_geo_distance

NS_END

#endif // IRESEARCH_GEO_FILTER_H
