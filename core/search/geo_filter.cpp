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

#include "geo_filter.hpp"

#include <boost/functional/hash.hpp>
#include <s2/s2earth.h>
#include <s2/s2cap.h>

#include "filter_visitor.hpp"

NS_LOCAL



using namespace irs;

class term_visitor final : public filter_visitor {
 public:
  term_visitor() { }

  virtual void prepare(const seek_term_iterator& terms) noexcept override {
  }

  virtual void visit() override {
  }
};

template<typename Visitor>
void visit(const term_reader& reader,
           const std::vector<std::string>& geo_terms,
           Visitor& visitor) {
  auto terms = reader.iterator();

  if (IRS_UNLIKELY(!terms)) {
    return;
  }

  for (auto& geo_term : geo_terms) {
    const bytes_ref term(
      reinterpret_cast<const byte_type*>(geo_term.c_str()), 
      geo_term.size());

    if (!terms->seek(term)) {
      continue;
    }

    visitor.prepare(*terms);

    // read term attributes
    terms->read();

    visitor.visit();
  }
}


NS_END

NS_ROOT

// ----------------------------------------------------------------------------
// --SECTION--                                                       geo_filter
// ----------------------------------------------------------------------------

geo_filter::geo_filter(const type_id& type)
  : filter(type) {
}

bool geo_filter::equals(const filter& rhs) const noexcept {
  const geo_filter& rhs_impl = static_cast<const geo_filter&>(rhs);
  return filter::equals(rhs) && field_ == rhs_impl.field_;
}

size_t geo_filter::hash() const noexcept {
  size_t seed = 0;
  ::boost::hash_combine(seed, filter::hash());
  ::boost::hash_combine(seed, field_);
  return seed;
}

filter::prepared::ptr geo_filter::prepare(
    const index_reader& index,
    const order::prepared& order,
    boost_t boost,
    const attribute_view& /*ctx*/) const {
  const string_ref field = field_;
  const auto geo_terms = get_geo_terms(indexer_);

  for (auto& segment : index) {
    const auto* reader = segment.field(field);

    if (!reader) {
      continue;
    }

    term_visitor tv;

    ::visit(*reader, geo_terms, tv);
  }

  return prepared::empty();
}

// ----------------------------------------------------------------------------
// --SECTION--                                                  by_geo_distance
// ----------------------------------------------------------------------------

DEFINE_FILTER_TYPE(by_geo_distance)
DEFINE_FACTORY_DEFAULT(by_geo_distance)

by_geo_distance::by_geo_distance()
  : geo_filter(by_geo_distance::type()) {
}

bool by_geo_distance::equals(const filter& rhs) const noexcept {
  const by_geo_distance& rhs_impl = static_cast<const by_geo_distance&>(rhs);

  return geo_filter::equals(rhs) &&
         point_ == rhs_impl.point_ &&
         distance_ == rhs_impl.distance_;
}

std::vector<std::string> by_geo_distance::get_geo_terms(S2RegionTermIndexer& indexer) const {
  const S1ChordAngle radius(S1Angle::Radians(S2Earth::MetersToRadians(distance_)));

  // FIXME  if (!region.is_valid()) {
  if (!(S2::IsUnitLength(point_) && radius.length2() <= 4)) {
    return {};
  }

  const S2Cap region(point_, radius);
  return indexer.GetQueryTerms(region, {});
}

size_t by_geo_distance::hash() const noexcept {
  size_t seed = geo_filter::hash();
  ::boost::hash_combine(seed, GoodFastHash<S2Point>()(point_));
  ::boost::hash_combine(seed, distance_);
  return seed;
}


NS_END

