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

#include "index/index_reader.hpp"
#include "search/filter_visitor.hpp"

NS_LOCAL

//using namespace irs;
//
//class term_visitor final : public filter_visitor {
// public:
//  term_visitor() { }
//
//  virtual void prepare(const seek_term_iterator& terms) noexcept override {
//  }
//
//  virtual void visit() override {
//  }
//};
//
//template<typename Visitor>
//void visit(const term_reader& reader,
//           const std::vector<std::string>& geo_terms,
//           Visitor& visitor) {
//  auto terms = reader.iterator();
//
//  if (IRS_UNLIKELY(!terms)) {
//    return;
//  }
//
//  for (auto& geo_term : geo_terms) {
//    const bytes_ref term(
//      reinterpret_cast<const byte_type*>(geo_term.c_str()),
//      geo_term.size());
//
//    if (!terms->seek(term)) {
//      continue;
//    }
//
//    visitor.prepare(*terms);
//
//    // read term attributes
//    terms->read();
//
//    visitor.visit();
//  }
//}


std::vector<std::string> get_geo_terms(
    S2RegionTermIndexer& indexer,
    const irs::by_geo_distance_options& opts) {
  const S1ChordAngle radius(S1Angle::Radians(S2Earth::MetersToRadians(opts.distance)));

  // FIXME  if (!region.is_valid()) {
  if (!(S2::IsUnitLength(opts.point) && radius.length2() <= 4)) {
    return {};
  }

  const S2Cap region(opts.point, radius);
  return indexer.GetQueryTerms(region, {});
}

irs::filter::prepared::ptr prepare(
    const irs::string_ref& field,
    const std::vector<std::string>& geo_terms,
    const irs::index_reader& index,
    const irs::order::prepared& order,
    irs::boost_t boost) {
  for (auto& segment : index) {
    const auto* reader = segment.field(field);

    if (!reader) {
      continue;
    }

//    term_visitor tv;
//
//    ::visit(*reader, geo_terms, tv);
  }

  return irs::filter::prepared::empty();
}

NS_END

NS_ROOT

// ----------------------------------------------------------------------------
// --SECTION--                                                  by_geo_distance
// ----------------------------------------------------------------------------

filter::prepared::ptr by_geo_distance::prepare(
    const index_reader& index,
    const order::prepared& order,
    boost_t boost,
    const attribute_provider* /*ctx*/) const {
  return ::prepare(this->field(), ::get_geo_terms(indexer_, options()),
                   index, order, boost);
}


DEFINE_FACTORY_DEFAULT(by_geo_distance)

NS_END
