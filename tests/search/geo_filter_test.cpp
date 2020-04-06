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

#include "tests_shared.hpp"

#include "index/doc_generator.hpp"
#include "filter_test_case_base.hpp"

#include "analysis/geo_token_stream.hpp"
#include "search/geo_filter.hpp"

NS_LOCAL

struct geo_field final : tests::field_base {
  virtual irs::token_stream& get_tokens() const override {
    return stream;
  }

  virtual bool write(irs::data_output&) const override {
    return false;
  }

  mutable irs::analysis::geo_token_stream stream;
};

class geo_filter_test_case : public tests::filter_test_case_base { };

TEST_P(geo_filter_test_case, test) {
  // add segment
  {
    tests::json_doc_generator gen(
      resource("simple_sequential_geo.json"),
      [](tests::document& doc,
         const std::string& name,
         const tests::json_doc_generator::json_value& data) {

    });

    add_segment(gen);
  }

}

//#ifndef IRESEARCH_DLL
//TEST_P(term_filter_test_case, visit) {
//  // add segment
//  {
//    tests::json_doc_generator gen(
//      resource("simple_sequential.json"),
//      &tests::generic_json_field_factory);
//    add_segment(gen);
//  }
//  tests::empty_filter_visitor visitor;
//  std::string fld = "prefix";
//  irs::string_ref field = irs::string_ref(fld);
//  auto term = irs::ref_cast<irs::byte_type>(irs::string_ref("abc"));
//  // read segment
//  auto index = open_reader();
//  for (const auto& segment : index) {
//    // get term dictionary for field
//    const auto* reader = segment.field(field);
//    ASSERT_TRUE(reader != nullptr);
//
//    irs::term_query::visit(*reader, term, visitor);
//    ASSERT_EQ(1, visitor.prepare_calls_counter());
//    ASSERT_EQ(1, visitor.visit_calls_counter());
//    visitor.reset();
//  }
//}
//#endif
//
TEST(by_geo_distance_test, ctor) {
  irs::by_geo_distance q;
  ASSERT_EQ(irs::by_geo_distance::type(), q.type());
  ASSERT_EQ(S2Point(), q.point());
  ASSERT_EQ(0., q.distance());
  ASSERT_EQ("", q.field());
  ASSERT_EQ(irs::no_boost(), q.boost());
}

TEST(by_geo_distance_test, equal) {
  irs::by_geo_distance q;
  q.point(S2Point{1., 2., 3.}).distance(5.).field("field");
  ASSERT_EQ(q, irs::by_geo_distance().point(S2Point{1., 2., 3.}).distance(5.).field("field"));
  ASSERT_EQ(q.hash(), irs::by_geo_distance().point(S2Point{1., 2., 3.}).distance(5.).field("field").hash());
  ASSERT_NE(q, irs::by_geo_distance().point(S2Point{1., 2., 3.}).distance(5.).field("field1"));
  ASSERT_NE(q, irs::by_geo_distance().point(S2Point{1., 2., 3.}).distance(6.).field("field"));
  ASSERT_NE(q, irs::by_geo_distance().point(S2Point{2., 2., 3.}).distance(5.).field("field"));
  ASSERT_EQ(q, irs::by_geo_distance().point(S2Point{1., 2., 3.}).distance(5.).field("field").boost(2.));
}

TEST(by_geo_distance_test, boost) {
  // no boost
  {
    irs::by_geo_distance q;
    q.point(S2Point{1., 2., 3.}).distance(5.).field("field");

    auto prepared = q.prepare(irs::sub_reader::empty());
    ASSERT_EQ(irs::no_boost(), prepared->boost());
  }

  // with boost
  {
    irs::boost_t boost = 1.5f;
    irs::by_geo_distance q;
    q.point(S2Point{1., 2., 3.}).distance(5.).field("field");
    q.boost(boost);

    auto prepared = q.prepare(irs::sub_reader::empty());
    ASSERT_EQ(boost, prepared->boost());
  }
}

//
//INSTANTIATE_TEST_CASE_P(
//  term_filter_test,
//  term_filter_test_case,
//  ::testing::Combine(
//    ::testing::Values(
//      &tests::memory_directory,
//      &tests::fs_directory,
//      &tests::mmap_directory
//    ),
//    ::testing::Values("1_0")
//  ),
//  tests::to_string
//);

NS_END
