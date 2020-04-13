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

#include "tests_shared.hpp"
#include "filter_test_case_base.hpp"

#include "search/terms_filter.hpp"

NS_LOCAL

irs::by_terms make_filter(
    const irs::string_ref& field,
    const std::vector<std::pair<irs::string_ref, irs::boost_t>>& terms,
    size_t num_match) {
  irs::by_terms q;
  *q.mutable_field() = field;
  for (auto& term : terms) {
    q.mutable_options()->terms.emplace_back(
      irs::ref_cast<irs::byte_type>(term.first),
      term.second);
  }
  q.mutable_options()->num_match = num_match;
  return q;
}

NS_END

TEST(by_terms_test, options) {
  irs::by_terms_options opts;
  ASSERT_TRUE(opts.terms.empty());
  ASSERT_EQ(0, opts.num_match);
}

TEST(by_terms_test, ctor) {
  irs::by_terms q;
  ASSERT_EQ(irs::by_terms::type(), q.type());
  ASSERT_EQ(irs::by_terms_options{}, q.options());
  ASSERT_TRUE(q.field().empty());
  ASSERT_EQ(irs::no_boost(), q.boost());
}

TEST(by_terms_test, equal) {
  const irs::by_terms q0 = make_filter("field", { { "bar", 0.5f }, {"baz", 0.25f} }, 1);
  const irs::by_terms q1 = make_filter("field", { { "bar", 0.5f }, {"baz", 0.25f} }, 1);
  ASSERT_EQ(q0, q1);
  ASSERT_EQ(q0.hash(), q1.hash());

  const irs::by_terms q2 = make_filter("field1", { { "bar", 0.5f }, {"baz", 0.25f} }, 1);
  ASSERT_NE(q0, q2);

  const irs::by_terms q3 = make_filter("field", { { "bar1", 0.5f }, {"baz", 0.25f} }, 1);
  ASSERT_NE(q0, q3);

  const irs::by_terms q4 = make_filter("field", { { "bar", 0.5f }, {"baz", 0.5f} }, 1);
  ASSERT_NE(q0, q4);

  const irs::by_terms q5 = make_filter("field", { { "bar", 0.5f }, {"baz", 0.25f} }, 2);
  ASSERT_NE(q0, q5);
}

TEST(by_terms_test, boost) {
  // no boost
  {
    irs::by_terms q = make_filter("field", { { "bar", 0.5f }, {"baz", 0.25f} }, 1);

    auto prepared = q.prepare(irs::sub_reader::empty());
    ASSERT_EQ(irs::no_boost(), prepared->boost());
  }

  // with boost
  {
    irs::boost_t boost = 1.5f;

    irs::by_terms q = make_filter("field", { { "bar", 0.5f }, {"baz", 0.25f} }, 1);
    q.boost(boost);

    auto prepared = q.prepare(irs::sub_reader::empty());
    ASSERT_EQ(boost, prepared->boost());
  }
}

class terms_filter_test_case : public tests::filter_test_case_base { };

TEST_P(terms_filter_test_case, simple_sequential) {
  // add segment
  {
    tests::json_doc_generator gen(
      resource("simple_sequential_utf8.json"),
      &tests::generic_json_field_factory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // empty query
  check_query(irs::by_terms(), docs_t{}, costs_t{0}, rdr);

  // empty field
  check_query(make_filter("", { { "xyz", 0.5f} }, 2), docs_t{}, costs_t{0}, rdr);

  // invalid field
  check_query(make_filter("same1", { { "xyz", 0.5f} }, 1), docs_t{}, costs_t{0}, rdr);

  // invalid term
  check_query(make_filter("same", { { "invalid_term", 0.5f} }, 1), docs_t{}, costs_t{0}, rdr);

  // empty pattern - no match
  check_query(make_filter("duplicated", { }, 1 ), docs_t{}, costs_t{0}, rdr);

  // no fields requested to match
  check_query(make_filter("same", { { "xyz", 0.5f} }, 0), docs_t{}, costs_t{0}, rdr);

  // too many fields requested to match
  check_query(make_filter("same", { { "xyz", 0.5f} }, 2), docs_t{}, costs_t{0}, rdr);

  // FIXME check duplicate terms with different boosts

  // match all
  {
    docs_t result;
    for(size_t i = 0; i < 32; ++i) {
      result.push_back(irs::doc_id_t((irs::type_limits<irs::type_t::doc_id_t>::min)() + i));
    }
    costs_t costs{ result.size() };
    const auto filter = make_filter("same", { { "xyz", 1.f } }, 1);
    check_query(filter, result, costs, rdr);
  }

  // match something
  {
    const docs_t result{ 1, 21, 31, 32 };
    const costs_t costs{ result.size() };
    const auto filter = make_filter("prefix", { { "abcd", 1.f }, {"abc", 0.5f}, {"abcy", 0.5f} }, 1);
    check_query(filter, result, costs, rdr);
  }
}

INSTANTIATE_TEST_CASE_P(
  terms_filter_test,
  terms_filter_test_case,
  ::testing::Combine(
    ::testing::Values(
      &tests::memory_directory,
      &tests::fs_directory,
      &tests::mmap_directory
    ),
    ::testing::Values(tests::format_info{"1_0"},
                      tests::format_info{"1_1", "1_0"},
                      tests::format_info{"1_2", "1_0"},
                      tests::format_info{"1_3", "1_0"})
  ),
  tests::to_string
);
