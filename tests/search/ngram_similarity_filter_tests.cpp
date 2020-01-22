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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#include "tests_shared.hpp"
#include "filter_test_case_base.hpp"

#include "search/ngram_similarity_filter.hpp"


#include <functional>

NS_BEGIN(tests)

class ngram_similarity_filter_test_case : public tests::filter_test_case_base {
protected:
};

TEST_P(ngram_similarity_filter_test_case, missed_last_test) {
  // add segment
  {
    tests::json_doc_generator gen(
      resource("ngram_similarity.json"),
      &tests::generic_json_field_factory);
    add_segment( gen );
  }

  auto rdr = open_reader();

  irs::by_ngram_similarity filter;
  filter.threshold(0.5).field("field").push_back("at")
    .push_back("tl").push_back("la").push_back("as")
    .push_back("ll").push_back("never_match");

  std::vector<irs::doc_id_t> expected{ 1, 2, 5, 8};
  const size_t expected_size = expected.size();
  auto prepared = filter.prepare(rdr, irs::order::prepared::unordered());
  size_t count = 0;
  for (const auto& sub : rdr) {
    auto docs = prepared->execute(sub);

    auto& doc = docs->attributes().get<irs::document>();
    ASSERT_TRUE(bool(doc)); // ensure all iterators contain "document" attribute
    while (docs->next()) {
      ASSERT_EQ(docs->value(), doc->value);
      expected.erase(std::remove(expected.begin(), expected.end(), docs->value()), expected.end());
      ++count;
    }
  }
  ASSERT_EQ(expected_size, count);
  ASSERT_EQ(0, expected.size());
}

TEST_P(ngram_similarity_filter_test_case, missed_middle_test) {
  // add segment
  {
    tests::json_doc_generator gen(
      resource("ngram_similarity.json"),
      &tests::generic_json_field_factory);
    add_segment( gen );
  }

  auto rdr = open_reader();

  irs::by_ngram_similarity filter;
  filter.threshold(0.333).field("field").push_back("at")
    .push_back("never_match").push_back("la").push_back("as")
    .push_back("ll");

  std::vector<irs::doc_id_t> expected{ 1, 2, 3, 4, 5, 7, 8, 11};
  const size_t expected_size = expected.size();

  auto prepared = filter.prepare(rdr, irs::order::prepared::unordered());
  size_t count = 0;
  for (const auto& sub : rdr) {
    auto docs = prepared->execute(sub);

    auto& doc = docs->attributes().get<irs::document>();
    ASSERT_TRUE(bool(doc)); // ensure all iterators contain "document" attribute
    while (docs->next()) {
      ASSERT_EQ(docs->value(), doc->value);
      expected.erase(std::remove(expected.begin(), expected.end(), docs->value()), expected.end());
      ++count;
    }
  }
  ASSERT_EQ(expected_size, count);
  ASSERT_EQ(0, expected.size());
}

TEST_P(ngram_similarity_filter_test_case, missed_middle2_test) {
  // add segment
  {
    tests::json_doc_generator gen(
      resource("ngram_similarity.json"),
      &tests::generic_json_field_factory);
    add_segment( gen );
  }

  auto rdr = open_reader();

  irs::by_ngram_similarity filter;
  filter.threshold(0.333).field("field").push_back("at")
    .push_back("never_match").push_back("never_match2").push_back("la").push_back("as")
    .push_back("ll");

  std::vector<irs::doc_id_t> expected{ 1, 2, 3, 4, 5, 8, 11, 12};
  const size_t expected_size = expected.size();

  auto prepared = filter.prepare(rdr, irs::order::prepared::unordered());
  size_t count = 0;
  for (const auto& sub : rdr) {
    auto docs = prepared->execute(sub);

    auto& doc = docs->attributes().get<irs::document>();
    ASSERT_TRUE(bool(doc)); // ensure all iterators contain "document" attribute
    while (docs->next()) {
      ASSERT_EQ(docs->value(), doc->value);
      expected.erase(std::remove(expected.begin(), expected.end(), docs->value()), expected.end());
      ++count;
    }
  }
  ASSERT_EQ(expected_size, count);
  ASSERT_EQ(0, expected.size());
}

TEST_P(ngram_similarity_filter_test_case, missed_middle3_test) {
  // add segment
  {
    tests::json_doc_generator gen(
      resource("ngram_similarity.json"),
      &tests::generic_json_field_factory);
    add_segment( gen );
  }

  auto rdr = open_reader();

  irs::by_ngram_similarity filter;
  filter.threshold(0.28).field("field").push_back("at")
    .push_back("never_match").push_back("tl").push_back("never_match2").push_back("la").push_back("as")
    .push_back("ll");

  std::vector<irs::doc_id_t> expected{ 1, 2, 3, 4, 5, 8};
  const size_t expected_size = expected.size();

  auto prepared = filter.prepare(rdr, irs::order::prepared::unordered());
  size_t count = 0;
  for (const auto& sub : rdr) {
    auto docs = prepared->execute(sub);

    auto& doc = docs->attributes().get<irs::document>();
    ASSERT_TRUE(bool(doc)); // ensure all iterators contain "document" attribute
    while (docs->next()) {
      ASSERT_EQ(docs->value(), doc->value);
      expected.erase(std::remove(expected.begin(), expected.end(), docs->value()), expected.end());
      ++count;
    }
  }
  ASSERT_EQ(expected_size, count);
  ASSERT_EQ(0, expected.size());
}

INSTANTIATE_TEST_CASE_P(
  ngram_similarity_test,
  ngram_similarity_filter_test_case,
  ::testing::Combine(
    ::testing::Values(
      &tests::memory_directory,
      &tests::fs_directory,
      &tests::mmap_directory
    ),
    ::testing::Values("1_0", "1_1", "1_2", "1_3")
  ),
  tests::to_string
);

NS_END // tests