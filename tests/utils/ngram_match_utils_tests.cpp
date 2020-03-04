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
#include "utils/ngram_match_utils.hpp"


TEST(ngram_match_utils_test, test_similarity_empty_left) {
  const irs::string_ref lhs = ""; 
  const irs::string_ref rhs = "abcd";
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 1));
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 2));
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 3));
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 4));
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 5));
}

TEST(ngram_match_utils_test, test_similarity_empty_right) {
  const irs::string_ref lhs = "abcd"; 
  const irs::string_ref rhs = "";
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 1));
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 2));
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 3));
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 4));
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 5));
}

TEST(ngram_match_utils_test, test_similarity_no_match) {
  const irs::string_ref lhs = "abcd"; 
  const irs::string_ref rhs = "efgh";
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 1));
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 2));
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 3));
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 4));
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 5));
}

TEST(ngram_match_utils_test, test_similarity_simple) {
  const irs::string_ref lhs = "aecd"; 
  const irs::string_ref rhs = "abcd";
  ASSERT_DOUBLE_EQ(0.75f, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 1));
  ASSERT_DOUBLE_EQ(1.f/3.f, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 2));
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 3));
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 4));
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 5));
}

TEST(ngram_match_utils_test, test_similarity_different_length) {
  const irs::string_ref lhs = "applejuice"; 
  const irs::string_ref rhs = "aplejuice"; 
  ASSERT_DOUBLE_EQ(0.9f, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 1));
  ASSERT_DOUBLE_EQ(8.f/9.f, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 2));
  ASSERT_DOUBLE_EQ(0.75, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 3));
  ASSERT_DOUBLE_EQ(5.f/7.f, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 4));
  ASSERT_DOUBLE_EQ(4.f/6.f, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 5));
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 9));
}

TEST(ngram_match_utils_test, test_similarity_with_gaps) {
  const irs::string_ref lhs = "apple1234juice"; 
  const irs::string_ref rhs = "aple567juice"; 
  ASSERT_DOUBLE_EQ(9.f/14.f, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 1));
  ASSERT_DOUBLE_EQ(7.f/13.f, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 2));
  ASSERT_DOUBLE_EQ(4.f/12.f, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 3));
  ASSERT_DOUBLE_EQ(2.f/11.f, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 4));
  ASSERT_DOUBLE_EQ(1.f/10.f, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 5));
  ASSERT_DOUBLE_EQ(0, irs::ngram_similarity(lhs.begin(), lhs.size(), rhs.begin(), rhs.size(), 6));
}