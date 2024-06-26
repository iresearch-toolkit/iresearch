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
  const std::string_view lhs = "";
  const std::string_view rhs = "abcd";

  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 0)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 2)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 3)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 4)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 5)));

  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 0)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 2)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 3)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 4)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 5)));
}

TEST(ngram_match_utils_test, test_similarity_empty_right) {
  const std::string_view lhs = "abcd";
  const std::string_view rhs = "";

  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 0)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 2)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 3)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 4)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 5)));

  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 0)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 2)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 3)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 4)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 5)));
}

TEST(ngram_match_utils_test, test_similarity_no_match) {
  const std::string_view lhs = "abcd";
  const std::string_view rhs = "efgh";
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 0)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 2)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 3)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 4)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 5)));

  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 0)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 2)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 3)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 4)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 5)));
}

TEST(ngram_match_utils_test, test_similarity_simple) {
  const std::string_view lhs = "aecd";
  const std::string_view rhs = "abcd";

  ASSERT_DOUBLE_EQ(
    0.75f, (irs::ngram_similarity<char, false>(lhs.data(), lhs.size(),
                                               rhs.data(), rhs.size(), 1)));
  ASSERT_DOUBLE_EQ(
    0.75f, (irs::ngram_similarity<char, false>(rhs.data(), rhs.size(),
                                               lhs.data(), lhs.size(), 1)));
  ASSERT_DOUBLE_EQ(
    0.75f, (irs::ngram_similarity<char, true>(lhs.data(), lhs.size(),
                                              rhs.data(), rhs.size(), 1)));
  ASSERT_DOUBLE_EQ(
    0.75f, (irs::ngram_similarity<char, true>(rhs.data(), rhs.size(),
                                              lhs.data(), lhs.size(), 1)));

  ASSERT_DOUBLE_EQ(
    2.f / 3.f, (irs::ngram_similarity<char, false>(lhs.data(), lhs.size(),
                                                   rhs.data(), rhs.size(), 2)));
  ASSERT_DOUBLE_EQ(
    2.f / 3.f, (irs::ngram_similarity<char, false>(rhs.data(), rhs.size(),
                                                   lhs.data(), lhs.size(), 2)));
  ASSERT_DOUBLE_EQ(
    1.f / 3.f, (irs::ngram_similarity<char, true>(lhs.data(), lhs.size(),
                                                  rhs.data(), rhs.size(), 2)));
  ASSERT_DOUBLE_EQ(
    1.f / 3.f, (irs::ngram_similarity<char, true>(rhs.data(), rhs.size(),
                                                  lhs.data(), lhs.size(), 2)));

  ASSERT_DOUBLE_EQ(
    2.f / 3.f, (irs::ngram_similarity<char, false>(lhs.data(), lhs.size(),
                                                   rhs.data(), rhs.size(), 3)));
  ASSERT_DOUBLE_EQ(
    2.f / 3.f, (irs::ngram_similarity<char, false>(rhs.data(), rhs.size(),
                                                   lhs.data(), lhs.size(), 3)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 3)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        rhs.data(), rhs.size(), lhs.data(), lhs.size(), 3)));

  ASSERT_DOUBLE_EQ(
    3.f / 4.f, (irs::ngram_similarity<char, false>(lhs.data(), lhs.size(),
                                                   rhs.data(), rhs.size(), 4)));
  ASSERT_DOUBLE_EQ(
    3.f / 4.f, (irs::ngram_similarity<char, false>(rhs.data(), rhs.size(),
                                                   lhs.data(), lhs.size(), 4)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 4)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        rhs.data(), rhs.size(), lhs.data(), lhs.size(), 4)));
}

TEST(ngram_match_utils_test, test_similarity_different_length) {
  const std::string_view lhs = "applejuice";
  const std::string_view rhs = "aplejuice";
  ASSERT_DOUBLE_EQ(0.9f, (irs::ngram_similarity<char, false>(
                           lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
  ASSERT_DOUBLE_EQ(0.9f, (irs::ngram_similarity<char, false>(
                           rhs.data(), rhs.size(), lhs.data(), lhs.size(), 1)));
  ASSERT_DOUBLE_EQ(0.9f, (irs::ngram_similarity<char, true>(
                           lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
  ASSERT_DOUBLE_EQ(1, (irs::ngram_similarity<char, true>(
                        rhs.data(), rhs.size(), lhs.data(), lhs.size(), 1)));

  ASSERT_DOUBLE_EQ(
    8.f / 9.f, (irs::ngram_similarity<char, false>(lhs.data(), lhs.size(),
                                                   rhs.data(), rhs.size(), 2)));
  ASSERT_DOUBLE_EQ(
    8.f / 9.f, (irs::ngram_similarity<char, false>(rhs.data(), rhs.size(),
                                                   lhs.data(), lhs.size(), 2)));
  ASSERT_DOUBLE_EQ(
    8.f / 9.f, (irs::ngram_similarity<char, true>(lhs.data(), lhs.size(),
                                                  rhs.data(), rhs.size(), 2)));
  ASSERT_DOUBLE_EQ(1, (irs::ngram_similarity<char, true>(
                        rhs.data(), rhs.size(), lhs.data(), lhs.size(), 2)));

  ASSERT_DOUBLE_EQ((2.f / 3.f + 1.f + 1.f + 1.f + 1.f + 1.f + 1.f) / 8.f,
                   (irs::ngram_similarity<char, false>(
                     lhs.data(), lhs.size(), rhs.data(), rhs.size(), 3)));
  ASSERT_DOUBLE_EQ((2.f / 3.f + 1.f + 1.f + 1.f + 1.f + 1.f + 1.f) / 8.f,
                   (irs::ngram_similarity<char, false>(
                     rhs.data(), rhs.size(), lhs.data(), lhs.size(), 3)));
  ASSERT_DOUBLE_EQ(
    0.75f, (irs::ngram_similarity<char, true>(lhs.data(), lhs.size(),
                                              rhs.data(), rhs.size(), 3)));
  ASSERT_DOUBLE_EQ(
    6.f / 7.f, (irs::ngram_similarity<char, true>(rhs.data(), rhs.size(),
                                                  lhs.data(), lhs.size(), 3)));

  ASSERT_DOUBLE_EQ(5.75f / 7.f,
                   (irs::ngram_similarity<char, false>(
                     lhs.data(), lhs.size(), rhs.data(), rhs.size(), 4)));
  ASSERT_DOUBLE_EQ(5.75f / 7.f,
                   (irs::ngram_similarity<char, false>(
                     rhs.data(), rhs.size(), lhs.data(), lhs.size(), 4)));
  ASSERT_DOUBLE_EQ(
    5.f / 7.f, (irs::ngram_similarity<char, true>(lhs.data(), lhs.size(),
                                                  rhs.data(), rhs.size(), 4)));
  ASSERT_DOUBLE_EQ(
    5.f / 6.f, (irs::ngram_similarity<char, true>(rhs.data(), rhs.size(),
                                                  lhs.data(), lhs.size(), 4)));

  ASSERT_DOUBLE_EQ(4.8f / 6.f,
                   (irs::ngram_similarity<char, false>(
                     lhs.data(), lhs.size(), rhs.data(), rhs.size(), 5)));
  ASSERT_DOUBLE_EQ(4.8f / 6.f,
                   (irs::ngram_similarity<char, false>(
                     rhs.data(), rhs.size(), lhs.data(), lhs.size(), 5)));
  ASSERT_DOUBLE_EQ(
    4.f / 6.f, (irs::ngram_similarity<char, true>(lhs.data(), lhs.size(),
                                                  rhs.data(), rhs.size(), 5)));
  ASSERT_DOUBLE_EQ(
    4.f / 5.f, (irs::ngram_similarity<char, true>(rhs.data(), rhs.size(),
                                                  lhs.data(), lhs.size(), 5)));

  ASSERT_DOUBLE_EQ((8.f / 9.f) / 2.f,
                   (irs::ngram_similarity<char, false>(
                     lhs.data(), lhs.size(), rhs.data(), rhs.size(), 9)));
  ASSERT_DOUBLE_EQ((8.f / 9.f) / 2.f,
                   (irs::ngram_similarity<char, false>(
                     rhs.data(), rhs.size(), lhs.data(), lhs.size(), 9)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 9)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        rhs.data(), rhs.size(), lhs.data(), lhs.size(), 9)));
}

TEST(ngram_match_utils_test, test_similarity_with_gaps) {
  const std::string_view lhs = "apple1234juice";
  const std::string_view rhs = "aple567juice";

  ASSERT_DOUBLE_EQ(9.f / 14.f,
                   (irs::ngram_similarity<char, false>(
                     lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
  ASSERT_DOUBLE_EQ(9.f / 14.f,
                   (irs::ngram_similarity<char, false>(
                     rhs.data(), rhs.size(), lhs.data(), lhs.size(), 1)));
  ASSERT_DOUBLE_EQ(
    9.f / 14.f, (irs::ngram_similarity<char, true>(lhs.data(), lhs.size(),
                                                   rhs.data(), rhs.size(), 1)));
  ASSERT_DOUBLE_EQ(
    9.f / 12.f, (irs::ngram_similarity<char, true>(rhs.data(), rhs.size(),
                                                   lhs.data(), lhs.size(), 1)));

  ASSERT_DOUBLE_EQ(8.f / 13.f,
                   (irs::ngram_similarity<char, false>(
                     lhs.data(), lhs.size(), rhs.data(), rhs.size(), 2)));
  ASSERT_DOUBLE_EQ(8.f / 13.f,
                   (irs::ngram_similarity<char, false>(
                     rhs.data(), rhs.size(), lhs.data(), lhs.size(), 2)));
  ASSERT_DOUBLE_EQ(
    7.f / 13.f, (irs::ngram_similarity<char, true>(lhs.data(), lhs.size(),
                                                   rhs.data(), rhs.size(), 2)));
  ASSERT_DOUBLE_EQ(
    7.f / 11.f, (irs::ngram_similarity<char, true>(rhs.data(), rhs.size(),
                                                   lhs.data(), lhs.size(), 2)));

  ASSERT_DOUBLE_EQ(
    (2.f / 3.f + 1 + 2.f / 3.f + 1.f / 3.f + 2.f / 3.f + 1.f / 3.f + 3.f) /
      12.f,
    (irs::ngram_similarity<char, false>(lhs.data(), lhs.size(), rhs.data(),
                                        rhs.size(), 3)));
  ASSERT_DOUBLE_EQ(
    (2.f / 3.f + 1 + 2.f / 3.f + 1.f / 3.f + 2.f / 3.f + 1.f / 3.f + 3.f) /
      12.f,
    (irs::ngram_similarity<char, false>(rhs.data(), rhs.size(), lhs.data(),
                                        lhs.size(), 3)));
  ASSERT_DOUBLE_EQ(
    4.f / 12.f, (irs::ngram_similarity<char, true>(lhs.data(), lhs.size(),
                                                   rhs.data(), rhs.size(), 3)));
  ASSERT_DOUBLE_EQ(
    4.f / 10.f, (irs::ngram_similarity<char, true>(rhs.data(), rhs.size(),
                                                   lhs.data(), lhs.size(), 3)));

  ASSERT_DOUBLE_EQ(
    (0.75f + 0.75f + 0.5f + 0.25f + 0.25f + 0.5f + 0.75f + 2) / 11.f,
    (irs::ngram_similarity<char, false>(lhs.data(), lhs.size(), rhs.data(),
                                        rhs.size(), 4)));
  ASSERT_DOUBLE_EQ(
    (0.75f + 0.75f + 0.5f + 0.25f + 0.25f + 0.5f + 0.75f + 2) / 11.f,
    (irs::ngram_similarity<char, false>(rhs.data(), rhs.size(), lhs.data(),
                                        lhs.size(), 4)));
  ASSERT_DOUBLE_EQ(
    2.f / 11.f, (irs::ngram_similarity<char, true>(lhs.data(), lhs.size(),
                                                   rhs.data(), rhs.size(), 4)));
  ASSERT_DOUBLE_EQ(
    2.f / 9.f, (irs::ngram_similarity<char, true>(rhs.data(), rhs.size(),
                                                  lhs.data(), lhs.size(), 4)));

  ASSERT_DOUBLE_EQ((3.f / 5.f + 3.f / 5.f + 2.f / 5.f + 1.f / 5.f + 2.f / 5.f +
                    3.f / 5.f + 4.f / 5.f + 1) /
                     10.f,
                   (irs::ngram_similarity<char, false>(
                     lhs.data(), lhs.size(), rhs.data(), rhs.size(), 5)));
  ASSERT_DOUBLE_EQ((3.f / 5.f + 3.f / 5.f + 2.f / 5.f + 1.f / 5.f + 2.f / 5.f +
                    3.f / 5.f + 4.f / 5.f + 1) /
                     10.f,
                   (irs::ngram_similarity<char, false>(
                     rhs.data(), rhs.size(), lhs.data(), lhs.size(), 5)));
  ASSERT_DOUBLE_EQ(
    1.f / 10.f, (irs::ngram_similarity<char, true>(lhs.data(), lhs.size(),
                                                   rhs.data(), rhs.size(), 5)));
  ASSERT_DOUBLE_EQ(
    1.f / 8.f, (irs::ngram_similarity<char, true>(rhs.data(), rhs.size(),
                                                  lhs.data(), lhs.size(), 5)));

  ASSERT_DOUBLE_EQ((1.f + 4.f / 6.f + 0.5f + 4.f / 6.f + 5.f / 6.f) / 9.f,
                   (irs::ngram_similarity<char, false>(
                     lhs.data(), lhs.size(), rhs.data(), rhs.size(), 6)));
  ASSERT_DOUBLE_EQ((1.f + 4.f / 6.f + 0.5f + 4.f / 6.f + 5.f / 6.f) / 9.f,
                   (irs::ngram_similarity<char, false>(
                     rhs.data(), rhs.size(), lhs.data(), lhs.size(), 6)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 6)));
  ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                        rhs.data(), rhs.size(), lhs.data(), lhs.size(), 6)));
}

TEST(ngram_match_utils_test, test_similarity) {
  {  // bin and pos similarity shows different results as first ngram has 0.5
     // pos match but 0 bin match
    const std::string_view lhs = "abba";
    const std::string_view rhs = "apba";

    ASSERT_DOUBLE_EQ(2.f / 3.f,
                     (irs::ngram_similarity<char, false>(
                       lhs.data(), lhs.size(), rhs.data(), rhs.size(), 2)));
    ASSERT_DOUBLE_EQ(2.f / 3.f,
                     (irs::ngram_similarity<char, false>(
                       rhs.data(), rhs.size(), lhs.data(), lhs.size(), 2)));
    ASSERT_DOUBLE_EQ(1.f / 3.f,
                     (irs::ngram_similarity<char, true>(
                       lhs.data(), lhs.size(), rhs.data(), rhs.size(), 2)));
    ASSERT_DOUBLE_EQ(1.f / 3.f,
                     (irs::ngram_similarity<char, true>(
                       rhs.data(), rhs.size(), lhs.data(), lhs.size(), 2)));
  }
  {  // first 2-gram has 0.5 sim but second one is 1 (and is chosen by pos) so
     // bin and pos similarity works the same
    // result is different for bin similarity only due to abscence of length
    // normalization
    const std::string_view lhs = "11234561";
    const std::string_view rhs = "1234561";

    ASSERT_DOUBLE_EQ(6.f / 7.f,
                     (irs::ngram_similarity<char, false>(
                       lhs.data(), lhs.size(), rhs.data(), rhs.size(), 2)));
    ASSERT_DOUBLE_EQ(6.f / 7.f,
                     (irs::ngram_similarity<char, false>(
                       rhs.data(), rhs.size(), lhs.data(), lhs.size(), 2)));
    ASSERT_DOUBLE_EQ(6.f / 7.f,
                     (irs::ngram_similarity<char, true>(
                       lhs.data(), lhs.size(), rhs.data(), rhs.size(), 2)));
    ASSERT_DOUBLE_EQ(1, (irs::ngram_similarity<char, true>(
                          rhs.data(), rhs.size(), lhs.data(), lhs.size(), 2)));
  }
}

TEST(ngram_match_utils_test, test_similarity_shorter) {
  {
    // strings are shorter than ngram length.
    const std::string_view lhs = "abb";
    const std::string_view rhs = "abpa";

    // for "positional" semantics this will force to find best matched ngram
    ASSERT_DOUBLE_EQ(
      0.5f, (irs::ngram_similarity<char, false>(lhs.data(), lhs.size(),
                                                rhs.data(), rhs.size(), 5)));
    ASSERT_DOUBLE_EQ(
      0.5f, (irs::ngram_similarity<char, false>(rhs.data(), rhs.size(),
                                                lhs.data(), lhs.size(), 5)));
    // for "search" semantics this is 0 as there will be no binary matching
    // ngrams found in index
    ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                          lhs.data(), lhs.size(), rhs.data(), rhs.size(), 5)));
    ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                          rhs.data(), rhs.size(), lhs.data(), lhs.size(), 5)));
  }
  {
    // strings are shorter than ngram length.
    const std::string_view lhs = "abb";
    const std::string_view rhs = "abb";

    // for "positional" semantics this will force to find best matched ngram
    ASSERT_DOUBLE_EQ(1, (irs::ngram_similarity<char, false>(
                          lhs.data(), lhs.size(), rhs.data(), rhs.size(), 5)));
    ASSERT_DOUBLE_EQ(1, (irs::ngram_similarity<char, false>(
                          rhs.data(), rhs.size(), lhs.data(), lhs.size(), 5)));
    // for "search" semantics this is 1 as there is binary match of shorter
    // string
    ASSERT_DOUBLE_EQ(1, (irs::ngram_similarity<char, true>(
                          lhs.data(), lhs.size(), rhs.data(), rhs.size(), 5)));
    ASSERT_DOUBLE_EQ(1, (irs::ngram_similarity<char, true>(
                          rhs.data(), rhs.size(), lhs.data(), lhs.size(), 5)));
  }
  {
    // strings are shorter than ngram length.
    const std::string_view lhs = "abb";
    const std::string_view rhs = "abc";

    // for "positional" semantics this will force to find best matched ngram
    ASSERT_DOUBLE_EQ(2.f / 3.f,
                     (irs::ngram_similarity<char, false>(
                       lhs.data(), lhs.size(), rhs.data(), rhs.size(), 5)));
    ASSERT_DOUBLE_EQ(2.f / 3.f,
                     (irs::ngram_similarity<char, false>(
                       rhs.data(), rhs.size(), lhs.data(), lhs.size(), 5)));
    // for "search" semantics this is 0 as there no binary match of shorter
    // string
    ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                          lhs.data(), lhs.size(), rhs.data(), rhs.size(), 5)));
    ASSERT_DOUBLE_EQ(0, (irs::ngram_similarity<char, true>(
                          rhs.data(), rhs.size(), lhs.data(), lhs.size(), 5)));
  }
}

TEST(ngram_match_utils_test, test_similarity_all_empty) {
  // strings are shorter than ngram length.
  const std::string_view lhs = "";
  const std::string_view rhs = "";

  // for "positional" semantics this will mean full similarity
  ASSERT_DOUBLE_EQ(1, (irs::ngram_similarity<char, false>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 5)));
  ASSERT_DOUBLE_EQ(1, (irs::ngram_similarity<char, false>(
                        rhs.data(), rhs.size(), lhs.data(), lhs.size(), 5)));
  // for "search" semantics this is 1 as binary strings match
  ASSERT_DOUBLE_EQ(1, (irs::ngram_similarity<char, true>(
                        lhs.data(), lhs.size(), rhs.data(), rhs.size(), 5)));
  ASSERT_DOUBLE_EQ(1, (irs::ngram_similarity<char, true>(
                        rhs.data(), rhs.size(), lhs.data(), lhs.size(), 5)));
}
