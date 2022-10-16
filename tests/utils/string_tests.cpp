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

#include <climits>

#include "tests_shared.hpp"
#include "utils/string.hpp"

void expect_sign_eq(long double lhs, long double rhs) {
  EXPECT_TRUE((lhs == 0 && rhs == 0) || std::signbit(lhs) == std::signbit(rhs));
}

TEST(string_ref_tests, common_prefix) {
  using namespace iresearch;

  {
    const string_ref lhs = "20-MAR-2012 19:56:11.00";
    const string_ref rhs = "20-MAR-2012 19:56:11.00\0\0";
    EXPECT_EQ(23, common_prefix_length(lhs, rhs));
    EXPECT_EQ(23, common_prefix_length(rhs, lhs));
  }

  {
    const string_ref lhs = "quick brown fox";
    const string_ref rhs = "quick brown fax";
    EXPECT_EQ(13, common_prefix_length(lhs, rhs));
    EXPECT_EQ(13, common_prefix_length(rhs, lhs));
  }

  {
    const string_ref lhs = "quick brown foxies";
    const string_ref rhs = "quick brown fax";
    EXPECT_EQ(13, common_prefix_length(lhs, rhs));
    EXPECT_EQ(13, common_prefix_length(rhs, lhs));
  }

  {
    const string_ref lhs = "quick brown foxies";
    const string_ref rhs = "fuick brown fax";
    EXPECT_EQ(0, common_prefix_length(lhs, rhs));
    EXPECT_EQ(0, common_prefix_length(rhs, lhs));
  }

  {
    const string_ref lhs = "quick brown foxies";
    const string_ref rhs = "q1ick brown fax";
    EXPECT_EQ(1, common_prefix_length(lhs, rhs));
    EXPECT_EQ(1, common_prefix_length(rhs, lhs));
  }

  {
    const string_ref lhs = "qui";
    const string_ref rhs = "q1";
    EXPECT_EQ(1, common_prefix_length(lhs, rhs));
    EXPECT_EQ(1, common_prefix_length(rhs, lhs));
  }

  {
    const string_ref lhs = "qui";
    const string_ref rhs = "f1";
    EXPECT_EQ(0, common_prefix_length(lhs, rhs));
    EXPECT_EQ(0, common_prefix_length(rhs, lhs));
  }

  {
    const string_ref lhs = "quick brown foxies";
    const string_ref rhs = "qui";
    EXPECT_EQ(3, common_prefix_length(lhs, rhs));
    EXPECT_EQ(3, common_prefix_length(rhs, lhs));
  }

  {
    const string_ref str = "quick brown foxies";
    EXPECT_EQ(0, common_prefix_length(string_ref{}, str));
    EXPECT_EQ(0, common_prefix_length(str, string_ref{}));
  }

  {
    const string_ref str = "quick brown foxies";
    EXPECT_EQ(0, common_prefix_length(string_ref{""}, str));
    EXPECT_EQ(0, common_prefix_length(str, string_ref{""}));
  }
}
