////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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


#include <vector>
#include "gtest/gtest.h"
#include "tests_config.hpp"

#include "analysis/segmentation_token_stream.hpp"

struct analyzer_token {
  irs::string_ref value;
  size_t start;
  size_t end;
  uint32_t pos;
};

using analyzer_tokens = std::vector<analyzer_token>;


void assert_stream(irs::analysis::analyzer* pipe, const std::string& data, const analyzer_tokens& expected_tokens) {
  SCOPED_TRACE(data);
  auto* offset = irs::get<irs::offset>(*pipe);
  ASSERT_TRUE(offset);
  auto* term = irs::get<irs::term_attribute>(*pipe);
  ASSERT_TRUE(term);
  auto* inc = irs::get<irs::increment>(*pipe);
  ASSERT_TRUE(inc);
  ASSERT_TRUE(pipe->reset(data));
  uint32_t pos{ irs::integer_traits<uint32_t>::const_max };
  auto expected_token = expected_tokens.begin();
  while (pipe->next()) {
    auto term_value = std::string(irs::ref_cast<char>(term->value).c_str(), term->value.size());
    SCOPED_TRACE(testing::Message("Term:") << term_value);
    auto old_pos = pos;
    pos += inc->value;
    ASSERT_NE(expected_token, expected_tokens.end());
    ASSERT_EQ(irs::ref_cast<irs::byte_type>(expected_token->value), term->value);
    ASSERT_EQ(expected_token->start, offset->start);
    ASSERT_EQ(expected_token->end, offset->end);
    ASSERT_EQ(expected_token->pos, pos);
    ++expected_token;
  }
  ASSERT_EQ(expected_token, expected_tokens.end());
  ASSERT_FALSE(pipe->next());
}


TEST(segmentation_token_stream_test, consts) {
  static_assert("segmentation" == irs::type<irs::analysis::segmentation_token_stream>::name());
}

TEST(segmentation_token_stream_test, no_case_test) {
  irs::analysis::segmentation_token_stream::options_t opt;
  opt.case_convert = irs::analysis::segmentation_token_stream::options_t::case_convert_t::NONE;
  irs::analysis::segmentation_token_stream stream(std::move(opt));
  auto* term = irs::get<irs::term_attribute>(stream);
  std::string data = "File:Constantinople(1878)-Turkish Goverment information brocure (1950s) - Istanbul coffee house.png";
  const analyzer_tokens expected{
    { "File:Constantinople", 0, 19, 0},
    { "1878", 20, 24, 1},
    { "Turkish", 26, 33, 2},
    { "Goverment", 34, 43, 3},
    { "information", 44, 55, 4},
    { "brocure", 56, 63, 5},
    { "1950s", 65, 70, 6},
    { "Istanbul", 74, 82, 7},
    { "coffee", 83, 89, 8},
    { "house.png", 90, 99, 9}
  };
  assert_stream(&stream, data, expected);
}

TEST(segmentation_token_stream_test, lower_case_test) {
  irs::analysis::segmentation_token_stream::options_t opt; // LOWER is default
  irs::analysis::segmentation_token_stream stream(std::move(opt));
  auto* term = irs::get<irs::term_attribute>(stream);
  std::string data = "File:Constantinople(1878)-Turkish Goverment information brocure (1950s) - Istanbul coffee house.png";
  const analyzer_tokens expected{
    { "file:constantinople", 0, 19, 0},
    { "1878", 20, 24, 1},
    { "turkish", 26, 33, 2},
    { "goverment", 34, 43, 3},
    { "information", 44, 55, 4},
    { "brocure", 56, 63, 5},
    { "1950s", 65, 70, 6},
    { "istanbul", 74, 82, 7},
    { "coffee", 83, 89, 8},
    { "house.png", 90, 99, 9}
  };
  assert_stream(&stream, data, expected);
}

TEST(segmentation_token_stream_test, upper_case_test) {
  irs::analysis::segmentation_token_stream::options_t opt;
  opt.case_convert = irs::analysis::segmentation_token_stream::options_t::case_convert_t::UPPER;
  irs::analysis::segmentation_token_stream stream(std::move(opt));
  auto* term = irs::get<irs::term_attribute>(stream);
  std::string data = "File:Constantinople(1878)-Turkish Goverment information brocure (1950s) - Istanbul coffee house.png";
  const analyzer_tokens expected{
    { "FILE:CONSTANTINOPLE", 0, 19, 0},
    { "1878", 20, 24, 1},
    { "TURKISH", 26, 33, 2},
    { "GOVERMENT", 34, 43, 3},
    { "INFORMATION", 44, 55, 4},
    { "BROCURE", 56, 63, 5},
    { "1950S", 65, 70, 6},
    { "ISTANBUL", 74, 82, 7},
    { "COFFEE", 83, 89, 8},
    { "HOUSE.PNG", 90, 99, 9}
  };
  assert_stream(&stream, data, expected);
}