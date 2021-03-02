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

TEST(segmentation_token_stream_test, consts) {
  static_assert("segmentation" == irs::type<irs::analysis::segmentation_token_stream>::name());
}

TEST(segmentation_token_stream_test, smoke_test) {
  irs::analysis::segmentation_token_stream::options_t opt;
  irs::analysis::segmentation_token_stream stream(std::move(opt));
  auto* term = irs::get<irs::term_attribute>(stream);
  std::string data = "ХУУУУУУУУЙ     ХУй пуй File:Constantinople(1878)-Turkish Goverment information brocure (1950s) - Istanbul coffee house.png";
  ASSERT_TRUE(stream.reset(data));
  //while (stream.next()) {
   // std::cout << term->value.c_str();
  //}
}
