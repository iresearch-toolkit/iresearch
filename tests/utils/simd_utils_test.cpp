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
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "tests_shared.hpp"

#include "utils/misc.hpp"
#include "utils/std.hpp"
#include "utils/simd_utils.hpp"

TEST(simd_utils_test, all_equal) {
  uint32_t values[SIMDBlockSize*2];
  std::fill(std::begin(values), std::end(values), 42);
  ASSERT_TRUE(irs::simd::all_equal(std::begin(values), std::end(values)));

  values[0] = 0;
  ASSERT_FALSE(irs::simd::all_equal(std::begin(values), std::end(values)));

  {
    auto* begin = values;
    for (size_t i = 0; i < 32; ++i) {
      begin[0] = 0;
      begin[1] = 1;
      begin[2] = 2;
      begin[3] = 4;
      begin += 4;
    }
  }
  ASSERT_FALSE(irs::simd::all_equal(std::begin(values), std::end(values)));
}

TEST(simd_utils_test, fill_n) {
  uint32_t values[SIMDBlockSize*2];
  std::fill(std::begin(values), std::end(values), 42);
  irs::simd::fill_n<IRESEARCH_COUNTOF(values)>(values, 84);
  ASSERT_TRUE(std::all_of(std::begin(values), std::end(values),
              [](const auto v) { return v == 84; }));
  irs::simd::fill_n<SIMDBlockSize>(values, 128);
  ASSERT_TRUE(std::all_of(std::begin(values), std::begin(values) + SIMDBlockSize,
              [](const auto v) { return v == 128; }));
  ASSERT_TRUE(std::all_of(std::begin(values) + SIMDBlockSize, std::end(values),
              [](const auto v) { return v == 84; }));
}

TEST(simd_utils_test, maxmin) {
  uint32_t values[SIMDBlockSize*2];
  std::iota(std::begin(values), std::end(values), 42);
  ASSERT_EQ(
    (std::pair<uint32_t, uint32_t>(42, 42 + IRESEARCH_COUNTOF(values) - 1)),
    irs::simd::maxmin<IRESEARCH_COUNTOF(values)>(values));
}



