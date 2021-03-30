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

TEST(simd_utils_test, avg32) {
  using namespace hwy::HWY_NAMESPACE;
  using namespace irs;

  HWY_ALIGN uint32_t values[1024];
  std::iota(std::begin(values), std::end(values), 42);

  HWY_ALIGN uint32_t encoded[1024];
  std::memcpy(encoded, values, sizeof values);
  const auto stats = irs::simd::avg_encode32<IRESEARCH_COUNTOF(encoded)>(encoded);

  HWY_ALIGN uint32_t decoded[IRESEARCH_COUNTOF(encoded)];
  irs::simd::avg_decode32<IRESEARCH_COUNTOF(encoded)>(encoded, decoded, stats.first, stats.second);

  ASSERT_TRUE(std::equal(std::begin(values), std::end(values),
                         std::begin(decoded), std::end(decoded)));
}

TEST(simd_utils_test, zigzag32) {
  using namespace hwy::HWY_NAMESPACE;
  using namespace irs;

  auto expected = Iota(irs::simd::vi32, 0);
  auto encoded = irs::simd::zig_zag_encode32(expected);
  auto decoded = irs::simd::zig_zag_decode32(encoded);
  ASSERT_TRUE(AllTrue(expected == decoded));
}

TEST(simd_utils_test, zigzag64) {
  using namespace hwy::HWY_NAMESPACE;
  using namespace irs;

  auto expected = Iota(irs::simd::vi64, 0);
  auto encoded = irs::simd::zig_zag_encode64(expected);
  auto decoded = irs::simd::zig_zag_decode64(encoded);
  ASSERT_TRUE(AllTrue(expected == decoded));
}

TEST(simd_utils_test, all_equal) {
  constexpr size_t BLOCK_SIZE = 128;
  HWY_ALIGN uint32_t values[BLOCK_SIZE*2];
  std::fill(std::begin(values), std::end(values), 42);
  ASSERT_TRUE(irs::simd::all_equal(irs::simd::vu32, std::begin(values), std::end(values)));

  values[0] = 0;
  ASSERT_FALSE(irs::simd::all_equal(irs::simd::vu32, std::begin(values), std::end(values)));

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
  ASSERT_FALSE(irs::simd::all_equal(irs::simd::vu32, std::begin(values), std::end(values)));
}

TEST(simd_utils_test, fill_n) {
  constexpr size_t BLOCK_SIZE = 128;
  HWY_ALIGN uint32_t values[BLOCK_SIZE*2];
  std::fill(std::begin(values), std::end(values), 42);
  irs::simd::fill_n<IRESEARCH_COUNTOF(values)>(irs::simd::vu32, values, 84U);
  ASSERT_TRUE(std::all_of(std::begin(values), std::end(values),
              [](const auto v) { return v == 84; }));
  irs::simd::fill_n<BLOCK_SIZE>(irs::simd::vu32, values, 128U);
  ASSERT_TRUE(std::all_of(std::begin(values), std::begin(values) + BLOCK_SIZE,
              [](const auto v) { return v == 128; }));
  ASSERT_TRUE(std::all_of(std::begin(values) + BLOCK_SIZE, std::end(values),
              [](const auto v) { return v == 84; }));
}

TEST(simd_utils_test, maxmin) {
  constexpr size_t BLOCK_SIZE = 128;
  HWY_ALIGN uint32_t values[BLOCK_SIZE*2];
  std::iota(std::begin(values), std::end(values), 42);
  ASSERT_EQ(
    (std::pair<uint32_t, uint32_t>(42, 42 + IRESEARCH_COUNTOF(values) - 1)),
    irs::simd::maxmin<IRESEARCH_COUNTOF(values)>(irs::simd::vu32, values));
}



