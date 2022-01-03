////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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

#include "index_tests.hpp"

#include "index/norm.hpp"

namespace  {

void AssertNorm2Header(irs::bytes_ref header,
                       irs::Norm2Version version,
                       uint32_t min,
                       uint32_t max) {
  ASSERT_FALSE(header.null());
  ASSERT_EQ(12, header.size());

  auto* p = header.c_str();
  ASSERT_EQ(static_cast<uint32_t>(version),
            irs::read<uint32_t>(p)); // Version
  ASSERT_EQ(min, irs::read<uint32_t>(p)); // Min
  ASSERT_EQ(max, irs::read<uint32_t>(p)); // Max
  ASSERT_EQ(p, header.end());
}

TEST(Norm2HeaderTest, Construct) {
  irs::norm2_header hdr{irs::Norm2Version::kMin};
  ASSERT_EQ(1, hdr.num_bytes());

  irs::bstring buf;
  hdr.write(buf);
  AssertNorm2Header(buf,
                    irs::Norm2Version::kMin,
                    std::numeric_limits<uint32_t>::max(),
                    std::numeric_limits<uint32_t>::min());
}

TEST(Norm2HeaderTest, ResetByValue) {
  auto AssertNumBytes = [](auto value) {
    using value_type = decltype(value);

    irs::norm2_header hdr{irs::Norm2Version::kMin};
    hdr.reset(std::numeric_limits<value_type>::max()-2);
    hdr.reset(std::numeric_limits<value_type>::max());
    hdr.reset(std::numeric_limits<value_type>::max()-1);
    ASSERT_EQ(sizeof(value_type), hdr.num_bytes());

    irs::bstring buf;
    hdr.write(buf);
    AssertNorm2Header(buf,
                      irs::Norm2Version::kMin,
                      std::numeric_limits<value_type>::max()-2,
                      std::numeric_limits<value_type>::max());
  };

  AssertNumBytes(irs::byte_type{}); // 1-byte header
  AssertNumBytes(uint16_t{}); // 2-byte header
  AssertNumBytes(uint32_t{}); // 4-byte header
}

TEST(Norm2HeaderTest, ResetByPayload) {
  auto WriteHeader = [](auto value, irs::Norm2Version version,
                        irs::bstring& buf) {
    using value_type = decltype(value);

    irs::norm2_header hdr{version};
    hdr.reset(std::numeric_limits<value_type>::max()-2);
    hdr.reset(std::numeric_limits<value_type>::max());
    hdr.reset(std::numeric_limits<value_type>::max()-1);
    ASSERT_EQ(sizeof(value_type), hdr.num_bytes());

    buf.clear();
    hdr.write(buf);
    AssertNorm2Header(buf,
                      version,
                      std::numeric_limits<value_type>::max()-2,
                      std::numeric_limits<value_type>::max());
  };

  irs::norm2_header acc{irs::Norm2Version::kMin};

  // 1-byte header
  {
    irs::bstring buf;
    WriteHeader(irs::byte_type{}, irs::Norm2Version::kMin, buf);
    ASSERT_TRUE(acc.reset(buf));
    buf.clear();
    acc.write(buf);

    AssertNorm2Header(buf,
                      irs::Norm2Version::kMin,
                      std::numeric_limits<irs::byte_type>::max()-2,
                      std::numeric_limits<irs::byte_type>::max());
  }

  // Inconsistent header
  {
    irs::bstring buf;
    WriteHeader(uint16_t{}, static_cast<irs::Norm2Version>(42), buf);
    ASSERT_FALSE(acc.reset(buf));
    buf.clear();
    acc.write(buf);

    AssertNorm2Header(buf,
                      irs::Norm2Version::kMin,
                      std::numeric_limits<irs::byte_type>::max()-2,
                      std::numeric_limits<irs::byte_type>::max());
  }

  // 2-byte header
  {
    irs::bstring buf;
    WriteHeader(uint16_t{}, irs::Norm2Version::kMin, buf);
    ASSERT_TRUE(acc.reset(buf));
    buf.clear();
    acc.write(buf);

    AssertNorm2Header(buf,
                      irs::Norm2Version::kMin,
                      std::numeric_limits<irs::byte_type>::max()-2,
                      std::numeric_limits<uint16_t>::max());
  }

  // 4-byte header
  {
    irs::bstring buf;
    WriteHeader(uint32_t{}, irs::Norm2Version::kMin, buf);
    ASSERT_TRUE(acc.reset(buf));
    buf.clear();
    acc.write(buf);

    AssertNorm2Header(buf,
                      irs::Norm2Version::kMin,
                      std::numeric_limits<irs::byte_type>::max()-2,
                      std::numeric_limits<uint32_t>::max());
  }
}

class Norm2TestCase : public tests::index_test_base { };

// Separate definition as MSVC parser fails to do conditional defines in macro expansion
#ifdef IRESEARCH_SSE2
const auto kNorm2TestCaseValues = ::testing::Values(
    tests::format_info{"1_4", "1_0"},
    tests::format_info{"1_4simd", "1_0"});
#else
const auto kNorm2TestCaseValues = ::testing::Values(
    tests::format_info{"1_4", "1_0"});
#endif

INSTANTIATE_TEST_SUITE_P(
  Norm2Test,
  Norm2TestCase,
  ::testing::Combine(
    ::testing::Values(
      &tests::directory<&tests::memory_directory>,
      &tests::directory<&tests::fs_directory>,
      &tests::directory<&tests::mmap_directory>),
    kNorm2TestCaseValues),
  Norm2TestCase::to_string);

} // namespace {
