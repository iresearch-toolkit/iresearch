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
  ASSERT_EQ(9, header.size());

  auto* p = header.c_str();
  ASSERT_EQ(static_cast<uint32_t>(version),
            irs::read<uint32_t>(p)); // Version
  ASSERT_EQ(min, irs::read<uint32_t>(p)); // Min
  ASSERT_EQ(max, irs::read<uint32_t>(p)); // Max
  ASSERT_EQ(p, header.end());
}

TEST(Norm2HeaderTest, Construct) {
  irs::Norm2Header hdr{irs::Norm2Version::kMin};
  ASSERT_EQ(1, hdr.NumBytes());

  irs::bstring buf;
  irs::Norm2Header::Write(hdr, buf);
  AssertNorm2Header(buf,
                    irs::Norm2Version::kMin,
                    std::numeric_limits<uint32_t>::max(),
                    std::numeric_limits<uint32_t>::min());
}

TEST(Norm2HeaderTest, ResetByValue) {
  auto AssertNumBytes = [](auto value) {
    using value_type = decltype(value);

    irs::Norm2Header hdr{irs::Norm2Version::kMin};
    hdr.Reset(std::numeric_limits<value_type>::max()-2);
    hdr.Reset(std::numeric_limits<value_type>::max());
    hdr.Reset(std::numeric_limits<value_type>::max()-1);
    ASSERT_EQ(sizeof(value_type), hdr.NumBytes());

    irs::bstring buf;
    irs::Norm2Header::Write(hdr, buf);
    AssertNorm2Header(buf,
                      irs::Norm2Version::kMin,
                      std::numeric_limits<value_type>::max()-2,
                      std::numeric_limits<value_type>::max());
  };

  AssertNumBytes(irs::byte_type{}); // 1-byte header
  AssertNumBytes(uint16_t{}); // 2-byte header
  AssertNumBytes(uint32_t{}); // 4-byte header
}

TEST(Norm2HeaderTest, ReadInvalid) {
  ASSERT_FALSE(irs::Norm2Header::Read(irs::bytes_ref::NIL).has_value());
  ASSERT_FALSE(irs::Norm2Header::Read(irs::bytes_ref::EMPTY).has_value());
  {
    constexpr irs::byte_type kBuf[3]{};
    ASSERT_FALSE(irs::Norm2Header::Read({ kBuf, sizeof kBuf}).has_value());
  }
}

TEST(Norm2HeaderTest, ResetByPayload) {
  auto WriteHeader = [](auto value, irs::Norm2Version version,
                        irs::bstring& buf) {
    using value_type = decltype(value);

    irs::Norm2Header hdr{version};
    hdr.Reset(std::numeric_limits<value_type>::max()-2);
    hdr.Reset(std::numeric_limits<value_type>::max());
    hdr.Reset(std::numeric_limits<value_type>::max()-1);
    ASSERT_EQ(sizeof(value_type), hdr.NumBytes());

    buf.clear();
    irs::Norm2Header::Write(hdr, buf);
    AssertNorm2Header(buf,
                      version,
                      std::numeric_limits<value_type>::max()-2,
                      std::numeric_limits<value_type>::max());
  };

  irs::Norm2Header acc{irs::Norm2Version::kMin};

  // 1-byte header
  {
    irs::bstring buf;
    WriteHeader(irs::byte_type{}, irs::Norm2Version::kMin, buf);
    auto hdr = irs::Norm2Header::Read(buf);
    ASSERT_TRUE(hdr.has_value());
    ASSERT_TRUE(acc.Reset(hdr.value()));
    buf.clear();
    irs::Norm2Header::Write(acc, buf);

    AssertNorm2Header(buf,
                      irs::Norm2Version::kMin,
                      std::numeric_limits<irs::byte_type>::max()-2,
                      std::numeric_limits<irs::byte_type>::max());
  }

  // Inconsistent header
  {
    irs::bstring buf;
    WriteHeader(uint16_t{}, static_cast<irs::Norm2Version>(42), buf);
    auto hdr = irs::Norm2Header::Read(buf);
    ASSERT_TRUE(hdr.has_value());
    ASSERT_TRUE(acc.Reset(hdr.value()));
    buf.clear();
    irs::Norm2Header::Write(acc, buf);

    AssertNorm2Header(buf,
                      irs::Norm2Version::kMin,
                      std::numeric_limits<irs::byte_type>::max()-2,
                      std::numeric_limits<irs::byte_type>::max());
  }

  // 2-byte header
  {
    irs::bstring buf;
    WriteHeader(uint16_t{}, irs::Norm2Version::kMin, buf);
    auto hdr = irs::Norm2Header::Read(buf);
    ASSERT_TRUE(hdr.has_value());
    ASSERT_TRUE(acc.Reset(hdr.value()));
    buf.clear();
    irs::Norm2Header::Write(acc, buf);

    AssertNorm2Header(buf,
                      irs::Norm2Version::kMin,
                      std::numeric_limits<irs::byte_type>::max()-2,
                      std::numeric_limits<uint16_t>::max());
  }

  // 4-byte header
  {
    irs::bstring buf;
    WriteHeader(uint32_t{}, irs::Norm2Version::kMin, buf);
    auto hdr = irs::Norm2Header::Read(buf);
    ASSERT_TRUE(hdr.has_value());
    ASSERT_TRUE(acc.Reset(hdr.value()));
    buf.clear();
    irs::Norm2Header::Write(acc, buf);

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
