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
                       uint32_t num_bytes,
                       uint32_t min,
                       uint32_t max) {
  constexpr irs::Norm2Version kVersion{irs::Norm2Version::kMin};

  ASSERT_FALSE(header.null());
  ASSERT_EQ(10, header.size());

  auto* p = header.c_str();
  ASSERT_EQ(static_cast<uint32_t>(kVersion), *p++); // Version
  ASSERT_EQ(num_bytes, *p++); // Number of bytes
  ASSERT_EQ(min, irs::read<uint32_t>(p)); // Min
  ASSERT_EQ(max, irs::read<uint32_t>(p)); // Max
  ASSERT_EQ(p, header.end());
}

TEST(Norm2HeaderTest, Construct) {
  irs::Norm2Header hdr{irs::Norm2Encoding::Int};
  ASSERT_EQ(1, hdr.MaxNumBytes());
  ASSERT_EQ(sizeof(uint32_t), hdr.NumBytes());

  irs::bstring buf;
  irs::Norm2Header::Write(hdr, buf);
  AssertNorm2Header(buf,
                    sizeof(uint32_t),
                    std::numeric_limits<uint32_t>::max(),
                    std::numeric_limits<uint32_t>::min());
}

TEST(Norm2HeaderTest, ResetByValue) {
  auto AssertNumBytes = [](auto value) {
    using value_type = decltype(value);

    static_assert(std::is_same_v<value_type, irs::byte_type> ||
                  std::is_same_v<value_type, uint16_t> ||
                  std::is_same_v<value_type, uint32_t>);

    irs::Norm2Encoding encoding;
    if constexpr (std::is_same_v<value_type, irs::byte_type>) {
      encoding = irs::Norm2Encoding::Byte;
    } else if (std::is_same_v<value_type, uint16_t>) {
      encoding = irs::Norm2Encoding::Short;
    } else if (std::is_same_v<value_type, uint32_t>) {
      encoding = irs::Norm2Encoding::Int;
    }

    irs::Norm2Header hdr{encoding};
    hdr.Reset(std::numeric_limits<value_type>::max()-2);
    hdr.Reset(std::numeric_limits<value_type>::max());
    hdr.Reset(std::numeric_limits<value_type>::max()-1);
    ASSERT_EQ(sizeof(value_type), hdr.MaxNumBytes());
    ASSERT_EQ(sizeof(value_type), hdr.NumBytes());

    irs::bstring buf;
    irs::Norm2Header::Write(hdr, buf);
    AssertNorm2Header(buf,
                      sizeof(value_type),
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

  // Invalid size
  {
    constexpr irs::byte_type kBuf[3]{};
    static_assert(sizeof kBuf != irs::Norm2Header::ByteSize());
    ASSERT_FALSE(irs::Norm2Header::Read({ kBuf, sizeof kBuf}).has_value());
  }

  // Invalid encoding
  {
    constexpr irs::byte_type kBuf[irs::Norm2Header::ByteSize()]{};
    ASSERT_FALSE(irs::Norm2Header::Read({ kBuf, sizeof kBuf}).has_value());
  }

  // Invalid encoding
  {
    constexpr irs::byte_type kBuf[irs::Norm2Header::ByteSize()]{ 0, 3 };
    ASSERT_FALSE(irs::Norm2Header::Read({ kBuf, sizeof kBuf}).has_value());
  }

  // Invalid version
  {
    constexpr irs::byte_type kBuf[irs::Norm2Header::ByteSize()]{ 42, 1 };
    ASSERT_FALSE(irs::Norm2Header::Read({ kBuf, sizeof kBuf}).has_value());
  }
}

TEST(Norm2HeaderTest, ResetByPayload) {
  auto WriteHeader = [](auto value, irs::bstring& buf) {
    using value_type = decltype(value);

    static_assert(std::is_same_v<value_type, irs::byte_type> ||
                  std::is_same_v<value_type, uint16_t> ||
                  std::is_same_v<value_type, uint32_t>);

    irs::Norm2Encoding encoding;
    if constexpr (std::is_same_v<value_type, irs::byte_type>) {
      encoding = irs::Norm2Encoding::Byte;
    } else if (std::is_same_v<value_type, uint16_t>) {
      encoding = irs::Norm2Encoding::Short;
    } else if (std::is_same_v<value_type, uint32_t>) {
      encoding = irs::Norm2Encoding::Int;
    }

    irs::Norm2Header hdr{encoding};
    hdr.Reset(std::numeric_limits<value_type>::max()-2);
    hdr.Reset(std::numeric_limits<value_type>::max());
    hdr.Reset(std::numeric_limits<value_type>::max()-1);
    ASSERT_EQ(sizeof(value_type), hdr.NumBytes());

    buf.clear();
    irs::Norm2Header::Write(hdr, buf);
    AssertNorm2Header(buf,
                      sizeof(value_type),
                      std::numeric_limits<value_type>::max()-2,
                      std::numeric_limits<value_type>::max());
  };

  irs::Norm2Header acc{irs::Norm2Encoding::Byte};

  // 1-byte header
  {
    irs::bstring buf;
    WriteHeader(irs::byte_type{}, buf);
    auto hdr = irs::Norm2Header::Read(buf);
    ASSERT_TRUE(hdr.has_value());
    acc.Reset(hdr.value());
    buf.clear();
    irs::Norm2Header::Write(acc, buf);

    AssertNorm2Header(buf,
                      sizeof(irs::byte_type),
                      std::numeric_limits<irs::byte_type>::max()-2,
                      std::numeric_limits<irs::byte_type>::max());
  }

  // 2-byte header
  {
    irs::bstring buf;
    WriteHeader(uint16_t{}, buf);
    auto hdr = irs::Norm2Header::Read(buf);
    ASSERT_TRUE(hdr.has_value());
    acc.Reset(hdr.value());
    buf.clear();
    irs::Norm2Header::Write(acc, buf);

    AssertNorm2Header(buf,
                      sizeof(uint16_t),
                      std::numeric_limits<irs::byte_type>::max()-2,
                      std::numeric_limits<uint16_t>::max());
  }

  // 4-byte header
  {
    irs::bstring buf;
    WriteHeader(uint32_t{}, buf);
    auto hdr = irs::Norm2Header::Read(buf);
    ASSERT_TRUE(hdr.has_value());
    acc.Reset(hdr.value());
    buf.clear();
    irs::Norm2Header::Write(acc, buf);

    AssertNorm2Header(buf,
                      sizeof(uint32_t),
                      std::numeric_limits<irs::byte_type>::max()-2,
                      std::numeric_limits<uint32_t>::max());
  }
}

class Norm2TestCase : public tests::index_test_base {
 protected:
  irs::feature_info_provider_t Features() {
    return [](irs::type_info::type_id id) {
      if (irs::type<irs::Norm2>::id() == id) {
        return std::make_pair(
            irs::column_info{irs::type<irs::compression::none>::get(), {}, false},
            &irs::Norm2::MakeWriter);
      }

      return std::make_pair(
          irs::column_info{irs::type<irs::compression::none>::get(), {}, false},
          irs::feature_writer_factory_t{});
    };
  }

  std::vector<irs::type_info::type_id> FieldFeatures() {
    return { irs::type<irs::Norm2>::id() };
  }

  void assert_index() {
    index_test_base::assert_index(irs::IndexFeatures::NONE);
    index_test_base::assert_index(
      irs::IndexFeatures::NONE | irs::IndexFeatures::FREQ);
    index_test_base::assert_index(
      irs::IndexFeatures::NONE | irs::IndexFeatures::FREQ
        | irs::IndexFeatures::POS);
    index_test_base::assert_index(
      irs::IndexFeatures::NONE | irs::IndexFeatures::FREQ
        | irs::IndexFeatures::POS | irs::IndexFeatures::OFFS);
    index_test_base::assert_index(
      irs::IndexFeatures::NONE | irs::IndexFeatures::FREQ
        | irs::IndexFeatures::POS | irs::IndexFeatures::PAY);
    index_test_base::assert_index(irs::IndexFeatures::ALL);
    index_test_base::assert_columnstore();
  }
};

TEST_P(Norm2TestCase, CheckNorms) {
  size_t count = 1;

  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [this, &count](tests::document& doc,
           const std::string& name,
           const tests::json_doc_generator::json_value& data) {
      if (data.is_string()) {
        const bool is_name = (name == "name");
        const size_t n = is_name ? count : 1;

        for (size_t i = 0; i < n; ++i) {
          auto field = std::make_shared<tests::string_field>(
            name, data.str, irs::IndexFeatures::ALL, FieldFeatures());
          doc.insert(field);

          if (is_name) {
            doc.sorted = field;
          }
        }
      }
  });

  count = 1; auto* doc0 = gen.next(); // name == 'A'
  count = 2; auto* doc1 = gen.next(); // name == 'B'
  count = 3; auto* doc2 = gen.next(); // name == 'C'
  count = 4; auto* doc3 = gen.next(); // name == 'D'

  irs::index_writer::init_options opts;
  opts.features = Features();

  // Create actual index
  auto writer = open_writer(irs::OM_CREATE, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_TRUE(insert(*writer,
    doc0->indexed.begin(), doc0->indexed.end(),
    doc0->stored.begin(), doc0->stored.end()));
  ASSERT_TRUE(insert(*writer,
    doc1->indexed.begin(), doc1->indexed.end(),
    doc1->stored.begin(), doc1->stored.end()));
  ASSERT_TRUE(insert(*writer,
    doc2->indexed.begin(), doc2->indexed.end(),
    doc2->stored.begin(), doc2->stored.end()));
  ASSERT_TRUE(insert(*writer,
    doc3->indexed.begin(), doc3->indexed.end(),
    doc3->stored.begin(), doc3->stored.end()));
  writer->commit();

  // Create expected index
  auto& expected_index = index();
  expected_index.emplace_back(writer->feature_info());
  expected_index.back().insert(
    doc0->indexed.begin(), doc0->indexed.end(),
    doc0->stored.begin(), doc0->stored.end());
  expected_index.back().insert(
    doc1->indexed.begin(), doc1->indexed.end(),
    doc1->stored.begin(), doc1->stored.end());
  expected_index.back().insert(
    doc2->indexed.begin(), doc2->indexed.end(),
    doc2->stored.begin(), doc2->stored.end());
  expected_index.back().insert(
    doc3->indexed.begin(), doc3->indexed.end(),
    doc3->stored.begin(), doc3->stored.end());
  assert_index();
}

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
