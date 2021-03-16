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
#include "tests_param.hpp"

#include "formats/sparse_bitmap.hpp"

class sparse_bitmap_test_case : public tests::directory_test_case_base {
 protected:
  using range_type = std::pair<irs::doc_id_t, irs::doc_id_t>;

  template<size_t N>
  void test_read_write(const range_type (&ranges)[N]);
};

template<size_t N>
void sparse_bitmap_test_case::test_read_write(const range_type (&ranges)[N]) {
  {
    auto stream = dir().create("tmp");
    ASSERT_NE(nullptr, stream);

    irs::sparse_bitmap_writer writer(*stream);

    for (const auto& range : ranges) {
      irs::doc_id_t doc = range.first;
      std::generate_n(
        std::back_inserter(writer),
        range.second - range.first,
        [&doc] { return doc++; });
    }

    writer.finish();
  }

  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    irs::sparse_bitmap_iterator it(*stream);
    auto* index = irs::get<irs::value_index>(it);
    ASSERT_NE(nullptr, index); // index value is unspecified for invalid docs
    auto* doc = irs::get<irs::document>(it);
    ASSERT_NE(nullptr, doc);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    auto* cost = irs::get<irs::document>(it);
    ASSERT_NE(nullptr, cost);
    // FIXME check cost value

    irs::doc_id_t expected_index = 0;

    for (const auto range : ranges) {
      irs::doc_id_t expected_doc = range.first;

      while(expected_doc < range.second) {
        ASSERT_TRUE(it.next());
        ASSERT_EQ(expected_doc, it.value());
        ASSERT_EQ(expected_doc, doc->value);
        ASSERT_EQ(expected_index, it.index());
        ASSERT_EQ(expected_index, index->value);
        ++expected_doc;
        ++expected_index;
      }
    }

    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
    ASSERT_FALSE(it.next());
    ASSERT_EQ(irs::doc_limits::eof(), it.value());
  }
}


TEST_P(sparse_bitmap_test_case, read_write_empty) {
  {
    auto stream = dir().create("tmp");
    ASSERT_NE(nullptr, stream);

    irs::sparse_bitmap_writer writer(*stream);
    writer.finish();
  }

  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    irs::sparse_bitmap_iterator it(*stream);
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }
}

TEST_P(sparse_bitmap_test_case, read_write_dense) {
  constexpr std::pair<irs::doc_id_t, irs::doc_id_t> ranges[] {
    { 1, 32 }, { 160, 1184, }, { 1201, 1734 }, { 60000, 64500 },
    { 328007, 328284 }, { 328412, 329489 }, { 329490, 333586 }
  };

  test_read_write(ranges);
}

TEST_P(sparse_bitmap_test_case, read_write_sparse) {
  constexpr std::pair<irs::doc_id_t, irs::doc_id_t> ranges[] {
    { 1, 32 }, { 160, 1184, }, { 1201, 1734 },
    { 328007, 328284 }, { 328412, 329489 }
  };

  test_read_write(ranges);
}

TEST_P(sparse_bitmap_test_case, read_write_all) {
  constexpr std::pair<irs::doc_id_t, irs::doc_id_t> ranges[] {
    { 65536, 131072 },
    { 196608, 262144 }
  };

  test_read_write(ranges);
}

INSTANTIATE_TEST_SUITE_P(
  sparse_bitmap_test,
  sparse_bitmap_test_case,
  ::testing::Values(
    &tests::memory_directory,
    &tests::fs_directory,
    &tests::mmap_directory),
  sparse_bitmap_test_case::to_string
);
