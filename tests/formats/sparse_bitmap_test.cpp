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
  using seek_type = range_type;

  template<size_t N>
  void test_rw_next(const range_type (&ranges)[N]);

  template<size_t N>
  void test_rw_seek(const range_type (&ranges)[N]);

  template<size_t N>
  void test_rw_seek_next(const range_type (&ranges)[N]);

  template<size_t N, size_t K>
  void test_rw_random_seek(
    const range_type (&ranges)[N],
    const seek_type(&seeks)[K]);
};

template<size_t N, size_t K>
void sparse_bitmap_test_case::test_rw_random_seek(
    const range_type (&ranges)[N],
    const seek_type (&seeks)[K]) {
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

    for (auto& seek : seeks) {
      ASSERT_EQ(seek.second, it.seek(seek.first));
    }
  }
}

template<size_t N>
void sparse_bitmap_test_case::test_rw_next(const range_type (&ranges)[N]) {
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
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }
}

template<size_t N>
void sparse_bitmap_test_case::test_rw_seek(const range_type (&ranges)[N]) {
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

  // seek
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
    irs::doc_id_t expected_doc = 0;

    for (const auto range : ranges) {
      expected_doc = range.first;

      while(expected_doc < range.second) {
        ASSERT_EQ(expected_doc, it.seek(expected_doc));
        ASSERT_EQ(expected_doc, it.value());
        ASSERT_EQ(expected_doc, doc->value);
        ASSERT_EQ(expected_index, it.index());
        ASSERT_EQ(expected_index, index->value);
        ++expected_doc;
        ++expected_index;
      }
    }

    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_TRUE(irs::doc_limits::eof(it.seek(expected_doc)));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }
}

template<size_t N>
void sparse_bitmap_test_case::test_rw_seek_next(const range_type (&ranges)[N]) {
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

    irs::doc_id_t expected_doc;
    irs::doc_id_t max_doc;

    auto begin = std::begin(ranges);
    for (; begin != std::end(ranges); ++begin) {
      std::tie(expected_doc, max_doc) = *begin;

      while (expected_doc < max_doc) {
        irs::doc_id_t expected_index = 0;

        for (auto range = std::begin(ranges); range != begin; ++range) {
          ASSERT_LE(range->first, range->second);
          expected_index += range->second - range->first;
        }

        stream->seek(0);
        ASSERT_EQ(0, stream->file_pointer());

        irs::sparse_bitmap_iterator it(*stream);
        auto* index = irs::get<irs::value_index>(it);
        ASSERT_NE(nullptr, index); // index value is unspecified for invalid docs
        auto* doc = irs::get<irs::document>(it);
        ASSERT_NE(nullptr, doc);
        ASSERT_FALSE(irs::doc_limits::valid(doc->value));
        auto* cost = irs::get<irs::document>(it);
        ASSERT_NE(nullptr, cost);
        // FIXME check cost value

        ASSERT_EQ(expected_doc, it.seek(expected_doc));
        ASSERT_EQ(expected_doc, it.value());
        ASSERT_EQ(expected_doc, doc->value);
        ASSERT_EQ(expected_index, it.index());
        ASSERT_EQ(expected_index, index->value);

        ++expected_doc;
        ++expected_index;

        for (auto range = begin;;) {
          for (; expected_doc < max_doc; ) {
            ASSERT_TRUE(it.next());
            ASSERT_EQ(expected_doc, it.value());
            ASSERT_EQ(expected_doc, doc->value);
            ASSERT_EQ(expected_index, it.index());
            ASSERT_EQ(expected_index, index->value);
            ++expected_doc;
            ++expected_index;
          }

          if (++range < std::end(ranges)) {
            // attempt to seek backwards
            ASSERT_EQ(it.value(), it.seek(it.value()-1));

            std::tie(expected_doc, max_doc) = *range;
          } else {
            break;
          }
        }

        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
        ASSERT_TRUE(irs::doc_limits::eof(it.seek(expected_doc)));
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
    }
  }

  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    irs::doc_id_t seek_expected_index = 0;
    irs::doc_id_t seek_expected_doc;

    irs::sparse_bitmap_iterator seek_it(*stream);
    auto* index = irs::get<irs::value_index>(seek_it);
    ASSERT_NE(nullptr, index); // index value is unspecified for invalid docs
    auto* doc = irs::get<irs::document>(seek_it);
    ASSERT_NE(nullptr, doc);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    auto* cost = irs::get<irs::document>(seek_it);
    ASSERT_NE(nullptr, cost);

    // FIXME check cost value
    auto begin = std::begin(ranges);
    for (; begin != std::end(ranges); ++begin) {
      seek_expected_doc = begin->first;

      while (seek_expected_doc < begin->second) {
        ASSERT_EQ(seek_expected_doc, seek_it.seek(seek_expected_doc));
        ASSERT_EQ(seek_expected_doc, seek_it.value());
        ASSERT_EQ(seek_expected_doc, doc->value);
        ASSERT_EQ(seek_expected_index, seek_it.index());
        ASSERT_EQ(seek_expected_index, index->value);
        ++seek_expected_doc;
        ++seek_expected_index;
      }

      auto dup = stream->dup();
      ASSERT_NE(nullptr, dup);
      dup->seek(0);
      ASSERT_EQ(0, dup->file_pointer());

      irs::sparse_bitmap_iterator it(*dup);
      auto* index = irs::get<irs::value_index>(it);
      ASSERT_NE(nullptr, index); // index value is unspecified for invalid docs
      auto* doc = irs::get<irs::document>(it);
      ASSERT_NE(nullptr, doc);
      ASSERT_FALSE(irs::doc_limits::valid(doc->value));
      auto* cost = irs::get<irs::document>(it);
      ASSERT_NE(nullptr, cost);
      // FIXME check cost value

      ASSERT_EQ(seek_expected_doc-1, it.seek(seek_it.value()));
      ASSERT_EQ(seek_expected_doc-1, it.value());
      ASSERT_EQ(seek_expected_doc-1, doc->value);
      ASSERT_EQ(seek_expected_index-1, it.index());
      ASSERT_EQ(seek_expected_index-1, index->value);

      for (; begin != std::end(ranges); ++begin) {
        auto expected_doc = seek_expected_doc;
        auto max_doc = begin->second;

        while (expected_doc < max_doc) {
          irs::doc_id_t expected_index = 0;

          for (auto range = std::begin(ranges); range != begin; ++range) {
            ASSERT_LE(range->first, range->second);
            expected_index += range->second - range->first;
          }

          auto dup = stream->dup();
          ASSERT_NE(nullptr, dup);
          dup->seek(0);
          ASSERT_EQ(0, dup->file_pointer());

          irs::sparse_bitmap_iterator it(*stream);
          auto* index = irs::get<irs::value_index>(it);
          ASSERT_NE(nullptr, index); // index value is unspecified for invalid docs
          auto* doc = irs::get<irs::document>(it);
          ASSERT_NE(nullptr, doc);
          ASSERT_FALSE(irs::doc_limits::valid(doc->value));
          auto* cost = irs::get<irs::document>(it);
          ASSERT_NE(nullptr, cost);
          // FIXME check cost value

          ASSERT_EQ(expected_doc, it.seek(expected_doc));
          ASSERT_EQ(expected_doc, it.value());
          ASSERT_EQ(expected_doc, doc->value);
          ASSERT_EQ(expected_index, it.index());
          ASSERT_EQ(expected_index, index->value);

          ++expected_doc;
          ++expected_index;

          for (auto range = begin;;) {
            for (; expected_doc < max_doc; ) {
              ASSERT_TRUE(it.next());
              ASSERT_EQ(expected_doc, it.value());
              ASSERT_EQ(expected_doc, doc->value);
              ASSERT_EQ(expected_index, it.index());
              ASSERT_EQ(expected_index, index->value);
              ++expected_doc;
              ++expected_index;
            }

            if (++range < std::end(ranges)) {
              // attempt to seek backwards
              ASSERT_EQ(it.value(), it.seek(it.value()-1));

              std::tie(expected_doc, max_doc) = *range;
            } else {
              break;
            }
          }

          ASSERT_FALSE(it.next());
          ASSERT_TRUE(irs::doc_limits::eof(it.value()));
          ASSERT_TRUE(irs::doc_limits::eof(it.seek(expected_doc)));
          ASSERT_TRUE(irs::doc_limits::eof(it.value()));
        }
      }

      ASSERT_FALSE(seek_it.next());
      ASSERT_TRUE(irs::doc_limits::eof(seek_it.value()));
      ASSERT_TRUE(irs::doc_limits::eof(seek_it.seek(seek_expected_doc)));
      ASSERT_TRUE(irs::doc_limits::eof(seek_it.value()));
    }
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

TEST_P(sparse_bitmap_test_case, rw_mixed) {
  constexpr range_type ranges[] {
    { 1, 32 }, { 160, 1184, }, { 1201, 1734 }, { 60000, 64500 },
    { 196608, 262144 },
    { 328007, 328284 }, { 328412, 329489 }, { 329490, 333586 },
    { 458757, 458758 }, { 458777, 460563 }
  };

  test_rw_next(ranges);
  test_rw_seek(ranges);
  test_rw_seek_next(ranges);

  constexpr seek_type seeks[] { };

  test_rw_random_seek(ranges, seeks);
}

TEST_P(sparse_bitmap_test_case, rw_dense) {
  constexpr range_type ranges[] {
    { 1, 32 }, { 160, 1184, }, { 1201, 1734 }, { 60000, 64500 },
    { 328007, 328284 }, { 328412, 329489 }, { 329490, 333586 }
  };

  test_rw_next(ranges);
  test_rw_seek(ranges);
  test_rw_seek_next(ranges);

  constexpr seek_type seeks[] { };
  test_rw_random_seek(ranges, seeks);
}

TEST_P(sparse_bitmap_test_case, rw_sparse) {
  constexpr range_type ranges[] {
    { 1, 32 }, { 160, 1184, }, { 1201, 1734 },
    { 328007, 328284 }, { 328412, 329489 }
  };

  test_rw_next(ranges);
  test_rw_seek(ranges);
  test_rw_seek_next(ranges);

  constexpr seek_type seeks[] { };

  test_rw_random_seek(ranges, seeks);
}

TEST_P(sparse_bitmap_test_case, rw_all) {
  constexpr std::pair<irs::doc_id_t, irs::doc_id_t> ranges[] {
    { 65536, 131072 },
    { 196608, 262144 }
  };

  test_rw_next(ranges);
  test_rw_seek(ranges);
  test_rw_seek_next(ranges);
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
