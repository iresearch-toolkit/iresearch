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

#include "gtest/gtest.h"
#include "utils/container_utils.hpp"

TEST(container_utils_tests, test_bucket_allocator) {
  static bool WAS_MADE{};
  static size_t ACTUAL_SIZE{};

  struct bucket_builder {
    typedef std::unique_ptr<irs::byte_type[]> ptr;

    static ptr make(size_t size) {
      ACTUAL_SIZE = size;
      WAS_MADE = true;

      return std::make_unique<irs::byte_type[]>(size);
    }
  };

  // how many buckets to cache for each level
  using raw_block_vector_t = irs::container_utils::raw_block_vector<16, 8>;

  raw_block_vector_t blocks;

  // just created
  ASSERT_TRUE(blocks.empty());
  ASSERT_EQ(0, blocks.buffer_count());

  // first bucket
  ACTUAL_SIZE = 0;
  WAS_MADE = false;
  blocks.push_buffer();
  ASSERT_TRUE(WAS_MADE);        // buffer was actually built by builder
  ASSERT_EQ(256, ACTUAL_SIZE);  // first bucket 2^8
  ASSERT_TRUE(1 == blocks.buffer_count());
  ASSERT_TRUE(!blocks.empty());
  blocks.pop_buffer();  // pop recently pushed buffer
  ASSERT_TRUE(blocks.empty());
  ASSERT_TRUE(0 == blocks.buffer_count());
  ACTUAL_SIZE = 0;
  WAS_MADE = false;
  blocks.push_buffer();
  ASSERT_FALSE(WAS_MADE);     // buffer has been reused from the pool
  ASSERT_EQ(0, ACTUAL_SIZE);  // didn't change
  ASSERT_TRUE(1 == blocks.buffer_count());
  ASSERT_TRUE(!blocks.empty());

  // second bukcet
  ACTUAL_SIZE = 0;
  WAS_MADE = false;
  blocks.push_buffer();
  ASSERT_TRUE(WAS_MADE);        // buffer was actually built by builder
  ASSERT_EQ(512, ACTUAL_SIZE);  // first bucket 2^9
  ASSERT_TRUE(2 == blocks.buffer_count());
  ASSERT_TRUE(!blocks.empty());
  blocks.pop_buffer();  // pop recently pushed buffer
  ASSERT_TRUE(!blocks.empty());
  ASSERT_TRUE(1 == blocks.buffer_count());
  ACTUAL_SIZE = 0;
  WAS_MADE = false;
  blocks.push_buffer();
  ASSERT_FALSE(WAS_MADE);     // buffer has been reused from the pool
  ASSERT_EQ(0, ACTUAL_SIZE);  // didn't change
  ASSERT_TRUE(2 == blocks.buffer_count());
  ASSERT_TRUE(!blocks.empty());

  // cleanup
  blocks.clear();
  ASSERT_TRUE(blocks.empty());
  ASSERT_EQ(0, blocks.buffer_count());
}

TEST(container_utils_tests, test_compute_bucket_meta) {
  // test meta for num buckets == 1, skip bits == 0
  {
    auto meta = irs::container_utils::BucketMeta<1, 0>{};
    ASSERT_EQ(0, meta[0].offset);
    ASSERT_EQ(1, meta[0].size);
  }

  // test meta for num buckets == 2, skip bits == 0
  {
    auto meta = irs::container_utils::BucketMeta<2, 0>{};
    ASSERT_EQ(0, meta[0].offset);
    ASSERT_EQ(1, meta[0].size);
    ASSERT_EQ(1, meta[1].offset);
    ASSERT_EQ(2, meta[1].size);
  }

  // test meta for num buckets == 3, skip bits == 0
  {
    auto meta = irs::container_utils::BucketMeta<3, 0>{};
    ASSERT_EQ(0, meta[0].offset);
    ASSERT_EQ(1, meta[0].size);
    ASSERT_EQ(1, meta[1].offset);
    ASSERT_EQ(2, meta[1].size);
    ASSERT_EQ(3, meta[2].offset);
    ASSERT_EQ(4, meta[2].size);
  }

  // test meta for num buckets == 1, skip bits == 2
  {
    auto meta = irs::container_utils::BucketMeta<1, 2>{};
    ASSERT_EQ(0, meta[0].offset);
    ASSERT_EQ(4, meta[0].size);
  }

  // test meta for num buckets == 2, skip bits == 2
  {
    auto meta = irs::container_utils::BucketMeta<2, 2>{};
    ASSERT_EQ(0, meta[0].offset);
    ASSERT_EQ(4, meta[0].size);
    ASSERT_EQ(4, meta[1].offset);
    ASSERT_EQ(8, meta[1].size);
  }

  // test meta for num buckets == 3, skip bits == 2
  {
    auto meta = irs::container_utils::BucketMeta<3, 2>{};
    ASSERT_EQ(0, meta[0].offset);
    ASSERT_EQ(4, meta[0].size);
    ASSERT_EQ(4, meta[1].offset);
    ASSERT_EQ(8, meta[1].size);
    ASSERT_EQ(12, meta[2].offset);
    ASSERT_EQ(16, meta[2].size);
  }
}

TEST(container_utils_tests, ComputeBucketOffset) {
  // test boundaries for skip bits == 0
  {
    ASSERT_EQ(0, (irs::container_utils::ComputeBucketOffset<0>(0)));
    ASSERT_EQ(1, (irs::container_utils::ComputeBucketOffset<0>(1)));
    ASSERT_EQ(1, (irs::container_utils::ComputeBucketOffset<0>(2)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<0>(3)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<0>(4)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<0>(5)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<0>(6)));
    ASSERT_EQ(3, (irs::container_utils::ComputeBucketOffset<0>(7)));
  }

  // test boundaries for skip bits == 2
  {
    ASSERT_EQ(0, (irs::container_utils::ComputeBucketOffset<2>(0)));
    ASSERT_EQ(0, (irs::container_utils::ComputeBucketOffset<2>(1)));
    ASSERT_EQ(0, (irs::container_utils::ComputeBucketOffset<2>(2)));
    ASSERT_EQ(0, (irs::container_utils::ComputeBucketOffset<2>(3)));
    ASSERT_EQ(1, (irs::container_utils::ComputeBucketOffset<2>(4)));
    ASSERT_EQ(1, (irs::container_utils::ComputeBucketOffset<2>(5)));
    ASSERT_EQ(1, (irs::container_utils::ComputeBucketOffset<2>(6)));
    ASSERT_EQ(1, (irs::container_utils::ComputeBucketOffset<2>(7)));
    ASSERT_EQ(1, (irs::container_utils::ComputeBucketOffset<2>(8)));
    ASSERT_EQ(1, (irs::container_utils::ComputeBucketOffset<2>(9)));
    ASSERT_EQ(1, (irs::container_utils::ComputeBucketOffset<2>(10)));
    ASSERT_EQ(1, (irs::container_utils::ComputeBucketOffset<2>(11)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<2>(12)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<2>(13)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<2>(14)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<2>(15)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<2>(16)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<2>(17)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<2>(18)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<2>(19)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<2>(20)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<2>(21)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<2>(22)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<2>(23)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<2>(24)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<2>(25)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<2>(26)));
    ASSERT_EQ(2, (irs::container_utils::ComputeBucketOffset<2>(27)));
    ASSERT_EQ(3, (irs::container_utils::ComputeBucketOffset<2>(28)));
  }
}
