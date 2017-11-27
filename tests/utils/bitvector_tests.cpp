////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "tests_shared.hpp"
#include "utils/bitvector.hpp"

TEST(bitvector_tests, ctor) {
  // zero size bitvector
  {
    const irs::bitset::index_t words = 0;
    const irs::bitset::index_t size = 0;
    const irs::bitvector bv(size);
    ASSERT_EQ(nullptr, bv.data());
    ASSERT_EQ(bv.data(), bv.begin());
    ASSERT_EQ(bv.data() + words, bv.end());
    ASSERT_EQ(size, bv.size());
    ASSERT_EQ(words, std::distance(bv.begin(), bv.end()));
    ASSERT_EQ(0, bv.capacity());
    ASSERT_EQ(0, bv.count());
    ASSERT_TRUE(bv.none());
    ASSERT_FALSE(bv.any());
    ASSERT_TRUE(bv.all());
  }

  // less that 1 word size bitvector
  {
    const irs::bitset::index_t words = 1;
    const irs::bitset::index_t size = 32;
    const irs::bitvector bv(size);
    ASSERT_NE(nullptr, bv.data());
    ASSERT_EQ(bv.data(), bv.begin());
    ASSERT_EQ(bv.data() + words, bv.end());
    ASSERT_EQ(size, bv.size());
    ASSERT_EQ(words, std::distance(bv.begin(), bv.end()));
    ASSERT_EQ(words * irs::bits_required<irs::bitset::word_t>(), bv.capacity());
    ASSERT_EQ(0, bv.count());
    ASSERT_TRUE(bv.none());
    ASSERT_FALSE(bv.any());
    ASSERT_FALSE(bv.all());
  }

  // uint64_t size bitvector
  {
    const irs::bitset::index_t size = 64;
    const irs::bitset::index_t words = 1;
    const irs::bitvector bv(size);
    ASSERT_NE(nullptr, bv.data());
    ASSERT_EQ(bv.data(), bv.begin());
    ASSERT_EQ(bv.data() + words, bv.end());
    ASSERT_EQ(size, bv.size());
    ASSERT_EQ(words, std::distance(bv.begin(), bv.end()));
    ASSERT_EQ(size, bv.capacity());
    ASSERT_EQ(0, bv.count());
    ASSERT_TRUE(bv.none());
    ASSERT_FALSE(bv.any());
    ASSERT_FALSE(bv.all());
  }

  // nonzero size bitvector
  {
    const irs::bitset::index_t words = 2;
    const irs::bitset::index_t size = 78;
    const irs::bitvector bv(size);
    ASSERT_NE(nullptr, bv.data());
    ASSERT_EQ(bv.data(), bv.begin());
    ASSERT_EQ(bv.data() + words, bv.end());
    ASSERT_EQ(size, bv.size());
    ASSERT_EQ(bv.data() + words, bv.end());
    ASSERT_EQ(words * irs::bits_required<irs::bitset::word_t>(), bv.capacity());
    ASSERT_EQ(0, bv.count());
    ASSERT_TRUE(bv.none());
    ASSERT_FALSE(bv.any());
    ASSERT_FALSE(bv.all());
  }
}

TEST(bitvector_tests, set_unset) {
  const irs::bitset::index_t words = 3;
  const irs::bitset::index_t size = 155;
  irs::bitvector bv(size);
  ASSERT_NE(nullptr, bv.data());
  ASSERT_EQ(bv.data(), bv.begin());
  ASSERT_EQ(size, bv.size());
  ASSERT_EQ(words * irs::bits_required<irs::bitset::word_t>(), bv.capacity());
  ASSERT_EQ(0, bv.count());
  ASSERT_TRUE(bv.none());
  ASSERT_FALSE(bv.any());
  ASSERT_FALSE(bv.all());

  // set
  const irs::bitset::index_t i = 43;
  ASSERT_FALSE(bv.test(i));
  bv.set(i);
  ASSERT_TRUE(bv.test(i));
  ASSERT_EQ(1, bv.count());

  // unset
  bv.unset(i);
  ASSERT_FALSE(bv.test(i));
  ASSERT_EQ(0, bv.count());

  // reset
  bv.reset(i, true);
  ASSERT_TRUE(bv.test(i));
  bv.reset(i, false);
  ASSERT_FALSE(bv.test(i));
}

TEST(bitvector_tests, resize) {
  irs::bitvector bv;
  ASSERT_EQ(nullptr, bv.data());
  ASSERT_EQ(bv.data(), bv.begin());
  ASSERT_EQ(0, bv.size());
  ASSERT_EQ(0, bv.capacity());
  ASSERT_EQ(0, bv.count());
  ASSERT_TRUE(bv.none());
  ASSERT_FALSE(bv.any());
  ASSERT_TRUE(bv.all());

  // reset to bigger size
  irs::bitset::index_t words = 3;
  irs::bitset::index_t size = 155;

  bv.resize(size);
  ASSERT_NE(nullptr, bv.data());
  ASSERT_EQ(size, bv.size());
  ASSERT_EQ(words * irs::bits_required<irs::bitset::word_t>(), bv.capacity());
  ASSERT_EQ(0, bv.count());
  ASSERT_TRUE(bv.none());
  ASSERT_FALSE(bv.any());
  ASSERT_FALSE(bv.all());
  bv.set(42);
  ASSERT(1, bv.count());
  bv.set(42);
  ASSERT(1, bv.count());
  bv.set(73);
  ASSERT(2, bv.count());
  ASSERT_FALSE(bv.none());
  ASSERT_TRUE(bv.any());
  ASSERT_FALSE(bv.all());
  auto* prev_data = bv.data();

  // reset to smaller size
  words = 2;
  size = 89;

  bv.resize(size); // reset to smaller size
  ASSERT_NE(nullptr, bv.data());
  ASSERT_EQ(size, bv.size());
  ASSERT_NE(prev_data, bv.data()); // storage was reallocated
  ASSERT_EQ(words * irs::bits_required<irs::bitset::word_t>(), bv.capacity());;
  ASSERT_EQ(2, bv.count());
  ASSERT_FALSE(bv.none()); // content not cleared
  ASSERT_TRUE(bv.any()); // content not cleared
  ASSERT_FALSE(bv.all()); // content not cleared
  bv.set(43);
  ASSERT(3, bv.count());
  bv.set(43);
  ASSERT(3, bv.count());
  bv.set(74);
  ASSERT(4, bv.count());
  ASSERT_FALSE(bv.none());
  ASSERT_TRUE(bv.any());
  ASSERT_FALSE(bv.all());
  prev_data = bv.data();

  // reset to bigger size
  words = 5;
  size = 319;

  bv.resize(size); // reset to bigger size
  ASSERT_NE(nullptr, bv.data());
  ASSERT_EQ(size, bv.size());
  ASSERT_NE(prev_data, bv.data()); // storage was reallocated
  ASSERT_EQ(words * irs::bits_required<irs::bitset::word_t>(), bv.capacity());
  ASSERT_EQ(4, bv.count());
  ASSERT_FALSE(bv.none()); // content not cleared
  ASSERT_TRUE(bv.any()); // content not cleared
  ASSERT_FALSE(bv.all()); // content not cleared
  bv.set(44);
  ASSERT(5, bv.count());
  bv.set(44);
  ASSERT(5, bv.count());
  bv.set(75);
  ASSERT(6, bv.count());
  ASSERT_FALSE(bv.none());
  ASSERT_TRUE(bv.any());
  ASSERT_FALSE(bv.all());
}

TEST(bitvector_tests, clear_count) {
  const irs::bitset::index_t words = 3;
  const irs::bitset::index_t size = 155;
  irs::bitvector bv(size);
  ASSERT_NE(nullptr, bv.data());
  ASSERT_EQ(size, bv.size());
  ASSERT_EQ(words * irs::bits_required<irs::bitset::word_t>(), bv.capacity());
  ASSERT_EQ(0, bv.count());
  ASSERT_TRUE(bv.none());
  ASSERT_FALSE(bv.any());
  ASSERT_FALSE(bv.all());

  bv.set(42);
  ASSERT(1, bv.count());
  bv.set(42);
  ASSERT(1, bv.count());
  bv.set(73);
  ASSERT(2, bv.count());
  ASSERT_FALSE(bv.none());
  ASSERT_TRUE(bv.any());
  ASSERT_FALSE(bv.all());

  // set almost all bits
  const irs::bitset::index_t end = 100;
  for (irs::bitset::index_t i = 0; i < end; ++i) {
    bv.set(i);
  }
  ASSERT_EQ(end, bv.count());
  ASSERT_FALSE(bv.none());
  ASSERT_TRUE(bv.any());
  ASSERT_FALSE(bv.all());

  // set almost all
  for (irs::bitset::index_t i = 0; i < bv.size(); ++i) {
    bv.set(i);
  }
  ASSERT_EQ(bv.size(), bv.count());
  ASSERT_FALSE(bv.none());
  ASSERT_TRUE(bv.any());
  ASSERT_TRUE(bv.all());

  // clear all bits
  bv.clear();
  ASSERT_TRUE(bv.none());
  ASSERT_FALSE(bv.any());
  ASSERT_FALSE(bv.all());
}

TEST(bitvector_tests, memset) {
  // empty bitvector
  {
    irs::bitvector bv;
    ASSERT_EQ(nullptr, bv.data());
    ASSERT_EQ(0, bv.size());
    ASSERT_EQ(0, bv.capacity());
    ASSERT_EQ(0, bv.count());
    ASSERT_TRUE(bv.none());
    ASSERT_FALSE(bv.any());
    ASSERT_TRUE(bv.all());

    bv.memset(0x723423);

    ASSERT_EQ(10, bv.count());
    ASSERT_FALSE(bv.none());
    ASSERT_TRUE(bv.any());
    ASSERT_FALSE(bv.all());
  }

  // single word bitvector
  {
    const irs::bitset::index_t words = 1;
    const irs::bitset::index_t size = 15;

    irs::bitvector bv(size);
    ASSERT_NE(nullptr, bv.data());
    ASSERT_EQ(size, bv.size());
    ASSERT_EQ(words * irs::bits_required<irs::bitset::word_t>(), bv.capacity());
    ASSERT_EQ(0, bv.count());
    ASSERT_TRUE(bv.none());
    ASSERT_FALSE(bv.any());
    ASSERT_FALSE(bv.all());

    irs::bitset::word_t value = 0x723423;
    bv.memset(&value, 2);

    ASSERT_EQ(6, bv.count()); // only first 16 bits from 'value' are set
    ASSERT_EQ(*bv.begin(), value & 0x7FFF);
    ASSERT_FALSE(bv.none());
    ASSERT_TRUE(bv.any());
    ASSERT_FALSE(bv.all());

    bv.memset(value);

    ASSERT_EQ(10, bv.count()); // only first 64 bits from 'value' are set
    ASSERT_EQ(*bv.begin(), value);
    ASSERT_FALSE(bv.none());
    ASSERT_TRUE(bv.any());
    ASSERT_FALSE(bv.all());

    value = 0xFFFFFFFFFFFFFFFF;
    bv.memset(value); // set another value

    ASSERT_EQ(64, bv.count()); // only first 64 bits from 'value' are set
    ASSERT_EQ(*bv.begin(), value);
    ASSERT_FALSE(bv.none());
    ASSERT_TRUE(bv.any());
    ASSERT_TRUE(bv.all());

    value = 0x42;
    bv.memset(&value, 1); // memset another value less that size

    ASSERT_EQ(58, bv.count()); // only first 8 bits from 'value' are set
    ASSERT_EQ(irs::bitset::word_t(0xFFFFFFFFFFFFFF42), *bv.begin());
    ASSERT_FALSE(bv.none());
    ASSERT_TRUE(bv.any());
    ASSERT_FALSE(bv.all());
  }

  // multiple words bitvector
  {
    const irs::bitset::index_t words = 2;
    const irs::bitset::index_t size = 78;

    irs::bitvector bv(size);
    ASSERT_NE(nullptr, bv.data());
    ASSERT_EQ(size, bv.size());
    ASSERT_EQ(words * irs::bits_required<irs::bitset::word_t>(), bv.capacity());
    ASSERT_EQ(0, bv.count());
    ASSERT_TRUE(bv.none());
    ASSERT_FALSE(bv.any());
    ASSERT_FALSE(bv.all());

    const uint64_t value = UINT64_C(0x14FFFFFFFFFFFFFF);
    bv.memset(value);

    ASSERT_EQ(58, bv.count()); // only first 64 bits from 'value' are set
    ASSERT_EQ(*(bv.begin() + irs::bitset::word(0)), value); // 1st word
    ASSERT_EQ(*(bv.begin() + irs::bitset::word(64)), 0); // 2nd word
    ASSERT_FALSE(bv.none());
    ASSERT_TRUE(bv.any());
    ASSERT_FALSE(bv.all());
  }

  // multiple words bitvector (all set)
  {
    const irs::bitset::index_t words = 2;
    const irs::bitset::index_t size = 2 * irs::bits_required<irs::bitset::word_t>();

    irs::bitvector bv(size);
    ASSERT_NE(nullptr, bv.data());
    ASSERT_EQ(size, bv.size());
    ASSERT_EQ(words * irs::bits_required<irs::bitset::word_t>(), bv.capacity());
    ASSERT_EQ(0, bv.count());
    ASSERT_TRUE(bv.none());
    ASSERT_FALSE(bv.any());
    ASSERT_FALSE(bv.all());

    struct value_t {
      uint64_t value0 = UINT64_C(0xFFFFFFFFFFFFFFFF);
      uint64_t value1 = UINT64_C(0xFFFFFFFFFFFFFFFF);
    } value;
    bv.memset(value);

    ASSERT_EQ(sizeof(value_t) * irs::bits_required<uint8_t>(), bv.size()); // full size of bitset
    ASSERT_EQ(128, bv.count()); // all 128 from 'value' are set
    ASSERT_EQ(*(bv.begin() + irs::bitset::word(0)), value.value0); // 1st word
    ASSERT_EQ(*(bv.begin() + irs::bitset::word(64)), value.value1); // 2nd word
    ASSERT_FALSE(bv.none());
    ASSERT_TRUE(bv.any());
    ASSERT_TRUE(bv.all());
  }
}

TEST(bitvector_tests, reserve) {
  irs::bitvector bv(1);
  ASSERT_NE(nullptr, bv.data());
  ASSERT_EQ(1, bv.size());
  ASSERT_EQ(irs::bits_required<irs::bitset::word_t>(), bv.capacity());
  ASSERT_EQ(0, bv.count());
  ASSERT_TRUE(bv.none());
  ASSERT_FALSE(bv.any());
  ASSERT_FALSE(bv.all());
  auto* prev_data = bv.data();

  bv.set(0);
  ASSERT_EQ(1, bv.count());
  ASSERT_FALSE(bv.none());
  ASSERT_TRUE(bv.any());
  ASSERT_TRUE(bv.all());
  ASSERT_EQ(prev_data, bv.data());

  bv.reserve(2);
  ASSERT_EQ(1, bv.count());
  ASSERT_FALSE(bv.none());
  ASSERT_TRUE(bv.any());
  ASSERT_TRUE(bv.all());
  ASSERT_EQ(prev_data, bv.data());

  bv.reserve(irs::bits_required<irs::bitset::word_t>() + 1);
  ASSERT_EQ(1, bv.count());
  ASSERT_FALSE(bv.none());
  ASSERT_TRUE(bv.any());
  ASSERT_TRUE(bv.all());
  ASSERT_NE(prev_data, bv.data());
  prev_data = bv.data();

  bv.reserve(1);
  ASSERT_EQ(1, bv.count());
  ASSERT_FALSE(bv.none());
  ASSERT_TRUE(bv.any());
  ASSERT_TRUE(bv.all());
  ASSERT_EQ(prev_data, bv.data());
}

TEST(bitvector_tests, set_unset_pas_end) {
  irs::bitvector bv(1);
  ASSERT_NE(nullptr, bv.data());
  ASSERT_EQ(1, bv.size());
  ASSERT_EQ(irs::bits_required<irs::bitset::word_t>(), bv.capacity());
  ASSERT_EQ(0, bv.count());
  ASSERT_TRUE(bv.none());
  ASSERT_FALSE(bv.any());
  ASSERT_FALSE(bv.all());
  auto* prev_data = bv.data();

  bv.set(irs::bits_required<irs::bitset::word_t>());
  ASSERT_EQ(irs::bits_required<irs::bitset::word_t>() * 2, bv.capacity());
  ASSERT_EQ(1, bv.count());
  ASSERT_FALSE(bv.none());
  ASSERT_TRUE(bv.any());
  ASSERT_FALSE(bv.all());
  ASSERT_NE(prev_data, bv.data());
  prev_data = bv.data();

  bv.unset(irs::bits_required<irs::bitset::word_t>());
  ASSERT_EQ(irs::bits_required<irs::bitset::word_t>() * 2, bv.capacity());
  ASSERT_EQ(0, bv.count());
  ASSERT_TRUE(bv.none());
  ASSERT_FALSE(bv.any());
  ASSERT_FALSE(bv.all());
  ASSERT_EQ(prev_data, bv.data());

  bv.unset(irs::bits_required<irs::bitset::word_t>() * 2);
  ASSERT_EQ(irs::bits_required<irs::bitset::word_t>() * 2, bv.capacity());
  ASSERT_EQ(0, bv.count());
  ASSERT_TRUE(bv.none());
  ASSERT_FALSE(bv.any());
  ASSERT_FALSE(bv.all());
  ASSERT_EQ(prev_data, bv.data());

  bv.set(irs::bits_required<irs::bitset::word_t>() + 1);
  ASSERT_EQ(irs::bits_required<irs::bitset::word_t>() * 3, bv.capacity());
  ASSERT_EQ(1, bv.count());
  ASSERT_FALSE(bv.none());
  ASSERT_TRUE(bv.any());
  ASSERT_FALSE(bv.all());
  ASSERT_NE(prev_data, bv.data());
}

TEST(bitvector_tests, shrink_to_fit) {
  // shrink from null
  {
    irs::bitvector bv;
    ASSERT_EQ(nullptr, bv.data());
    ASSERT_EQ(0, bv.size());
    ASSERT_EQ(0, bv.capacity());
    ASSERT_EQ(0, bv.count());
    ASSERT_TRUE(bv.none());
    ASSERT_FALSE(bv.any());
    ASSERT_TRUE(bv.all());

    bv.shrink_to_fit();
    ASSERT_EQ(nullptr, bv.data());
    ASSERT_EQ(0, bv.size());
    ASSERT_EQ(0, bv.capacity());
    ASSERT_EQ(0, bv.count());
    ASSERT_TRUE(bv.none());
    ASSERT_FALSE(bv.any());
    ASSERT_TRUE(bv.all());
  }

  // shrink to null
  {
    irs::bitvector bv;
    bv.set(2);
    ASSERT_NE(nullptr, bv.data());
    ASSERT_EQ(3, bv.size());
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>(), bv.capacity());
    ASSERT_EQ(1, bv.count());
    ASSERT_FALSE(bv.none());
    ASSERT_TRUE(bv.any());
    ASSERT_FALSE(bv.all());
    auto* prev_data = bv.data();

    bv.unset(2);
    ASSERT_EQ(3, bv.size());
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>(), bv.capacity());
    ASSERT_EQ(0, bv.count());
    ASSERT_TRUE(bv.none());
    ASSERT_FALSE(bv.any());
    ASSERT_FALSE(bv.all());
    ASSERT_EQ(prev_data, bv.data());

    bv.shrink_to_fit();
    ASSERT_EQ(nullptr, bv.data());
    ASSERT_EQ(0, bv.size());
    ASSERT_EQ(0, bv.capacity());
    ASSERT_EQ(0, bv.count());
    ASSERT_TRUE(bv.none());
    ASSERT_FALSE(bv.any());
    ASSERT_TRUE(bv.all());
  }

  // shrink some
  {
    irs::bitvector bv;
    bv.set(0);
    bv.set(irs::bits_required<irs::bitset::word_t>());
    ASSERT_NE(nullptr, bv.data());
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>() + 1, bv.size());
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>() * 2, bv.capacity());
    ASSERT_EQ(2, bv.count());
    ASSERT_FALSE(bv.none());
    ASSERT_TRUE(bv.any());
    ASSERT_FALSE(bv.all());
    auto* prev_data = bv.data();

    bv.unset(irs::bits_required<irs::bitset::word_t>());
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>() + 1, bv.size());
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>() * 2, bv.capacity());
    ASSERT_EQ(1, bv.count());
    ASSERT_FALSE(bv.none());
    ASSERT_TRUE(bv.any());
    ASSERT_FALSE(bv.all());
    ASSERT_EQ(prev_data, bv.data());

    bv.shrink_to_fit();
    ASSERT_NE(nullptr, bv.data());
    ASSERT_EQ(1, bv.size());
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>(), bv.capacity());
    ASSERT_EQ(1, bv.count());
    ASSERT_FALSE(bv.none());
    ASSERT_TRUE(bv.any());
    ASSERT_TRUE(bv.all());
    ASSERT_NE(prev_data, bv.data());
  }

  // shrink none (only size)
  {
    irs::bitvector bv;
    bv.set(0);
    bv.set(irs::bits_required<irs::bitset::word_t>() - 1);
    ASSERT_NE(nullptr, bv.data());
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>(), bv.size());
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>(), bv.capacity());
    ASSERT_EQ(2, bv.count());
    ASSERT_FALSE(bv.none());
    ASSERT_TRUE(bv.any());
    ASSERT_FALSE(bv.all());
    auto* prev_data = bv.data();

    bv.unset(irs::bits_required<irs::bitset::word_t>() - 1);
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>(), bv.size());
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>(), bv.capacity());
    ASSERT_EQ(1, bv.count());
    ASSERT_FALSE(bv.none());
    ASSERT_TRUE(bv.any());
    ASSERT_FALSE(bv.all());
    ASSERT_EQ(prev_data, bv.data());

    bv.shrink_to_fit();
    ASSERT_NE(nullptr, bv.data());
    ASSERT_EQ(1, bv.size());
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>(), bv.capacity());
    ASSERT_EQ(1, bv.count());
    ASSERT_FALSE(bv.none());
    ASSERT_TRUE(bv.any());
    ASSERT_TRUE(bv.all());
    ASSERT_EQ(prev_data, bv.data());
  }

  // shrink none (noop)
  {
    irs::bitvector bv;
    bv.set(1);
    bv.set(irs::bits_required<irs::bitset::word_t>() - 1);
    ASSERT_NE(nullptr, bv.data());
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>(), bv.size());
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>(), bv.capacity());
    ASSERT_EQ(2, bv.count());
    ASSERT_FALSE(bv.none());
    ASSERT_TRUE(bv.any());
    ASSERT_FALSE(bv.all());
    auto* prev_data = bv.data();

    bv.unset(1);
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>(), bv.size());
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>(), bv.capacity());
    ASSERT_EQ(1, bv.count());
    ASSERT_FALSE(bv.none());
    ASSERT_TRUE(bv.any());
    ASSERT_FALSE(bv.all());
    ASSERT_EQ(prev_data, bv.data());

    bv.shrink_to_fit();
    ASSERT_NE(nullptr, bv.data());
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>(), bv.size());
    ASSERT_EQ(irs::bits_required<irs::bitset::word_t>(), bv.capacity());
    ASSERT_EQ(1, bv.count());
    ASSERT_FALSE(bv.none());
    ASSERT_TRUE(bv.any());
    ASSERT_FALSE(bv.all());
    ASSERT_EQ(prev_data, bv.data());
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------