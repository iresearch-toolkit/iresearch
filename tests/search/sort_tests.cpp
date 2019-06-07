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
#include "search/scorers.hpp"
#include "search/sort.hpp"

NS_LOCAL

template<size_t Size, size_t Align>
struct aligned_value {
  irs::memory::aligned_storage<Size, Align> data;

  // need these operators to be sort API compliant
  bool operator<(const aligned_value&) const { return false; }
  aligned_value operator+=(const aligned_value&) { return *this; }
};

template<typename ScoreType, typename StatsType>
struct aligned_scorer : public irs::sort {
  class prepared : public irs::sort::prepared_basic<ScoreType, StatsType> {
   public:
    virtual field_collector::ptr prepare_field_collector() const override {
      return nullptr;
    }
    virtual term_collector::ptr prepare_term_collector() const override {
      return nullptr;
    }
    virtual void collect(
        irs::byte_type*,
        const irs::index_reader&,
        const field_collector*,
        const term_collector*
    ) const override {
      // NOOP
    }
    virtual scorer::ptr prepare_scorer(
        const irs::sub_reader& segment,
        const irs::term_reader& field,
        const irs::byte_type* stats,
        const irs::attribute_view& doc_attrs,
        irs::boost_t boost
    ) const {
      return nullptr;
    }
    const irs::flags& features() const {
      return irs::flags::empty_instance();
    }
  };

  DECLARE_SORT_TYPE() { static irs::sort::type_id type("algined_scorer"); return type; }
  static ptr make() { return std::make_shared<aligned_scorer>(); }
  aligned_scorer(): irs::sort(aligned_scorer::type()) { }
  virtual irs::sort::prepared::ptr prepare() const override {
    return irs::memory::make_unique<aligned_scorer<ScoreType, StatsType>::prepared>();
  }
};

NS_END

TEST(sort_tests, order_equal) {
  struct dummy_scorer0: public irs::sort {
    DECLARE_SORT_TYPE() { static irs::sort::type_id type("dummy_scorer0"); return type; }
    static ptr make() { return std::make_shared<dummy_scorer0>(); }
    dummy_scorer0(): irs::sort(dummy_scorer0::type()) { }
    virtual prepared::ptr prepare() const override { return nullptr; }
  };

  struct dummy_scorer1: public irs::sort {
    DECLARE_SORT_TYPE() { static irs::sort::type_id type("dummy_scorer1"); return type; }
    static ptr make() { return std::make_shared<dummy_scorer1>(); }
    dummy_scorer1(): irs::sort(dummy_scorer1::type()) { }
    virtual prepared::ptr prepare() const override { return nullptr; }
  };

  // empty == empty
  {
    irs::order ord0;
    irs::order ord1;
    ASSERT_TRUE(ord0 == ord1);
    ASSERT_FALSE(ord0 != ord1);
  }

  // empty == !empty
  {
    irs::order ord0;
    irs::order ord1;
    ord1.add<dummy_scorer1>(false);
    ASSERT_FALSE(ord0 == ord1);
    ASSERT_TRUE(ord0 != ord1);
  }

  // different sort types
  {
    irs::order ord0;
    irs::order ord1;
    ord0.add<dummy_scorer0>(false);
    ord1.add<dummy_scorer1>(false);
    ASSERT_FALSE(ord0 == ord1);
    ASSERT_TRUE(ord0 != ord1);
  }

  // different order same sort type
  {
    irs::order ord0;
    irs::order ord1;
    ord0.add<dummy_scorer0>(false);
    ord0.add<dummy_scorer1>(false);
    ord1.add<dummy_scorer1>(false);
    ord1.add<dummy_scorer0>(false);
    ASSERT_FALSE(ord0 == ord1);
    ASSERT_TRUE(ord0 != ord1);
  }

  // different number same sorts
  {
    irs::order ord0;
    irs::order ord1;
    ord0.add<dummy_scorer0>(false);
    ord1.add<dummy_scorer0>(false);
    ord1.add<dummy_scorer0>(false);
    ASSERT_FALSE(ord0 == ord1);
    ASSERT_TRUE(ord0 != ord1);
  }

  // different number different sorts
  {
    irs::order ord0;
    irs::order ord1;
    ord0.add<dummy_scorer0>(false);
    ord1.add<dummy_scorer1>(false);
    ord1.add<dummy_scorer1>(false);
    ASSERT_FALSE(ord0 == ord1);
    ASSERT_TRUE(ord0 != ord1);
  }

  // same sorts same types
  {
    irs::order ord0;
    irs::order ord1;
    ord0.add<dummy_scorer0>(false);
    ord0.add<dummy_scorer1>(false);
    ord1.add<dummy_scorer0>(false);
    ord1.add<dummy_scorer1>(false);
    ASSERT_TRUE(ord0 == ord1);
    ASSERT_FALSE(ord0 != ord1);
  }
}

TEST(sort_tests, prepare_order) {
  {
    irs::order ord;
    ord.add<aligned_scorer<aligned_value<1, 1>, aligned_value<1, 1>>>(true);
    ord.add<aligned_scorer<aligned_value<1, 1>, aligned_value<1, 1>>>(true);
    ord.add<aligned_scorer<aligned_value<1, 1>, aligned_value<1, 1>>>(true);

    // first - score offset
    // second - stats offset
    const std::vector<std::pair<size_t, size_t>> expected_offsets {
      { 0, 0 }, { 1, 1 }, { 2, 2 }
    };

    auto prepared = ord.prepare();
    ASSERT_FALSE(prepared.empty());
    ASSERT_EQ(3, prepared.score_size());
    ASSERT_EQ(3, prepared.stats_size());

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_offset);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ASSERT_TRUE(bucket.reverse);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());
  }

  {
    irs::order ord;
    ord.add<aligned_scorer<aligned_value<1, 1>, aligned_value<1, 1>>>(true);
    ord.add<aligned_scorer<aligned_value<2, 2>, aligned_value<2, 2>>>(true);

    // first - score offset
    // second - stats offset
    const std::vector<std::pair<size_t, size_t>> expected_offsets {
      { 0, 0 }, { 2, 2 }
    };

    auto prepared = ord.prepare();
    ASSERT_FALSE(prepared.empty());
    ASSERT_EQ(4, prepared.score_size());
    ASSERT_EQ(4, prepared.stats_size());

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_offset);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ASSERT_TRUE(bucket.reverse);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());
  }

  {
    irs::order ord;
    ord.add<aligned_scorer<aligned_value<1, 1>, aligned_value<1, 1>>>(true);
    ord.add<aligned_scorer<aligned_value<2, 2>, aligned_value<2, 2>>>(true);
    ord.add<aligned_scorer<aligned_value<4, 4>, aligned_value<4, 4>>>(true);

    auto prepared = ord.prepare();
    ASSERT_FALSE(prepared.empty());
    ASSERT_EQ(8, prepared.score_size());
    ASSERT_EQ(8, prepared.stats_size());

    // first - score offset
    // second - stats offset
    const std::vector<std::pair<size_t, size_t>> expected_offsets {
      { 0, 0 }, { 2, 2 }, { 4, 4 }
    };

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_offset);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ASSERT_TRUE(bucket.reverse);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());
  }

  {
    irs::order ord;
    ord.add<aligned_scorer<aligned_value<1, 1>, aligned_value<1, 1>>>(false);
    ord.add<aligned_scorer<aligned_value<5, 4>, aligned_value<5, 4>>>(false);
    ord.add<aligned_scorer<aligned_value<2, 2>, aligned_value<2, 2>>>(false);

    // first - score offset
    // second - stats offset
    const std::vector<std::pair<size_t, size_t>> expected_offsets {
      { 0, 0 }, { 4, 4 }, { 12, 12 }
    };

    auto prepared = ord.prepare();
    ASSERT_FALSE(prepared.empty());
    ASSERT_EQ(16, prepared.score_size());
    ASSERT_EQ(16, prepared.stats_size());

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_offset);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ASSERT_FALSE(bucket.reverse);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());
  }
}
