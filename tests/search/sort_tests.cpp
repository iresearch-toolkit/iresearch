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

#include <algorithm>

#include "tests_shared.hpp"
#include "analysis/token_attributes.hpp"
#include "formats/empty_term_reader.hpp"
#include "search/scorers.hpp"
#include "search/score.hpp"
#include "utils/misc.hpp"

namespace {

struct empty_attribute_provider : irs::attribute_provider {
  virtual irs::attribute* get_mutable(irs::type_info::type_id) {
    return nullptr;
  }
};

empty_attribute_provider EMPTY_ATTRIBUTE_PROVIDER;

template<size_t Size, size_t Align>
struct aligned_value {
  irs::memory::aligned_storage<Size, Align> data;

  // need these operators only to be sort API compliant
  bool operator<(const aligned_value&) const noexcept { return false; }
  const aligned_value& operator+=(const aligned_value&) const noexcept { return *this; }
  const aligned_value& operator+(const aligned_value&) const noexcept { return *this; }
};

template<typename StatsType>
struct aligned_scorer : public irs::sort {
  class prepared final : public irs::PreparedSortBase<StatsType> {
   public:
    explicit prepared(irs::IndexFeatures index_features, bool empty_scorer) noexcept
      : empty_scorer_(empty_scorer), index_features_(index_features) {
    }

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
        const term_collector*) const override {
      // NOOP
    }
    virtual irs::score_function prepare_scorer(
        const irs::sub_reader& /*segment*/,
        const irs::term_reader& /*field*/,
        const irs::byte_type* /*stats*/,
        irs::score_t* score_buf,
        const irs::attribute_provider& /*doc_attrs*/,
        irs::boost_t /*boost*/) const override {
      if (empty_scorer_) {
        return { nullptr, nullptr };
      }

      struct ctx : public irs::score_ctx {
        ctx(const irs::score_t* score_buf)
          : score_buf(score_buf) {
        }

        const irs::score_t* score_buf;
      };

      return {
        std::make_unique<ctx>(score_buf),
        [](irs::score_ctx* ctx) noexcept {
          return reinterpret_cast<struct ctx*>(ctx)->score_buf;
        }
      };
    }

    virtual irs::IndexFeatures features() const override {
      return index_features_;
    }

    irs::IndexFeatures index_features_;
    bool empty_scorer_;
  };

  static ptr make(irs::IndexFeatures index_features = irs::IndexFeatures::NONE,
                  bool empty_scorer = true) {
    return std::make_unique<aligned_scorer>(index_features, empty_scorer);
  }

  explicit aligned_scorer(
      irs::IndexFeatures index_features = irs::IndexFeatures::NONE,
      bool empty_scorer = true)
    : irs::sort(irs::type<aligned_scorer>::get()),
      index_features_(index_features),
      empty_scorer_(empty_scorer) {
  }

  virtual irs::sort::prepared::ptr prepare() const override {
    return irs::memory::make_unique<aligned_scorer<StatsType>::prepared>(
      index_features_, empty_scorer_);
  }

  irs::IndexFeatures index_features_;
  bool empty_scorer_;
};

struct dummy_scorer0: public irs::sort {
  static ptr make() { return std::make_unique<dummy_scorer0>(); }
  dummy_scorer0(): irs::sort(irs::type<dummy_scorer0>::get()) { }
  virtual prepared::ptr prepare() const override { return nullptr; }
};

}

TEST(sort_tests, static_const) {
  static_assert("iresearch::filter_boost" == irs::type<irs::filter_boost>::name());
  static_assert(irs::kNoBoost == irs::filter_boost().value);

  ASSERT_TRUE(irs::Order::kUnordered.buckets.empty());
  ASSERT_EQ(0, irs::Order::kUnordered.score_size);
  ASSERT_EQ(0, irs::Order::kUnordered.stats_size);
  ASSERT_EQ(irs::IndexFeatures::NONE, irs::Order::kUnordered.features);
}

TEST(sort_tests, score_traits) {
  const irs::score_t values[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
  const irs::score_t* ptrs[std::size(values)];
  std::iota(std::begin(ptrs), std::end(ptrs), values);

  for (size_t i = 0; i < std::size(values); ++i) {
    float_t max_dst = 0;
    float_t aggregated_dst = 0;

    irs::Aggregator<irs::SumMerger, 1>{}(&aggregated_dst, ptrs, i);
    irs::Aggregator<irs::MaxMerger, 1>{}(&max_dst, ptrs, i);

    const auto begin = std::begin(values);
    const auto end = begin + i;

    ASSERT_EQ(std::accumulate(begin, end, 0), aggregated_dst);
    const auto it = std::max_element(begin, end);
    ASSERT_EQ(end == it ? 0 : *it, max_dst);
  }
}

TEST(sort_tests, prepare_order) {
  {
    std::array<irs::sort::ptr, 2> ord{
      std::make_unique<dummy_scorer0>(),
      std::make_unique<aligned_scorer<aligned_value<1, 4>>>() };

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 1> expected_offsets {
      std::pair{ size_t{0}, size_t{0} }, // score: 0-0
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::NONE, prepared.features);
    ASSERT_FALSE(prepared.buckets.empty());
    ASSERT_EQ(1, prepared.buckets.size());
    ASSERT_EQ(4, prepared.score_size);
    ASSERT_EQ(4, prepared.stats_size);

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_index);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size, 0);
    irs::bstring score_buf(prepared.score_size, 0);
    irs::Scorers scorers(
      prepared, irs::sub_reader::empty(),
      irs::empty_term_reader(0), stats_buf.c_str(),
      reinterpret_cast<irs::score_t*>(score_buf.data()),
      EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(0 == scorers.scorers.size());

    irs::score score;
    ASSERT_TRUE(score.is_default());
    irs::reset(score, std::move(scorers));
    ASSERT_TRUE(score.is_default());
  }

  {
    std::array<irs::sort::ptr, 4> ord {
      std::make_unique<dummy_scorer0>(),
      std::make_unique<aligned_scorer<aligned_value<2, 2>>>(),
      std::make_unique<aligned_scorer<aligned_value<2, 2>>>(),
      std::make_unique<aligned_scorer<aligned_value<4, 4>>>() };

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 3> expected_offsets {
      std::pair{ 0, 0 }, // score: 0-1
      std::pair{ 2, 2 }, // score: 2-3
      std::pair{ 4, 4 }, // score: 4-7
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::NONE, prepared.features);
    ASSERT_FALSE(prepared.buckets.empty());
    ASSERT_EQ(3, prepared.buckets.size());
    ASSERT_EQ(8, prepared.score_size);
    ASSERT_EQ(8, prepared.stats_size);

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_index);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size, 0);
    irs::bstring score_buf(prepared.score_size, 0);
    irs::Scorers scorers(
      prepared, irs::sub_reader::empty(),
      irs::empty_term_reader(0), stats_buf.c_str(),
      reinterpret_cast<irs::score_t*>(score_buf.data()),
      EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(0 == scorers.scorers.size());

    irs::score score;
    ASSERT_TRUE(score.is_default());
    irs::reset(score, std::move(scorers));
    ASSERT_TRUE(score.is_default());
  }

  {
    std::array<irs::sort::ptr, 4> ord{
        std::make_unique<dummy_scorer0>(),
        std::make_unique<aligned_scorer<aligned_value<2, 2>>>(irs::IndexFeatures::NONE, false), // returns valid scorers
        std::make_unique<aligned_scorer<aligned_value<2, 2>>>(),
        std::make_unique<aligned_scorer<aligned_value<4, 4>>>() };

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 3> expected_offsets {
      std::pair{ 0, 0 }, // score: 0-1
      std::pair{ 2, 2 }, // score: 2-3
      std::pair{ 4, 4 }, // score: 4-7
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::NONE, prepared.features);
    ASSERT_FALSE(prepared.buckets.empty());
    ASSERT_EQ(3, prepared.buckets.size());
    ASSERT_EQ(8, prepared.score_size);
    ASSERT_EQ(8, prepared.stats_size);

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_index);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size, 0);
    irs::bstring score_buf(prepared.score_size, 0);
    irs::Scorers scorers(
      prepared, irs::sub_reader::empty(),
      irs::empty_term_reader(0), stats_buf.c_str(),
      reinterpret_cast<irs::score_t*>(score_buf.data()),
      EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(1 == scorers.scorers.size());
    auto& scorer = scorers.scorers.front();
    ASSERT_NE(nullptr, scorer.func());
    ASSERT_EQ(&prepared.buckets[0], scorer.bucket);

    irs::score score;
    ASSERT_TRUE(score.is_default());
    irs::reset(score, std::move(scorers));
    ASSERT_FALSE(score.is_default());

    // returns pointer to the beginning
    ASSERT_EQ(score_buf.c_str(), reinterpret_cast<const irs::byte_type*>(score.evaluate()));
  }

  {
    std::array<irs::sort::ptr, 4> ord{
        std::make_unique<dummy_scorer0>(),
        std::make_unique<aligned_scorer<aligned_value<2, 2>>>(),
        std::make_unique<aligned_scorer<aligned_value<2, 2>>>(irs::IndexFeatures::FREQ, false),  // returns valid scorer
        std::make_unique<aligned_scorer<aligned_value<4, 4>>>()};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 3> expected_offsets {
      std::pair{ 0, 0 }, // score: 0-1
      std::pair{ 2, 2 }, // score: 2-3
      std::pair{ 4, 4 }, // score: 4-7
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::FREQ, prepared.features);
    ASSERT_FALSE(prepared.buckets.empty());
    ASSERT_EQ(3, prepared.buckets.size());
    ASSERT_EQ(8, prepared.score_size);
    ASSERT_EQ(8, prepared.stats_size);

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_index);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size, 0);
    irs::bstring score_buf(prepared.score_size, 0);
    irs::Scorers scorers(
      prepared, irs::sub_reader::empty(),
      irs::empty_term_reader(0), stats_buf.c_str(),
      reinterpret_cast<irs::score_t*>(score_buf.data()),
      EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(1 == scorers.scorers.size());
    auto& scorer = scorers.scorers.front();
    ASSERT_NE(nullptr, scorer.func());
    ASSERT_EQ(&prepared.buckets[1], scorer.bucket);

    irs::score score;
    ASSERT_TRUE(score.is_default());
    irs::reset(score, std::move(scorers));
    ASSERT_FALSE(score.is_default());

    // returns pointer to the beginning of score_buf
    ASSERT_EQ(score_buf.c_str(), reinterpret_cast<const irs::byte_type*>(score.evaluate()));
  }

  {
    std::array<irs::sort::ptr, 4> ord{
        std::make_unique<dummy_scorer0>(),
        std::make_unique<aligned_scorer<aligned_value<1, 1>>>(),
        std::make_unique<aligned_scorer<aligned_value<1, 1>>>(),
        std::make_unique<aligned_scorer<aligned_value<1, 1>>>()};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 3> expected_offsets {
      std::pair{ 0, 0 }, // score: 0-0
      std::pair{ 1, 1 }, // score: 1-1
      std::pair{ 2, 2 }  // score: 2-2
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::NONE, prepared.features);
    ASSERT_FALSE(prepared.buckets.empty());
    ASSERT_EQ(3, prepared.buckets.size());
    ASSERT_EQ(3, prepared.score_size);
    ASSERT_EQ(3, prepared.stats_size);

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_index);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size, 0);
    irs::bstring score_buf(prepared.score_size, 0);
    irs::Scorers scorers(
      prepared, irs::sub_reader::empty(),
      irs::empty_term_reader(0), stats_buf.c_str(),
      reinterpret_cast<irs::score_t*>(score_buf.data()),
      EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(0 == scorers.scorers.size());

    irs::score score;
    ASSERT_TRUE(score.is_default());
    irs::reset(score, std::move(scorers));
    ASSERT_TRUE(score.is_default());
  }

  {
    std::array<irs::sort::ptr, 3> ord{
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(irs::IndexFeatures::NONE, false),
      std::make_unique<aligned_scorer<aligned_value<2, 2>>>(irs::IndexFeatures::NONE, false),
      std::make_unique<dummy_scorer0>() };

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 2> expected_offsets {
      std::pair{ 0, 0 }, // score: 0-0, padding: 1-1
      std::pair{ 2, 2 }  // score: 2-3
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_FALSE(prepared.buckets.empty());
    ASSERT_EQ(irs::IndexFeatures::NONE, prepared.features);
    ASSERT_EQ(2, prepared.buckets.size());
    ASSERT_EQ(4, prepared.score_size);
    ASSERT_EQ(4, prepared.stats_size);

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_index);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size, 0);
    irs::bstring score_buf(prepared.score_size, 0);
    irs::Scorers scorers(
      prepared, irs::sub_reader::empty(),
      irs::empty_term_reader(0), stats_buf.c_str(),
      reinterpret_cast<irs::score_t*>(score_buf.data()),
      EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(2 == scorers.scorers.size());
    auto& front = scorers.scorers.front();
    ASSERT_NE(nullptr, front.func());
    ASSERT_EQ(&prepared.buckets[0], front.bucket);
    auto& back = scorers.scorers.back();
    ASSERT_NE(nullptr, back.func());
    ASSERT_EQ(&prepared.buckets[1], back.bucket);

    irs::score score;
    ASSERT_TRUE(score.is_default());
    irs::reset(score, std::move(scorers));
    ASSERT_FALSE(score.is_default());

    // returns pointer to the beginning of score_buf
    ASSERT_EQ(score_buf.c_str(), reinterpret_cast<const irs::byte_type*>(score.evaluate()));
  }

  {
    std::array<irs::sort::ptr, 4> ord{
        std::make_unique<aligned_scorer<aligned_value<1, 1>>>(irs::IndexFeatures::NONE, false),
        std::make_unique<dummy_scorer0>(),
        std::make_unique<aligned_scorer<aligned_value<2, 2>>>(irs::IndexFeatures::NONE, false),
        std::make_unique<aligned_scorer<aligned_value<4, 4>>>(irs::IndexFeatures::NONE, false) };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::NONE, prepared.features);
    ASSERT_FALSE(prepared.buckets.empty());
    ASSERT_EQ(3, prepared.buckets.size());
    ASSERT_EQ(8, prepared.score_size);
    ASSERT_EQ(8, prepared.stats_size);

    // first - score offset
    // second - stats offset
    const std::vector<std::pair<size_t, size_t>> expected_offsets {
      { 0, 0 }, // score: 0-0, padding: 1-1
      { 2, 2 }, // score: 2-3
      { 4, 4 }  // score: 4-7
    };

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_index);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size, 0);
    irs::bstring score_buf(prepared.score_size, 0);
    irs::Scorers scorers(
      prepared, irs::sub_reader::empty(),
      irs::empty_term_reader(0), stats_buf.c_str(),
      reinterpret_cast<irs::score_t*>(score_buf.data()),
      EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(3 == scorers.scorers.size());
    {
      auto& scorer = scorers.scorers[0];
      ASSERT_NE(nullptr, scorer.func());
      ASSERT_EQ(&prepared.buckets[0], scorer.bucket);
    }
    {
      auto& scorer = scorers.scorers[1];
      ASSERT_NE(nullptr, scorer.func());
      ASSERT_EQ(&prepared.buckets[1], scorer.bucket);
    }
    {
      auto& scorer = scorers.scorers[2];
      ASSERT_NE(nullptr, scorer.func());
      ASSERT_EQ(&prepared.buckets[2], scorer.bucket);
    }

    irs::score score;
    ASSERT_TRUE(score.is_default());
    irs::reset(score, std::move(scorers));
    ASSERT_FALSE(score.is_default());

    // returns pointer to the beginning of score_buf
    ASSERT_EQ(score_buf.c_str(), reinterpret_cast<const irs::byte_type*>(score.evaluate()));
  }

  {
    std::array<irs::sort::ptr, 4> ord{
        std::make_unique<aligned_scorer<aligned_value<1, 1>>>(irs::IndexFeatures::NONE, false),
        std::make_unique<aligned_scorer<aligned_value<5, 4>>>(),
        std::make_unique<dummy_scorer0>(),
        std::make_unique<aligned_scorer<aligned_value<2, 2>>>(irs::IndexFeatures::FREQ, false) };

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 3> expected_offsets {
      std::pair{ 0, 0 },  // score: 0-0, padding: 1-3
      std::pair{ 4, 4 },  // score: 4-8, padding: 9-11
      std::pair{ 12, 12 } // score: 12-14
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::FREQ, prepared.features);
    ASSERT_FALSE(prepared.buckets.empty());
    ASSERT_EQ(3, prepared.buckets.size());
    ASSERT_EQ(16, prepared.score_size);
    ASSERT_EQ(16, prepared.stats_size);

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_index);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size, 0);
    irs::bstring score_buf(prepared.score_size, 0);
    irs::Scorers scorers(
      prepared, irs::sub_reader::empty(),
      irs::empty_term_reader(0), stats_buf.c_str(),
      reinterpret_cast<irs::score_t*>(score_buf.data()),
      EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(2 == scorers.scorers.size());
    {
      auto& scorer = scorers.scorers[0];
      ASSERT_NE(nullptr, scorer.func());
      ASSERT_EQ(&prepared.buckets[0], scorer.bucket);
    }
    {
      auto& scorer = scorers.scorers[1];
      ASSERT_NE(nullptr, scorer.func());
      ASSERT_EQ(&prepared.buckets[2], scorer.bucket);
    }

    irs::score score;
    ASSERT_TRUE(score.is_default());
    irs::reset(score, std::move(scorers));
    ASSERT_FALSE(score.is_default());

    // returns pointer to the beginning of score_buf
    ASSERT_EQ(score_buf.c_str(), reinterpret_cast<const irs::byte_type*>(score.evaluate()));
  }

  {
    std::array<irs::sort::ptr, 11> ord {
        std::make_unique<dummy_scorer0>(),
        std::make_unique<aligned_scorer<aligned_value<3, 1>>>(irs::IndexFeatures::NONE),
        std::make_unique<dummy_scorer0>(),
        std::make_unique<aligned_scorer< aligned_value<27, 8>>>(),
        std::make_unique<dummy_scorer0>(),
        std::make_unique<aligned_scorer<aligned_value<7, 4>>>(irs::IndexFeatures::FREQ),
        std::make_unique<dummy_scorer0>(),
        std::make_unique<aligned_scorer<aligned_value<1, 1>>>(irs::IndexFeatures::FREQ),
        std::make_unique<dummy_scorer0>(),
        std::make_unique<aligned_scorer<aligned_value<1, 1>>>(irs::IndexFeatures::FREQ),
        std::make_unique<dummy_scorer0>() };

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> expected_offsets {
      std::pair{ 0, 0 },   // score: 0-2, padding: 3-7
      std::pair{ 8, 8 },   // score: 8-34, padding: 35-39
      std::pair{ 40, 40 }, // score: 40-46, padding: 47-47
      std::pair{ 48, 48 }, // score: 48-48
      std::pair{ 49, 49 }  // score: 49-49
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::FREQ, prepared.features);
    ASSERT_FALSE(prepared.buckets.empty());
    ASSERT_EQ(5, prepared.buckets.size());
    ASSERT_EQ(56, prepared.score_size);
    ASSERT_EQ(56, prepared.stats_size);

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_index);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size, 0);
    irs::bstring score_buf(prepared.score_size, 0);
    irs::Scorers scorers(
      prepared, irs::sub_reader::empty(),
      irs::empty_term_reader(0), stats_buf.c_str(),
      reinterpret_cast<irs::score_t*>(score_buf.data()),
      EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(0 == scorers.scorers.size());

    irs::score score;
    ASSERT_TRUE(score.is_default());
    irs::reset(score, std::move(scorers));
    ASSERT_TRUE(score.is_default());
  }

  {
    std::array<irs::sort::ptr, 5> ord {
        std::make_unique<aligned_scorer<aligned_value<27, 8>>>(),
        std::make_unique<aligned_scorer<aligned_value<3, 1>>>(irs::IndexFeatures::NONE),
        std::make_unique<aligned_scorer<aligned_value<7, 4>>>(irs::IndexFeatures::FREQ),
        std::make_unique<aligned_scorer<aligned_value<1, 1>>>(irs::IndexFeatures::FREQ),
        std::make_unique<aligned_scorer<aligned_value<1, 1>>>(irs::IndexFeatures::FREQ) };

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> expected_offsets {
      std::pair{ 0, 0 },   // score: 0-26, padding: 27-31
      std::pair{ 32, 32 }, // score: 32-34, padding: 34-35
      std::pair{ 36, 36 }, // score: 36-42, padding: 43-43
      std::pair{ 44, 44 }, // score: 44-44
      std::pair{ 45, 45 }  // score: 45-45
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::FREQ, prepared.features);
    ASSERT_FALSE(prepared.buckets.empty());
    ASSERT_EQ(5, prepared.buckets.size());
    ASSERT_EQ(48, prepared.score_size);
    ASSERT_EQ(48, prepared.stats_size);

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_index);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size, 0);
    irs::bstring score_buf(prepared.score_size, 0);
    irs::Scorers scorers(
      prepared, irs::sub_reader::empty(),
      irs::empty_term_reader(0), stats_buf.c_str(),
      reinterpret_cast<irs::score_t*>(score_buf.data()),
      EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(0 == scorers.scorers.size());

    irs::score score;
    ASSERT_TRUE(score.is_default());
    irs::reset(score, std::move(scorers));
    ASSERT_TRUE(score.is_default());
  }

  {
    std::array<irs::sort::ptr, 5> ord {
        std::make_unique<aligned_scorer<aligned_value<27, 8>>>(),
        std::make_unique<aligned_scorer<aligned_value<7, 4>>>(irs::IndexFeatures::FREQ),
        std::make_unique<aligned_scorer<aligned_value<3, 1>>>(irs::IndexFeatures::POS),
        std::make_unique<aligned_scorer<aligned_value<1, 1>>>(irs::IndexFeatures::FREQ),
        std::make_unique<aligned_scorer<aligned_value<1, 1>>>(irs::IndexFeatures::FREQ) };

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> expected_offsets {
      std::pair{ 0, 0 },   // score: 0-26, padding: 27-31
      std::pair{ 32, 32 }, // score: 32-38, padding: 39-39
      std::pair{ 40, 40 }, // score: 40-42
      std::pair{ 43, 43 }, // score: 43-43
      std::pair{ 44, 44 }  // score: 44-44
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::FREQ | irs::IndexFeatures::POS, prepared.features);
    ASSERT_FALSE(prepared.buckets.empty());
    ASSERT_EQ(5, prepared.buckets.size());
    ASSERT_EQ(48, prepared.score_size);
    ASSERT_EQ(48, prepared.stats_size);

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_index);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size, 0);
    irs::bstring score_buf(prepared.score_size, 0);
    irs::Scorers scorers(
      prepared, irs::sub_reader::empty(),
      irs::empty_term_reader(0), stats_buf.c_str(),
      reinterpret_cast<irs::score_t*>(score_buf.data()),
      EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(0 == scorers.scorers.size());

    irs::score score;
    ASSERT_TRUE(score.is_default());
    irs::reset(score, std::move(scorers));
    ASSERT_TRUE(score.is_default());
  }

  {
    std::array<irs::sort::ptr, 5> ord {
        std::make_unique<aligned_scorer<aligned_value<27, 8>>>(),
        std::make_unique<aligned_scorer<aligned_value<2, 2>>>(irs::IndexFeatures::NONE),
        std::make_unique<aligned_scorer<aligned_value<4, 4>>>(irs::IndexFeatures::FREQ),
        std::make_unique<aligned_scorer<aligned_value<1, 1>>>(irs::IndexFeatures::FREQ),
        std::make_unique<aligned_scorer<aligned_value<1, 1>>>(irs::IndexFeatures::FREQ)};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> expected_offsets {
      std::pair{ 0,  0 },  // score: 0-26, padding: 27-31
      std::pair{ 32, 32 }, // score: 32-33, padding: 34-35
      std::pair{ 36, 36 }, // score: 36-39
      std::pair{ 40, 40 }, // score: 40-40
      std::pair{ 41, 41 }  // score: 41-41
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::FREQ, prepared.features);
    ASSERT_FALSE(prepared.buckets.empty());
    ASSERT_EQ(5, prepared.buckets.size());
    ASSERT_EQ(48, prepared.score_size);
    ASSERT_EQ(48, prepared.stats_size);

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_index);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size, 0);
    irs::bstring score_buf(prepared.score_size, 0);
    irs::Scorers scorers(
      prepared, irs::sub_reader::empty(),
      irs::empty_term_reader(0), stats_buf.c_str(),
      reinterpret_cast<irs::score_t*>(score_buf.data()),
      EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(0 == scorers.scorers.size());

    irs::score score;
    ASSERT_TRUE(score.is_default());
    irs::reset(score, std::move(scorers));
    ASSERT_TRUE(score.is_default());
  }

  {
    std::array<irs::sort::ptr, 5> ord{
        std::make_unique<aligned_scorer<aligned_value<27, 8>>>(),
        std::make_unique<aligned_scorer<aligned_value<4, 4>>>(irs::IndexFeatures::FREQ),
        std::make_unique<aligned_scorer<aligned_value<2, 2>>>(irs::IndexFeatures::NONE),
        std::make_unique<aligned_scorer<aligned_value<1, 1>>>(irs::IndexFeatures::FREQ),
        std::make_unique<aligned_scorer<aligned_value<1, 1>>>(irs::IndexFeatures::FREQ)};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> expected_offsets {
      std::pair{ 0,  0  }, // score: 0-26, padding: 27-31
      std::pair{ 32, 32 }, // score: 32-35
      std::pair{ 36, 36 }, // score: 36-37
      std::pair{ 38, 38 }, // score: 38-38
      std::pair{ 39, 39 }  // score: 39-39
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::FREQ, prepared.features);
    ASSERT_FALSE(prepared.buckets.empty());
    ASSERT_EQ(5, prepared.buckets.size());
    ASSERT_EQ(40, prepared.score_size);
    ASSERT_EQ(40, prepared.stats_size);

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_index);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size, 0);
    irs::bstring score_buf(prepared.score_size, 0);
    irs::Scorers scorers(
      prepared, irs::sub_reader::empty(),
      irs::empty_term_reader(0), stats_buf.c_str(),
      reinterpret_cast<irs::score_t*>(score_buf.data()),
      EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(0 == scorers.scorers.size());

    irs::score score;
    ASSERT_TRUE(score.is_default());
    irs::reset(score, std::move(scorers));
    ASSERT_TRUE(score.is_default());
  }

  {
    std::array<irs::sort::ptr, 5> ord{
        std::make_unique<aligned_scorer<aligned_value<27, 8>>>(),
        std::make_unique<aligned_scorer<aligned_value<4, 4>>>(irs::IndexFeatures::FREQ),
        std::make_unique<aligned_scorer<aligned_value<2, 2>>>(irs::IndexFeatures::NONE),
        std::make_unique<aligned_scorer<aligned_value<1, 1>>>(irs::IndexFeatures::FREQ),
        std::make_unique<aligned_scorer<aligned_value<1, 1>>>(irs::IndexFeatures::FREQ) };

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> expected_offsets {
      std::pair{ 0,  0  }, // score: 0-26, padding: 27-31
      std::pair{ 32, 32 }, // score: 32-35
      std::pair{ 36, 36 }, // score: 36-37
      std::pair{ 38, 38 }, // score: 38-38
      std::pair{ 39, 39 }  // score: 39-39
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::FREQ, prepared.features);
    ASSERT_FALSE(prepared.buckets.empty());
    ASSERT_EQ(5, prepared.buckets.size());
    ASSERT_EQ(40, prepared.score_size);
    ASSERT_EQ(40, prepared.stats_size);

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->first, bucket.score_index);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size, 0);
    irs::bstring score_buf(prepared.score_size, 0);
    irs::Scorers scorers(
      prepared, irs::sub_reader::empty(),
      irs::empty_term_reader(0), stats_buf.c_str(),
      reinterpret_cast<irs::score_t*>(score_buf.data()),
      EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(0 == scorers.scorers.size());

    irs::score score;
    ASSERT_TRUE(score.is_default());
    irs::reset(score, std::move(scorers));
    ASSERT_TRUE(score.is_default());
  }
}

TEST(score_function_test, construct) {
  struct ctx : irs::score_ctx {
    irs::score_t buf[1]{};
  };

  {
    irs::score_function func;
    ASSERT_TRUE(func);
    ASSERT_NE(nullptr, func.func());
    ASSERT_EQ(nullptr, func.ctx());
    ASSERT_EQ(nullptr, func());
  }

  {
    struct ctx ctx;

    auto score_func = [](irs::score_ctx* ctx) -> const irs::score_t* {
      return static_cast<struct ctx*>(ctx)->buf;
    };

    irs::score_function func(&ctx, score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(static_cast<irs::score_f>(score_func), func.func());
    ASSERT_EQ(&ctx, func.ctx());
    ASSERT_EQ(ctx.buf, func());
  }

  {
    struct ctx ctx;

    auto score_func = [](irs::score_ctx* ctx) -> const irs::score_t* {
      return static_cast<struct ctx*>(ctx)->buf;
    };

    irs::score_function func(
      irs::memory::to_managed<irs::score_ctx, false>(&ctx),
      score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(static_cast<irs::score_f>(score_func), func.func());
    ASSERT_EQ(&ctx, func.ctx());
    ASSERT_EQ(ctx.buf, func());
  }

  {
    auto score_func = [](irs::score_ctx* ctx) -> const irs::score_t* {
      auto* buf = static_cast<struct ctx*>(ctx)->buf;
      buf[0] = 42;
      return buf;
    };

    irs::score_function func(std::make_unique<struct ctx>(), score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(static_cast<irs::score_f>(score_func), func.func());
    ASSERT_NE(nullptr, func.ctx());
    auto* value = func();
    ASSERT_NE(nullptr, value);
    ASSERT_EQ(42, *value);
  }

  {
    auto score_func = [](irs::score_ctx* ctx) -> const irs::score_t* {
      auto* buf = static_cast<struct ctx*>(ctx)->buf;
      buf[0] = 42;
      return buf;
    };

    irs::score_function func(
      irs::memory::to_managed<irs::score_ctx>(std::make_unique<struct ctx>()),
      score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(static_cast<irs::score_f>(score_func), func.func());
    ASSERT_NE(nullptr, func.ctx());
    auto* value = func();
    ASSERT_NE(nullptr, value);
    ASSERT_EQ(42, *value);
  }
}

TEST(score_function_test, reset) {
  struct ctx : irs::score_ctx {
    irs::score_t buf[1]{};
  };

  irs::score_function func;

  ASSERT_TRUE(func);
  ASSERT_NE(nullptr, func.func());
  ASSERT_EQ(nullptr, func.ctx());
  ASSERT_EQ(nullptr, func());

  {
    struct ctx ctx;

    auto score_func = [](irs::score_ctx* ctx) -> const irs::score_t* {
      return static_cast<struct ctx*>(ctx)->buf;
    };

    func.reset(&ctx, score_func);

    ASSERT_TRUE(func);
    ASSERT_EQ(static_cast<irs::score_f>(score_func), func.func());
    ASSERT_EQ(&ctx, func.ctx());
    ASSERT_EQ(ctx.buf, func());

    func.reset(
      irs::memory::to_managed<irs::score_ctx, false>(&ctx),
      score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(static_cast<irs::score_f>(score_func), func.func());
    ASSERT_EQ(&ctx, func.ctx());
    ASSERT_EQ(ctx.buf, func());
  }

  {
    auto score_func = [](irs::score_ctx* ctx) -> const irs::score_t* {
      auto* buf = static_cast<struct ctx*>(ctx)->buf;
      buf[0] = 42;
      return buf;
    };

    func.reset(std::make_unique<struct ctx>(), score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(static_cast<irs::score_f>(score_func), func.func());
    ASSERT_NE(nullptr, func.ctx());
    auto* value = func();
    ASSERT_NE(nullptr, value);
    ASSERT_EQ(42, *value);
  }

  {
    auto score_func = [](irs::score_ctx* ctx) -> const irs::score_t* {
      auto* buf = static_cast<struct ctx*>(ctx)->buf;
      buf[0] = 43;
      return buf;
    };

    func.reset(
      irs::memory::to_managed<irs::score_ctx>(std::make_unique<struct ctx>()),
      score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(static_cast<irs::score_f>(score_func), func.func());
    ASSERT_NE(nullptr, func.ctx());
    auto* value = func();
    ASSERT_NE(nullptr, value);
    ASSERT_EQ(43, *value);
  }

  {
    struct ctx ctx;
    func.reset(&ctx, nullptr);
    ASSERT_FALSE(func);
  }
}

TEST(score_function_test, move) {
  struct ctx : irs::score_ctx {
    irs::score_t buf[1]{};
  };

  // move construction
  {
    struct ctx ctx;

    auto score_func = [](irs::score_ctx* ctx) -> const irs::score_t* {
      return static_cast<struct ctx*>(ctx)->buf;
    };

    irs::score_function func(&ctx, score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(&ctx, func.ctx());
    ASSERT_EQ(static_cast<irs::score_f>(score_func), func.func());
    ASSERT_EQ(ctx.buf, func());
    irs::score_function moved(std::move(func));
    ASSERT_TRUE(moved);
    ASSERT_EQ(&ctx, moved.ctx());
    ASSERT_EQ(static_cast<irs::score_f>(score_func), moved.func());
    ASSERT_EQ(ctx.buf, moved());
    ASSERT_TRUE(func);
    ASSERT_EQ(nullptr, func.ctx());
    ASSERT_NE(static_cast<irs::score_f>(score_func), func.func());
    ASSERT_EQ(nullptr, func());
  }

  // move assignment
  {
    struct ctx ctx;

    auto score_func = [](irs::score_ctx* ctx) -> const irs::score_t* {
      return static_cast<struct ctx*>(ctx)->buf;
    };

    irs::score_function moved;
    ASSERT_TRUE(moved);
    ASSERT_EQ(nullptr, moved.ctx());
    ASSERT_NE(static_cast<irs::score_f>(score_func), moved.func());
    ASSERT_EQ(nullptr, moved());
    irs::score_function func(&ctx, score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(&ctx, func.ctx());
    ASSERT_EQ(static_cast<irs::score_f>(score_func), func.func());
    ASSERT_EQ(ctx.buf, func());
    moved = std::move(func);
    ASSERT_TRUE(moved);
    ASSERT_EQ(&ctx, moved.ctx());
    ASSERT_EQ(static_cast<irs::score_f>(score_func), moved.func());
    ASSERT_EQ(ctx.buf, moved());
    ASSERT_TRUE(func);
    ASSERT_EQ(nullptr, func.ctx());
    ASSERT_NE(static_cast<irs::score_f>(score_func), func.func());
    ASSERT_EQ(nullptr, func());
  }
}

TEST(score_function_test, equality) {
  struct ctx : irs::score_ctx {
    irs::score_t buf[1]{};
  } ctx0, ctx1;

  auto score_func0 = [](irs::score_ctx* ctx) -> const irs::score_t* {
    return static_cast<struct ctx*>(ctx)->buf;
  };

  auto score_func1 = [](irs::score_ctx* ctx) -> const irs::score_t* {
    return static_cast<struct ctx*>(ctx)->buf;
  };

  irs::score_function func0;
  irs::score_function func1(&ctx0, score_func0);
  irs::score_function func2(&ctx1, score_func1);
  irs::score_function func3(&ctx0, score_func1);
  irs::score_function func4(&ctx1, score_func0);

  ASSERT_EQ(func0, irs::score_function());
  ASSERT_NE(func0, func1);
  ASSERT_NE(func2, func3);
  ASSERT_NE(func2, func4);
  ASSERT_EQ(func1, irs::score_function(&ctx0, score_func0));
  ASSERT_EQ(func2, irs::score_function(&ctx1, score_func1));
}
