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

#include "analysis/token_attributes.hpp"
#include "formats/empty_term_reader.hpp"
#include "search/score.hpp"
#include "search/scorers.hpp"
#include "tests_shared.hpp"
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
  const aligned_value& operator+=(const aligned_value&) const noexcept {
    return *this;
  }
  const aligned_value& operator+(const aligned_value&) const noexcept {
    return *this;
  }
};

template<typename StatsType>
struct aligned_scorer : public irs::Sort {
  class prepared final : public irs::PreparedSortBase<StatsType> {
   public:
    explicit prepared(irs::IndexFeatures index_features,
                      bool empty_scorer) noexcept
      : empty_scorer_(empty_scorer), index_features_(index_features) {}

    irs::FieldCollector::ptr prepare_field_collector() const final {
      return nullptr;
    }
    irs::TermCollector::ptr prepare_term_collector() const final {
      return nullptr;
    }
    void collect(irs::byte_type*, const irs::IndexReader&,
                 const irs::FieldCollector*,
                 const irs::TermCollector*) const final {
      // NOOP
    }
    irs::ScoreFunction prepare_scorer(
      const irs::SubReader& /*segment*/, const irs::term_reader& /*field*/,
      const irs::byte_type* /*stats*/,
      const irs::attribute_provider& /*doc_attrs*/,
      irs::score_t /*boost*/) const final {
      if (empty_scorer_) {
        return irs::ScoreFunction::Invalid();
      }
      return irs::ScoreFunction::Empty();
    }

    irs::IndexFeatures features() const final { return index_features_; }

    irs::IndexFeatures index_features_;
    bool empty_scorer_;
  };

  static ptr make(irs::IndexFeatures index_features = irs::IndexFeatures::NONE,
                  bool empty_scorer = true) {
    return std::make_unique<aligned_scorer>(index_features, empty_scorer);
  }

  explicit aligned_scorer(
    irs::IndexFeatures index_features_ = irs::IndexFeatures::NONE,
    bool empty_scorer = true)
    : irs::Sort(irs::type<aligned_scorer>::get()),
      index_features_(index_features_),
      empty_scorer_(empty_scorer) {}

  irs::PreparedSort::ptr prepare() const final {
    return std::make_unique<aligned_scorer<StatsType>::prepared>(
      index_features_, empty_scorer_);
  }

  irs::IndexFeatures index_features_;
  bool empty_scorer_;
};

struct dummy_scorer0 : public irs::Sort {
  dummy_scorer0() : irs::Sort(irs::type<dummy_scorer0>::get()) {}
  irs::PreparedSort::ptr prepare() const final { return nullptr; }
};

}  // namespace

TEST(sort_tests, static_const) {
  static_assert("irs::filter_boost" == irs::type<irs::filter_boost>::name());
  static_assert(irs::kNoBoost == irs::filter_boost().value);

  ASSERT_TRUE(irs::Order::kUnordered.buckets().empty());
  ASSERT_EQ(0, irs::Order::kUnordered.score_size());
  ASSERT_EQ(0, irs::Order::kUnordered.stats_size());
  ASSERT_EQ(irs::IndexFeatures::NONE, irs::Order::kUnordered.features());
}

TEST(sort_tests, prepare_order) {
  {
    std::array<irs::Sort::ptr, 2> ord{
      std::make_unique<dummy_scorer0>(),
      std::make_unique<aligned_scorer<aligned_value<1, 4>>>()};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 1> expected_offsets{
      std::pair{size_t{0}, size_t{0}},  // score: 0-0
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::NONE, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(1, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(4, prepared.stats_size());

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size(), 0);
    irs::bstring score_buf(prepared.score_size(), 0);
    auto scorers = irs::PrepareScorers(
      prepared.buckets(), irs::SubReader::empty(), irs::empty_term_reader(0),
      stats_buf.c_str(), EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(1 == scorers.size());

    irs::score score;
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    score = irs::CompileScorers(std::move(scorers));
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
  }

  {
    std::array<irs::Sort::ptr, 4> ord{
      std::make_unique<dummy_scorer0>(),
      std::make_unique<aligned_scorer<aligned_value<2, 2>>>(),
      std::make_unique<aligned_scorer<aligned_value<2, 2>>>(),
      std::make_unique<aligned_scorer<aligned_value<4, 4>>>()};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 3> expected_offsets{
      std::pair{0, 0},  // score: 0-1
      std::pair{1, 2},  // score: 2-3
      std::pair{2, 4},  // score: 4-7
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::NONE, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(3, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(8, prepared.stats_size());

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size(), 0);
    irs::bstring score_buf(prepared.score_size(), 0);

    irs::score score;
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    score = irs::CompileScore(prepared.buckets(), irs::SubReader::empty(),
                              irs::empty_term_reader(0), stats_buf.c_str(),
                              EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_NE(score.Func(), &irs::ScoreFunction::DefaultScore);
  }

  {
    std::array<irs::Sort::ptr, 4> ord{
      std::make_unique<dummy_scorer0>(),
      std::make_unique<aligned_scorer<aligned_value<2, 2>>>(
        irs::IndexFeatures::NONE, false),  // returns valid scorers
      std::make_unique<aligned_scorer<aligned_value<2, 2>>>(),
      std::make_unique<aligned_scorer<aligned_value<4, 4>>>()};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 3> expected_offsets{
      std::pair{0, 0},  // score: 0-1
      std::pair{1, 2},  // score: 2-3
      std::pair{2, 4},  // score: 4-7
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::NONE, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(3, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(8, prepared.stats_size());

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size(), 0);
    irs::bstring score_buf(prepared.score_size(), 1);
    auto scorers = irs::PrepareScorers(
      prepared.buckets(), irs::SubReader::empty(), irs::empty_term_reader(0),
      stats_buf.c_str(), EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(3 == scorers.size());
    ASSERT_NE(nullptr, scorers[0]);
    ASSERT_NE(&irs::ScoreFunction::DefaultScore, scorers[0]);
    ASSERT_NE(nullptr, scorers[1]);
    ASSERT_EQ(&irs::ScoreFunction::DefaultScore, scorers[1]);
    ASSERT_NE(nullptr, scorers[2]);
    ASSERT_EQ(&irs::ScoreFunction::DefaultScore, scorers[2]);

    irs::score score;
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    score = irs::CompileScorers(std::move(scorers));
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);

    score(reinterpret_cast<irs::score_t*>(score_buf.data()));

    irs::bstring expected(prepared.score_size(), 0);
    std::fill_n(expected.data(), sizeof(irs::score_t), 1);
    ASSERT_EQ(expected, score_buf);
  }

  {
    std::array<irs::Sort::ptr, 4> ord{
      std::make_unique<dummy_scorer0>(),
      std::make_unique<aligned_scorer<aligned_value<2, 2>>>(),
      std::make_unique<aligned_scorer<aligned_value<2, 2>>>(
        irs::IndexFeatures::FREQ, false),  // returns valid scorer
      std::make_unique<aligned_scorer<aligned_value<4, 4>>>()};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 3> expected_offsets{
      std::pair{0, 0},  // score: 0-1
      std::pair{1, 2},  // score: 2-3
      std::pair{2, 4},  // score: 4-7
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::FREQ, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(3, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(8, prepared.stats_size());

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size(), 0);
    irs::bstring score_buf(prepared.score_size(), 1);
    auto scorers = irs::PrepareScorers(
      prepared.buckets(), irs::SubReader::empty(), irs::empty_term_reader(0),
      stats_buf.c_str(), EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(3 == scorers.size());
    ASSERT_NE(nullptr, scorers[0]);
    ASSERT_EQ(&irs::ScoreFunction::DefaultScore, scorers[0]);
    ASSERT_NE(nullptr, scorers[1]);
    ASSERT_NE(&irs::ScoreFunction::DefaultScore, scorers[1]);
    ASSERT_NE(nullptr, scorers[2]);
    ASSERT_EQ(&irs::ScoreFunction::DefaultScore, scorers[2]);

    irs::score score;
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    score = irs::CompileScorers(std::move(scorers));
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);

    score(reinterpret_cast<irs::score_t*>(score_buf.data()));

    irs::bstring expected(prepared.score_size(), 0);
    std::fill_n(expected.data() + sizeof(irs::score_t), sizeof(irs::score_t),
                1);
    ASSERT_EQ(expected, score_buf);
  }

  {
    std::array<irs::Sort::ptr, 4> ord{
      std::make_unique<dummy_scorer0>(),
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(),
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(),
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>()};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 3> expected_offsets{
      std::pair{0, 0},  // score: 0-0
      std::pair{1, 1},  // score: 1-1
      std::pair{2, 2}   // score: 2-2
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::NONE, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(3, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(3, prepared.stats_size());

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size(), 0);
    irs::bstring score_buf(prepared.score_size(), 1);
    auto scorers = irs::PrepareScorers(
      prepared.buckets(), irs::SubReader::empty(), irs::empty_term_reader(0),
      stats_buf.c_str(), EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(3 == scorers.size());
    ASSERT_NE(nullptr, scorers[0]);
    ASSERT_EQ(&irs::ScoreFunction::DefaultScore, scorers[0]);
    ASSERT_NE(nullptr, scorers[1]);
    ASSERT_EQ(&irs::ScoreFunction::DefaultScore, scorers[1]);
    ASSERT_NE(nullptr, scorers[2]);
    ASSERT_EQ(&irs::ScoreFunction::DefaultScore, scorers[2]);

    irs::score score;
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    score = irs::CompileScorers(std::move(scorers));
    ASSERT_NE(&irs::ScoreFunction::DefaultScore, score.Func());

    score(reinterpret_cast<irs::score_t*>(score_buf.data()));
    irs::bstring expected(prepared.score_size(), 0);
    ASSERT_EQ(expected, score_buf);
  }

  {
    std::array<irs::Sort::ptr, 3> ord{
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(
        irs::IndexFeatures::NONE, false),
      std::make_unique<aligned_scorer<aligned_value<2, 2>>>(
        irs::IndexFeatures::NONE, false),
      std::make_unique<dummy_scorer0>()};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 2> expected_offsets{
      std::pair{0, 0},  // score: 0-0, padding: 1-1
      std::pair{1, 2}   // score: 2-3
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(irs::IndexFeatures::NONE, prepared.features());
    ASSERT_EQ(2, prepared.buckets().size());
    ASSERT_EQ(8, prepared.score_size());
    ASSERT_EQ(4, prepared.stats_size());

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size(), 0);
    irs::bstring score_buf(prepared.score_size(), 0);
    auto scorers = irs::PrepareScorers(
      prepared.buckets(), irs::SubReader::empty(), irs::empty_term_reader(0),
      stats_buf.c_str(), EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(2 == scorers.size());
    auto& front = scorers.front();
    ASSERT_NE(nullptr, front);
    auto& back = scorers.back();
    ASSERT_NE(nullptr, back);

    irs::score score;
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    score = irs::CompileScorers(std::move(scorers));
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);

    score(reinterpret_cast<irs::score_t*>(score_buf.data()));
  }

  {
    std::array<irs::Sort::ptr, 4> ord{
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(
        irs::IndexFeatures::NONE, false),
      std::make_unique<dummy_scorer0>(),
      std::make_unique<aligned_scorer<aligned_value<2, 2>>>(
        irs::IndexFeatures::NONE, false),
      std::make_unique<aligned_scorer<aligned_value<4, 4>>>(
        irs::IndexFeatures::NONE, false)};

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::NONE, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(3, prepared.buckets().size());
    ASSERT_EQ(12, prepared.score_size());
    ASSERT_EQ(8, prepared.stats_size());

    // first - score offset
    // second - stats offset
    const std::vector<std::pair<size_t, size_t>> expected_offsets{
      {0, 0},  // score: 0-0, padding: 1-1
      {1, 2},  // score: 2-3
      {2, 4}   // score: 4-7
    };

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size(), 0);
    irs::bstring score_buf(prepared.score_size(), 0);
    auto scorers = irs::PrepareScorers(
      prepared.buckets(), irs::SubReader::empty(), irs::empty_term_reader(0),
      stats_buf.c_str(), EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(3 == scorers.size());
    ASSERT_NE(nullptr, scorers[0]);
    ASSERT_NE(nullptr, scorers[1]);
    ASSERT_NE(nullptr, scorers[2]);

    irs::score score;
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    score = irs::CompileScorers(std::move(scorers));
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);

    score(reinterpret_cast<irs::score_t*>(score_buf.data()));
  }

  {
    std::array<irs::Sort::ptr, 4> ord{
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(
        irs::IndexFeatures::NONE, false),
      std::make_unique<aligned_scorer<aligned_value<5, 4>>>(),
      std::make_unique<dummy_scorer0>(),
      std::make_unique<aligned_scorer<aligned_value<2, 2>>>(
        irs::IndexFeatures::FREQ, false)};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 3> expected_offsets{
      std::pair{0, 0},  // score: 0-0, padding: 1-3
      std::pair{1, 4},  // score: 4-8, padding: 9-11
      std::pair{2, 12}  // score: 12-14
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::FREQ, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(3, prepared.buckets().size());
    ASSERT_EQ(12, prepared.score_size());
    ASSERT_EQ(16, prepared.stats_size());

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size(), 0);
    irs::bstring score_buf(prepared.score_size(), 0);
    auto scorers = irs::PrepareScorers(
      prepared.buckets(), irs::SubReader::empty(), irs::empty_term_reader(0),
      stats_buf.c_str(), EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(3 == scorers.size());
    ASSERT_NE(nullptr, scorers[0]);
    ASSERT_NE(nullptr, scorers[1]);
    ASSERT_NE(nullptr, scorers[2]);

    irs::score score;
    ASSERT_TRUE(score == &irs::ScoreFunction::DefaultScore);
    score = irs::CompileScorers(std::move(scorers));
    ASSERT_FALSE(score == &irs::ScoreFunction::DefaultScore);

    score(reinterpret_cast<irs::score_t*>(score_buf.data()));
  }

  {
    std::array<irs::Sort::ptr, 11> ord{
      std::make_unique<dummy_scorer0>(),
      std::make_unique<aligned_scorer<aligned_value<3, 1>>>(
        irs::IndexFeatures::NONE),
      std::make_unique<dummy_scorer0>(),
      std::make_unique<aligned_scorer<aligned_value<27, 8>>>(),
      std::make_unique<dummy_scorer0>(),
      std::make_unique<aligned_scorer<aligned_value<7, 4>>>(
        irs::IndexFeatures::FREQ),
      std::make_unique<dummy_scorer0>(),
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(
        irs::IndexFeatures::FREQ),
      std::make_unique<dummy_scorer0>(),
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(
        irs::IndexFeatures::FREQ),
      std::make_unique<dummy_scorer0>()};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> expected_offsets{
      std::pair{0, 0},   // score: 0-2, padding: 3-7
      std::pair{1, 8},   // score: 8-34, padding: 35-39
      std::pair{2, 40},  // score: 40-46, padding: 47-47
      std::pair{3, 48},  // score: 48-48
      std::pair{4, 49}   // score: 49-49
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::FREQ, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(5, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(56, prepared.stats_size());

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size(), 0);
    irs::bstring score_buf(prepared.score_size(), 0);
    auto scorers = irs::PrepareScorers(
      prepared.buckets(), irs::SubReader::empty(), irs::empty_term_reader(0),
      stats_buf.c_str(), EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_EQ(5, scorers.size());

    irs::score score;
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    score = irs::CompileScorers(std::move(scorers));
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
  }

  {
    std::array<irs::Sort::ptr, 5> ord{
      std::make_unique<aligned_scorer<aligned_value<27, 8>>>(),
      std::make_unique<aligned_scorer<aligned_value<3, 1>>>(
        irs::IndexFeatures::NONE),
      std::make_unique<aligned_scorer<aligned_value<7, 4>>>(
        irs::IndexFeatures::FREQ),
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(
        irs::IndexFeatures::FREQ),
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(
        irs::IndexFeatures::FREQ)};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> expected_offsets{
      std::pair{0, 0},   // score: 0-26, padding: 27-31
      std::pair{1, 32},  // score: 32-34, padding: 34-35
      std::pair{2, 36},  // score: 36-42, padding: 43-43
      std::pair{3, 44},  // score: 44-44
      std::pair{4, 45}   // score: 45-45
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::FREQ, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(5, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(48, prepared.stats_size());

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size(), 0);
    irs::bstring score_buf(prepared.score_size(), 0);
    auto scorers = irs::PrepareScorers(
      prepared.buckets(), irs::SubReader::empty(), irs::empty_term_reader(0),
      stats_buf.c_str(), EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(5 == scorers.size());

    irs::score score;
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    score = irs::CompileScorers(std::move(scorers));
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
  }

  {
    std::array<irs::Sort::ptr, 5> ord{
      std::make_unique<aligned_scorer<aligned_value<27, 8>>>(),
      std::make_unique<aligned_scorer<aligned_value<7, 4>>>(
        irs::IndexFeatures::FREQ),
      std::make_unique<aligned_scorer<aligned_value<3, 1>>>(
        irs::IndexFeatures::POS),
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(
        irs::IndexFeatures::FREQ),
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(
        irs::IndexFeatures::FREQ)};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> expected_offsets{
      std::pair{0, 0},   // score: 0-26, padding: 27-31
      std::pair{1, 32},  // score: 32-38, padding: 39-39
      std::pair{2, 40},  // score: 40-42
      std::pair{3, 43},  // score: 43-43
      std::pair{4, 44}   // score: 44-44
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::FREQ | irs::IndexFeatures::POS,
              prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(5, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(48, prepared.stats_size());

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size(), 0);
    irs::bstring score_buf(prepared.score_size(), 0);
    auto scorers = irs::PrepareScorers(
      prepared.buckets(), irs::SubReader::empty(), irs::empty_term_reader(0),
      stats_buf.c_str(), EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(5 == scorers.size());

    irs::score score;
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    score = irs::CompileScorers(std::move(scorers));
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
  }

  {
    std::array<irs::Sort::ptr, 5> ord{
      std::make_unique<aligned_scorer<aligned_value<27, 8>>>(),
      std::make_unique<aligned_scorer<aligned_value<2, 2>>>(
        irs::IndexFeatures::NONE),
      std::make_unique<aligned_scorer<aligned_value<4, 4>>>(
        irs::IndexFeatures::FREQ),
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(
        irs::IndexFeatures::FREQ),
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(
        irs::IndexFeatures::FREQ)};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> expected_offsets{
      std::pair{0, 0},   // score: 0-26, padding: 27-31
      std::pair{1, 32},  // score: 32-33, padding: 34-35
      std::pair{2, 36},  // score: 36-39
      std::pair{3, 40},  // score: 40-40
      std::pair{4, 41}   // score: 41-41
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::FREQ, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(5, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(48, prepared.stats_size());

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size(), 0);
    irs::bstring score_buf(prepared.score_size(), 0);
    auto scorers = irs::PrepareScorers(
      prepared.buckets(), irs::SubReader::empty(), irs::empty_term_reader(0),
      stats_buf.c_str(), EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(5 == scorers.size());

    irs::score score;
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    score = irs::CompileScorers(std::move(scorers));
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
  }

  {
    std::array<irs::Sort::ptr, 5> ord{
      std::make_unique<aligned_scorer<aligned_value<27, 8>>>(),
      std::make_unique<aligned_scorer<aligned_value<4, 4>>>(
        irs::IndexFeatures::FREQ),
      std::make_unique<aligned_scorer<aligned_value<2, 2>>>(
        irs::IndexFeatures::NONE),
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(
        irs::IndexFeatures::FREQ),
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(
        irs::IndexFeatures::FREQ)};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> expected_offsets{
      std::pair{0, 0},   // score: 0-26, padding: 27-31
      std::pair{1, 32},  // score: 32-35
      std::pair{2, 36},  // score: 36-37
      std::pair{3, 38},  // score: 38-38
      std::pair{4, 39}   // score: 39-39
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::FREQ, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(5, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(40, prepared.stats_size());

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size(), 0);
    irs::bstring score_buf(prepared.score_size(), 0);
    auto scorers = irs::PrepareScorers(
      prepared.buckets(), irs::SubReader::empty(), irs::empty_term_reader(0),
      stats_buf.c_str(), EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(5 == scorers.size());

    irs::score score;
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    score = irs::CompileScorers(std::move(scorers));
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
  }

  {
    std::array<irs::Sort::ptr, 5> ord{
      std::make_unique<aligned_scorer<aligned_value<27, 8>>>(),
      std::make_unique<aligned_scorer<aligned_value<4, 4>>>(
        irs::IndexFeatures::FREQ),
      std::make_unique<aligned_scorer<aligned_value<2, 2>>>(
        irs::IndexFeatures::NONE),
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(
        irs::IndexFeatures::FREQ),
      std::make_unique<aligned_scorer<aligned_value<1, 1>>>(
        irs::IndexFeatures::FREQ)};

    // first - score offset
    // second - stats offset
    constexpr std::array<std::pair<size_t, size_t>, 5> expected_offsets{
      std::pair{0, 0},   // score: 0-26, padding: 27-31
      std::pair{1, 32},  // score: 32-35
      std::pair{2, 36},  // score: 36-37
      std::pair{3, 38},  // score: 38-38
      std::pair{4, 39}   // score: 39-39
    };

    auto prepared = irs::Order::Prepare(ord);
    ASSERT_EQ(irs::IndexFeatures::FREQ, prepared.features());
    ASSERT_FALSE(prepared.buckets().empty());
    ASSERT_EQ(5, prepared.buckets().size());
    ASSERT_EQ(prepared.buckets().size() * sizeof(irs::score_t),
              prepared.score_size());
    ASSERT_EQ(40, prepared.stats_size());

    auto expected_offset = expected_offsets.begin();
    for (auto& bucket : prepared.buckets()) {
      ASSERT_NE(nullptr, bucket.bucket);
      ASSERT_EQ(expected_offset->second, bucket.stats_offset);
      ++expected_offset;
    }
    ASSERT_EQ(expected_offset, expected_offsets.end());

    irs::bstring stats_buf(prepared.stats_size(), 0);
    irs::bstring score_buf(prepared.score_size(), 0);
    auto scorers = irs::PrepareScorers(
      prepared.buckets(), irs::SubReader::empty(), irs::empty_term_reader(0),
      stats_buf.c_str(), EMPTY_ATTRIBUTE_PROVIDER, irs::kNoBoost);
    ASSERT_TRUE(5 == scorers.size());

    irs::score score;
    ASSERT_TRUE(score.IsNoop());
    ASSERT_TRUE(score.Func() == &irs::ScoreFunction::DefaultScore);
    score = irs::CompileScorers(std::move(scorers));
    ASSERT_FALSE(score.IsNoop());
    ASSERT_FALSE(score.Func() == &irs::ScoreFunction::DefaultScore);
  }
}

TEST(ScoreFunctionTest, Invalid) {
  auto func = irs::ScoreFunction::Invalid();
  ASSERT_FALSE(func);
  ASSERT_FALSE(func.IsNoop());
}

TEST(ScoreFunctionTest, Noop) {
  irs::score_t value{42.f};

  {
    auto func = irs::ScoreFunction::Default(0);
    ASSERT_TRUE(func);
    ASSERT_TRUE(func.IsNoop());
    func(&value);
    ASSERT_EQ(42.f, value);
  }

  {
    auto func = irs::ScoreFunction::Constant(0.f, 0);
    ASSERT_TRUE(func);
    ASSERT_TRUE(func.IsNoop());
    func(&value);
    ASSERT_EQ(42.f, value);
  }
}

TEST(ScoreFunctionTest, Default) {
  std::array<irs::score_t, 7> values;
  std::fill_n(std::begin(values), values.size(), 42.f);
  auto func = irs::ScoreFunction::Default(values.size());
  ASSERT_TRUE(func);
  ASSERT_FALSE(func.IsNoop());
  func(values.data());
  ASSERT_TRUE(std::all_of(std::begin(values), std::end(values),
                          [](auto v) { return 0.f == v; }));
}

TEST(ScoreFunctionTest, Constant) {
  std::array<irs::score_t, 7> values;
  std::fill_n(std::begin(values), values.size(), 42.f);

  {
    auto func = irs::ScoreFunction::Constant(43.f, values.size());
    ASSERT_TRUE(func);
    ASSERT_FALSE(func.IsNoop());
    func(values.data());
    ASSERT_TRUE(std::all_of(std::begin(values), std::end(values),
                            [](auto v) { return 43.f == v; }));
  }

  {
    auto func = irs::ScoreFunction::Constant(42.f, 1);
    ASSERT_TRUE(func);
    ASSERT_FALSE(func.IsNoop());
    func(values.data());
    ASSERT_EQ(42.f, values.front());
    ASSERT_TRUE(std::all_of(std::begin(values) + 1, std::end(values),
                            [](auto v) { return 43.f == v; }));
  }

  {
    auto func = irs::ScoreFunction::Constant(43.f);
    ASSERT_TRUE(func);
    ASSERT_FALSE(func.IsNoop());
    func(values.data());
    ASSERT_TRUE(std::all_of(std::begin(values), std::end(values),
                            [](auto v) { return 43.f == v; }));
  }
}

TEST(ScoreFunctionTest, construct) {
  struct Ctx final : irs::score_ctx {
    irs::score_t buf[1]{};
    Ctx() = default;
  };

  {
    irs::ScoreFunction func;
    ASSERT_TRUE(func);
    ASSERT_NE(nullptr, func.Func());
    ASSERT_EQ(nullptr, func.Ctx());
    irs::score_t tmp{1};
    func(&tmp);  // noop by default
    ASSERT_EQ(1.f, tmp);
  }

  {
    Ctx ctx;

    auto score_func =
      +[](irs::score_ctx*, irs::score_t* res) noexcept { *res = 42; };

    irs::ScoreFunction func(ctx, score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(score_func, func.Func());
    ASSERT_EQ(&ctx, func.Ctx());
    irs::score_t tmp{1};
    func(&tmp);
    ASSERT_EQ(42, tmp);
  }

  {
    Ctx ctx;

    auto score_func =
      +[](irs::score_ctx*, irs::score_t* res) noexcept { *res = 42; };

    irs::ScoreFunction func(ctx, score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(score_func, func.Func());
    ASSERT_EQ(&ctx, func.Ctx());
    irs::score_t tmp{1};
    func(&tmp);
    ASSERT_EQ(42, tmp);
  }

  {
    auto score_func = +[](irs::score_ctx* ctx, irs::score_t* res) noexcept {
      auto* buf = static_cast<Ctx*>(ctx)->buf;
      buf[0] = 42;
      *res = 42;
    };

    auto func = irs::ScoreFunction::Make<Ctx>(score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(score_func, func.Func());
    ASSERT_NE(nullptr, func.Ctx());
    irs::score_t tmp;
    func(&tmp);
    ASSERT_EQ(42, static_cast<const Ctx*>(func.Ctx())->buf[0]);
    ASSERT_EQ(42, tmp);
  }

  {
    auto score_func = +[](irs::score_ctx* ctx, irs::score_t* res) noexcept {
      auto* buf = static_cast<struct Ctx*>(ctx)->buf;
      buf[0] = 42;
      *res = 42;
    };

    auto func = irs::ScoreFunction::Make<Ctx>(score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(score_func, func.Func());
    ASSERT_NE(nullptr, func.Ctx());
    irs::score_t tmp;
    func(&tmp);
    ASSERT_EQ(42, static_cast<const Ctx*>(func.Ctx())->buf[0]);
    ASSERT_EQ(42, tmp);
  }
}

TEST(ScoreFunctionTest, reset) {
  struct Ctx final : irs::score_ctx {
    irs::score_t buf[1]{};
    Ctx() = default;
  };

  irs::ScoreFunction func;

  ASSERT_TRUE(func);
  ASSERT_NE(nullptr, func.Func());
  ASSERT_EQ(nullptr, func.Ctx());
  {
    irs::score_t tmp{42.f};
    func(&tmp);
    ASSERT_EQ(42.f, tmp);
  }

  {
    Ctx ctx;

    auto score_func =
      +[](irs::score_ctx*, irs::score_t* res) noexcept { *res = 42; };

    func.Reset(ctx, score_func);

    ASSERT_TRUE(func);
    ASSERT_EQ(score_func, func.Func());
    ASSERT_EQ(&ctx, func.Ctx());
    irs::score_t tmp{1};
    func(&tmp);
    ASSERT_EQ(42, tmp);

    func.Reset(ctx, score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(score_func, func.Func());
    ASSERT_EQ(&ctx, func.Ctx());
    tmp = 1;
    func(&tmp);
    ASSERT_EQ(42, tmp);
  }

  {
    auto score_func = +[](irs::score_ctx* ctx, irs::score_t* res) noexcept {
      auto* buf = static_cast<Ctx*>(ctx)->buf;
      buf[0] = 42;
      *res = 42;
    };

    func = irs::ScoreFunction::Make<Ctx>(score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(score_func, func.Func());
    ASSERT_NE(nullptr, func.Ctx());
    irs::score_t tmp;
    func(&tmp);
    ASSERT_EQ(42, static_cast<const Ctx*>(func.Ctx())->buf[0]);
    ASSERT_EQ(42, tmp);
  }

  {
    auto score_func = +[](irs::score_ctx* ctx, irs::score_t* res) noexcept {
      auto* buf = static_cast<Ctx*>(ctx)->buf;
      buf[0] = 43;
      *res = 43;
    };

    func = irs::ScoreFunction::Make<Ctx>(score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(score_func, func.Func());
    ASSERT_NE(nullptr, func.Ctx());
    irs::score_t tmp;
    func(&tmp);
    ASSERT_EQ(43, static_cast<const Ctx*>(func.Ctx())->buf[0]);
    ASSERT_EQ(43, tmp);
  }

  {
    Ctx ctx;
    func.Reset(ctx, nullptr);
    ASSERT_FALSE(func);
  }
}

TEST(ScoreFunctionTest, move) {
  struct Ctx final : irs::score_ctx {
    irs::score_t buf[1]{};
  };

  // move construction
  {
    Ctx ctx;

    auto score_func =
      +[](irs::score_ctx*, irs::score_t* res) noexcept { *res = 42; };

    float_t tmp{1};
    irs::ScoreFunction func(ctx, score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(&ctx, func.Ctx());
    ASSERT_EQ(score_func, func.Func());
    func(&tmp);
    ASSERT_EQ(42, tmp);
    irs::ScoreFunction moved(std::move(func));
    ASSERT_TRUE(moved);
    ASSERT_EQ(&ctx, moved.Ctx());
    ASSERT_EQ(score_func, moved.Func());
    tmp = 1;
    moved(&tmp);
    ASSERT_EQ(42, tmp);
    ASSERT_TRUE(func);
    ASSERT_EQ(nullptr, func.Ctx());
    ASSERT_NE(score_func, func.Func());
    tmp = 1;
    func(&tmp);
    ASSERT_EQ(1, tmp);
  }

  // move assignment
  {
    Ctx ctx;

    auto score_func =
      +[](irs::score_ctx*, irs::score_t* res) noexcept { *res = 42; };
    float_t tmp{1};

    irs::ScoreFunction moved;
    ASSERT_TRUE(moved);
    ASSERT_EQ(nullptr, moved.Ctx());
    ASSERT_NE(score_func, moved.Func());
    moved(&tmp);
    ASSERT_EQ(1, tmp);
    irs::ScoreFunction func(ctx, score_func);
    ASSERT_TRUE(func);
    ASSERT_EQ(&ctx, func.Ctx());
    ASSERT_EQ(score_func, func.Func());
    func(&tmp);
    ASSERT_EQ(42, tmp);
    moved = std::move(func);
    ASSERT_TRUE(moved);
    ASSERT_EQ(&ctx, moved.Ctx());
    ASSERT_EQ(score_func, moved.Func());
    tmp = 1;
    moved(&tmp);
    ASSERT_EQ(42, tmp);
    ASSERT_TRUE(func);
    ASSERT_EQ(nullptr, func.Ctx());
    ASSERT_NE(score_func, func.Func());
    tmp = 1;
    func(&tmp);
    ASSERT_EQ(1, tmp);
  }
}

TEST(ScoreFunctionTest, equality) {
  struct score_ctx final : irs::score_ctx {
    irs::score_t buf[1]{};
    irs::score_t* ptr{};
  } ctx0, ctx1;

  auto score_func0 = [](irs::score_ctx*, irs::score_t*) noexcept {};
  auto score_func1 = [](irs::score_ctx*, irs::score_t*) noexcept {};

  irs::ScoreFunction func0;
  irs::ScoreFunction func1(ctx0, score_func0);
  irs::ScoreFunction func2(ctx1, score_func1);
  irs::ScoreFunction func3(ctx0, score_func1);
  irs::ScoreFunction func4(ctx1, score_func0);

  ASSERT_EQ(func0, irs::ScoreFunction());
  ASSERT_NE(func0, func1);
  ASSERT_NE(func2, func3);
  ASSERT_NE(func2, func4);
  ASSERT_EQ(func1, irs::ScoreFunction(ctx0, score_func0));
  ASSERT_EQ(func2, irs::ScoreFunction(ctx1, score_func1));
}
