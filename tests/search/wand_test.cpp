////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023 ArangoDB GmbH, Cologne, Germany
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

#include "index/index_tests.hpp"
#include "index/norm.hpp"
#include "search/bm25.hpp"
#include "search/filter.hpp"
#include "search/score.hpp"
#include "search/term_filter.hpp"
#include "search/tfidf.hpp"
#include "utils/index_utils.hpp"

namespace {

class Scorers {
 public:
  const irs::Scorer& PushBack(irs::Scorer::ptr scorer) {
    EXPECT_NE(nullptr, scorer);
    auto& bucket = scorers_.emplace_back();
    bucket = scorer.release();
    return *bucket;
  }

  const irs::Scorer& operator[](size_t i) const noexcept {
    EXPECT_LT(i, scorers_.size());
    return *scorers_[i];
  }

  irs::ScorersView GetView() const noexcept { return scorers_; }

  ~Scorers() {
    for (auto* scorer : scorers_) {
      delete scorer;
    }
  }

 private:
  std::vector<irs::Scorer*> scorers_;
};

struct Doc {
  Doc(size_t segment, irs::doc_id_t doc) : segment{segment}, doc{doc} {}

  bool operator==(const Doc&) const = default;

  size_t segment;
  irs::doc_id_t doc;
};

class WandTestCase : public tests::index_test_base {
 public:
  static irs::IndexWriterOptions GetWriterOptions(irs::ScorersView scorers,
                                                  bool write_norms);

  std::vector<Doc> Collect(const irs::DirectoryReader& index,
                           const irs::filter& filter, const irs::Scorer& scorer,
                           irs::byte_type wand_idx, bool can_use_wand,
                           size_t limit);

  void AssertResults(const irs::DirectoryReader& index,
                     const irs::filter& filter, const irs::Scorer& scorer,
                     irs::byte_type wand_idx, bool can_use_wand, size_t limit);

  void GenerateData(irs::ScorersView scorers, bool write_norms,
                    bool append_data = false);
  void ConsolidateAll(irs::ScorersView scorers, bool write_norms);
  void AssertTermFilter(irs::ScorersView scorers, const irs::Scorer& scorer,
                        irs::byte_type wand_index);
  void AssertTermFilter(irs::ScorersView scorers);
};

std::vector<Doc> WandTestCase::Collect(const irs::DirectoryReader& index,
                                       const irs::filter& filter,
                                       const irs::Scorer& scorer,
                                       irs::byte_type wand_idx,
                                       bool can_use_wand, size_t limit) {
  struct ScoredDoc : Doc {
    ScoredDoc(size_t segment, irs::doc_id_t doc, float score)
      : Doc{segment, doc}, score{score} {}

    float score;
  };

  auto less = [](const ScoredDoc& lhs, const ScoredDoc& rhs) noexcept {
    return lhs.score > rhs.score;
  };

  auto scorers = irs::Scorers::Prepare(scorer);
  EXPECT_FALSE(scorers.empty());
  auto query = filter.prepare(index, scorers);
  EXPECT_NE(nullptr, query);

  const irs::WandContext mode{.index = wand_idx};

  irs::score_threshold tmp;
  std::vector<ScoredDoc> sorted;
  sorted.reserve(limit);

  for (size_t left = limit, segment_id = 0; const auto& segment : index) {
    auto docs = query->execute(irs::ExecutionContext{
      .segment = segment, .scorers = scorers, .wand = mode});
    EXPECT_NE(nullptr, docs);

    const auto* doc = irs::get<irs::document>(*docs);
    EXPECT_NE(nullptr, doc);
    const auto* score = irs::get<irs::score>(*docs);
    EXPECT_NE(nullptr, score);
    auto* threshold = irs::get_mutable<irs::score_threshold>(docs.get());
    if (wand_idx != irs::WandContext::kDisable && can_use_wand) {
      EXPECT_NE(nullptr, threshold);
    } else {
      EXPECT_EQ(nullptr, threshold);
      threshold = &tmp;
    }

    if (!left) {
      EXPECT_TRUE(!sorted.empty());
      EXPECT_TRUE(std::is_heap(std::begin(sorted), std::end(sorted), less));
      threshold->value = sorted.front().score;
    }

    for (float_t score_value; docs->next();) {
      (*score)(&score_value);

      if (left) {
        sorted.emplace_back(segment_id, doc->value, score_value);

        if (0 == --left) {
          std::make_heap(std::begin(sorted), std::end(sorted), less);
          threshold->value = sorted.front().score;
        }
      } else if (sorted.front().score < score_value) {
        std::pop_heap(std::begin(sorted), std::end(sorted), less);

        auto& min_doc = sorted.back();
        min_doc.segment = segment_id;
        min_doc.doc = doc->value;
        min_doc.score = score_value;

        std::push_heap(std::begin(sorted), std::end(sorted), less);

        threshold->value = sorted.front().score;
      }
    }

    std::sort(std::begin(sorted), std::end(sorted), less);

    ++segment_id;
  }

  return {std::begin(sorted), std::end(sorted)};
}

void WandTestCase::AssertResults(const irs::DirectoryReader& index,
                                 const irs::filter& filter,
                                 const irs::Scorer& scorer,
                                 irs::byte_type scorer_idx, bool can_use_wand,
                                 size_t limit) {
  auto wand_result =
    Collect(index, filter, scorer, scorer_idx, can_use_wand, limit);
  auto result = Collect(index, filter, scorer, irs::WandContext::kDisable,
                        can_use_wand, limit);
  ASSERT_EQ(result, wand_result);
}

void WandTestCase::ConsolidateAll(irs::ScorersView scorers, bool write_norms) {
  const irs::index_utils::ConsolidateCount consolidate_all;
  auto writer =
    open_writer(irs::OM_APPEND, GetWriterOptions(scorers, write_norms));
  ASSERT_TRUE(
    writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
  ASSERT_TRUE(writer->Commit());
  ASSERT_EQ(1, writer->GetSnapshot().size());
}

irs::IndexWriterOptions WandTestCase::GetWriterOptions(irs::ScorersView scorers,
                                                       bool write_norms) {
  EXPECT_TRUE(std::all_of(scorers.begin(), scorers.end(),
                          [](auto* scorer) { return scorer != nullptr; }));

  irs::IndexWriterOptions writer_options;
  writer_options.reader_options.scorers = scorers;
  writer_options.features = [write_norms](irs::type_info::type_id id) {
    if (write_norms && irs::type<irs::Norm2>::id() == id) {
      return std::make_pair(
        irs::ColumnInfo{irs::type<irs::compression::none>::get(), {}, false},
        &irs::Norm2::MakeWriter);
    }

    return std::make_pair(
      irs::ColumnInfo{irs::type<irs::compression::none>::get(), {}, false},
      irs::FeatureWriterFactory{});
  };

  return writer_options;
}

void WandTestCase::GenerateData(irs::ScorersView scorers, bool write_norms,
                                bool append_data) {
  tests::json_doc_generator gen(
    resource("simple_single_column_multi_term.json"),
    [](tests::document& doc, std::string_view name,
       const tests::json_doc_generator::json_value& data) {
      using TextField = tests::text_field<std::string>;

      if (tests::json_doc_generator::ValueType::STRING == data.vt) {
        doc.indexed.push_back(std::make_shared<TextField>(
          std::string{name}, data.str, false,
          std::vector{irs::type<irs::Norm2>::id()}));
      }
    });

  auto open_mode = irs::OM_CREATE;
  if (append_data) {
    open_mode |= irs::OM_APPEND;
  }

  add_segment(gen, open_mode, GetWriterOptions(scorers, write_norms));
}

void WandTestCase::AssertTermFilter(irs::ScorersView scorers) {
  ASSERT_FALSE(scorers.empty());
  for (size_t idx = 0; auto* scorer : scorers) {
    AssertTermFilter(scorers, *scorer, idx);
    ++idx;
  }
  // Invalid scorer
  AssertTermFilter(scorers, *scorers[0], scorers.size());
}

void WandTestCase::AssertTermFilter(irs::ScorersView scorers,
                                    const irs::Scorer& scorer,
                                    irs::byte_type wand_index) {
  static constexpr std::string_view kFieldName = "name";

  irs::by_term filter;
  *filter.mutable_field() = kFieldName;

  auto reader = irs::DirectoryReader{
    dir(), codec(), irs::IndexReaderOptions{.scorers = scorers}};
  ASSERT_NE(nullptr, reader);

  for (const auto& segment : reader) {
    const auto* field = segment.field(kFieldName);
    ASSERT_NE(nullptr, field);

    const auto can_use_wand = [&]() {
      const auto& field_meta = field->meta();
      const auto index_features = scorer.index_features();
      if (index_features != (index_features & field_meta.index_features)) {
        return false;
      }

      irs::feature_set_t features;
      scorer.get_features(features);

      for (const auto feature : features) {
        const auto it = field_meta.features.find(feature);
        if (it == field_meta.features.end() ||
            !irs::field_limits::valid(it->second)) {
          return false;
        }
      }

      return wand_index < scorers.size();
    }();

    ASSERT_EQ(can_use_wand, field->has_scorer(wand_index));

    for (auto terms = field->iterator(irs::SeekMode::NORMAL); terms->next();) {
      filter.mutable_options()->term = terms->value();

      AssertResults(reader, filter, scorer, wand_index, can_use_wand, 10);
      AssertResults(reader, filter, scorer, wand_index, can_use_wand, 100);
    }
  }
}

TEST_P(WandTestCase, TermFilterMultipleScorersDense) {
  Scorers scorers;
  scorers.PushBack(std::make_unique<irs::TFIDF>(false));
  scorers.PushBack(std::make_unique<irs::TFIDF>(true));
  scorers.PushBack(std::make_unique<irs::BM25>());
  scorers.PushBack(std::make_unique<irs::BM25>(irs::BM25::K(), 0));

  GenerateData(scorers.GetView(), true);
  AssertTermFilter(scorers.GetView());

  GenerateData(scorers.GetView(), true, true);  // Add another segment
  ConsolidateAll(scorers.GetView(), true);
  AssertTermFilter(scorers.GetView());
}

TEST_P(WandTestCase, TermFilterMultipleScorersSparse) {
  Scorers scorers;
  scorers.PushBack(std::make_unique<irs::TFIDF>(false));
  scorers.PushBack(std::make_unique<irs::TFIDF>(true));
  scorers.PushBack(std::make_unique<irs::BM25>());
  scorers.PushBack(std::make_unique<irs::BM25>(irs::BM25::K(), 0));

  GenerateData(scorers.GetView(), false);
  AssertTermFilter(scorers.GetView());
}

TEST_P(WandTestCase, TermFilterTFIDF) {
  Scorers scorers;
  auto& scorer = static_cast<const irs::TFIDF&>(
    scorers.PushBack(std::make_unique<irs::TFIDF>(false)));

  GenerateData(scorers.GetView(), true);
  AssertTermFilter(scorers.GetView());
}

TEST_P(WandTestCase, TermFilterTFIDFWithNorms) {
  Scorers scorers;
  auto& scorer = scorers.PushBack(std::make_unique<irs::TFIDF>(true));

  GenerateData(scorers.GetView(), true);
  AssertTermFilter(scorers.GetView());
}

TEST_P(WandTestCase, TermFilterBM25) {
  Scorers scorers;
  auto& scorer = static_cast<const irs::BM25&>(
    scorers.PushBack(std::make_unique<irs::BM25>()));
  ASSERT_FALSE(scorer.IsBM15());
  ASSERT_FALSE(scorer.IsBM11());

  GenerateData(scorers.GetView(), true);
  AssertTermFilter(scorers.GetView());
}

TEST_P(WandTestCase, TermFilterBM15) {
  Scorers scorers;
  auto& scorer = static_cast<const irs::BM25&>(
    scorers.PushBack(std::make_unique<irs::BM25>(irs::BM25::K(), 0)));
  ASSERT_TRUE(scorer.IsBM15());

  GenerateData(scorers.GetView(), true);
  AssertTermFilter(scorers.GetView());
}

static constexpr auto kTestDirs = tests::getDirectories<tests::kTypesDefault>();

static const auto kTestValues =
  ::testing::Combine(::testing::ValuesIn(kTestDirs),
                     ::testing::Values(tests::format_info{"1_5", "1_0"},
                                       tests::format_info{"1_5simd", "1_0"}));

INSTANTIATE_TEST_SUITE_P(WandTest, WandTestCase, kTestValues,
                         WandTestCase::to_string);

}  // namespace
