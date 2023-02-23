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
#include "search/filter.hpp"
#include "search/score.hpp"
#include "search/term_filter.hpp"
#include "search/tfidf.hpp"

namespace {

struct Doc {
  Doc(size_t segment, irs::doc_id_t doc) : segment{segment}, doc{doc} {}

  bool operator==(const Doc&) const = default;

  size_t segment;
  irs::doc_id_t doc;
};

class WandTestCase : public tests::index_test_base {
 public:
  std::vector<Doc> Collect(const irs::IndexReader& index,
                           const irs::filter& filter, const irs::ScorerFactory& scorer,
                           size_t limit, bool use_wand);

  void AssertResults(const irs::IndexReader& index, const irs::filter& filter,
                     const irs::ScorerFactory& scorer, size_t limit);
};

std::vector<Doc> WandTestCase::Collect(const irs::IndexReader& index,
                                       const irs::filter& filter,
                                       const irs::ScorerFactory& scorer, size_t limit,
                                       bool use_wand) {
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

  const auto mode =
    (use_wand ? irs::ExecutionMode::kTop : irs::ExecutionMode::kAll);

  irs::score_threshold tmp;
  std::vector<ScoredDoc> sorted;
  sorted.reserve(limit);

  for (size_t left = limit, segment_id = 0; const auto& segment : index) {
    auto docs = query->execute(irs::ExecutionContext{
      .segment = segment, .scorers = scorers, .mode = mode});
    EXPECT_NE(nullptr, docs);

    const auto* doc = irs::get<irs::document>(*docs);
    EXPECT_NE(nullptr, doc);
    const auto* score = irs::get<irs::score>(*docs);
    EXPECT_NE(nullptr, score);
    auto* threshold = irs::get_mutable<irs::score_threshold>(docs.get());
    if (use_wand) {
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

void WandTestCase::AssertResults(const irs::IndexReader& index,
                                 const irs::filter& filter,
                                 const irs::ScorerFactory& scorer, size_t limit) {
  auto wand_result = Collect(index, filter, scorer, limit, true);
  auto result = Collect(index, filter, scorer, limit, false);
  ASSERT_EQ(result, wand_result);
}

TEST_P(WandTestCase, TermFilter) {
  {
    tests::json_doc_generator gen(
      resource("simple_single_column_multi_term.json"),
      &tests::payloaded_json_field_factory);
    add_segment(gen);
  }

  auto reader = irs::DirectoryReader{dir(), codec()};
  ASSERT_NE(nullptr, reader);

  irs::by_term filter;
  *filter.mutable_field() = "name_anl";
  filter.mutable_options()->term =
    irs::ViewCast<irs::byte_type>(std::string_view{"tempor"});

  irs::tfidf_sort scorer{false, false};

  AssertResults(reader, filter, scorer, 10);
  AssertResults(reader, filter, scorer, 100);
}

static constexpr auto kTestDirs = tests::getDirectories<tests::kTypesDefault>();

static const auto kTestValues =
  ::testing::Combine(::testing::ValuesIn(kTestDirs),
                     ::testing::Values(tests::format_info{"1_5", "1_0"},
                                       tests::format_info{"1_5simd", "1_0"}));

INSTANTIATE_TEST_SUITE_P(WandTest, WandTestCase, kTestValues,
                         WandTestCase::to_string);

}  // namespace
