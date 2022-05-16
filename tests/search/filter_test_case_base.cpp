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

#include "filter_test_case_base.hpp"

#include <compare>

namespace tests {
namespace sort {

DEFINE_FACTORY_DEFAULT(boost)

DEFINE_FACTORY_DEFAULT(custom_sort)

DEFINE_FACTORY_DEFAULT(frequency_sort)

}  // namespace sort

void FilterTestCaseBase::GetQueryResult(const irs::filter::prepared::ptr& q,
                                        const irs::index_reader& rdr,
                                        Docs& result, Costs& result_costs) {
  result_costs.reserve(rdr.size());

  for (const auto& sub : rdr) {
    auto random_docs = q->execute(sub);
    ASSERT_NE(nullptr, random_docs);
    auto sequential_docs = q->execute(sub);
    ASSERT_NE(nullptr, sequential_docs);

    auto* doc = irs::get<irs::document>(*sequential_docs);
    ASSERT_NE(nullptr, doc);

    result_costs.emplace_back(irs::cost::extract(*sequential_docs));

    while (sequential_docs->next()) {
      auto stateless_random_docs = q->execute(sub);
      ASSERT_NE(nullptr, stateless_random_docs);
      ASSERT_EQ(sequential_docs->value(), doc->value);
      ASSERT_EQ(doc->value, random_docs->seek(doc->value));
      ASSERT_EQ(doc->value, random_docs->value());
      ASSERT_EQ(doc->value, stateless_random_docs->seek(doc->value));
      ASSERT_EQ(doc->value, stateless_random_docs->value());

      result.push_back(sequential_docs->value());
    }
    ASSERT_FALSE(sequential_docs->next());
    ASSERT_FALSE(random_docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(sequential_docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(doc->value));

    // seek to eof
    ASSERT_TRUE(
        irs::doc_limits::eof(q->execute(sub)->seek(irs::doc_limits::eof())));
  }
}

void FilterTestCaseBase::GetQueryResult(const irs::filter::prepared::ptr& q,
                                        const irs::index_reader& rdr,
                                        const irs::Order& ord,
                                        ScoredDocs& result,
                                        Costs& result_costs) {
  result_costs.reserve(rdr.size());

  for (const auto& sub : rdr) {
    auto random_docs = q->execute(sub, ord);
    ASSERT_NE(nullptr, random_docs);
    auto sequential_docs = q->execute(sub, ord);
    ASSERT_NE(nullptr, sequential_docs);

    auto* doc = irs::get<irs::document>(*sequential_docs);
    ASSERT_NE(nullptr, doc);

    auto* score = irs::get<irs::score>(*sequential_docs);

    result_costs.emplace_back(irs::cost::extract(*sequential_docs));

    auto assert_equal_scores = [&ord](const std::vector<irs::score_t>& lhs,
                                      irs::doc_iterator& rhs) {
      auto* score = irs::get<irs::score>(rhs);
      std::vector<irs::score_t> tmp(ord.buckets().size());
      if (score) {
        (*score)(tmp.data());
      }
      ASSERT_EQ(lhs, tmp);
    };

    while (sequential_docs->next()) {
      auto stateless_random_docs = q->execute(sub, ord);
      ASSERT_NE(nullptr, stateless_random_docs);
      ASSERT_EQ(sequential_docs->value(), doc->value);
      ASSERT_EQ(doc->value, random_docs->seek(doc->value));
      ASSERT_EQ(doc->value, random_docs->value());
      ASSERT_EQ(doc->value, stateless_random_docs->seek(doc->value));
      ASSERT_EQ(doc->value, stateless_random_docs->value());

      std::vector<irs::score_t> score_value(ord.buckets().size());
      if (score) {
        (*score)(score_value.data());
      }

      assert_equal_scores(score_value, *stateless_random_docs);
      assert_equal_scores(score_value, *random_docs);

      result.emplace_back(sequential_docs->value(), std::move(score_value));
    }
    ASSERT_FALSE(sequential_docs->next());
    ASSERT_FALSE(random_docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(sequential_docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(doc->value));

    // seek to eof
    ASSERT_TRUE(
        irs::doc_limits::eof(q->execute(sub)->seek(irs::doc_limits::eof())));
  }
}

void FilterTestCaseBase::CheckQuery(const irs::filter& filter,
                                    std::span<const irs::sort::ptr> order,
                                    const std::vector<irs::doc_id_t>& expected,
                                    const irs::index_reader& rdr,
                                    bool score_must_be_present, bool reverse) {
  auto prepared_order = irs::Order::Prepare(order);
  auto prepared_filter = filter.prepare(rdr, prepared_order);
  auto score_less =
      [reverse, size = prepared_order.buckets().size()](
          const std::pair<irs::bstring, irs::doc_id_t>& lhs,
          const std::pair<irs::bstring, irs::doc_id_t>& rhs) -> bool {
    const auto& [lhs_buf, lhs_doc] = lhs;
    const auto& [rhs_buf, rhs_doc] = rhs;

    const auto* lhs_score = reinterpret_cast<const float*>(lhs_buf.c_str());
    const auto* rhs_score = reinterpret_cast<const float*>(rhs_buf.c_str());

    for (size_t i = 0; i < size; ++i) {
      const auto r = (lhs_score[i] <=> rhs_score[i]);

      if (r < 0) {
        return !reverse;
      }

      if (r > 0) {
        return reverse;
      }
    }

    return lhs_doc < rhs_doc;
  };

  std::multiset<std::pair<irs::bstring, irs::doc_id_t>, decltype(score_less)>
      scored_result{score_less};

  for (const auto& sub : rdr) {
    auto docs = prepared_filter->execute(sub, prepared_order);

    auto* doc = irs::get<irs::document>(*docs);
    // ensure all iterators contain "document" attribute
    ASSERT_TRUE(bool(doc));

    const auto* score = irs::get<irs::score>(*docs);

    if (!score) {
      ASSERT_FALSE(score_must_be_present);
    }

    irs::bstring score_value(prepared_order.score_size(), 0);

    while (docs->next()) {
      ASSERT_EQ(docs->value(), doc->value);

      if (score && score->Func() != irs::ScoreFunction::kDefault) {
        (*score)(reinterpret_cast<irs::score_t*>(score_value.data()));

        scored_result.emplace(score_value, docs->value());
      } else {
        scored_result.emplace(irs::bstring(prepared_order.score_size(), 0),
                              docs->value());
      }
    }
    ASSERT_FALSE(docs->next());
  }

  std::vector<irs::doc_id_t> result;

  for (auto& entry : scored_result) {
    result.emplace_back(entry.second);
  }

  ASSERT_EQ(expected, result);
}

}  // namespace tests
