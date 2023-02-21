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
////////////////////////////////////////////////////////////////////////////////

#include "term_query.hpp"

#include "index/index_reader.hpp"
#include "search/prepared_state_visitor.hpp"
#include "search/score.hpp"

namespace irs {

TermQuery::TermQuery(States&& states, bstring&& stats, score_t boost)
  : filter::prepared(boost),
    states_{std::move(states)},
    stats_{std::move(stats)} {}

doc_iterator::ptr TermQuery::execute(const ExecutionContext& ctx) const {
  auto& rdr = ctx.segment;
  auto& ord = ctx.scorers;
  auto state = states_.find(rdr);  // Get term state for the specified reader

  if (!state) {
    // Invalid state
    return doc_iterator::empty();
  }

  auto* reader = state->reader;
  IRS_ASSERT(reader);

  auto docs = [&]() -> irs::doc_iterator::ptr {
    if (ctx.mode != ExecutionMode::kAll && !ord.empty()) {
      return reader->wanderator(
        *state->cookie, ord.features(),
        {.factory = [&, bucket = ord.buckets().front().bucket.get()](
                      const attribute_provider& attrs) -> ScoreFunction {
           return bucket->prepare_scorer(rdr, *state->reader, stats_.c_str(),
                                         attrs, boost());
         },
         .strict = (ctx.mode == ExecutionMode::kStrictTop)});
    } else {
      return reader->postings(*state->cookie, ord.features());
    }
  }();

  if (IRS_UNLIKELY(!docs)) {
    IRS_ASSERT(false);
    return doc_iterator::empty();
  }

  if (!ord.empty()) {
    auto* score = irs::get_mutable<irs::score>(docs.get());

    if (score) {
      // FIXME(gnusi): honor cached wanderator score
      *score = CompileScore(ord.buckets(), rdr, *state->reader, stats_.c_str(),
                            *docs, boost());
    }
  }

  return docs;
}

void TermQuery::visit(const SubReader& segment, PreparedStateVisitor& visitor,
                      score_t boost) const {
  if (auto state = states_.find(segment); state) {
    visitor.Visit(*this, *state, boost * this->boost());
    return;
  }
}

}  // namespace irs
