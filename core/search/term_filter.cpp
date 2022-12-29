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

#include "term_filter.hpp"

#include "index/index_reader.hpp"
#include "search/collectors.hpp"
#include "search/filter_visitor.hpp"
#include "search/term_query.hpp"

namespace {

using namespace irs;

// Filter visitor for term queries
class term_visitor : private util::noncopyable {
 public:
  term_visitor(const term_collectors& term_stats, TermQuery::States& states)
    : term_stats_(term_stats), states_(states) {}

  void prepare(const SubReader& segment, const term_reader& field,
               const seek_term_iterator& terms) noexcept {
    segment_ = &segment;
    reader_ = &field;
    terms_ = &terms;
  }

  void visit(score_t /*boost*/) {
    // collect statistics
    IRS_ASSERT(segment_ && reader_ && terms_);
    term_stats_.collect(*segment_, *reader_, 0, *terms_);

    // Cache term state in prepared query attributes.
    // Later, using cached state we could easily "jump" to
    // postings without relatively expensive FST traversal
    auto& state = states_.insert(*segment_);
    state.reader = reader_;
    state.cookie = terms_->cookie();
  }

 private:
  const term_collectors& term_stats_;
  TermQuery::States& states_;
  const SubReader* segment_{};
  const term_reader* reader_{};
  const seek_term_iterator* terms_{};
};

template<typename Visitor>
void visit(const SubReader& segment, const term_reader& field, bytes_view term,
           Visitor& visitor) {
  // find term
  auto terms = field.iterator(SeekMode::RANDOM_ONLY);

  if (IRS_UNLIKELY(!terms) || !terms->seek(term)) {
    return;
  }

  visitor.prepare(segment, field, *terms);

  // read term attributes
  terms->read();

  visitor.visit(kNoBoost);
}

}  // namespace

namespace irs {

void by_term::visit(const SubReader& segment, const term_reader& field,
                    bytes_view term, filter_visitor& visitor) {
  ::visit(segment, field, term, visitor);
}

filter::prepared::ptr by_term::prepare(const IndexReader& index,
                                       const Order& ord, score_t boost,
                                       std::string_view field,
                                       bytes_view term) {
  TermQuery::States states(index.size());
  field_collectors field_stats(ord);
  term_collectors term_stats(ord, 1);

  term_visitor visitor(term_stats, states);

  // iterate over the segments
  for (const auto& segment : index) {
    // get field
    const auto* reader = segment.field(field);

    if (!reader) {
      continue;
    }

    field_stats.collect(segment,
                        *reader);  // collect field statistics once per segment

    ::visit(segment, *reader, term, visitor);
  }

  bstring stats(ord.stats_size(), 0);
  auto* stats_buf = const_cast<byte_type*>(stats.data());

  term_stats.finish(stats_buf, 0, field_stats, index);

  return memory::make_managed<TermQuery>(std::move(states), std::move(stats),
                                         boost);
}

}  // namespace irs
