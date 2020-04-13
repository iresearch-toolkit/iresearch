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

#include "search/term_query.hpp"

#include "shared.hpp"
#include "index/index_reader.hpp"
#include "search/collectors.hpp"
#include "search/score_doc_iterators.hpp"
#include "search/filter_visitor.hpp"

NS_LOCAL

using namespace irs;

//////////////////////////////////////////////////////////////////////////////
/// @class term_visitor
/// @brief filter visitor for term queries
//////////////////////////////////////////////////////////////////////////////
class term_visitor final : public filter_visitor,
                           private util::noncopyable {
 public:
  term_visitor(
      const term_collectors& term_stats,
      term_query::states_t& states,
      size_t term_offset)
    : term_offset_(term_offset),
      term_stats_(term_stats),
      states_(states) {
  }

  virtual void prepare(
      const sub_reader& segment,
      const term_reader& field,
      const seek_term_iterator& terms) noexcept override {
    segment_ = &segment;
    reader_ = &field;
    terms_ = &terms;
  }

  virtual void visit() override {
    // collect statistics
    assert(segment_ && reader_ && terms_);
    term_stats_.collect(*segment_, *reader_, term_offset_, terms_->attributes());

    // Cache term state in prepared query attributes.
    // Later, using cached state we could easily "jump" to
    // postings without relatively expensive FST traversal
    auto& state = states_.insert(*segment_);
    state.reader = reader_;
    assert(terms_);
    state.cookie = terms_->cookie();
  }

 private:
  const size_t term_offset_;
  const sub_reader* segment_;
  const term_reader* reader_;
  const term_collectors& term_stats_;
  term_query::states_t& states_;
  const seek_term_iterator* terms_ = nullptr;
};

template<typename Visitor>
void visit(
    const sub_reader& segment,
    const term_reader& field,
    const bytes_ref& term,
    Visitor& visitor) {
  // find term
  auto terms = field.iterator();

  if (IRS_UNLIKELY(!terms) || !terms->seek(term)) {
    return;
  }

  visitor.prepare(segment, field, *terms);

  // read term attributes
  terms->read();

  visitor.visit();
}

NS_END

NS_ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                         term_query implementation
// -----------------------------------------------------------------------------

/*static*/ term_query::ptr term_query::make(
    const index_reader& index,
    const order::prepared& ord,
    boost_t boost,
    const string_ref& field,
    const bytes_ref& term) {
  term_query::states_t states(index.size());
  field_collectors field_stats(ord);
  term_collectors term_stats(ord, 1);

  // term_offset = 0 because only 1 term
  term_visitor tv(term_stats, states, 0);

  // iterate over the segments
  for (const auto& segment : index) {
    // get field
    const auto* reader = segment.field(field);

    if (!reader) {
      continue;
    }

    field_stats.collect(segment, *reader); // collect field statistics once per segment

    ::visit(segment, *reader, term, tv);
  }

  bstring stats(ord.stats_size(), 0);
  auto* stats_buf = const_cast<byte_type*>(stats.data());

  ord.prepare_stats(stats_buf);
  term_stats.finish(stats_buf, field_stats, index);

  return memory::make_shared<term_query>(
    std::move(states), std::move(stats), boost
  );
}

/*static*/ void term_query::visit(
    const sub_reader& segment,
    const term_reader& field,
    const bytes_ref& term,
    filter_visitor& visitor) {
  ::visit(segment, field, term, visitor);
}

term_query::term_query(
    term_query::states_t&& states,
    bstring&& stats,
    boost_t boost)
  : filter::prepared(boost),
    states_(std::move(states)),
    stats_(std::move(stats)) {
}

doc_iterator::ptr term_query::execute(
    const sub_reader& rdr,
    const order::prepared& ord,
    const attribute_view& /*ctx*/) const {
  // get term state for the specified reader
  auto state = states_.find(rdr);
  if (!state) {
    // invalid state
    return doc_iterator::empty();
  }

  // find term using cached state
  auto terms = state->reader->iterator();

  if (IRS_UNLIKELY(!terms)) {
    return doc_iterator::empty();
  }

  // use bytes_ref::blank here since we need just to "jump" to the cached state,
  // and we are not interested in term value itself
  if (!terms->seek(bytes_ref::NIL, *state->cookie)) {
    return doc_iterator::empty();
  }

  auto docs = terms->postings(ord.features());
  auto& attrs = docs->attributes();

  // set score
  auto& score = attrs.get<irs::score>();

  if (score) {
    score->prepare(ord, ord.prepare_scorers(rdr, *state->reader, stats_.c_str(), attrs, boost()));
  }

  return docs;
}

NS_END // ROOT
