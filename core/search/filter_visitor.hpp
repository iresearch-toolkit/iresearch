////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
/// @author Yuriy Popov
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_FILTER_VISITOR_H
#define IRESEARCH_FILTER_VISITOR_H

#include "granular_range_filter.hpp"
#include "multiterm_query.hpp"
#include "formats/formats.hpp"

NS_ROOT

//////////////////////////////////////////////////////////////////////////////
/// @class filter_visitor
/// @brief base filter visitor interface
//////////////////////////////////////////////////////////////////////////////
struct filter_visitor {
  //////////////////////////////////////////////////////////////////////////////
  /// @brief makes preparations for a visitor
  //////////////////////////////////////////////////////////////////////////////
  virtual void prepare(const seek_term_iterator& terms) = 0;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief applies actions to a current term iterator
  //////////////////////////////////////////////////////////////////////////////
  virtual void visit() = 0;

  virtual ~filter_visitor() = default;
};

//////////////////////////////////////////////////////////////////////////////
/// @class multiterm_visitor
/// @brief filter visitor for multiterm queries
//////////////////////////////////////////////////////////////////////////////
template<typename States>
class multiterm_visitor final : public filter_visitor {
 public:
  multiterm_visitor(
      const sub_reader& segment,
      const term_reader& reader,
      limited_sample_collector<term_frequency>& collector,
      States& states)
    : segment_(segment), reader_(reader),
      collector_(collector), states_(states) {
  }

  multiterm_state& get_state();

  virtual void prepare(const seek_term_iterator& terms) override {
    // get term metadata
    auto& meta = terms.attributes().get<term_meta>();

    // NOTE: we can't use reference to 'docs_count' here, like
    // 'const auto& docs_count = meta ? meta->docs_count : NO_DOCS;'
    // since not gcc4.9 nor msvc2015-2019 can handle this correctly
    // probably due to broken optimization
    docs_count_ = meta ? &meta->docs_count : &no_docs_;

    // get state for current segment
    auto& state = get_state();
    state.reader = &reader_;

    collector_.prepare(segment_, terms, state);
    key_.offset = 0;
  }

  virtual void visit() override {
    // fill scoring candidates
    assert(docs_count_);
    key_.frequency = *docs_count_;
    collector_.collect(key_);
    ++key_.offset;
  }

 private:
  const decltype(term_meta::docs_count) no_docs_ = 0;
  const sub_reader& segment_;
  const term_reader& reader_;
  limited_sample_collector<term_frequency>& collector_;
  States& states_;
  term_frequency key_;
  const decltype(term_meta::docs_count)* docs_count_ = nullptr;
};

template<>
inline multiterm_state& multiterm_visitor<multiterm_query::states_t>::get_state() {
  // get state for current segment
  return states_.insert(segment_);
}

template<>
inline multiterm_state& multiterm_visitor<by_granular_range::states_t>::get_state() {
  return states_.emplace(
    std::piecewise_construct,
    std::forward_as_tuple(&segment_),
    std::forward_as_tuple()
  )->second; // create a new range state
}

NS_END

#endif // IRESEARCH_FILTER_VISITOR_H
