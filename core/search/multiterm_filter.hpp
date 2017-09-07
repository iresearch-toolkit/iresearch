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

#ifndef IRESEARCH_MULTITERM_FILTER_H
#define IRESEARCH_MULTITERM_FILTER_H

#include "filter.hpp"
#include "index/index_reader.hpp"

NS_ROOT

class term_selector {
 public:
  term_selector(
      const index_reader& index,
      const order::prepared& ord) {
  }

  void insert(
      const sub_reader& segment,
      const term_reader& field,
      const term_iterator& term) {
  }

  filter::prepared::ptr build(
      filter::boost_t /* boost */) {
    return nullptr;
  }
};

template<typename Selector>
class multiterm_filter : filter {
 public:
  explicit multiterm_filter(const type_id& type) NOEXCEPT;

  const std::string& field() const {
    return field_;
  }

  virtual filter::prepared::ptr prepare(
      const index_reader& index,
      const order::prepared& ord,
      boost_t boost) const override {
    const irs::string_ref field_ref = field_; // avoid casting to irs::string_ref

    Selector selector(index, ord);

    for (auto& segment : index) {
      const auto* field = segment.field(field_ref);

      if (!field) {
        // unable to find field in a segment
        continue;
      }

      auto terms = field->iterator();

      if (!terms) {
        // got invalid term iterator
        continue;
      }

      collect_terms(selector, segment, *field, *terms);
    }

    return selector.build(boost*this->boost());
  }

 protected:
  virtual void collect_terms(
    Selector& selector,
    const sub_reader& segment,
    const term_reader& field,
    seek_term_iterator& terms
  ) const = 0;

  std::string field_;
}; // multiterm_filter

NS_END // ROOT

#endif // IRESEARCH_MULTITERM_FILTER_H

