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

#include "attribute_range.hpp"
#include "search/disjunction.hpp"

NS_ROOT

template<>
bool attribute_range<position_score_iterator_adapter<doc_iterator::ptr>>::next() {
  if (current_index_ < iterators_.size()) {
    value_ = iterators_[current_index_++];
    return true;
  }
  return false;
}

#define ADD_ATTRIBUTE_RANGE(AttributeRange) template<> \
const attribute::type_id& AttributeRange::type() { \
  static attribute::type_id type(#AttributeRange); \
  return type; \
} \
template class AttributeRange; \
template struct attribute_view::ref<AttributeRange>; \
REGISTER_ATTRIBUTE(AttributeRange);

ADD_ATTRIBUTE_RANGE(attribute_range<score_iterator_adapter<doc_iterator::ptr>>);
ADD_ATTRIBUTE_RANGE(attribute_range<position_score_iterator_adapter<doc_iterator::ptr>>);

NS_END // ROOT
