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

#ifndef IRESEARCH_ATTRIBUTE_RANGE_H
#define IRESEARCH_ATTRIBUTE_RANGE_H

#include "attributes.hpp"
#include "attributes_provider.hpp"

#include <vector>

NS_ROOT

template<typename Adapter>
class IRESEARCH_API_TEMPLATE attribute_range final : public attribute {
 public:
  typedef std::vector<size_t> indexes_t;
  typedef std::vector<Adapter*> iterators_t;

  DECLARE_REFERENCE(attribute_range);
  DECLARE_TYPE_ID(attribute::type_id);

  static attribute_range<Adapter>* extract(const attribute_view& attrs) noexcept {
    return attrs.get<irs::attribute_range<Adapter>>().get();
  }

  void set(iterators_t&& iterators) noexcept {
    iterators_ = std::move(iterators);
  }

  Adapter* value() noexcept {
    return value_;
  }

  bool next() {
    return false; // see specializations
  }

  void reset() {
    current_index_ = 0;
  }

 private:
  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  Adapter* value_{nullptr};
  iterators_t iterators_;
  attribute_view attrs_;
  size_t current_index_{0};
  IRESEARCH_API_PRIVATE_VARIABLES_END
}; // attribute_range

NS_END // ROOT

#endif
