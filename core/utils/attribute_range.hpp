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
struct attribute_range_state {
  virtual Adapter* get_next_iterator() = 0;
  virtual void reset_next_iterator_state() = 0;
  virtual ~attribute_range_state() = default;
};

template<typename Adapter>
class IRESEARCH_API_TEMPLATE attribute_range final : public attribute {
 public:
  DECLARE_TYPE_ID(attribute::type_id);

  static attribute_range<Adapter>* extract(const attribute_view& attrs) noexcept {
    return attrs.get<attribute_range<Adapter>>().get();
  }

  void set_state(attribute_range_state<Adapter>* ars) noexcept {
    ars_ = ars;
  }

  Adapter* value() noexcept {
    return value_;
  }

  bool next();

  void reset() {
    value_ = nullptr;
    assert(ars_);
    ars_->reset_next_iterator_state();
  }

 private:
  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  attribute_range_state<Adapter>* ars_ = nullptr;
  Adapter* value_ = nullptr;
  IRESEARCH_API_PRIVATE_VARIABLES_END
}; // attribute_range

NS_END // ROOT

#endif
