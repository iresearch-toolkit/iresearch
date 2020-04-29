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
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_FROZEN_ATTRIBUTES_PROVIDER_H
#define IRESEARCH_FROZEN_ATTRIBUTES_PROVIDER_H

#include "frozen/map.h"

#include "attributes_provider.hpp"


NS_ROOT
NS_BEGIN(util)

template<size_t Size>
class frozen_attributes_provider : public attributes_provider {
 public:
  using attributes_map = frozen::map<type_info::type_id, const attribute*, Size>;

  constexpr frozen_attribute_provider(
    typename attributes_map::value_type const (&value)[Size] values) noexcept
    : attrs_(values) {
  }

  virtual const attribute* get(type_info::type_id type) const noexcept override {
    const auto it = attrs_.find(type);
    return it == attrs_.end() ? nullptr : it->second;
  }


 private:
  attributes_map attrs_;
}; // frozen_attributes_provider

NS_END // util
NS_END

#endif // IRESEARCH_FROZEN_ATTRIBUTES_PROVIDER_H
