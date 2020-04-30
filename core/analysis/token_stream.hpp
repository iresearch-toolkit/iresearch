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

#ifndef IRESEARCH_TOKEN_STREAM_H
#define IRESEARCH_TOKEN_STREAM_H

#include <memory>

#include "frozen/map.h"

#include "utils/attributes_provider.hpp"

NS_ROOT

class IRESEARCH_API token_stream : public attribute_provider {
 public:
  using ptr = std::unique_ptr<token_stream>;

  virtual ~token_stream() = default;
  virtual bool next() = 0;
};

template<typename Stream, size_t Size>
class frozen_attributes : public Stream {
 public:
  virtual const attribute* get(type_info::type_id type) const noexcept final {
    const auto it = attrs_.find(type);
    return it == attrs_.end() ? nullptr : it->second;
  }

 protected:
  using attributes_map = frozen::map<type_info::type_id, const attribute*, Size>;
  using value_type = typename attributes_map::value_type;

  template<typename... Args>
  constexpr explicit frozen_attributes(
      std::initializer_list<value_type> values,
      Args&&... args)
    : Stream(std::forward<Args>(args)...),
      attrs_(values) {
  }

 private:
  attributes_map attrs_;
}; // frozen_token_stream

NS_END

#endif
