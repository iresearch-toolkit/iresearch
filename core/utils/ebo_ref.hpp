#pragma once

////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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

#pragma once

#include <type_traits>

#include "shared.hpp"

namespace iresearch {

// Temporary workaround to avoid making huge changes
template<typename T>
class EboRef {
 private:
  template<typename U>
  class Pointer {
   public:
    Pointer(const U& value) noexcept : value_{const_cast<T*>(&value)} {}

    explicit operator U&() const noexcept { return *value_; }

   private:
    U* value_;
  };

  using EboType = std::conditional_t<std::is_empty_v<T>, T, Pointer<T>>;

 public:
  explicit EboRef(const T& value) noexcept : value_{value} {}

  EboRef& operator=(T& value) noexcept {
    value_ = EboType{value};
    return *this;
  }

  T& get() noexcept { return static_cast<T&>(value_); }
  const T& get() const noexcept { return static_cast<const T&>(value_); }

 private:
  IRS_NO_UNIQUE_ADDRESS EboType value_;
};

}  // namespace iresearch
