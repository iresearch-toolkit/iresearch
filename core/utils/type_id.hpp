////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_TYPE_ID_H
#define IRESEARCH_TYPE_ID_H

#include <functional>

#include "shared.hpp"
#include "string.hpp"
#include "utils/noncopyable.hpp"

NS_ROOT

class type_info {
 public:
  using type_id = type_info(*)() noexcept;

  // invalid id
  constexpr type_info() noexcept
    : type_info(nullptr, string_ref::NIL) {
  }

  constexpr explicit operator bool() const noexcept {
    return nullptr != id_;
  }

  constexpr bool operator==(const type_info& rhs) const noexcept {
    return id_ == rhs.id_;
  }

  constexpr bool operator!=(const type_info& rhs) const noexcept {
    return !(*this == rhs);
  }

  constexpr bool operator<(const type_info& rhs) const noexcept {
    return id_ < rhs.id_;
  }

  constexpr const string_ref& name() const noexcept { return name_; }
  constexpr type_id id() const noexcept { return id_; }

 private:
  template<typename T>
  friend struct type;

  constexpr type_info(type_id id, const string_ref& name) noexcept
    : id_(id), name_(name) {
  }

  type_id id_;
  string_ref name_;
}; // type_id

template<typename T>
struct type {
  static constexpr type_info get() noexcept {
    return type_info{id(), name()};
  }

  static constexpr string_ref name() noexcept {
    return T::type_name();
  }

  static constexpr type_info::type_id id() noexcept {
    return &get;
  }
};

NS_END // root 

NS_BEGIN(std)

template<>
struct hash<::iresearch::type_info> {
  size_t operator()(const ::iresearch::type_info& key) const {
    return std::hash<decltype(key.id())>()(key.id());
  }
};

NS_END // std

#endif
