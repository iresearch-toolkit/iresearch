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

#ifndef IRESEARCH_FROZEN_ATTRIBUTES_H
#define IRESEARCH_FROZEN_ATTRIBUTES_H

#include "attribute_provider.hpp"

namespace iresearch {

template<typename T>
struct attribute_ptr {
  T* ptr{};

  attribute_ptr() = default;
  attribute_ptr(T& v) noexcept : ptr(&v) {}
  attribute_ptr(T* v) noexcept : ptr(v) {}

  operator attribute_ptr<attribute>() const noexcept {
    return attribute_ptr<attribute>{ptr};
  }
};  // attribute_ptr

template<typename T>
struct type<attribute_ptr<T>> : type<T> {};

namespace detail {

template<size_t I, typename... T>
constexpr attribute_ptr<attribute> get_mutable_helper(
    std::tuple<T...>& t, type_info::type_id id) noexcept {
  auto& v = std::get<I>(t);
  if (type<std::remove_reference_t<decltype(v)>>::id() == id) {
    return v;
  }

  if constexpr (I < std::tuple_size<std::tuple<T...>>::value - 1) {
    return get_mutable_helper<I + 1>(t, id);
  } else {
    return {};
  }
}

}  // namespace detail

template<typename... T>
constexpr attribute* get_mutable(std::tuple<T...>& t,
                                 type_info::type_id id) noexcept {
  return detail::get_mutable_helper<0>(t, id).ptr;
}

}  // namespace iresearch

#endif  // IRESEARCH_FROZEN_ATTRIBUTES_H
