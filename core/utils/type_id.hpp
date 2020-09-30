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

#include "shared.hpp"
#include "string.hpp"
#include "std.hpp"

NS_ROOT

NS_BEGIN(detail)

DEFINE_HAS_MEMBER(type_name);

template<typename T>
constexpr string_ref ctti() noexcept {
  return { IRESEARCH_CURRENT_FUNCTION };
}

NS_END // detail

////////////////////////////////////////////////////////////////////////////////
/// @class type_info
/// @brief holds meta information obout a type, e.g. name and identifier
////////////////////////////////////////////////////////////////////////////////
class type_info {
 public:
  //////////////////////////////////////////////////////////////////////////////
  /// @brief unique type identifier
  /// @note can be used to get an instance of underlying type
  //////////////////////////////////////////////////////////////////////////////
  using type_id = type_info(*)() noexcept;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief default constructor produces invalid type identifier
  //////////////////////////////////////////////////////////////////////////////
  constexpr type_info() noexcept
    : type_info(nullptr, string_ref::NIL) {
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @return true if type_info is valid, false - otherwise
  //////////////////////////////////////////////////////////////////////////////
  constexpr explicit operator bool() const noexcept {
    return nullptr != id_;
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @return true if current object is equal to a denoted by 'rhs'
  //////////////////////////////////////////////////////////////////////////////
  constexpr bool operator==(const type_info& rhs) const noexcept {
    return id_ == rhs.id_;
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @return true if current object is not equal to a denoted by 'rhs'
  //////////////////////////////////////////////////////////////////////////////
  constexpr bool operator!=(const type_info& rhs) const noexcept {
    return !(*this == rhs);
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @return true if current object is less than to a denoted by 'rhs'
  //////////////////////////////////////////////////////////////////////////////
  constexpr bool operator<(const type_info& rhs) const noexcept {
    return id_ < rhs.id_;
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @return type name
  //////////////////////////////////////////////////////////////////////////////
  constexpr const string_ref& name() const noexcept { return name_; }

  //////////////////////////////////////////////////////////////////////////////
  /// @return type identifier
  //////////////////////////////////////////////////////////////////////////////
  constexpr type_id id() const noexcept { return id_; }

 private:
  template<typename T>
  friend struct type;

  constexpr type_info(type_id id, const string_ref& name) noexcept
    : id_(id), name_(name) {
  }

  type_id id_;
  string_ref name_;
}; // type_info

////////////////////////////////////////////////////////////////////////////////
/// @class type
/// @tparam T type for which one needs access meta information
/// @brief convenient helper for accessing meta information
////////////////////////////////////////////////////////////////////////////////
template<typename T>
struct type {
  //////////////////////////////////////////////////////////////////////////////
  /// @returns an instance of "type_info" object holding meta information of
  ///          type denoted by template parameter "T"
  //////////////////////////////////////////////////////////////////////////////
  static constexpr type_info get() noexcept {
    return type_info{id(), name()};
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @returns type name of a type denoted by template parameter "T"
  /// @note Do never persist type name provided by detail::ctti<T> as
  ///       it's platform dependent
  //////////////////////////////////////////////////////////////////////////////
  static constexpr string_ref name() noexcept {
    if constexpr (detail::has_member_type_name_v<T>) {
      return T::type_name();
    }

    return detail::ctti<T>();
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @returns type identifier of a type denoted by template parameter "T"
  //////////////////////////////////////////////////////////////////////////////
  static constexpr type_info::type_id id() noexcept {
    return &get;
  }
}; // type

NS_END

NS_BEGIN(std)

template<>
struct hash<::iresearch::type_info> {
  size_t operator()(const ::iresearch::type_info& key) const {
    return std::hash<decltype(key.id())>()(key.id());
  }
};

NS_END // std

#endif
