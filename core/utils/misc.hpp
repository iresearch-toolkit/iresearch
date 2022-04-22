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
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_MISC_H
#define IRESEARCH_MISC_H

#include <array>
#include <cassert>
#include <memory>

#include "shared.hpp"

namespace iresearch {

// Convenient helper for simulating 'try/catch/finally' semantic
template<typename Func>
class finally {
 public:
  static_assert(std::is_nothrow_invocable_v<Func>);

  explicit finally(Func&& func)
    : func_{std::forward<Func>(func)} {
  }
  ~finally() {
    func_();
  }

 private:
  Func func_;
};

template<typename Func>
finally<Func> make_finally(Func&& func) {
  return finally<Func>{std::forward<Func>(func)};
}

// Convenient helper for simulating copy semantic for move-only types
// e.g. lambda capture statement before c++14
template<typename T>
class move_on_copy {
 public:
  explicit move_on_copy(T&& value) noexcept : value_(std::forward<T>(value)) {}
  move_on_copy(const move_on_copy& rhs) noexcept : value_(std::move(rhs.value_)) {}

  T& value() noexcept { return value_; }
  const T& value() const noexcept { return value_; }

 private:
  move_on_copy& operator=(move_on_copy&&) = delete;
  move_on_copy& operator=(const move_on_copy&) = delete;

  mutable T value_;
};

template<typename T>
move_on_copy<T> make_move_on_copy(T&& value) noexcept {
  static_assert(std::is_rvalue_reference_v<decltype(value)>, "parameter must be an rvalue");
  return move_on_copy<T>(std::forward<T>(value));
}

// Convenient helper for caching function results
template<
    typename Input,
    Input Size,
    typename Func,
    typename = typename std::enable_if_t<std::is_integral_v<Input>>>
class cached_func {
 public:
  using input_type = Input;
  using output_type = std::invoke_result_t<Func, Input>;

  constexpr explicit cached_func(input_type offset, Func&& func)
    : func_{std::forward<Func>(func)} {
    for (; offset < Size; ++offset) {
      cache_[offset] = func_(offset);
    }
  }

  template<bool Checked>
  constexpr FORCE_INLINE output_type get(input_type value) const
      noexcept(std::is_nothrow_invocable_v<Func, Input>) {
    if constexpr (Checked) {
      return value < size() ? cache_[value] : func_(value);
    } else {
      assert(value < cache_.size());
      return cache_[value];
    }
  }

  constexpr size_t size() const noexcept {
    return cache_.size();
  }

 private:
  Func func_;
  std::array<output_type, Size> cache_{};
};

template<typename Input, size_t Size, typename Func>
constexpr cached_func<Input, Size, Func> cache_func(Input offset, Func&& func) {
  return cached_func<Input, Size, Func>{ offset, std::forward<Func>(func) };
}

template<typename To, typename From>
constexpr auto* down_cast(From* from) noexcept {
  static_assert(!std::is_pointer_v<To>);
  static_assert(!std::is_reference_v<To>);
  using CastTo =
      std::conditional_t<std::is_const_v<From>, std::add_const_t<To>, To>;
  assert(from == nullptr || dynamic_cast<CastTo*>(from) != nullptr);
  return static_cast<CastTo*>(from);
}

template<typename To, typename From>
constexpr auto& down_cast(From&& from) noexcept {
  return *down_cast<To>(std::addressof(from));
}

template<typename To, typename From>
auto down_cast(std::shared_ptr<From> from) noexcept {
  static_assert(!std::is_pointer_v<To>);
  static_assert(!std::is_reference_v<To>);
  using CastTo =
      std::conditional_t<std::is_const_v<From>, std::add_const_t<To>, To>;
  assert(from == nullptr ||
         std::dynamic_pointer_cast<CastTo>(from) != nullptr);
  return std::static_pointer_cast<CastTo>(std::move(from));
}

}

#endif
