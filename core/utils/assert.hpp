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
/// @author Valery Mironov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstddef>
#include <string_view>

#include "shared.hpp"

namespace iresearch {

#ifdef IRESEARCH_DEBUG
namespace detail {

void AssertMessage(std::string_view file, std::size_t line,
                   std::string_view func, std::string_view condition,
                   std::string_view message) noexcept;

}  // namespace detail
#endif

using LogCallback = void (*)(std::string_view file, std::size_t line,
                             std::string_view function,
                             std::string_view condition,
                             std::string_view message) noexcept;

void SetAssertCallback(LogCallback callback) noexcept;

}  // namespace iresearch

#define IRS_ASSERT_MESSAGE(cond, message)                                   \
  do {                                                                      \
    if (!(cond)) {                                                          \
      ::iresearch::detail::AssertMessage(__FILE__, __LINE__, IRS_FUNC_NAME, \
                                         #cond, message);                   \
    }                                                                       \
  } while (false)

#define IRS_GET_MACRO(arg1, arg2, MACRO, ...) MACRO

#ifdef NDEBUG

#define IRS_STUB(...) ((void)1)

#else

#define IRS_STUB1(first) \
  do {                   \
    if (false) {         \
      (void)(first);     \
    }                    \
  } while (false)

#define IRS_STUB2(first, second) \
  do {                           \
    if (false) {                 \
      (void)(first);             \
      (void)(second);            \
    }                            \
  } while (false)

#define IRS_STUB(...) \
  IRS_GET_MACRO(__VA_ARGS__, IRS_STUB2, IRS_STUB1)(__VA_ARGS__)

#endif

#ifdef IRESEARCH_DEBUG

#define IRS_ASSERT_CONDITION(cond) IRS_ASSERT_MESSAGE(cond, {})

#define IRS_ASSERT(...)                                                \
  IRS_GET_MACRO(__VA_ARGS__, IRS_ASSERT_MESSAGE, IRS_ASSERT_CONDITION) \
  (__VA_ARGS__)

#else

#define IRS_ASSERT(...) IRS_STUB(__VA_ARGS__)

#endif
