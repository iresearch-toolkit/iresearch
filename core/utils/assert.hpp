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

enum class LogLevel : unsigned char {
  Assert = 0,
  Count = 1,
};

#ifdef IRESEARCH_ASSERT
namespace detail {

void LogMessage(LogLevel level, std::string_view file, std::size_t line,
                std::string_view func, std::string_view condition) noexcept;

}  // namespace detail
#endif

using LogCallback = void (*)(std::string_view file, std::size_t line,
                             std::string_view function,
                             std::string_view condition) noexcept;

void SetCallback(LogLevel level, LogCallback callback) noexcept;

}  // namespace iresearch

#define IRS_LOG_MESSAGE(level, cond)                             \
  do {                                                           \
    if (!!(cond)) {                                              \
      ::iresearch::detail::LogMessage(level, __FILE__, __LINE__, \
                                      IRS_FUNC_NAME, #cond);     \
    }                                                            \
  } while (false)

#ifdef NDEBUG
#define IRS_STUB1(first) ((void)1)
#else
#define IRS_STUB1(first) \
  do {                   \
    if (false) {         \
      (void)(first);     \
    }                    \
  } while (false)
#endif

#ifdef IRESEARCH_ASSERT
#define IRS_ASSERT(cond) IRS_LOG_MESSAGE(::iresearch::LogLevel::Assert, !(cond))
#else
#define IRS_ASSERT(cond) IRS_STUB1(!(cond))
#endif
