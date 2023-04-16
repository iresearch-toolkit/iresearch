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

#include <string_view>

#include "shared.hpp"
#include "utils/source_location.hpp"

namespace irs::assert {

using Callback = void (*)(SourceLocation&& location, std::string_view message);
// not thread-safe
Callback SetCallback(Callback callback) noexcept;
void Message(SourceLocation&& location, std::string_view message);

}  // namespace irs::assert

#ifdef IRESEARCH_DEBUG

#define IRS_ASSERT2(condition, message)                     \
  do {                                                      \
    if (IRS_UNLIKELY(!(condition))) {                       \
      ::irs::assert::Message(IRS_SOURCE_LOCATION, message); \
    }                                                       \
  } while (false)

#define IRS_ASSERT1(condition)                                 \
  do {                                                         \
    if (IRS_UNLIKELY(!(condition))) {                          \
      ::irs::assert::Message(IRS_SOURCE_LOCATION, #condition); \
    }                                                          \
  } while (false)

#define IRS_GET_MACRO(arg1, arg2, macro, ...) macro

#define IRS_ASSERT(...) \
  IRS_GET_MACRO(__VA_ARGS__, IRS_ASSERT2, IRS_ASSERT1)(__VA_ARGS__)

#else

#define IRS_ASSERT(...) ((void)1)

#endif
