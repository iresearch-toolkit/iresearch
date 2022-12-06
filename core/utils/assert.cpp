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

#include "utils/assert.hpp"

#include <array>
#include <cstddef>

namespace iresearch {
namespace detail {

static std::array<LogCallback, static_cast<std::size_t>(LogLevel::Count)>
  sCallbacks = {};

void LogMessage(LogLevel level, std::string_view file, std::size_t line,
                std::string_view func, std::string_view condition) noexcept {
  if (const auto callback = sCallbacks[static_cast<std::size_t>(level)];
      IRS_LIKELY(callback != nullptr)) {
    callback(file, line, func, condition);
  }
}

}  // namespace detail

void SetCallback(LogLevel level, LogCallback callback) noexcept {
  detail::sCallbacks[static_cast<std::size_t>(level)] = callback;
}

}  // namespace iresearch
