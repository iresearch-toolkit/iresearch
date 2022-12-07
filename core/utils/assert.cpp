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

#include <cstddef>
#include <utility>

namespace iresearch {
namespace detail {

LogCallback kAssertCallback = nullptr;

void AssertMessage(std::string_view file, std::size_t line,
                   std::string_view func, std::string_view condition,
                   std::string_view message) noexcept {
  if (IRS_LIKELY(kAssertCallback != nullptr)) {
    kAssertCallback(file, line, func, condition, message);
  }
}

}  // namespace detail

LogCallback SetAssertCallback(LogCallback callback) noexcept {
  return std::exchange(detail::kAssertCallback, callback);
}

}  // namespace iresearch
