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

#ifndef IRESEARCH_COLUMN_INFO_H
#define IRESEARCH_COLUMN_INFO_H

#include <functional>

#include "utils/compression.hpp"
#include "utils/string.hpp"

namespace iresearch {

struct column_info {
  type_info compression;
  compression::options options;
  bool encryption;

  bool operator==(const column_info& rhs) const noexcept {
    return compression == rhs.compression && options == rhs.options &&
           encryption == rhs.encryption;
  }

  bool operator!=(const column_info& rhs) const noexcept {
    return !(*this == rhs);
  }
};

using column_info_provider_t = std::function<column_info(const string_ref)>;

}  // namespace iresearch

#endif  // IRESEARCH_COLUMN_INFO_H
