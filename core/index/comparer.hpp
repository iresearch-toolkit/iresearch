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

#pragma once

#include "shared.hpp"
#include "utils/string.hpp"

namespace irs {

class comparer {
 public:
  virtual ~comparer() = default;

  int operator()(bytes_view lhs, bytes_view rhs) const {
    IRS_ASSERT(!IsNull(lhs));
    IRS_ASSERT(!IsNull(rhs));
    return compare(lhs, rhs);
  }

 protected:
  virtual int compare(bytes_view lhs, bytes_view rhs) const = 0;
};  // comparer

inline bool use_dense_sort(size_t size, size_t total) noexcept {
  // check: N*logN > K
  return std::isgreaterequal(static_cast<double_t>(size) * std::log(size),
                             static_cast<double_t>(total));
}

}  // namespace irs

