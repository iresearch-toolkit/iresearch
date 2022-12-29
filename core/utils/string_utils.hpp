////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "shared.hpp"

#if defined(_MSC_VER) && _MSC_VER < 1900  // before MSVC2015
#define snprintf _snprintf
#endif

#include "utils/math_utils.hpp"

namespace irs {
namespace string_utils {

inline size_t oversize(size_t chunk_size, size_t size,
                       size_t min_size) noexcept {
  IRS_ASSERT(chunk_size);
  IRS_ASSERT(min_size > size);

  typedef math::math_traits<size_t> math_traits;

  return size + math_traits::ceil(min_size - size, chunk_size);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief resize string to the full capacity of resize to the specified size
////////////////////////////////////////////////////////////////////////////////
template<typename T>
inline std::basic_string<T>& oversize(
  // 31 == 32 - 1: because std::basic_string reserves a \0 at the end
  // 32 is the original default value used in bytes_builder
  std::basic_string<T>& buf, size_t size = 31) {
  buf.resize(size);
  buf.resize(buf.capacity());  // use up the entire buffer
  return buf;
}

}  // namespace string_utils
}  // namespace irs
