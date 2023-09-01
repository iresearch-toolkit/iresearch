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

#include "data_output.hpp"

#include <memory>

#include "shared.hpp"

namespace irs {

void BufferedOutput::WriteBytes(const byte_type* b, size_t len) {
  IRS_ASSERT(b != nullptr);  // memcpy needs if for len == 0 in such case
  if (auto remain = Remain(); IRS_UNLIKELY(remain < len)) {
    // The purpose of this approach is minimize count of
    // 1. WriteDirect
    // 2. not full WriteDirect
    // 3. memcpy
    // length != 0 && remain != 0: fill buffer to make remain == 0
    // length != 0 && remain == 0: flush buffer to make length == 0
    // length == 0 && remain != 0: direct write bigger than buffer size
    auto length = Length();
    const auto option =
      static_cast<size_t>(length != 0) * 2 + static_cast<size_t>(remain != 0);
    switch (option) {
      case 2 + 1:
        WriteBuffer(b, remain);
        b += remain;
        len -= remain;
        [[fallthrough]];
      case 2 + 0:
        FlushBuffer();
        remain = Remain();
        [[fallthrough]];
      case 0 + 1:
        if (const auto buffer_len = len % remain; len != buffer_len) {
          const auto direct_len = len - buffer_len;
          WriteDirect(b, direct_len);
          if (buffer_len == 0) {
            return;
          }
          b += direct_len;
          len = buffer_len;
        }
        break;
      default:
        IRS_UNREACHABLE();
    }
  }
  WriteBuffer(b, len);
}

}  // namespace irs
