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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_UTF8_UTILS_H
#define IRESEARCH_UTF8_UTILS_H

#include <vector>

#include "shared.hpp"
#include "log.hpp"
#include "string.hpp"

NS_ROOT
NS_BEGIN(utf8_utils)

// max number of bytes to represent single UTF8 code point
constexpr size_t MAX_CODE_POINT_SIZE = 4;
constexpr uint32_t MIN_CODE_POINT = 0;
constexpr uint32_t MAX_CODE_POINT = 0x10FFFF;

FORCE_INLINE const byte_type* next(const byte_type* it, const byte_type* end) noexcept {
  IRS_ASSERT(it);
  IRS_ASSERT(end);

  if (it < end) {
    const uint8_t symbol_start = *it;
    if (symbol_start < 0x80) {
      ++it;
    } else if ((symbol_start >> 5) == 0x06) {
      it += 2;
    } else if ((symbol_start >> 4) == 0x0E) {
      it += 3;
    } else if ((symbol_start >> 3) == 0x1E) {
      it += 4;
    } else {
      IR_FRMT_ERROR("Invalid UTF-8 symbol increment");
      it = end;
    }
  }
  return it > end ? end : it;
}

FORCE_INLINE uint32_t next(const byte_type*& it) noexcept {
  IRS_ASSERT(it);

  uint32_t cp = *it;

  if ((cp >> 5) == 0x06) {
    cp = ((cp << 6) & 0x7FF) + (uint32_t(*++it) & 0x3F);
  } else if ((cp >> 4) == 0x0E) {
    cp = ((cp << 12) & 0xFFFF) + ((uint32_t(*++it) << 6) & 0xFFF);
    cp += (*++it) & 0x3F;
  } else if ((cp >> 3) == 0x1E) {
    cp = ((cp << 18) & 0x1FFFFF) + ((uint32_t(*++it) << 12) & 0x3FFFF);
    cp += (uint32_t(*++it) << 6) & 0xFFF;
    cp += uint32_t(*++it) & 0x3F;
  }

  ++it;

  return cp;
}

#ifdef IRESEARCH_CXX14
constexpr
#endif
FORCE_INLINE size_t utf32_to_utf8(uint32_t cp, byte_type* begin) noexcept {
  if (cp < 0x80) {
    begin[0] = static_cast<byte_type>(cp);
    return 1;
  }

  if (cp < 0x800) {
    begin[0] = static_cast<byte_type>((cp >> 6) | 0xC0);
    begin[1] = static_cast<byte_type>((cp & 0x3F) | 0x80);
    return 2;
  }

  if (cp < 0x10000) {
    begin[0] = static_cast<byte_type>((cp >> 12) | 0xE0);
    begin[1] = static_cast<byte_type>(((cp >> 6) & 0x3F) | 0x80);
    begin[2] = static_cast<byte_type>((cp & 0x3F) | 0x80);
    return 3;
  }

  begin[0] = static_cast<byte_type>((cp >> 18) | 0xF0);
  begin[1] = static_cast<byte_type>(((cp >> 12) & 0x3F) | 0x80);
  begin[2] = static_cast<byte_type>(((cp >> 6) & 0x3F) | 0x80);
  begin[3] = static_cast<byte_type>((cp & 0x3F) | 0x80);
  return 4;
}

template<typename OutputIterator>
inline void utf8_to_utf32(const byte_type* begin, size_t size, OutputIterator out) {
  for (auto end = begin + size; begin < end; ++out) {
    *out = next(begin);
  }
}

template<typename OutputIterator>
inline void utf8_to_utf32(const bytes_ref& in, OutputIterator out) {
  utf8_to_utf32(in.begin(), in.size(), out);
}

NS_END
NS_END

#endif
