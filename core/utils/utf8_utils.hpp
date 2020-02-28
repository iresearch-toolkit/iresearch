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
constexpr uint32_t INVALID_CODE_POINT = integer_traits<uint32_t>::const_max;

FORCE_INLINE const byte_type* next(const byte_type* begin, const byte_type* end) noexcept {
  IRS_ASSERT(begin);
  IRS_ASSERT(end);

  if (begin < end) {
    const uint32_t cp_start = *begin++;
    if ((cp_start >> 5) == 0x06) {
      ++begin;
    } else if ((cp_start >> 4) == 0x0E) {
      begin += 2;
    } else if ((cp_start >> 3) == 0x1E) {
      begin += 3;
    } else if (cp_start >= 0x80) {
      begin = end;
    }
  }

  return begin > end ? end : begin;
}

inline uint32_t next_checked(const byte_type*& begin, const byte_type* end) noexcept {
  IRS_ASSERT(begin);
  IRS_ASSERT(end);

  if (begin >= end) {
    return INVALID_CODE_POINT;
  }

  uint32_t cp = *begin;
  size_t size = 0;

  if (cp < 0x80) {
    size = 1;
  } else if ((cp >> 5) == 0x06) {
    size = 2;
  } else if ((cp >> 4) == 0x0E) {
    size = 3;
  } else if ((cp >> 3) == 0x1E) {
    size = 4;
  }

  begin += size;

  if (begin <= end)  {
    switch (size) {
     case 1: return cp;

     case 2: return ((cp << 6) & 0x7FF) +
                    (uint32_t(begin[-1]) & 0x3F);

     case 3: return ((cp << 12) & 0xFFFF) +
                    ((uint32_t(begin[-2]) << 6) & 0xFFF) +
                    (uint32_t(begin[-1]) & 0x3F);

     case 4: return ((cp << 18) & 0x1FFFFF) +
                    ((uint32_t(begin[-3]) << 12) & 0x3FFFF) +
                    ((uint32_t(begin[-2]) << 6) & 0xFFF) +
                    (uint32_t(begin[-1]) & 0x3F);
    }
  }

  begin = end;
  return INVALID_CODE_POINT;
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
inline bool utf8_to_utf32_checked(const byte_type* begin, size_t size, OutputIterator out) {
  for (auto end = begin + size; begin < end; ) {
    const auto cp = next_checked(begin, end);

    if (cp == INVALID_CODE_POINT) {
      return false;
    }

    *out = cp;
  }

  return true;
}

template<typename OutputIterator>
inline bool utf8_to_utf32_checked(const bytes_ref& in, OutputIterator out) {
  return utf8_to_utf32_checked(in.begin(), in.size(), out);
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
