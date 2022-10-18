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

#pragma once

#include <cassert>
#include <cstring>
#include <string_view>

#include "shared.hpp"

namespace std {

#if defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wtautological-pointer-compare"
#endif

// MSVC++ > v14.0 (Visual Studio >2015) already implements this in <xstring>
// MacOS requires this definition to be before first usage (i.e. in bytes_view)
#if !defined(_MSC_VER) || (_MSC_VER <= 1900)
template<>
struct char_traits<::iresearch::byte_type> {
  typedef ::iresearch::byte_type char_type;
  typedef int int_type;
  typedef std::streamoff off_type;
  typedef std::streampos pos_type;

  static void assign(char_type& dst, const char_type& src) noexcept {
    dst = src;
  }

  static char_type* assign(char_type* ptr, size_t count, char_type ch) noexcept
    IRESEARCH_ATTRIBUTE_NONNULL() {
    assert(nullptr != ptr);
    return reinterpret_cast<char_type*>(std::memset(ptr, ch, count));
  }

  static int compare(const char_type* lhs, const char_type* rhs,
                     size_t count) noexcept IRESEARCH_ATTRIBUTE_NONNULL() {
    if (0 == count) {
      return 0;
    }

    assert(nullptr != lhs);
    assert(nullptr != rhs);
    return std::memcmp(lhs, rhs, count);
  }

  static char_type* copy(char_type* dst, const char_type* src,
                         size_t count) noexcept IRESEARCH_ATTRIBUTE_NONNULL() {
    if (0 == count) {
      return dst;
    }

    assert(nullptr != dst);
    assert(nullptr != src);
    return reinterpret_cast<char_type*>(std::memcpy(dst, src, count));
  }

  static constexpr int_type eof() noexcept { return -1; }

  static constexpr bool eq(char_type lhs, char_type rhs) noexcept {
    return lhs == rhs;
  }

  static constexpr bool eq_int_type(int_type lhs, int_type rhs) noexcept {
    return lhs == rhs;
  }

  static const char_type* find(const char_type* ptr, size_t count,
                               const char_type& ch) noexcept
    IRESEARCH_ATTRIBUTE_NONNULL() {
    if (0 == count) {
      return nullptr;
    }

    assert(nullptr != ptr);
    return reinterpret_cast<const char_type*>(std::memchr(ptr, ch, count));
  }

  static size_t length(const char_type* /*ptr*/) noexcept {
    // binary string length cannot be determined from binary content
    assert(false);
    return (std::numeric_limits<size_t>::max)();
  }

  static constexpr bool lt(char_type lhs, char_type rhs) noexcept {
    return lhs < rhs;
  }

  static char_type* move(char_type* dst, const char_type* src,
                         size_t count) noexcept IRESEARCH_ATTRIBUTE_NONNULL() {
    if (0 == count) {
      return dst;
    }

    return reinterpret_cast<char_type*>(std::memmove(dst, src, count));
  }

  static constexpr int_type not_eof(int_type i) noexcept { return i != eof(); }

  static constexpr char_type to_char_type(int_type i) noexcept {
    assert(int_type(char_type(i)) == i);
    return char_type(i);
  }

  static constexpr int_type to_int_type(char_type ch) noexcept { return ch; }

  MSVC_ONLY(static void _Copy_s(char_type* /*dst*/, size_t /*dst_size*/,
                                const char_type* /*src*/,
                                size_t /*src_size*/) { assert(false); });
};  // char_traits
#endif

#if defined(__clang__)
#pragma GCC diagnostic pop
#endif

}  // namespace std

namespace iresearch {

using bstring = std::basic_string<byte_type>;
using bytes_view = std::basic_string_view<byte_type>;

template<typename Char>
inline size_t CommonPrefixLength(const Char* lhs, size_t lhs_size,
                                 const Char* rhs, size_t rhs_size) noexcept {
  static_assert(1 == sizeof(Char), "1 != sizeof(Char)");

  const size_t* lhs_block = reinterpret_cast<const size_t*>(lhs);
  const size_t* rhs_block = reinterpret_cast<const size_t*>(rhs);

  size_t size = std::min(lhs_size, rhs_size);

  while (size >= sizeof(size_t) && *lhs_block == *rhs_block) {
    ++lhs_block;
    ++rhs_block;
    size -= sizeof(size_t);
  }

  const Char* lhs_block_start = reinterpret_cast<const Char*>(lhs_block);
  const Char* rhs_block_start = reinterpret_cast<const Char*>(rhs_block);

  while (size && *lhs_block_start == *rhs_block_start) {
    ++lhs_block_start;
    ++rhs_block_start;
    --size;
  }

  return lhs_block_start - lhs;
}

template<typename Char, typename Traits>
inline size_t CommonPrefixLength(
  std::basic_string_view<Char, Traits> lhs,
  std::basic_string_view<Char, Traits> rhs) noexcept {
  return CommonPrefixLength(lhs.data(), lhs.size(), rhs.data(), rhs.size());
}

template<typename Char>
inline std::basic_string_view<Char> EmptyStringView() noexcept {
  return {reinterpret_cast<const Char*>(""), 0};
}

template<typename Char>
constexpr bool IsNull(std::basic_string_view<Char> str) noexcept {
  return str.data() == nullptr;
}

template<typename ElemDst, typename ElemSrc>
constexpr inline std::basic_string_view<ElemDst> ViewCast(
  std::basic_string_view<ElemSrc> src) noexcept {
  static_assert(!std::is_same_v<ElemDst, ElemSrc>);
  static_assert(sizeof(ElemDst) == sizeof(ElemSrc));

  return {reinterpret_cast<const ElemDst*>(src.data()), src.size()};
}

namespace hash_utils {

size_t Hash(const char* value, size_t size) noexcept;
size_t Hash(const byte_type* value, size_t size) noexcept;

inline size_t Hash(bytes_view value) noexcept {
  return Hash(value.data(), value.size());
}
inline size_t Hash(std::string_view value) noexcept {
  return Hash(value.data(), value.size());
}
inline size_t Hash(const char* value) noexcept {
  return Hash(value, std::char_traits<char>::length(value));
}
inline size_t Hash(const wchar_t* value) noexcept {
  return Hash(reinterpret_cast<const char*>(value),
              std::char_traits<wchar_t>::length(value) * sizeof(wchar_t));
}

}  // namespace hash_utils
}  // namespace iresearch

namespace std {

template<>
struct hash<::iresearch::bstring> {
  size_t operator()(const ::iresearch::bstring& value) const noexcept {
    return ::iresearch::hash_utils::Hash(value);
  }
};

template<>
struct hash<::iresearch::bytes_view> {
  size_t operator()(::iresearch::bytes_view value) const noexcept {
    return ::iresearch::hash_utils::Hash(value);
  }
};

}  // namespace std
