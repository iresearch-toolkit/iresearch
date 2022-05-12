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

#ifndef IRESEARCH_STRING_H
#define IRESEARCH_STRING_H

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <vector>

#include "bit_utils.hpp"
#include "shared.hpp"

namespace std {

#if defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wtautological-pointer-compare"
#endif

// MSVC++ > v14.0 (Visual Studio >2015) already implements this in <xstring>
// MacOS requires this definition to be before first usage (i.e. in bytes_ref)
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

// Binary std::string
using bstring = std::basic_string<byte_type>;

template<typename Elem, typename Traits = std::char_traits<Elem>>
class basic_string_ref {
 public:
  typedef Traits traits_type;
  typedef Elem char_type;

  // beware of performing comparison against NIL value,
  // it may cause undefined behaviour in std::char_traits<Elem>
  // (e.g. becuase of memcmp function)

  // null string
  static const basic_string_ref NIL;
  // empty string
  static const basic_string_ref EMPTY;

  constexpr basic_string_ref() noexcept = default;

  // Constructs a string reference object from a ref and a size.
  constexpr basic_string_ref(basic_string_ref ref, size_t size) noexcept
      : data_(ref.data_), size_(size) {
    IRS_ASSERT(size <= ref.size_);
  }

  // Constructs a string reference object from a C string and a size.
  constexpr basic_string_ref(const char_type* s, size_t size) noexcept
      : data_(s), size_(size) {}

  // Constructs a string reference object from a C string computing
  // the size with ``std::char_traits<Char>::length``.
  // cppcheck-suppress noExplicitConstructor
  constexpr basic_string_ref(const char_type* s) noexcept
      : data_(s), size_(s ? traits_type::length(s) : 0) {}

  // cppcheck-suppress noExplicitConstructor
  constexpr basic_string_ref(const std::basic_string<char_type>& s) noexcept
      : data_(s.c_str()), size_(s.size()) {}

  // Constructs a string reference object from a std::basic_string_view<Elem>
  // cppcheck-suppress noExplicitConstructor
  constexpr basic_string_ref(std::basic_string_view<Elem> str) noexcept
      : data_(str.data()), size_(str.size()) {}

  constexpr basic_string_ref(const std::basic_string<char_type>& str,
                             size_t size) noexcept
      : data_(str.c_str()), size_(size) {}

  constexpr const char_type& operator[](size_t i) const noexcept {
    return IRS_ASSERT(i < size_), data_[i];
  }

  // Returns the pointer to a C string.
  constexpr const char_type* c_str() const noexcept { return data_; }
  constexpr const char_type* data() const noexcept { return data_; }

  // Returns the string size.
  constexpr size_t size() const noexcept { return size_; }

  constexpr bool null() const noexcept { return nullptr == data_; }
  constexpr bool empty() const noexcept { return null() || 0 == size_; }
  constexpr const char_type* begin() const noexcept { return data_; }
  constexpr const char_type* end() const noexcept { return data_ + size_; }

  constexpr std::reverse_iterator<const char_type*> rbegin() const noexcept {
    return std::make_reverse_iterator(end());
  }
  constexpr std::reverse_iterator<const char_type*> rend() const noexcept {
    return std::make_reverse_iterator(begin());
  }

  constexpr const char_type& back() const noexcept {
    return IRS_ASSERT(!empty()), data_[size() - 1];
  }

  constexpr const char_type& front() const noexcept {
    return IRS_ASSERT(!empty()), data_[0];
  }

  constexpr explicit operator std::basic_string<char_type>() const {
    return std::basic_string<char_type>(data_, size_);
  }

  constexpr operator std::basic_string_view<char_type>() const {
    return std::basic_string_view<char_type>(data_, size_);
  }

  // friends
  friend constexpr int compare(basic_string_ref lhs, const char_type* rhs,
                               size_t rhs_size) {
    const size_t lhs_size = lhs.size();
    int r =
        traits_type::compare(lhs.c_str(), rhs, (std::min)(lhs_size, rhs_size));

    if (!r) {
      r = int(lhs_size - rhs_size);
    }

    return r;
  }

  friend constexpr int compare(basic_string_ref lhs,
                               const std::basic_string<char_type>& rhs) {
    return compare(lhs, rhs.c_str(), rhs.size());
  }

  friend constexpr int compare(basic_string_ref lhs, const char_type* rhs) {
    return compare(lhs, rhs, traits_type::length(rhs));
  }

  friend constexpr int compare(basic_string_ref lhs, basic_string_ref rhs) {
    return compare(lhs, rhs.c_str(), rhs.size());
  }

  friend constexpr bool operator<(basic_string_ref lhs, basic_string_ref rhs) {
    return compare(lhs, rhs) < 0;
  }

  friend bool operator<(const std::basic_string<char_type>& lhs,
                        basic_string_ref rhs) {
    return lhs.compare(0, std::basic_string<char_type>::npos, rhs.c_str(),
                       rhs.size()) < 0;
  }

  friend constexpr bool operator>=(basic_string_ref lhs, basic_string_ref rhs) {
    return !(lhs < rhs);
  }

  friend constexpr bool operator>(basic_string_ref lhs, basic_string_ref rhs) {
    return compare(lhs, rhs) > 0;
  }

  friend constexpr bool operator<=(basic_string_ref lhs, basic_string_ref rhs) {
    return !(lhs > rhs);
  }

  friend constexpr bool operator==(basic_string_ref lhs, basic_string_ref rhs) {
    return 0 == compare(lhs, rhs);
  }

  friend constexpr bool operator!=(basic_string_ref lhs, basic_string_ref rhs) {
    return !(lhs == rhs);
  }

  friend std::basic_ostream<char_type, std::char_traits<char_type>>& operator<<(
      std::basic_ostream<char_type, std::char_traits<char_type>>& os,
      const basic_string_ref& d) {
    return os.write(d.c_str(), d.size());
  }

 protected:
  const char_type* data_{};
  size_t size_{};
};  // basic_string_ref

template<typename Elem, typename Traits>
/*static*/ const basic_string_ref<Elem, Traits>
    basic_string_ref<Elem, Traits>::NIL(nullptr, 0);

template<typename Elem, typename Traits>
/*static*/ const basic_string_ref<Elem, Traits>
    basic_string_ref<Elem, Traits>::EMPTY(reinterpret_cast<const Elem*>(""),
                                          0  // FIXME
    );

template<typename _Elem, typename _Traits>
inline constexpr bool starts_with(basic_string_ref<_Elem, _Traits> first,
                                  const _Elem* second, size_t second_size) {
  typedef typename basic_string_ref<_Elem, _Traits>::traits_type traits_type;

  return first.size() >= second_size &&
         0 == traits_type::compare(first.c_str(), second, second_size);
}

template<typename _Elem, typename _Traits>
inline bool starts_with(const std::basic_string<_Elem>& first,
                        basic_string_ref<_Elem, _Traits> second) {
  return 0 == first.compare(0, second.size(), second.c_str(), second.size());
}

template<typename _Elem>
inline bool starts_with(const std::basic_string<_Elem>& first,
                        const std::basic_string<_Elem>& second) {
  return 0 == first.compare(0, second.size(), second.c_str(), second.size());
}

template<typename Char>
inline size_t common_prefix_length(const Char* lhs, size_t lhs_size,
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
inline size_t common_prefix_length(
    basic_string_ref<Char, Traits> lhs,
    basic_string_ref<Char, Traits> rhs) noexcept {
  return common_prefix_length(lhs.c_str(), lhs.size(), rhs.c_str(), rhs.size());
}

template<typename T, typename U>
inline void assign(std::basic_string<T>& str, const basic_string_ref<U>& ref) {
  static_assert(sizeof(T) == 1 && sizeof(T) == sizeof(U));
  str.assign(reinterpret_cast<const T*>(ref.c_str()), ref.size());
}

template<typename _Elem, typename _Traits>
inline constexpr bool starts_with(basic_string_ref<_Elem, _Traits> first,
                                  const _Elem* second) {
  return starts_with(first, second, _Traits::length(second));
}

template<typename Elem, typename Traits>
inline bool starts_with(basic_string_ref<Elem, Traits> first,
                        const std::basic_string<Elem>& second) {
  return starts_with(first, second.c_str(), second.size());
}

template<typename _Elem, typename _Traits>
inline constexpr bool starts_with(basic_string_ref<_Elem, _Traits> first,
                                  basic_string_ref<_Elem, _Traits> second) {
  return starts_with(first, second.c_str(), second.size());
}

using string_ref = basic_string_ref<char>;
using bytes_ref = basic_string_ref<byte_type>;

template<typename ElemDst, typename ElemSrc>
constexpr basic_string_ref<ElemDst> ref_cast(
    basic_string_ref<ElemSrc> src) noexcept {
  return irs::bit_cast<basic_string_ref<ElemDst>>(src);
}

template<typename ElemDst, typename ElemSrc>
constexpr basic_string_ref<ElemDst> ref_cast(
    std::basic_string_view<ElemSrc> src) noexcept {
  return irs::bit_cast<basic_string_ref<ElemDst>>(
      basic_string_ref<ElemSrc>{src});
}

template<typename ElemDst, typename ElemSrc>
constexpr basic_string_ref<ElemDst> ref_cast(
    const std::basic_string<ElemSrc>& src) noexcept {
  return irs::bit_cast<basic_string_ref<ElemDst>>(
      basic_string_ref<ElemSrc>{src});
}

namespace hash_utils {

size_t hash(const char* value, size_t size) noexcept;
size_t hash(const byte_type* value, size_t size) noexcept;

inline size_t hash(bytes_ref value) noexcept {
  return hash(value.c_str(), value.size());
}
inline size_t hash(string_ref value) noexcept {
  return hash(value.c_str(), value.size());
}
inline size_t hash(const char* value) noexcept {
  return hash(value, std::char_traits<char>::length(value));
}
inline size_t hash(const wchar_t* value) noexcept {
  return hash(reinterpret_cast<const char*>(value),
              std::char_traits<wchar_t>::length(value) * sizeof(wchar_t));
}

}  // namespace hash_utils

namespace literals {

FORCE_INLINE constexpr irs::string_ref operator"" _sr(const char* src,
                                                      size_t size) noexcept {
  return irs::string_ref(src, size);
}

FORCE_INLINE constexpr irs::bytes_ref operator"" _bsr(const char* src,
                                                      size_t size) noexcept {
  return irs::ref_cast<irs::byte_type>(irs::string_ref{src, size});
}

}  // namespace literals

}  // namespace iresearch

namespace std {

template<>
struct hash<char*> {
  size_t operator()(const char* value) const noexcept {
    return ::iresearch::hash_utils::hash(value);
  }
};

template<>
struct hash<wchar_t*> {
  size_t operator()(const wchar_t* value) const noexcept {
    return ::iresearch::hash_utils::hash(value);
  }
};

template<>
struct hash<::iresearch::bstring> {
  size_t operator()(const ::iresearch::bstring& value) const noexcept {
    return ::iresearch::hash_utils::hash(value);
  }
};

template<>
struct hash<::iresearch::bytes_ref> {
  size_t operator()(const ::iresearch::bytes_ref& value) const noexcept {
    return ::iresearch::hash_utils::hash(value);
  }
};

template<>
struct hash<::iresearch::string_ref> {
  size_t operator()(const ::iresearch::string_ref& value) const noexcept {
    return ::iresearch::hash_utils::hash(value);
  }
};

}  // namespace std

#endif
