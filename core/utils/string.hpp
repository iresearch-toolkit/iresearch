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
#include <cmath>
#include <cstring>
#include <cassert>
#include <vector>

#include "shared.hpp"

// ----------------------------------------------------------------------------
// --SECTION--                                                   std extensions
// ----------------------------------------------------------------------------

// MSVC++ > v14.0 (Visual Studio >2015) already implements this in <xstring>
// MacOS requires this definition to be before first usage (i.e. in bytes_ref)
#if !defined(_MSC_VER) || (_MSC_VER <= 1900)
namespace std {

// We define this specialization because default implementation
// for unsigned char doesn't implement it effective
template<>
struct char_traits<::iresearch::byte_type> {
  using char_type = ::iresearch::byte_type;
  using int_type = int;
  using pos_type = std::streampos;
  using off_type = std::streamoff;
  using state_type = std::mbstate_t;

  static constexpr void assign(char_type& c1, const char_type& c2) noexcept {
    c1 = c2;
  }

  static constexpr bool eq(const char_type& c1, const char_type& c2) noexcept {
    return c1 == c2;
  }

  static constexpr bool lt(const char_type& c1, const char_type& c2) noexcept {
    return (static_cast<unsigned char>(c1) < static_cast<unsigned char>(c2));
  }

  static constexpr int compare(const char_type* s1, const char_type* s2,
                               size_t n) noexcept {
    if (n == 0) {
      return 0;
    }
    return std::memcmp(s1, s2, n);
  }

  static constexpr const char_type* find(const char_type* s, size_t n,
                                         const char_type& a) noexcept {
    if (n == 0) {
      return nullptr;
    }
    return static_cast<const char_type*>(std::memchr(s, a, n));
  }

  static constexpr char_type* move(char_type* s1, const char_type* s2,
                                   size_t n) noexcept {
    if (n == 0) {
      return s1;
    }
    return static_cast<char_type*>(std::memmove(s1, s2, n));
  }

  static constexpr char_type* copy(char_type* s1, const char_type* s2,
                                   size_t n) noexcept {
    if (n == 0) {
      return s1;
    }
    return static_cast<char_type*>(std::memcpy(s1, s2, n));
  }

  static constexpr char_type* assign(char_type* s, size_t n,
                                     char_type a) noexcept {
    if (n == 0) {
      return s;
    }
    return static_cast<char_type*>(std::memset(s, a, n));
  }

  static constexpr char_type to_char_type(const int_type& c) noexcept {
    return static_cast<char_type>(c);
  }

  // To keep both the byte 0xff and the eof symbol 0xffffffff
  // from ending up as 0xffffffff.
  static constexpr int_type to_int_type(const char_type& c) noexcept {
    return static_cast<int_type>(static_cast<unsigned char>(c));
  }

  static constexpr bool eq_int_type(const int_type& c1,
                                    const int_type& c2) noexcept {
    return c1 == c2;
  }

  static constexpr int_type eof() noexcept { return static_cast<int_type>(-1); }

  static constexpr int_type not_eof(const int_type& c) noexcept {
    return (c == eof()) ? 0 : c;
  }
};

}  // namespace std
#endif

namespace iresearch {

// ----------------------------------------------------------------------------
// --SECTION--                                               binary std::string
// ----------------------------------------------------------------------------

typedef std::basic_string<byte_type> bstring;

//////////////////////////////////////////////////////////////////////////////
/// @class basic_string_ref
//////////////////////////////////////////////////////////////////////////////
template<typename Elem, typename Traits = std::char_traits<Elem>>
class basic_string_ref {
 public:
  typedef Traits traits_type;
  typedef Elem char_type;

  // beware of performing comparison against NIL value,
  // it may cause undefined behaviour in std::char_traits<Elem>
  // (e.g. becuase of memcmp function)
  IRESEARCH_HELPER_DLL_LOCAL static const basic_string_ref NIL;  // null string
  IRESEARCH_HELPER_DLL_LOCAL static const basic_string_ref
    EMPTY;  // empty string

  constexpr basic_string_ref() noexcept : data_(nullptr), size_(0) {}

  // Constructs a string reference object from a ref and a size.
  constexpr basic_string_ref(const basic_string_ref& ref, size_t size) noexcept
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
  constexpr basic_string_ref(const std::basic_string_view<Elem>& str) noexcept
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
  friend constexpr int compare(const basic_string_ref& lhs,
                               const char_type* rhs, size_t rhs_size) {
    const size_t lhs_size = lhs.size();
    int r =
      traits_type::compare(lhs.c_str(), rhs, (std::min)(lhs_size, rhs_size));

    if (!r) {
      r = int(lhs_size - rhs_size);
    }

    return r;
  }

  friend constexpr int compare(const basic_string_ref& lhs,
                               const std::basic_string<char_type>& rhs) {
    return compare(lhs, rhs.c_str(), rhs.size());
  }

  friend constexpr int compare(const basic_string_ref& lhs,
                               const char_type* rhs) {
    return compare(lhs, rhs, traits_type::length(rhs));
  }

  friend constexpr int compare(const basic_string_ref& lhs,
                               const basic_string_ref& rhs) {
    return compare(lhs, rhs.c_str(), rhs.size());
  }

  friend constexpr bool operator<(const basic_string_ref& lhs,
                                  const basic_string_ref& rhs) {
    return compare(lhs, rhs) < 0;
  }

  friend bool operator<(const std::basic_string<char_type>& lhs,
                        const basic_string_ref& rhs) {
    return lhs.compare(0, std::basic_string<char_type>::npos, rhs.c_str(),
                       rhs.size()) < 0;
  }

  friend constexpr bool operator>=(const basic_string_ref& lhs,
                                   const basic_string_ref& rhs) {
    return !(lhs < rhs);
  }

  friend constexpr bool operator>(const basic_string_ref& lhs,
                                  const basic_string_ref& rhs) {
    return compare(lhs, rhs) > 0;
  }

  friend constexpr bool operator<=(const basic_string_ref& lhs,
                                   const basic_string_ref& rhs) {
    return !(lhs > rhs);
  }

  friend constexpr bool operator==(const basic_string_ref& lhs,
                                   const basic_string_ref& rhs) {
    return 0 == compare(lhs, rhs);
  }

  friend constexpr bool operator!=(const basic_string_ref& lhs,
                                   const basic_string_ref& rhs) {
    return !(lhs == rhs);
  }

  friend std::basic_ostream<char_type, std::char_traits<char_type>>& operator<<(
    std::basic_ostream<char_type, std::char_traits<char_type>>& os,
    const basic_string_ref& d) {
    return os.write(d.c_str(), d.size());
  }

 protected:
  const char_type* data_;
  size_t size_;
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
inline constexpr bool starts_with(const basic_string_ref<_Elem, _Traits>& first,
                                  const _Elem* second, size_t second_size) {
  typedef typename basic_string_ref<_Elem, _Traits>::traits_type traits_type;

  return first.size() >= second_size &&
         0 == traits_type::compare(first.c_str(), second, second_size);
}

template<typename _Elem, typename _Traits>
inline bool starts_with(const std::basic_string<_Elem>& first,
                        const basic_string_ref<_Elem, _Traits>& second) {
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
  const basic_string_ref<Char, Traits>& lhs,
  const basic_string_ref<Char, Traits>& rhs) noexcept {
  return common_prefix_length(lhs.c_str(), lhs.size(), rhs.c_str(), rhs.size());
}

template<typename T, typename U>
inline void assign(std::basic_string<T>& str, const basic_string_ref<U>& ref) {
  static_assert(sizeof(T) == 1 && sizeof(T) == sizeof(U));
  str.assign(reinterpret_cast<const T*>(ref.c_str()), ref.size());
}

template<typename _Elem, typename _Traits>
inline constexpr bool starts_with(const basic_string_ref<_Elem, _Traits>& first,
                                  const _Elem* second) {
  return starts_with(first, second, _Traits::length(second));
}

template<typename Elem, typename Traits>
inline bool starts_with(const basic_string_ref<Elem, Traits>& first,
                        const std::basic_string<Elem>& second) {
  return starts_with(first, second.c_str(), second.size());
}

template<typename _Elem, typename _Traits>
inline constexpr bool starts_with(
  const basic_string_ref<_Elem, _Traits>& first,
  const basic_string_ref<_Elem, _Traits>& second) {
  return starts_with(first, second.c_str(), second.size());
}

typedef basic_string_ref<char> string_ref;
typedef basic_string_ref<byte_type> bytes_ref;

template<typename _ElemDst, typename _ElemSrc>
constexpr inline basic_string_ref<_ElemDst> ref_cast(
  const basic_string_ref<_ElemSrc>& src) {
  return basic_string_ref<_ElemDst>(
    reinterpret_cast<const _ElemDst*>(src.c_str()), src.size());
}

template<typename ElemDst, typename ElemSrc>
constexpr inline basic_string_ref<ElemDst> ref_cast(
  const std::basic_string<ElemSrc>& src) {
  return basic_string_ref<ElemDst>(
    reinterpret_cast<const ElemDst*>(src.c_str()), src.size());
}

template<typename ElemDst, typename ElemSrc>
constexpr inline basic_string_ref<ElemDst> ref_cast(
  const std::basic_string_view<ElemSrc>& src) {
  return basic_string_ref<ElemDst>(reinterpret_cast<const ElemDst*>(src.data()),
                                   src.size());
}

// ----------------------------------------------------------------------------
// --SECTION--                                        String hashing algorithms
// ----------------------------------------------------------------------------

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
  return irs::ref_cast<irs::byte_type>(irs::string_ref(src, size));
}

}  // namespace literals

}  // namespace iresearch

// ----------------------------------------------------------------------------
// --SECTION--                                                   std extensions
// ----------------------------------------------------------------------------

namespace std {

template<>
struct hash<char*> {
  size_t operator()(const char* value) const noexcept {
    return ::iresearch::hash_utils::hash(value);
  }
};  // hash

template<>
struct hash<wchar_t*> {
  size_t operator()(const wchar_t* value) const noexcept {
    return ::iresearch::hash_utils::hash(value);
  }
};  // hash

template<>
struct hash<::iresearch::bstring> {
  size_t operator()(const ::iresearch::bstring& value) const noexcept {
    return ::iresearch::hash_utils::hash(value);
  }
};  // hash

template<>
struct hash<::iresearch::bytes_ref> {
  size_t operator()(const ::iresearch::bytes_ref& value) const noexcept {
    return ::iresearch::hash_utils::hash(value);
  }
};  // hash

template<>
struct hash<::iresearch::string_ref> {
  size_t operator()(const ::iresearch::string_ref& value) const noexcept {
    return ::iresearch::hash_utils::hash(value);
  }
};  // hash

}  // namespace std

#endif
