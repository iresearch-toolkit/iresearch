//
// IResearch search engine 
//
// Copyright (c) 2016 by EMC Corporation, All Rights Reserved
//
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
//

#ifndef IRESEARCH_TYPE_UTILS_H
#define IRESEARCH_TYPE_UTILS_H

#include "shared.hpp"

NS_ROOT

// ----------------------------------------------------------------------------
// template type traits
// ----------------------------------------------------------------------------

template <typename... Types>
struct template_traits_t;

template<typename First, typename... Second>
struct template_traits_t<First, Second...> {
  static CONSTEXPR size_t count() NOEXCEPT {
    return 1 + template_traits_t<Second...>::count();
  }

  static CONSTEXPR size_t size() NOEXCEPT {
    return sizeof(First) + template_traits_t<Second...>::size();
  }

  static CONSTEXPR size_t size_aligned(size_t start = 0) NOEXCEPT {
    return template_traits_t<Second...>::size_aligned(
      template_traits_t<First>::size_max_aligned(start)
    );
  }

  static CONSTEXPR size_t size_max(size_t max = 0) NOEXCEPT {
    return template_traits_t<Second...>::size_max(
      max < sizeof(First) ? sizeof(First) : max
    );
  }

  static CONSTEXPR size_t size_max_aligned(size_t start = 0, size_t max = 0) NOEXCEPT {
    auto size =
      start
      + ((std::alignment_of<First>() - (start % std::alignment_of<First>())) % std::alignment_of<First>()) // padding
      + sizeof(First)
      ;

    return template_traits_t<Second...>::size_max_aligned(
      start, max < size ? size : max
    );
  }
};

template<>
struct template_traits_t<> {
  static CONSTEXPR size_t count() NOEXCEPT {
    return 0;
  }

  static CONSTEXPR size_t size() NOEXCEPT {
    return 0;
  }

  static CONSTEXPR size_t size_aligned(size_t start = 0) NOEXCEPT {
    return start;
  }

  static CONSTEXPR size_t size_max(size_t max = 0) NOEXCEPT {
    return max;
  }

  static CONSTEXPR size_t size_max_aligned(size_t start = 0, size_t max = 0) NOEXCEPT {
    return max ? max : start;
  }
};

NS_END

#endif