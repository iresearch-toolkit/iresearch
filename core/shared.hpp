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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <bit>
#include <cfloat>  // for FLT_EVAL_METHOD

#include "types.hpp"  // iresearch types

////////////////////////////////////////////////////////////////////////////////
/// C++ standard
////////////////////////////////////////////////////////////////////////////////

#ifndef __cplusplus
#error C++ is required
#endif

#define IRESEARCH_CXX_98 199711L  // c++03/c++98
#define IRESEARCH_CXX_11 201103L  // c++11
#define IRESEARCH_CXX_14 201402L  // c++14
#define IRESEARCH_CXX_17 201703L  // c++17
#define IRESEARCH_CXX_20 202002L  // c++20

#if defined(_MSC_VER)
// MSVC doesn't honor __cplusplus macro,
// it always equals to IRESEARCH_CXX_98
// therefore we use _MSC_VER
#if _MSC_VER < 1920  // before MSVC2019
#error "at least C++17 is required"
#endif
#else  // GCC/Clang
#if __cplusplus < IRESEARCH_CXX_17
#error "at least C++17 is required"
#endif
#endif

#define IRESEARCH_CXX IRESEARCH_CXX_17

////////////////////////////////////////////////////////////////////////////////
/// Export/Import definitions
////////////////////////////////////////////////////////////////////////////////

// Generic helper definitions for shared library support
#if defined _MSC_VER || defined __CYGWIN__
#define IRESEARCH_HELPER_DLL_IMPORT __declspec(dllimport)
#define IRESEARCH_HELPER_DLL_EXPORT __declspec(dllexport)
#define IRESEARCH_HELPER_DLL_LOCAL
#define IRESEARCH_HELPER_TEMPLATE_IMPORT
#define IRESEARCH_HELPER_TEMPLATE_EXPORT

#if _MSC_VER < 1920  // before msvc2019
#error "compiler is not supported"
#endif

#define FORCE_INLINE inline __forceinline
#define NO_INLINE __declspec(noinline)
#define RESTRICT __restrict
#define IRS_NO_UNIQUE_ADDRESS [[msvc::no_unique_address]]
#else
#if ((defined(__GNUC__) && (__GNUC__ >= 10)) || \
     (defined(__clang__) && (__clang_major__ >= 11)))
#define IRESEARCH_HELPER_DLL_IMPORT __attribute__((visibility("default")))
#define IRESEARCH_HELPER_DLL_EXPORT __attribute__((visibility("default")))
#define IRESEARCH_HELPER_DLL_LOCAL __attribute__((visibility("hidden")))
#else  // before GCC9/clang11
#error "compiler is not supported"
#endif
#define IRESEARCH_HELPER_TEMPLATE_IMPORT IRESEARCH_HELPER_DLL_IMPORT
#define IRESEARCH_HELPER_TEMPLATE_EXPORT IRESEARCH_HELPER_DLL_EXPORT

#define FORCE_INLINE inline __attribute__((always_inline))
#define NO_INLINE __attribute__((noinline))
#define RESTRICT __restrict__
#define IRS_NO_UNIQUE_ADDRESS [[no_unique_address]]
#endif

// hook for MSVC-only code
#if defined(_MSC_VER) && !defined(__clang__)
#define MSVC_ONLY(...) __VA_ARGS__
#else
#define MSVC_ONLY(...)
#endif

// hook for MSVC2015-only code
#if defined(_MSC_VER) && _MSC_VER == 1900
#define MSVC2015_ONLY(...) __VA_ARGS__
#else
#define MSVC2015_ONLY(...)
#endif

// hook for GCC-only code
#if defined(__GNUC__)
#define GCC_ONLY(...) __VA_ARGS__
#else
#define GCC_ONLY(...)
#endif

// check if sizeof(float_t) == sizeof(double_t)
#if defined(FLT_EVAL_METHOD) && \
  ((FLT_EVAL_METHOD == 1) || (FLT_EVAL_METHOD == 2))
static_assert(sizeof(float_t) == sizeof(double_t),
              "sizeof(float_t) != sizeof(double_t)");
#define FLOAT_T_IS_DOUBLE_T
#else
static_assert(sizeof(float_t) != sizeof(double_t),
              "sizeof(float_t) == sizeof(double_t)");
#undef FLOAT_T_IS_DOUBLE_T
#endif

// Windows uses wchar_t for unicode handling
#if defined(_WIN32)
#define IR_NATIVE_STRING(s) L##s
#else
#define IR_NATIVE_STRING(s) s
#endif

// IRESEARCH_API is used for the public API symbols. It either DLL imports or
// DLL exports (or does nothing for static build)
#ifdef IRESEARCH_DLL
#ifdef IRESEARCH_DLL_EXPORTS
#define IRESEARCH_API IRESEARCH_HELPER_DLL_EXPORT
#else
#define IRESEARCH_API IRESEARCH_HELPER_DLL_IMPORT
#endif  // IRESEARCH_DLL_EXPORTS
#ifdef IRESEARCH_DLL_PLUGIN
#define IRESEARCH_PLUGIN_EXPORT extern "C" IRESEARCH_HELPER_DLL_EXPORT
#else
#define IRESEARCH_PLUGIN_EXPORT
#endif  // IRESEARCH_DLL_PLUGIN
#else   // IRESEARCH_DLL is not defined: this means IRESEARCH is a static lib.
#define IRESEARCH_API
#endif  // IRESEARCH_DLL

// MSVC 2015 does not define __cpp_lib_generic_associative_lookup macro
#if (defined(__cpp_lib_generic_associative_lookup) || \
     (defined(_MSC_VER) && _MSC_VER >= 1900))
#define IRESEARCH_GENERIC_ASSOCIATIVE_LOOKUP
#endif

#if defined(__GNUC__)
#define IRESEARCH_INIT(f)                           \
  static void f(void) __attribute__((constructor)); \
  static void f(void)
#define RESEARCH_FINI(f)                           \
  static void f(void) __attribute__((destructor)); \
  static void f(void)
#elif defined(_MSC_VER)
#define IRESEARCH_INIT(f)                                       \
  static void __cdecl f(void);                                  \
  static int f##_init_wrapper(void) {                           \
    f();                                                        \
    return 0;                                                   \
  }                                                             \
  __declspec(allocate(".CRT$XCU")) void(__cdecl * f##_)(void) = \
    f##_init_wrapper;                                           \
  static void __cdecl f(void)
#define RESEARCH_FINI(f)                                              \
  static void __cdecl f(void);                                        \
  static int f##_fini_wrapper(void) {                                 \
    atexit(f);                                                        \
    return 0;                                                         \
  }                                                                   \
  __declspec(allocate(".CRT$XCU")) static int(__cdecl * f##_)(void) = \
    f##_fini_wrapper;                                                 \
  static void __cdecl f(void)
#endif

// define function name used for pretty printing
// NOTE: the alias points to a compile time finction not a preprocessor macro
#if defined(__FUNCSIG__) || _MSC_FULL_VER >= 193000000
#define IRESEARCH_CURRENT_FUNCTION __FUNCSIG__
#elif defined(__PRETTY_FUNCTION__) || defined(__GNUC__) || defined(__clang__)
#define IRESEARCH_CURRENT_FUNCTION __PRETTY_FUNCTION__
#else
#error "compiler is not supported"
#endif

// IRESEARCH_COMPILER_HAS_FEATURE
#ifndef __has_feature
#define IRESEARCH_COMPILER_HAS_FEATURE(x) \
  0  // Compatibility with non-clang compilers.
#else
#define IRESEARCH_COMPILER_HAS_FEATURE(x) __has_feature(x)
#endif

// IRESEARCH_HAS_ATTRIBUTE
#ifndef __has_attribute
#define IRESEARCH_HAS_ATTRIBUTE(x) 0
#else
#define IRESEARCH_HAS_ATTRIBUTE(x) __has_attribute(x)
#endif

// IRESEARCH_ATTRIBUTE_NONNULL
#if IRESEARCH_HAS_ATTRIBUTE(nonnull) || \
  (defined(__GNUC__) && !defined(__clang__))
#define IRESEARCH_ATTRIBUTE_NONNULL(arg_index) \
  __attribute__((nonnull(arg_index)))
#else
#define IRESEARCH_ATTRIBUTE_NONNULL(...)
#endif

////////////////////////////////////////////////////////////////////////////////
/// SSE compatibility
////////////////////////////////////////////////////////////////////////////////

// for MSVC on x64 architecture SSE2 is always enabled
#if defined(__SSE2__) || defined(__ARM_NEON) || defined(__ARM_NEON__) || \
  (defined(_MSC_VER) && (defined(_M_AMD64) || defined(_M_X64)))
#define IRESEARCH_SSE2
#endif

#ifdef __SSE4_1__
#define IRESEARCH_SSE4_1
#endif

#ifdef __SSE4_2__
#define IRESEARCH_SSE4_2
#endif

#ifdef __AVX__
#define IRESEARCH_AVX
#endif

#ifdef __AVX2__
#define IRESEARCH_AVX2
#endif

////////////////////////////////////////////////////////////////////////////////

// likely/unlikely branch indicator
// macro definitions similar to the ones at
// https://kernelnewbies.org/FAQ/LikelyUnlikely
#if defined(__GNUC__) || defined(__GNUG__)
#define IRS_LIKELY(v) __builtin_expect(!!(v), 1)
#define IRS_UNLIKELY(v) __builtin_expect(!!(v), 0)
#else
#define IRS_LIKELY(v) v
#define IRS_UNLIKELY(v) v
#endif

#ifdef IRESEARCH_DEBUG
#define IRS_ASSERT(CHECK) ((CHECK) ? void(0) : [] { assert(!#CHECK); }())
#else
#define IRS_ASSERT(CHECK) void(0)
#endif

#define UNUSED(par) (void)(par)

namespace iresearch {
consteval bool is_big_endian() noexcept {
  return std::endian::native == std::endian::big;
}
}  // namespace iresearch
namespace irs = ::iresearch;

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

// CMPXCHG16B requires that the destination
// (memory) operand be 16-byte aligned
#define IRESEARCH_CMPXCHG16B_ALIGNMENT 16
