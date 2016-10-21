//
// IResearch search engine 
// 
// Copyright © 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#include "shared.hpp"
#include "bit_packing.hpp"

#include <cassert>
#include <cstring>

NS_LOCAL

MSVC_ONLY(__pragma(warning(push)))
MSVC_ONLY(__pragma(warning(disable:4127))) // constexp conditionals are intended to be optimized out
MSVC_ONLY(__pragma(warning(disable:4293))) // all negative shifts are masked by constexpr conditionals
MSVC_ONLY(__pragma(warning(disable:4724))) // all X % zero are masked by constexpr conditionals (must disable outside of fn body)
template<int N>
void __fastpack(const uint32_t* RESTRICT in, uint32_t* RESTRICT out) {
  // 32 == sizeof(uint32_t) * 8
  static_assert(N > 0 && N <= 32, "N <= 0 || N > 32");
  // ensure all computations are constexr, i.e. no conditional jumps, no loops, no variable increment/decrement
        *out |= ((*in) % (1U << N)) << (N *  0) % 32; if (((N *  1) % 32) < ((N *  0) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N *  1) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N *  1) % 32; if (((N *  2) % 32) < ((N *  1) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N *  2) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N *  2) % 32; if (((N *  3) % 32) < ((N *  2) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N *  3) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N *  3) % 32; if (((N *  4) % 32) < ((N *  3) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N *  4) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N *  4) % 32; if (((N *  5) % 32) < ((N *  4) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N *  5) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N *  5) % 32; if (((N *  6) % 32) < ((N *  5) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N *  6) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N *  6) % 32; if (((N *  7) % 32) < ((N *  6) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N *  7) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N *  7) % 32; if (((N *  8) % 32) < ((N *  7) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N *  8) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N *  8) % 32; if (((N *  9) % 32) < ((N *  8) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N *  9) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N *  9) % 32; if (((N * 10) % 32) < ((N *  9) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 10) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 10) % 32; if (((N * 11) % 32) < ((N * 10) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 11) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 11) % 32; if (((N * 12) % 32) < ((N * 11) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 12) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 12) % 32; if (((N * 13) % 32) < ((N * 12) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 13) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 13) % 32; if (((N * 14) % 32) < ((N * 13) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 14) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 14) % 32; if (((N * 15) % 32) < ((N * 14) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 15) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 15) % 32; if (((N * 16) % 32) < ((N * 15) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 16) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 16) % 32; if (((N * 17) % 32) < ((N * 16) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 17) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 17) % 32; if (((N * 18) % 32) < ((N * 17) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 18) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 18) % 32; if (((N * 19) % 32) < ((N * 18) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 19) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 19) % 32; if (((N * 20) % 32) < ((N * 19) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 20) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 20) % 32; if (((N * 21) % 32) < ((N * 20) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 21) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 21) % 32; if (((N * 22) % 32) < ((N * 21) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 22) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 22) % 32; if (((N * 23) % 32) < ((N * 22) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 23) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 23) % 32; if (((N * 24) % 32) < ((N * 23) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 24) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 24) % 32; if (((N * 25) % 32) < ((N * 24) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 25) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 25) % 32; if (((N * 26) % 32) < ((N * 25) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 26) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 26) % 32; if (((N * 27) % 32) < ((N * 26) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 27) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 27) % 32; if (((N * 28) % 32) < ((N * 27) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 28) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 28) % 32; if (((N * 29) % 32) < ((N * 28) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 29) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 29) % 32; if (((N * 30) % 32) < ((N * 29) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 30) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 30) % 32; if (((N * 31) % 32) < ((N * 30) % 32)) { ++out; *out |= ((*in) % (1U << N)) >> (N - ((N * 31) % 32)); }
  ++in; *out |= ((*in) % (1U << N)) << (N * 31) % 32;
}
MSVC_ONLY(__pragma(warning(pop)))

template<>
void __fastpack<32>(const uint32_t* RESTRICT in, uint32_t* RESTRICT out) {
  std::memcpy(out, in, sizeof(uint32_t)*iresearch::packed::BLOCK_SIZE_32);
}

MSVC_ONLY(__pragma(warning(push)))
MSVC_ONLY(__pragma(warning(disable:4127))) // constexp conditionals are intended to be optimized out
MSVC_ONLY(__pragma(warning(disable:4293))) // all negative shifts are masked by constexpr conditionals
MSVC_ONLY(__pragma(warning(disable:4724))) // all X % zero are masked by constexpr conditionals (must disable outside of fn body)
template<int N>
void __fastpack(const uint64_t* RESTRICT in, uint64_t* RESTRICT out) {
  // 64 == sizeof(uint64_t) * 8
  static_assert(N > 0 && N <= 64, "N <= 0 || N > 64");
  // ensure all computations are constexr, i.e. no conditional jumps, no loops, no variable increment/decrement
        *out |= ((*in) % (1ULL << N)) << (N *  0) % 64; if (((N *  1) % 64) < ((N *  0) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N *  1) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N *  1) % 64; if (((N *  2) % 64) < ((N *  1) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N *  2) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N *  2) % 64; if (((N *  3) % 64) < ((N *  2) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N *  3) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N *  3) % 64; if (((N *  4) % 64) < ((N *  3) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N *  4) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N *  4) % 64; if (((N *  5) % 64) < ((N *  4) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N *  5) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N *  5) % 64; if (((N *  6) % 64) < ((N *  5) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N *  6) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N *  6) % 64; if (((N *  7) % 64) < ((N *  6) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N *  7) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N *  7) % 64; if (((N *  8) % 64) < ((N *  7) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N *  8) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N *  8) % 64; if (((N *  9) % 64) < ((N *  8) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N *  9) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N *  9) % 64; if (((N * 10) % 64) < ((N *  9) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 10) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 10) % 64; if (((N * 11) % 64) < ((N * 10) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 11) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 11) % 64; if (((N * 12) % 64) < ((N * 11) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 12) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 12) % 64; if (((N * 13) % 64) < ((N * 12) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 13) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 13) % 64; if (((N * 14) % 64) < ((N * 13) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 14) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 14) % 64; if (((N * 15) % 64) < ((N * 14) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 15) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 15) % 64; if (((N * 16) % 64) < ((N * 15) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 16) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 16) % 64; if (((N * 17) % 64) < ((N * 16) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 17) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 17) % 64; if (((N * 18) % 64) < ((N * 17) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 18) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 18) % 64; if (((N * 19) % 64) < ((N * 18) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 19) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 19) % 64; if (((N * 20) % 64) < ((N * 19) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 20) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 20) % 64; if (((N * 21) % 64) < ((N * 20) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 21) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 21) % 64; if (((N * 22) % 64) < ((N * 21) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 22) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 22) % 64; if (((N * 23) % 64) < ((N * 22) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 23) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 23) % 64; if (((N * 24) % 64) < ((N * 23) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 24) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 24) % 64; if (((N * 25) % 64) < ((N * 24) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 25) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 25) % 64; if (((N * 26) % 64) < ((N * 25) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 26) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 26) % 64; if (((N * 27) % 64) < ((N * 26) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 27) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 27) % 64; if (((N * 28) % 64) < ((N * 27) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 28) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 28) % 64; if (((N * 29) % 64) < ((N * 28) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 29) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 29) % 64; if (((N * 30) % 64) < ((N * 29) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 30) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 30) % 64; if (((N * 31) % 64) < ((N * 30) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 31) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 31) % 64; if (((N * 32) % 64) < ((N * 31) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 32) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 32) % 64; if (((N * 33) % 64) < ((N * 32) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 33) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 33) % 64; if (((N * 34) % 64) < ((N * 33) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 34) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 34) % 64; if (((N * 35) % 64) < ((N * 34) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 35) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 35) % 64; if (((N * 36) % 64) < ((N * 35) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 36) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 36) % 64; if (((N * 37) % 64) < ((N * 36) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 37) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 37) % 64; if (((N * 38) % 64) < ((N * 37) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 38) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 38) % 64; if (((N * 39) % 64) < ((N * 38) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 39) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 39) % 64; if (((N * 40) % 64) < ((N * 39) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 40) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 40) % 64; if (((N * 41) % 64) < ((N * 40) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 41) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 41) % 64; if (((N * 42) % 64) < ((N * 41) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 42) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 42) % 64; if (((N * 43) % 64) < ((N * 42) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 43) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 43) % 64; if (((N * 44) % 64) < ((N * 43) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 44) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 44) % 64; if (((N * 45) % 64) < ((N * 44) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 45) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 45) % 64; if (((N * 46) % 64) < ((N * 45) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 46) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 46) % 64; if (((N * 47) % 64) < ((N * 46) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 47) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 47) % 64; if (((N * 48) % 64) < ((N * 47) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 48) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 48) % 64; if (((N * 49) % 64) < ((N * 48) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 49) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 49) % 64; if (((N * 50) % 64) < ((N * 49) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 50) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 50) % 64; if (((N * 51) % 64) < ((N * 50) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 51) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 51) % 64; if (((N * 52) % 64) < ((N * 51) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 52) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 52) % 64; if (((N * 53) % 64) < ((N * 52) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 53) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 53) % 64; if (((N * 54) % 64) < ((N * 53) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 54) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 54) % 64; if (((N * 55) % 64) < ((N * 54) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 55) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 55) % 64; if (((N * 56) % 64) < ((N * 55) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 56) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 56) % 64; if (((N * 57) % 64) < ((N * 56) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 57) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 57) % 64; if (((N * 58) % 64) < ((N * 57) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 58) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 58) % 64; if (((N * 59) % 64) < ((N * 58) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 59) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 59) % 64; if (((N * 60) % 64) < ((N * 59) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 60) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 60) % 64; if (((N * 61) % 64) < ((N * 60) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 61) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 61) % 64; if (((N * 62) % 64) < ((N * 61) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 62) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 62) % 64; if (((N * 63) % 64) < ((N * 62) % 64)) { ++out; *out |= ((*in) % (1ULL << N)) >> (N - ((N * 63) % 64)); }
  ++in; *out |= ((*in) % (1ULL << N)) << (N * 63) % 64;
}
MSVC_ONLY(__pragma(warning(pop)))

template<>
void __fastpack<64>(const uint64_t* RESTRICT in, uint64_t* RESTRICT out) {
  std::memcpy(out, in, sizeof(uint64_t)*iresearch::packed::BLOCK_SIZE_64);
}

MSVC_ONLY(__pragma(warning(push)))
MSVC_ONLY(__pragma(warning(disable:4127))) // constexp conditionals are intended to be optimized out
MSVC_ONLY(__pragma(warning(disable:4293))) // all negative shifts are masked by constexpr conditionals
MSVC_ONLY(__pragma(warning(disable:4724))) // all X % zero are masked by constexpr conditionals (must disable outside of fn body)
template<int N>
void __fastunpack(const uint32_t* RESTRICT in, uint32_t* RESTRICT out) {
  // 32 == sizeof(uint32_t) * 8
  static_assert(N > 0 && N <= 32, "N <= 0 || N > 32");
         *out = ((*in) >> (N *  0) % 32) % (1U << N); if (((N *  1) % 32) < ((N *  0) % 32)) { ++in; *out |= ((*in) % (1U << (N *  1) % 32)) << (N - ((N *  1) % 32)); }
  out++; *out = ((*in) >> (N *  1) % 32) % (1U << N); if (((N *  2) % 32) < ((N *  1) % 32)) { ++in; *out |= ((*in) % (1U << (N *  2) % 32)) << (N - ((N *  2) % 32)); }
  out++; *out = ((*in) >> (N *  2) % 32) % (1U << N); if (((N *  3) % 32) < ((N *  2) % 32)) { ++in; *out |= ((*in) % (1U << (N *  3) % 32)) << (N - ((N *  3) % 32)); }
  out++; *out = ((*in) >> (N *  3) % 32) % (1U << N); if (((N *  4) % 32) < ((N *  3) % 32)) { ++in; *out |= ((*in) % (1U << (N *  4) % 32)) << (N - ((N *  4) % 32)); }
  out++; *out = ((*in) >> (N *  4) % 32) % (1U << N); if (((N *  5) % 32) < ((N *  4) % 32)) { ++in; *out |= ((*in) % (1U << (N *  5) % 32)) << (N - ((N *  5) % 32)); }
  out++; *out = ((*in) >> (N *  5) % 32) % (1U << N); if (((N *  6) % 32) < ((N *  5) % 32)) { ++in; *out |= ((*in) % (1U << (N *  6) % 32)) << (N - ((N *  6) % 32)); }
  out++; *out = ((*in) >> (N *  6) % 32) % (1U << N); if (((N *  7) % 32) < ((N *  6) % 32)) { ++in; *out |= ((*in) % (1U << (N *  7) % 32)) << (N - ((N *  7) % 32)); }
  out++; *out = ((*in) >> (N *  7) % 32) % (1U << N); if (((N *  8) % 32) < ((N *  7) % 32)) { ++in; *out |= ((*in) % (1U << (N *  8) % 32)) << (N - ((N *  8) % 32)); }
  out++; *out = ((*in) >> (N *  8) % 32) % (1U << N); if (((N *  9) % 32) < ((N *  8) % 32)) { ++in; *out |= ((*in) % (1U << (N *  9) % 32)) << (N - ((N *  9) % 32)); }
  out++; *out = ((*in) >> (N *  9) % 32) % (1U << N); if (((N * 10) % 32) < ((N *  9) % 32)) { ++in; *out |= ((*in) % (1U << (N * 10) % 32)) << (N - ((N * 10) % 32)); }
  out++; *out = ((*in) >> (N * 10) % 32) % (1U << N); if (((N * 11) % 32) < ((N * 10) % 32)) { ++in; *out |= ((*in) % (1U << (N * 11) % 32)) << (N - ((N * 11) % 32)); }
  out++; *out = ((*in) >> (N * 11) % 32) % (1U << N); if (((N * 12) % 32) < ((N * 11) % 32)) { ++in; *out |= ((*in) % (1U << (N * 12) % 32)) << (N - ((N * 12) % 32)); }
  out++; *out = ((*in) >> (N * 12) % 32) % (1U << N); if (((N * 13) % 32) < ((N * 12) % 32)) { ++in; *out |= ((*in) % (1U << (N * 13) % 32)) << (N - ((N * 13) % 32)); }
  out++; *out = ((*in) >> (N * 13) % 32) % (1U << N); if (((N * 14) % 32) < ((N * 13) % 32)) { ++in; *out |= ((*in) % (1U << (N * 14) % 32)) << (N - ((N * 14) % 32)); }
  out++; *out = ((*in) >> (N * 14) % 32) % (1U << N); if (((N * 15) % 32) < ((N * 14) % 32)) { ++in; *out |= ((*in) % (1U << (N * 15) % 32)) << (N - ((N * 15) % 32)); }
  out++; *out = ((*in) >> (N * 15) % 32) % (1U << N); if (((N * 16) % 32) < ((N * 15) % 32)) { ++in; *out |= ((*in) % (1U << (N * 16) % 32)) << (N - ((N * 16) % 32)); }
  out++; *out = ((*in) >> (N * 16) % 32) % (1U << N); if (((N * 17) % 32) < ((N * 16) % 32)) { ++in; *out |= ((*in) % (1U << (N * 17) % 32)) << (N - ((N * 17) % 32)); }
  out++; *out = ((*in) >> (N * 17) % 32) % (1U << N); if (((N * 18) % 32) < ((N * 17) % 32)) { ++in; *out |= ((*in) % (1U << (N * 18) % 32)) << (N - ((N * 18) % 32)); }
  out++; *out = ((*in) >> (N * 18) % 32) % (1U << N); if (((N * 19) % 32) < ((N * 18) % 32)) { ++in; *out |= ((*in) % (1U << (N * 19) % 32)) << (N - ((N * 19) % 32)); }
  out++; *out = ((*in) >> (N * 19) % 32) % (1U << N); if (((N * 20) % 32) < ((N * 19) % 32)) { ++in; *out |= ((*in) % (1U << (N * 20) % 32)) << (N - ((N * 20) % 32)); }
  out++; *out = ((*in) >> (N * 20) % 32) % (1U << N); if (((N * 21) % 32) < ((N * 20) % 32)) { ++in; *out |= ((*in) % (1U << (N * 21) % 32)) << (N - ((N * 21) % 32)); }
  out++; *out = ((*in) >> (N * 21) % 32) % (1U << N); if (((N * 22) % 32) < ((N * 21) % 32)) { ++in; *out |= ((*in) % (1U << (N * 22) % 32)) << (N - ((N * 22) % 32)); }
  out++; *out = ((*in) >> (N * 22) % 32) % (1U << N); if (((N * 23) % 32) < ((N * 22) % 32)) { ++in; *out |= ((*in) % (1U << (N * 23) % 32)) << (N - ((N * 23) % 32)); }
  out++; *out = ((*in) >> (N * 23) % 32) % (1U << N); if (((N * 24) % 32) < ((N * 23) % 32)) { ++in; *out |= ((*in) % (1U << (N * 24) % 32)) << (N - ((N * 24) % 32)); }
  out++; *out = ((*in) >> (N * 24) % 32) % (1U << N); if (((N * 25) % 32) < ((N * 24) % 32)) { ++in; *out |= ((*in) % (1U << (N * 25) % 32)) << (N - ((N * 25) % 32)); }
  out++; *out = ((*in) >> (N * 25) % 32) % (1U << N); if (((N * 26) % 32) < ((N * 25) % 32)) { ++in; *out |= ((*in) % (1U << (N * 26) % 32)) << (N - ((N * 26) % 32)); }
  out++; *out = ((*in) >> (N * 26) % 32) % (1U << N); if (((N * 27) % 32) < ((N * 26) % 32)) { ++in; *out |= ((*in) % (1U << (N * 27) % 32)) << (N - ((N * 27) % 32)); }
  out++; *out = ((*in) >> (N * 27) % 32) % (1U << N); if (((N * 28) % 32) < ((N * 27) % 32)) { ++in; *out |= ((*in) % (1U << (N * 28) % 32)) << (N - ((N * 28) % 32)); }
  out++; *out = ((*in) >> (N * 28) % 32) % (1U << N); if (((N * 29) % 32) < ((N * 28) % 32)) { ++in; *out |= ((*in) % (1U << (N * 29) % 32)) << (N - ((N * 29) % 32)); }
  out++; *out = ((*in) >> (N * 29) % 32) % (1U << N); if (((N * 30) % 32) < ((N * 29) % 32)) { ++in; *out |= ((*in) % (1U << (N * 30) % 32)) << (N - ((N * 30) % 32)); }
  out++; *out = ((*in) >> (N * 30) % 32) % (1U << N); if (((N * 31) % 32) < ((N * 30) % 32)) { ++in; *out |= ((*in) % (1U << (N * 31) % 32)) << (N - ((N * 31) % 32)); }
  out++; *out = ((*in) >> (N * 31) % 32) % (1U << N);
}
MSVC_ONLY(__pragma(warning(pop)))

template<>
void __fastunpack<32>(const uint32_t* RESTRICT in, uint32_t* RESTRICT out) {
  std::memcpy(out, in, sizeof(uint32_t)*iresearch::packed::BLOCK_SIZE_32);
}

MSVC_ONLY(__pragma(warning(push)))
MSVC_ONLY(__pragma(warning(disable:4127))) // constexp conditionals are intended to be optimized out
MSVC_ONLY(__pragma(warning(disable:4293))) // all negative shifts are masked by constexpr conditionals
MSVC_ONLY(__pragma(warning(disable:4724))) // all X % zero are masked by constexpr conditionals (must disable outside of fn body)
template<int N>
void __fastunpack(const uint64_t* RESTRICT in, uint64_t* RESTRICT out) {
  // 64 == sizeof(uint32_t) * 8
  static_assert(N > 0 && N <= 64, "N <= 0 || N > 64");
         *out = ((*in) >> (N *  0) % 64) % (1ULL << N); if (((N *  1) % 64) < ((N *  0) % 64)) { ++in; *out |= ((*in) % (1ULL << (N *  1) % 64)) << (N - ((N *  1) % 64)); }
  out++; *out = ((*in) >> (N *  1) % 64) % (1ULL << N); if (((N *  2) % 64) < ((N *  1) % 64)) { ++in; *out |= ((*in) % (1ULL << (N *  2) % 64)) << (N - ((N *  2) % 64)); }
  out++; *out = ((*in) >> (N *  2) % 64) % (1ULL << N); if (((N *  3) % 64) < ((N *  2) % 64)) { ++in; *out |= ((*in) % (1ULL << (N *  3) % 64)) << (N - ((N *  3) % 64)); }
  out++; *out = ((*in) >> (N *  3) % 64) % (1ULL << N); if (((N *  4) % 64) < ((N *  3) % 64)) { ++in; *out |= ((*in) % (1ULL << (N *  4) % 64)) << (N - ((N *  4) % 64)); }
  out++; *out = ((*in) >> (N *  4) % 64) % (1ULL << N); if (((N *  5) % 64) < ((N *  4) % 64)) { ++in; *out |= ((*in) % (1ULL << (N *  5) % 64)) << (N - ((N *  5) % 64)); }
  out++; *out = ((*in) >> (N *  5) % 64) % (1ULL << N); if (((N *  6) % 64) < ((N *  5) % 64)) { ++in; *out |= ((*in) % (1ULL << (N *  6) % 64)) << (N - ((N *  6) % 64)); }
  out++; *out = ((*in) >> (N *  6) % 64) % (1ULL << N); if (((N *  7) % 64) < ((N *  6) % 64)) { ++in; *out |= ((*in) % (1ULL << (N *  7) % 64)) << (N - ((N *  7) % 64)); }
  out++; *out = ((*in) >> (N *  7) % 64) % (1ULL << N); if (((N *  8) % 64) < ((N *  7) % 64)) { ++in; *out |= ((*in) % (1ULL << (N *  8) % 64)) << (N - ((N *  8) % 64)); }
  out++; *out = ((*in) >> (N *  8) % 64) % (1ULL << N); if (((N *  9) % 64) < ((N *  8) % 64)) { ++in; *out |= ((*in) % (1ULL << (N *  9) % 64)) << (N - ((N *  9) % 64)); }
  out++; *out = ((*in) >> (N *  9) % 64) % (1ULL << N); if (((N * 10) % 64) < ((N *  9) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 10) % 64)) << (N - ((N * 10) % 64)); }
  out++; *out = ((*in) >> (N * 10) % 64) % (1ULL << N); if (((N * 11) % 64) < ((N * 10) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 11) % 64)) << (N - ((N * 11) % 64)); }
  out++; *out = ((*in) >> (N * 11) % 64) % (1ULL << N); if (((N * 12) % 64) < ((N * 11) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 12) % 64)) << (N - ((N * 12) % 64)); }
  out++; *out = ((*in) >> (N * 12) % 64) % (1ULL << N); if (((N * 13) % 64) < ((N * 12) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 13) % 64)) << (N - ((N * 13) % 64)); }
  out++; *out = ((*in) >> (N * 13) % 64) % (1ULL << N); if (((N * 14) % 64) < ((N * 13) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 14) % 64)) << (N - ((N * 14) % 64)); }
  out++; *out = ((*in) >> (N * 14) % 64) % (1ULL << N); if (((N * 15) % 64) < ((N * 14) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 15) % 64)) << (N - ((N * 15) % 64)); }
  out++; *out = ((*in) >> (N * 15) % 64) % (1ULL << N); if (((N * 16) % 64) < ((N * 15) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 16) % 64)) << (N - ((N * 16) % 64)); }
  out++; *out = ((*in) >> (N * 16) % 64) % (1ULL << N); if (((N * 17) % 64) < ((N * 16) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 17) % 64)) << (N - ((N * 17) % 64)); }
  out++; *out = ((*in) >> (N * 17) % 64) % (1ULL << N); if (((N * 18) % 64) < ((N * 17) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 18) % 64)) << (N - ((N * 18) % 64)); }
  out++; *out = ((*in) >> (N * 18) % 64) % (1ULL << N); if (((N * 19) % 64) < ((N * 18) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 19) % 64)) << (N - ((N * 19) % 64)); }
  out++; *out = ((*in) >> (N * 19) % 64) % (1ULL << N); if (((N * 20) % 64) < ((N * 19) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 20) % 64)) << (N - ((N * 20) % 64)); }
  out++; *out = ((*in) >> (N * 20) % 64) % (1ULL << N); if (((N * 21) % 64) < ((N * 20) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 21) % 64)) << (N - ((N * 21) % 64)); }
  out++; *out = ((*in) >> (N * 21) % 64) % (1ULL << N); if (((N * 22) % 64) < ((N * 21) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 22) % 64)) << (N - ((N * 22) % 64)); }
  out++; *out = ((*in) >> (N * 22) % 64) % (1ULL << N); if (((N * 23) % 64) < ((N * 22) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 23) % 64)) << (N - ((N * 23) % 64)); }
  out++; *out = ((*in) >> (N * 23) % 64) % (1ULL << N); if (((N * 24) % 64) < ((N * 23) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 24) % 64)) << (N - ((N * 24) % 64)); }
  out++; *out = ((*in) >> (N * 24) % 64) % (1ULL << N); if (((N * 25) % 64) < ((N * 24) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 25) % 64)) << (N - ((N * 25) % 64)); }
  out++; *out = ((*in) >> (N * 25) % 64) % (1ULL << N); if (((N * 26) % 64) < ((N * 25) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 26) % 64)) << (N - ((N * 26) % 64)); }
  out++; *out = ((*in) >> (N * 26) % 64) % (1ULL << N); if (((N * 27) % 64) < ((N * 26) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 27) % 64)) << (N - ((N * 27) % 64)); }
  out++; *out = ((*in) >> (N * 27) % 64) % (1ULL << N); if (((N * 28) % 64) < ((N * 27) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 28) % 64)) << (N - ((N * 28) % 64)); }
  out++; *out = ((*in) >> (N * 28) % 64) % (1ULL << N); if (((N * 29) % 64) < ((N * 28) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 29) % 64)) << (N - ((N * 29) % 64)); }
  out++; *out = ((*in) >> (N * 29) % 64) % (1ULL << N); if (((N * 30) % 64) < ((N * 29) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 30) % 64)) << (N - ((N * 30) % 64)); }
  out++; *out = ((*in) >> (N * 30) % 64) % (1ULL << N); if (((N * 31) % 64) < ((N * 30) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 31) % 64)) << (N - ((N * 31) % 64)); }
  out++; *out = ((*in) >> (N * 31) % 64) % (1ULL << N); if (((N * 32) % 64) < ((N * 31) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 32) % 64)) << (N - ((N * 32) % 64)); }
  out++; *out = ((*in) >> (N * 32) % 64) % (1ULL << N); if (((N * 33) % 64) < ((N * 32) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 33) % 64)) << (N - ((N * 33) % 64)); }
  out++; *out = ((*in) >> (N * 33) % 64) % (1ULL << N); if (((N * 34) % 64) < ((N * 33) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 34) % 64)) << (N - ((N * 34) % 64)); }
  out++; *out = ((*in) >> (N * 34) % 64) % (1ULL << N); if (((N * 35) % 64) < ((N * 34) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 35) % 64)) << (N - ((N * 35) % 64)); }
  out++; *out = ((*in) >> (N * 35) % 64) % (1ULL << N); if (((N * 36) % 64) < ((N * 35) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 36) % 64)) << (N - ((N * 36) % 64)); }
  out++; *out = ((*in) >> (N * 36) % 64) % (1ULL << N); if (((N * 37) % 64) < ((N * 36) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 37) % 64)) << (N - ((N * 37) % 64)); }
  out++; *out = ((*in) >> (N * 37) % 64) % (1ULL << N); if (((N * 38) % 64) < ((N * 37) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 38) % 64)) << (N - ((N * 38) % 64)); }
  out++; *out = ((*in) >> (N * 38) % 64) % (1ULL << N); if (((N * 39) % 64) < ((N * 38) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 39) % 64)) << (N - ((N * 39) % 64)); }
  out++; *out = ((*in) >> (N * 39) % 64) % (1ULL << N); if (((N * 40) % 64) < ((N * 39) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 40) % 64)) << (N - ((N * 40) % 64)); }
  out++; *out = ((*in) >> (N * 40) % 64) % (1ULL << N); if (((N * 41) % 64) < ((N * 40) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 41) % 64)) << (N - ((N * 41) % 64)); }
  out++; *out = ((*in) >> (N * 41) % 64) % (1ULL << N); if (((N * 42) % 64) < ((N * 41) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 42) % 64)) << (N - ((N * 42) % 64)); }
  out++; *out = ((*in) >> (N * 42) % 64) % (1ULL << N); if (((N * 43) % 64) < ((N * 42) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 43) % 64)) << (N - ((N * 43) % 64)); }
  out++; *out = ((*in) >> (N * 43) % 64) % (1ULL << N); if (((N * 44) % 64) < ((N * 43) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 44) % 64)) << (N - ((N * 44) % 64)); }
  out++; *out = ((*in) >> (N * 44) % 64) % (1ULL << N); if (((N * 45) % 64) < ((N * 44) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 45) % 64)) << (N - ((N * 45) % 64)); }
  out++; *out = ((*in) >> (N * 45) % 64) % (1ULL << N); if (((N * 46) % 64) < ((N * 45) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 46) % 64)) << (N - ((N * 46) % 64)); }
  out++; *out = ((*in) >> (N * 46) % 64) % (1ULL << N); if (((N * 47) % 64) < ((N * 46) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 47) % 64)) << (N - ((N * 47) % 64)); }
  out++; *out = ((*in) >> (N * 47) % 64) % (1ULL << N); if (((N * 48) % 64) < ((N * 47) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 48) % 64)) << (N - ((N * 48) % 64)); }
  out++; *out = ((*in) >> (N * 48) % 64) % (1ULL << N); if (((N * 49) % 64) < ((N * 48) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 49) % 64)) << (N - ((N * 49) % 64)); }
  out++; *out = ((*in) >> (N * 49) % 64) % (1ULL << N); if (((N * 50) % 64) < ((N * 49) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 50) % 64)) << (N - ((N * 50) % 64)); }
  out++; *out = ((*in) >> (N * 50) % 64) % (1ULL << N); if (((N * 51) % 64) < ((N * 50) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 51) % 64)) << (N - ((N * 51) % 64)); }
  out++; *out = ((*in) >> (N * 51) % 64) % (1ULL << N); if (((N * 52) % 64) < ((N * 51) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 52) % 64)) << (N - ((N * 52) % 64)); }
  out++; *out = ((*in) >> (N * 52) % 64) % (1ULL << N); if (((N * 53) % 64) < ((N * 52) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 53) % 64)) << (N - ((N * 53) % 64)); }
  out++; *out = ((*in) >> (N * 53) % 64) % (1ULL << N); if (((N * 54) % 64) < ((N * 53) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 54) % 64)) << (N - ((N * 54) % 64)); }
  out++; *out = ((*in) >> (N * 54) % 64) % (1ULL << N); if (((N * 55) % 64) < ((N * 54) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 55) % 64)) << (N - ((N * 55) % 64)); }
  out++; *out = ((*in) >> (N * 55) % 64) % (1ULL << N); if (((N * 56) % 64) < ((N * 55) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 56) % 64)) << (N - ((N * 56) % 64)); }
  out++; *out = ((*in) >> (N * 56) % 64) % (1ULL << N); if (((N * 57) % 64) < ((N * 56) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 57) % 64)) << (N - ((N * 57) % 64)); }
  out++; *out = ((*in) >> (N * 57) % 64) % (1ULL << N); if (((N * 58) % 64) < ((N * 57) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 58) % 64)) << (N - ((N * 58) % 64)); }
  out++; *out = ((*in) >> (N * 58) % 64) % (1ULL << N); if (((N * 59) % 64) < ((N * 58) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 59) % 64)) << (N - ((N * 59) % 64)); }
  out++; *out = ((*in) >> (N * 59) % 64) % (1ULL << N); if (((N * 60) % 64) < ((N * 59) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 60) % 64)) << (N - ((N * 60) % 64)); }
  out++; *out = ((*in) >> (N * 60) % 64) % (1ULL << N); if (((N * 61) % 64) < ((N * 60) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 61) % 64)) << (N - ((N * 61) % 64)); }
  out++; *out = ((*in) >> (N * 61) % 64) % (1ULL << N); if (((N * 62) % 64) < ((N * 61) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 62) % 64)) << (N - ((N * 62) % 64)); }
  out++; *out = ((*in) >> (N * 62) % 64) % (1ULL << N); if (((N * 63) % 64) < ((N * 62) % 64)) { ++in; *out |= ((*in) % (1ULL << (N * 63) % 64)) << (N - ((N * 63) % 64)); }
  out++; *out = ((*in) >> (N * 63) % 64) % (1ULL << N);
}
MSVC_ONLY(__pragma(warning(pop)))

template<>
void __fastunpack<64>(const uint64_t* RESTRICT in, uint64_t* RESTRICT out) {
  std::memcpy(out, in, sizeof(uint64_t)*iresearch::packed::BLOCK_SIZE_64);
}

NS_END // NS_LOCAL

NS_ROOT
NS_BEGIN(packed)

void pack_block(
  const uint32_t* RESTRICT in, uint32_t* RESTRICT out, const uint32_t bit
) {
  switch (bit) {
    case 1:   __fastpack<1>(in, out); break;
    case 2:   __fastpack<2>(in, out); break;
    case 3:   __fastpack<3>(in, out); break;
    case 4:   __fastpack<4>(in, out); break;
    case 5:   __fastpack<5>(in, out); break;
    case 6:   __fastpack<6>(in, out); break;
    case 7:   __fastpack<7>(in, out); break;
    case 8:   __fastpack<8>(in, out); break;
    case 9:   __fastpack<9>(in, out); break;
    case 10: __fastpack<10>(in, out); break;
    case 11: __fastpack<11>(in, out); break;
    case 12: __fastpack<12>(in, out); break;
    case 13: __fastpack<13>(in, out); break;
    case 14: __fastpack<14>(in, out); break;
    case 15: __fastpack<15>(in, out); break;
    case 16: __fastpack<16>(in, out); break;
    case 17: __fastpack<17>(in, out); break;
    case 18: __fastpack<18>(in, out); break;
    case 19: __fastpack<19>(in, out); break;
    case 20: __fastpack<20>(in, out); break;
    case 21: __fastpack<21>(in, out); break;
    case 22: __fastpack<22>(in, out); break;
    case 23: __fastpack<23>(in, out); break;
    case 24: __fastpack<24>(in, out); break;
    case 25: __fastpack<25>(in, out); break;
    case 26: __fastpack<26>(in, out); break;
    case 27: __fastpack<27>(in, out); break;
    case 28: __fastpack<28>(in, out); break;
    case 29: __fastpack<29>(in, out); break;
    case 30: __fastpack<30>(in, out); break;
    case 31: __fastpack<31>(in, out); break;
    case 32: __fastpack<32>(in, out); break;
    default: assert(false); break;
  }
}

void pack_block(
  const uint64_t* RESTRICT in, uint64_t* RESTRICT out, const uint32_t bit
) {
  switch (bit) {
    case 1:   __fastpack<1>(in, out); break;
    case 2:   __fastpack<2>(in, out); break;
    case 3:   __fastpack<3>(in, out); break;
    case 4:   __fastpack<4>(in, out); break;
    case 5:   __fastpack<5>(in, out); break;
    case 6:   __fastpack<6>(in, out); break;
    case 7:   __fastpack<7>(in, out); break;
    case 8:   __fastpack<8>(in, out); break;
    case 9:   __fastpack<9>(in, out); break;
    case 10: __fastpack<10>(in, out); break;
    case 11: __fastpack<11>(in, out); break;
    case 12: __fastpack<12>(in, out); break;
    case 13: __fastpack<13>(in, out); break;
    case 14: __fastpack<14>(in, out); break;
    case 15: __fastpack<15>(in, out); break;
    case 16: __fastpack<16>(in, out); break;
    case 17: __fastpack<17>(in, out); break;
    case 18: __fastpack<18>(in, out); break;
    case 19: __fastpack<19>(in, out); break;
    case 20: __fastpack<20>(in, out); break;
    case 21: __fastpack<21>(in, out); break;
    case 22: __fastpack<22>(in, out); break;
    case 23: __fastpack<23>(in, out); break;
    case 24: __fastpack<24>(in, out); break;
    case 25: __fastpack<25>(in, out); break;
    case 26: __fastpack<26>(in, out); break;
    case 27: __fastpack<27>(in, out); break;
    case 28: __fastpack<28>(in, out); break;
    case 29: __fastpack<29>(in, out); break;
    case 30: __fastpack<30>(in, out); break;
    case 31: __fastpack<31>(in, out); break;
    case 32: __fastpack<32>(in, out); break;
    case 33: __fastpack<33>(in, out); break;
    case 34: __fastpack<34>(in, out); break;
    case 35: __fastpack<35>(in, out); break;
    case 36: __fastpack<36>(in, out); break;
    case 37: __fastpack<37>(in, out); break;
    case 38: __fastpack<38>(in, out); break;
    case 39: __fastpack<39>(in, out); break;
    case 40: __fastpack<40>(in, out); break;
    case 41: __fastpack<41>(in, out); break;
    case 42: __fastpack<42>(in, out); break;
    case 43: __fastpack<43>(in, out); break;
    case 44: __fastpack<44>(in, out); break;
    case 45: __fastpack<45>(in, out); break;
    case 46: __fastpack<46>(in, out); break;
    case 47: __fastpack<47>(in, out); break;
    case 48: __fastpack<48>(in, out); break;
    case 49: __fastpack<49>(in, out); break;
    case 50: __fastpack<50>(in, out); break;
    case 51: __fastpack<51>(in, out); break;
    case 52: __fastpack<52>(in, out); break;
    case 53: __fastpack<53>(in, out); break;
    case 54: __fastpack<54>(in, out); break;
    case 55: __fastpack<55>(in, out); break;
    case 56: __fastpack<56>(in, out); break;
    case 57: __fastpack<57>(in, out); break;
    case 58: __fastpack<58>(in, out); break;
    case 59: __fastpack<59>(in, out); break;
    case 60: __fastpack<60>(in, out); break;
    case 61: __fastpack<61>(in, out); break;
    case 62: __fastpack<62>(in, out); break;
    case 63: __fastpack<63>(in, out); break;
    case 64: __fastpack<64>(in, out); break;
    default: assert(false); break;
  }
}

void unpack_block(
  const uint32_t* RESTRICT in, uint32_t* RESTRICT out, const uint32_t bit
) {
  switch (bit) {
    case 1:   __fastunpack<1>(in, out); break;
    case 2:   __fastunpack<2>(in, out); break;
    case 3:   __fastunpack<3>(in, out); break;
    case 4:   __fastunpack<4>(in, out); break;
    case 5:   __fastunpack<5>(in, out); break;
    case 6:   __fastunpack<6>(in, out); break;
    case 7:   __fastunpack<7>(in, out); break;
    case 8:   __fastunpack<8>(in, out); break;
    case 9:   __fastunpack<9>(in, out); break;
    case 10: __fastunpack<10>(in, out); break;
    case 11: __fastunpack<11>(in, out); break;
    case 12: __fastunpack<12>(in, out); break;
    case 13: __fastunpack<13>(in, out); break;
    case 14: __fastunpack<14>(in, out); break;
    case 15: __fastunpack<15>(in, out); break;
    case 16: __fastunpack<16>(in, out); break;
    case 17: __fastunpack<17>(in, out); break;
    case 18: __fastunpack<18>(in, out); break;
    case 19: __fastunpack<19>(in, out); break;
    case 20: __fastunpack<20>(in, out); break;
    case 21: __fastunpack<21>(in, out); break;
    case 22: __fastunpack<22>(in, out); break;
    case 23: __fastunpack<23>(in, out); break;
    case 24: __fastunpack<24>(in, out); break;
    case 25: __fastunpack<25>(in, out); break;
    case 26: __fastunpack<26>(in, out); break;
    case 27: __fastunpack<27>(in, out); break;
    case 28: __fastunpack<28>(in, out); break;
    case 29: __fastunpack<29>(in, out); break;
    case 30: __fastunpack<30>(in, out); break;
    case 31: __fastunpack<31>(in, out); break;
    case 32: __fastunpack<32>(in, out); break;
    default: assert(false); break;
  }
}

void unpack_block(
  const uint64_t* RESTRICT in, uint64_t* RESTRICT out, const uint32_t bit
) {
  switch (bit) {
    case 1:   __fastunpack<1>(in, out); break;
    case 2:   __fastunpack<2>(in, out); break;
    case 3:   __fastunpack<3>(in, out); break;
    case 4:   __fastunpack<4>(in, out); break;
    case 5:   __fastunpack<5>(in, out); break;
    case 6:   __fastunpack<6>(in, out); break;
    case 7:   __fastunpack<7>(in, out); break;
    case 8:   __fastunpack<8>(in, out); break;
    case 9:   __fastunpack<9>(in, out); break;
    case 10: __fastunpack<10>(in, out); break;
    case 11: __fastunpack<11>(in, out); break;
    case 12: __fastunpack<12>(in, out); break;
    case 13: __fastunpack<13>(in, out); break;
    case 14: __fastunpack<14>(in, out); break;
    case 15: __fastunpack<15>(in, out); break;
    case 16: __fastunpack<16>(in, out); break;
    case 17: __fastunpack<17>(in, out); break;
    case 18: __fastunpack<18>(in, out); break;
    case 19: __fastunpack<19>(in, out); break;
    case 20: __fastunpack<20>(in, out); break;
    case 21: __fastunpack<21>(in, out); break;
    case 22: __fastunpack<22>(in, out); break;
    case 23: __fastunpack<23>(in, out); break;
    case 24: __fastunpack<24>(in, out); break;
    case 25: __fastunpack<25>(in, out); break;
    case 26: __fastunpack<26>(in, out); break;
    case 27: __fastunpack<27>(in, out); break;
    case 28: __fastunpack<28>(in, out); break;
    case 29: __fastunpack<29>(in, out); break;
    case 30: __fastunpack<30>(in, out); break;
    case 31: __fastunpack<31>(in, out); break;
    case 32: __fastunpack<32>(in, out); break;
    case 33: __fastunpack<33>(in, out); break;
    case 34: __fastunpack<34>(in, out); break;
    case 35: __fastunpack<35>(in, out); break;
    case 36: __fastunpack<36>(in, out); break;
    case 37: __fastunpack<37>(in, out); break;
    case 38: __fastunpack<38>(in, out); break;
    case 39: __fastunpack<39>(in, out); break;
    case 40: __fastunpack<40>(in, out); break;
    case 41: __fastunpack<41>(in, out); break;
    case 42: __fastunpack<42>(in, out); break;
    case 43: __fastunpack<43>(in, out); break;
    case 44: __fastunpack<44>(in, out); break;
    case 45: __fastunpack<45>(in, out); break;
    case 46: __fastunpack<46>(in, out); break;
    case 47: __fastunpack<47>(in, out); break;
    case 48: __fastunpack<48>(in, out); break;
    case 49: __fastunpack<49>(in, out); break;
    case 50: __fastunpack<50>(in, out); break;
    case 51: __fastunpack<51>(in, out); break;
    case 52: __fastunpack<52>(in, out); break;
    case 53: __fastunpack<53>(in, out); break;
    case 54: __fastunpack<54>(in, out); break;
    case 55: __fastunpack<55>(in, out); break;
    case 56: __fastunpack<56>(in, out); break;
    case 57: __fastunpack<57>(in, out); break;
    case 58: __fastunpack<58>(in, out); break;
    case 59: __fastunpack<59>(in, out); break;
    case 60: __fastunpack<60>(in, out); break;
    case 61: __fastunpack<61>(in, out); break;
    case 62: __fastunpack<62>(in, out); break;
    case 63: __fastunpack<63>(in, out); break;
    case 64: __fastunpack<64>(in, out); break;
    default: assert(false); break;
  }
}

void pack(
  const uint32_t* first, const uint32_t* last, uint32_t* out, const uint32_t bit
) {
  assert(0 == (last - first) % BLOCK_SIZE_32);

  for (; first < last; first += BLOCK_SIZE_32, out += bit) {
    pack_block(first, out, bit);
  }
}

void pack(
  const uint64_t* first, const uint64_t* last, uint64_t* out, const uint32_t bit
) {
  assert(0 == (last - first) % BLOCK_SIZE_64);

  for (; first < last; first += BLOCK_SIZE_64, out += bit) {
    pack_block(first, out, bit);
  }
}

void unpack(
  uint32_t* first, uint32_t* last, const uint32_t* in, const uint32_t bit
) {
  for (; first < last; first += BLOCK_SIZE_32, in += bit) {
    unpack_block(in, first, bit);
  }
}

void unpack(
  uint64_t* first, uint64_t* last, const uint64_t* in, const uint32_t bit
) {
  for (; first < last; first += BLOCK_SIZE_64, in += bit) {
    unpack_block(in, first, bit);
  }
}

NS_END // packed
NS_END