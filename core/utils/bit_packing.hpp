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

#ifndef IRESEARCH_BIT_PACKING_H
#define IRESEARCH_BIT_PACKING_H

#include "shared.hpp"
#include "math_utils.hpp"

#include <cmath>
#include <limits>

NS_ROOT
NS_BEGIN( packed )

const uint32_t BLOCK_SIZE_32 = sizeof(uint32_t) * 8; // block size is tied to number of bits in value
const uint32_t BLOCK_SIZE_64 = sizeof(uint64_t) * 8; // block size is tied to number of bits in value

const uint32_t VERSION = 1U;

inline uint32_t bits_required_64(uint64_t val) {
  return 1 + iresearch::math::log2_64(val);
}

inline uint32_t bits_required_32(uint32_t val) {
  return 1 + iresearch::math::log2_32(val);
}

inline uint32_t bytes_required_32(uint32_t count, uint32_t bits) {
  return static_cast<uint32_t>(
    ceilf(static_cast<float_t>(count) * bits / 8)
  );
}

inline uint64_t bytes_required_64(uint64_t count, uint64_t bits) {
  return static_cast<uint64_t>(
    std::ceil(static_cast<double_t>(count) * bits / 8)
  );
}

inline uint32_t blocks_required_32(uint32_t count, uint32_t bits) {
  return static_cast<uint32_t>(
    ceilf(static_cast<float_t>(count) * bits / (8 * sizeof(uint32_t)))
  );
}

inline uint64_t blocks_required_64(uint64_t count, uint64_t bits) {
  return static_cast<uint64_t>(
    std::ceil(static_cast<double_t>(count) * bits / (8 * sizeof(uint64_t)))
  );
}

//////////////////////////////////////////////////////////////////////////////
/// @brief returns number of elements required to store unpacked data
//////////////////////////////////////////////////////////////////////////////
inline uint64_t items_required( uint32_t count ) {
  return BLOCK_SIZE_32 * static_cast<uint32_t>(std::ceil(double(count) / BLOCK_SIZE_32));
}

inline uint64_t iterations_required( uint32_t count ) {
  return items_required(count) / BLOCK_SIZE_32;
}

template< typename T >
inline T max_value(uint32_t bits) {
  assert( bits >= 0U && bits <= sizeof( T ) * 8U );

  return bits == sizeof( T ) * 8U
    ? (std::numeric_limits<T>::max)() 
    : ~(~T(0) << bits);
}

IRESEARCH_API uint32_t at(
  const uint32_t* encoded, 
  const size_t i, 
  const uint32_t bit) NOEXCEPT;

IRESEARCH_API uint64_t at(
  const uint64_t* encoded, 
  const size_t i, 
  const uint32_t bit) NOEXCEPT;

IRESEARCH_API void pack(
  const uint32_t* first, 
  const uint32_t* last, 
  uint32_t* out, 
  const uint32_t bit) NOEXCEPT;

IRESEARCH_API void pack(
  const uint64_t* first, 
  const uint64_t* last, 
  uint64_t* out, 
  const uint32_t bit) NOEXCEPT;

IRESEARCH_API void unpack(
  uint32_t* first, 
  uint32_t* last, 
  const uint32_t* in, 
  const uint32_t bit) NOEXCEPT;

IRESEARCH_API void unpack(
  uint64_t* first, 
  uint64_t* last, 
  const uint64_t* in, 
  const uint32_t bit) NOEXCEPT;

NS_END // packing
NS_END // root

#endif