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

#ifndef IRESEARCH_MATH_UTILS_H
#define IRESEARCH_MATH_UTILS_H

#ifdef _MSC_VER
  #pragma intrinsic(_BitScanReverse)
#endif

#include "shared.hpp"

#include "cpuinfo.hpp"

#include <numeric>
#include <cassert>

NS_ROOT
NS_BEGIN( math )

IRESEARCH_API uint32_t log2_64( uint64_t value );

IRESEARCH_API uint32_t log2_32( uint32_t value );

IRESEARCH_API uint32_t log( uint64_t x, uint64_t base );

/* returns number of set bits in a set of words */
template<typename T>
FORCE_INLINE size_t popcnt( const T* value, size_t count ) NOEXCEPT {
  const char *const bitsperbyte =
  "\0\1\1\2\1\2\2\3\1\2\2\3\2\3\3\4"
  "\1\2\2\3\2\3\3\4\2\3\3\4\3\4\4\5"
  "\1\2\2\3\2\3\3\4\2\3\3\4\3\4\4\5"
  "\2\3\3\4\3\4\4\5\3\4\4\5\4\5\5\6"
  "\1\2\2\3\2\3\3\4\2\3\3\4\3\4\4\5"
  "\2\3\3\4\3\4\4\5\3\4\4\5\4\5\5\6"
  "\2\3\3\4\3\4\4\5\3\4\4\5\4\5\5\6"
  "\3\4\4\5\4\5\5\6\4\5\5\6\5\6\6\7"
  "\1\2\2\3\2\3\3\4\2\3\3\4\3\4\4\5"
  "\2\3\3\4\3\4\4\5\3\4\4\5\4\5\5\6"
  "\2\3\3\4\3\4\4\5\3\4\4\5\4\5\5\6"
  "\3\4\4\5\4\5\5\6\4\5\5\6\5\6\6\7"
  "\2\3\3\4\3\4\4\5\3\4\4\5\4\5\5\6"
  "\3\4\4\5\4\5\5\6\4\5\5\6\5\6\6\7"
  "\3\4\4\5\4\5\5\6\4\5\5\6\5\6\6\7"
  "\4\5\5\6\5\6\6\7\5\6\6\7\6\7\7\x8";
  const unsigned char* begin = reinterpret_cast< const unsigned char* >( value );
  const unsigned char* end = begin + count;

  return std::accumulate(
    begin, end, size_t( 0 ),
    [bitsperbyte] ( size_t v, unsigned char c ) {
      return v + bitsperbyte[c];
  } );
}

/* Hamming weight for 64bit values */
FORCE_INLINE uint64_t popcnt64( uint64_t v ) NOEXCEPT{  
  v = v - ( ( v >> 1 ) & ( uint64_t ) ~( uint64_t ) 0 / 3 );                           
  v = ( v & ( uint64_t ) ~( uint64_t ) 0 / 15 * 3 ) + ( ( v >> 2 ) & ( uint64_t ) ~( uint64_t ) 0 / 15 * 3 );     
  v = ( v + ( v >> 4 ) ) & ( uint64_t ) ~( uint64_t ) 0 / 255 * 15;                      
  return ( ( uint64_t ) ( v * ( ( uint64_t ) ~( uint64_t ) 0 / 255 ) ) >> ( sizeof( uint64_t ) - 1 ) * CHAR_BIT); 
}

/* Hamming weight for 32bit values */
FORCE_INLINE uint32_t popcnt32( uint32_t v ) NOEXCEPT{
  v = v - ( ( v >> 1 ) & 0x55555555 );                    
  v = ( v & 0x33333333 ) + ( ( v >> 2 ) & 0x33333333 );  
  v = (v + (v >> 4)) & 0xF0F0F0F;
  return ((v * 0x1010101) >> 24);
}

FORCE_INLINE uint32_t pop32( uint32_t v ) NOEXCEPT {
#if __GNUC__ >= 4 
  return __builtin_popcount(v);
#elif defined(_MSC_VER) 
  //TODO: compile time
  return cpuinfo::support_popcnt() ?__popcnt( v ) : popcnt32( v );
#endif
}

FORCE_INLINE uint64_t pop64( uint64_t v ) NOEXCEPT{
#if  __GNUC__ >= 4  
  return __builtin_popcountll( v );
#elif defined(_MSC_VER) && defined(_M_X64)
  //TODO: compile time
  return cpuinfo::support_popcnt() ? __popcnt64( v ) : popcnt64( v ); 
#endif
}

FORCE_INLINE uint32_t clz32(uint32_t v) NOEXCEPT {
  if (!v) return 32;
#if  __GNUC__ >= 4  
  return __builtin_clz(v);
#elif defined(_MSC_VER) 
  unsigned long idx;
  _BitScanReverse(&idx, v);
  return 31 - idx;
#endif
}

FORCE_INLINE uint64_t clz64(uint64_t v) NOEXCEPT {
  if (!v) return 64;
#if  __GNUC__ >= 4  
  return __builtin_clzl(v);
#elif defined(_MSC_VER) 
  unsigned long idx;
  _BitScanReverse64(&idx, v);
  return 63 - idx;
#endif
}

template<typename T>
struct math_traits {
  static size_t clz(T value);
  static size_t pop(T value);
}; // math_traits 

template<>
struct math_traits<uint32_t> {
  typedef uint32_t type;
  
  static size_t clz(type value) { return clz32(value); }
  static size_t pop(type value) { return pop32(value); }
}; // math_traits

template<>
struct math_traits<uint64_t> {
  typedef uint64_t type;
  
  static size_t clz(type value) { return clz64(value); }
  static size_t pop(type value) { return pop64(value); }
}; // math_traits

NS_END // math
NS_END // root

#endif
