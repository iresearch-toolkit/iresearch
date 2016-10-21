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

#ifndef IRESEARCH_BIT_UTILS_H
#define IRESEARCH_BIT_UTILS_H

#include "shared.hpp"
#include <numeric>

NS_ROOT

template< typename T >
CONSTEXPR uint32_t bits_required() NOEXCEPT {
  return sizeof(T)*8U;
}

template< typename T >
inline void set_bit( T& value, size_t bit ) NOEXCEPT { 
  value |= ( T(1) << bit ); 
}

template< unsigned Bit, typename T >
inline void set_bit( T& value ) NOEXCEPT { 
  value |= ( T(1) << (Bit) ); 
}

template< typename T >
inline void unset_bit( T& value, size_t bit ) NOEXCEPT { 
  value &= ~( T(1) << bit ); 
}

template< unsigned Bit, typename T >
inline void unset_bit( T& value ) NOEXCEPT { 
  value &= ~( T(1) << (Bit) ); 
}

template< unsigned Bit, typename T >
inline void set_bit( bool set, T& value ) NOEXCEPT { 
  set ? set_bit< Bit >( value ) : unset_bit< Bit >( value ); 
}

template< typename T >
inline void set_bit( T& value, size_t bit, bool set ) NOEXCEPT{
  set ? set_bit( value, bit ) : unset_bit( value, bit );
}

template< unsigned Bit, typename T >
inline CONSTEXPR bool check_bit( T value ) NOEXCEPT{
  return ( value & ( T(1) << (Bit) ) ) != 0; 
}

template< typename T >
inline bool check_bit( T value, size_t bit ) NOEXCEPT {
  return ( value & ( T(1) << bit ) ) != 0; 
}

template< unsigned Offset, typename T >
inline CONSTEXPR T rol( T value ) NOEXCEPT{
  static_assert( Offset >= 0 && Offset <= sizeof( T ) * 8,
                 "Offset out of range" );

  return ( value << Offset ) | ( value >> ( sizeof( T ) * 8 - Offset ) );
}

template< unsigned Offset, typename T >
inline CONSTEXPR T ror( T value ) NOEXCEPT{
  static_assert( Offset >= 0 && Offset <= sizeof( T ) * 8,
                 "Offset out of range" );

  return ( value >> Offset ) | ( value << ( sizeof( T ) * 8 - Offset ) );
}

#if defined(_MSC_VER)
  #pragma warning( disable : 4146 )
#endif

inline uint32_t zig_zag_encode32( int32_t v ) NOEXCEPT { return ( v >> 31 ) ^ ( v << 1 ); }
inline int32_t zig_zag_decode32( uint32_t v ) NOEXCEPT { return ( v >> 1U ) ^ -( v & 1U ); }

inline uint64_t zig_zag_encode64( int64_t v ) NOEXCEPT { return ( v >> 63 ) ^ ( v << 1 ); }
inline int64_t zig_zag_decode64( uint64_t v ) NOEXCEPT { return ( v >> 1U ) ^ -( v & 1U ); }

#if defined(_MSC_VER)
  #pragma warning( default  : 4146 )
#endif

NS_END

#endif
