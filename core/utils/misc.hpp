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

#ifndef IRESEARCH_MISC_H
#define IRESEARCH_MISC_H

#include "types.hpp"
#include "math_utils.hpp"

#include <iterator>
#include <functional>

NS_ROOT

template<typename Func>
class finally {
 public:
  finally(const Func& func) : func_(func) { }
  finally(Func&& func) : func_(std::move(func)) { }
  ~finally() { func_(); }

 private:
  Func func_;
};

template<typename Func>
finally<Func> make_finally(Func&& func) {
  return finally<Func>(std::forward<Func>(func));
}

template< typename _T >
class byte_iterator : public std::iterator < std::forward_iterator_tag, byte_type > {
 public:
  typedef _T type;
  typedef uint32_t size_type;
  typedef std::iterator< std::forward_iterator_tag, byte_type > base;
  typedef typename base::reference reference;
  typedef typename base::value_type value_type;
  typedef typename base::difference_type difference_type;

  static const size_type BITS = 8;
  static const size_type SIZE = sizeof(type) * BITS;
  static const size_type OFFSET = (SIZE - 1)*BITS;

  explicit byte_iterator( type value )
    : value( value ) 
  { }

  ~byte_iterator() { }

  inline byte_iterator& operator++( ) {
    value <<= BITS;

    return *this;
  }

  inline difference_type operator-( const byte_iterator& rhs ) const {
    return sizeof( type );
  }

  inline bool operator!=( const byte_iterator& rhs ) const {
    return value != rhs.value;
  }

  inline value_type operator*( ) const {
    return static_cast< value_type >( ( value >> OFFSET ) & 0xFF );
  }

  template< typename _OI, size_type _Count = sizeof( type ) >
  static inline _OI copy( byte_iterator begin, _OI out ) {
    for ( size_type i = _Count; i; --i ) {
      *out = *begin;
      ++out;
      ++begin;
    }

    return out;
  }

 private:
  type value;
};

////////////////////////////////////////////////////////////////////////////////
/// @returns number of bytes required to store value in variable length format
////////////////////////////////////////////////////////////////////////////////
FORCE_INLINE uint32_t vencode_size_32(uint32_t value) {
  return 1 + ((32 - math::clz32(value)) >> 3);
}

////////////////////////////////////////////////////////////////////////////////
/// @returns number of bytes required to store value in variable length format
////////////////////////////////////////////////////////////////////////////////
FORCE_INLINE uint64_t vencode_size_64(uint64_t value) {
  return 1 + ((64 - math::clz64(value)) >> 3);
}

template<typename T>
struct vencode_traits {
  static size_t vencode_size(T value);
};

template<>
struct vencode_traits<uint32_t> {
  typedef uint32_t type;

  static size_t vencode_size(type v) {
    return vencode_size_32(v);
  }
};

template<>
struct vencode_traits<uint64_t> {
  typedef uint64_t type;

  static size_t vencode_size(type v) {
    return vencode_size_64(v);
  }
};

////////////////////////////////////////////////////////////////////////////////
/// @returns number of bytes required to store value in variable length format
////////////////////////////////////////////////////////////////////////////////
FORCE_INLINE size_t vencode_size(size_t value) {
  return vencode_traits<size_t>::vencode_size(value);
}

template< typename T >
struct bytes_io {
  typedef typename std::enable_if< 
    std::is_integral< T >::value, T 
  >::type source_type;
  typedef byte_type target_type;

  template< typename _Iterator >
  static inline source_type vread( _Iterator& in ) {
    return vread( in, typename std::iterator_traits< _Iterator >::iterator_category() );
  }

  template< typename _Iterator >
  static inline source_type read( _Iterator& in ) {
    return read( in, typename std::iterator_traits< _Iterator >::iterator_category() );
  }

  template< typename _Iterator >
  static inline void vwrite( _Iterator& out, source_type value ) {
    vwrite< _Iterator >( out, value, typename std::iterator_traits< _Iterator >::iterator_category() );
  }

  template< typename _Iterator >
  static inline void write( _Iterator& out, source_type value ) {
    write< _Iterator >( out, value, typename std::iterator_traits< _Iterator >::iterator_category() );
  }
  
  template< typename _OutputIterator >
  static void vwrite( _OutputIterator& out, source_type value,
                      std::output_iterator_tag ) {
    while ( value & ~0x7FL ) {
      *out = static_cast< target_type >( ( value & 0x7F ) | 0x80 ); ++out;
      value >>= 7;
    }

    *out = static_cast< target_type >( value ); ++out;
  }

  template< typename _OutputIterator >
  static void write( _OutputIterator& out, source_type value,
                     std::output_iterator_tag ) {
    uint32_t size = sizeof( source_type );

    for ( uint32_t shift = ( size - 1 ) * 8; size; --size, shift -= 8, ++out ) {
      *out = static_cast< target_type >( value >> shift );
    }
  }

  template< typename _InputIterator >
  static source_type read( _InputIterator& in, std::input_iterator_tag ) {
    source_type value = 0;
    uint32_t size = sizeof( source_type );

    for ( uint32_t shift = ( size - 1 ) * 8; size; --size, shift -= 8, ++in ) {
      value |= *in << shift;
    }

    return value;
  }

  template< typename _InputIterator >
  static source_type vread( _InputIterator& in, std::input_iterator_tag ) {
    target_type b = *in; ++in;
    source_type i = b & 0x7F;

    for ( uint32_t shift = 7; ( b & 0x80 ) != 0; shift += 7, ++in ) {
      b = *in;
      i |= ( b & 0x7F ) << shift;
    }

    return i;
  }
};

NS_END

#endif
