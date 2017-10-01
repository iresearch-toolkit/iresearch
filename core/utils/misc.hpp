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
