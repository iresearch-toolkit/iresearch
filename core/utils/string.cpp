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

#include "string.hpp"

NS_ROOT

/* -------------------------------------------------------------------
 * basic_string_ref
 * ------------------------------------------------------------------*/

#if defined(_MSC_VER) && defined(IRESEARCH_DLL)

template class IRESEARCH_API basic_string_ref<char>;
template class IRESEARCH_API basic_string_ref<wchar_t>;
template class IRESEARCH_API basic_string_ref<byte_type>;

#endif

NS_BEGIN( detail )

size_t _char_hash_32(const iresearch::byte_type* _First, size_t _Count) {
  return _char_hash_32(reinterpret_cast<const char*>(_First), _Count);
}

size_t _char_hash_64(const iresearch::byte_type* _First, size_t _Count) {
  return _char_hash_64(reinterpret_cast<const char*>(_First), _Count);
}

size_t _char_hash_32( const char* _First, size_t _Count ) {
  //static_assert(sizeof(size_t) == 4, "This code is for 32-bit size_t.");

  const size_t _FNV_offset_basis = 2166136261U;
  const size_t _FNV_prime = 16777619U;

  size_t _Val = _FNV_offset_basis;
  for ( size_t _Next = 0; _Next < _Count; ++_Next ) {	// fold in another byte
    _Val ^= ( size_t ) _First[_Next];
    _Val *= _FNV_prime;
  }

  return _Val;
}

size_t _char_hash_64( const char* _First, size_t _Count ) {
  //static_assert(sizeof(size_t) == 8, "This code is for 64-bit size_t.");
  const size_t _FNV_offset_basis = 14695981039346656037UL;
  const size_t _FNV_prime = 1099511628211UL;

  size_t _Val = _FNV_offset_basis;
  for ( size_t _Next = 0; _Next < _Count; ++_Next ) {	// fold in another byte
    _Val ^= ( size_t ) _First[_Next];
    _Val *= _FNV_prime;
  }

  _Val ^= _Val >> 32;

  return _Val;
}

NS_END
NS_END
