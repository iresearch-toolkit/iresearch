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

#include <chrono>

#include "MurmurHash/MurmurHash3.h"

#include "hash_utils.hpp"

// -----------------------------------------------------------------------------
// --SECTION--                                                        hash utils
// -----------------------------------------------------------------------------

NS_LOCAL

inline uint32_t get_seed() {
  using namespace std::chrono;
  milliseconds ms = duration_cast< milliseconds >(
    high_resolution_clock::now().time_since_epoch()
  );
  return static_cast< uint32_t >( ms.count() );
}

template<typename T>
inline size_t get_hash(const T& value) {
  static const uint32_t seed = get_seed();
  uint32_t code;

  MurmurHash3_x86_32(value.c_str(), static_cast<int>(value.size()), seed, &code);

  return code;
}

NS_END

NS_ROOT

size_t bytes_ref_hash_t::operator()(const bytes_ref& value) const {
  return get_hash(value);
}

size_t string_ref_hash_t::operator()(const string_ref& value) const {
  return get_hash(value);
}

NS_END