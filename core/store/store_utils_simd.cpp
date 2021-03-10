////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////


#include "shared.hpp"

#ifndef IRESEARCH_SSE2
#error "SSE2 not supported"
#endif

#include "store_utils_simd.hpp"

#include "store/store_utils.hpp"
#include "utils/std.hpp"
#include "utils/simd_utils.hpp"

extern "C" {
#include <simdcomp/include/simdbitpacking.h>
}

namespace {

bool all_equal(
    const uint32_t* RESTRICT begin,
    const uint32_t* RESTRICT end) noexcept {
#ifdef IRESEARCH_SSE4_2
  return irs::simd::all_equal(begin, end);
#else
  return irs::irstd::all_equal(begin, end);
#endif
}

}

namespace iresearch {
namespace encode {
namespace bitpack {

void read_block_simd(
    data_input& in,
    uint32_t* RESTRICT encoded,
    uint32_t* RESTRICT decoded) {
  assert(encoded);
  assert(decoded);

  const uint32_t bits = in.read_vint();
  if (ALL_EQUAL == bits) {
    simd::fill_n<SIMDBlockSize>(decoded, in.read_vint());
  } else {
    const size_t required = packed::bytes_required_32(SIMDBlockSize, bits);
    const auto* buf = in.read_buffer(required, BufferHint::NORMAL);

    if (buf) {
      ::simdunpack(reinterpret_cast<const __m128i*>(buf), decoded, bits);
      return;
    }

#ifdef IRESEARCH_DEBUG
    const auto read = in.read_bytes(
      reinterpret_cast<byte_type*>(encoded),
      required);
    assert(read == required);
    UNUSED(read);
#else
    in.read_bytes(
      reinterpret_cast<byte_type*>(encoded),
      required);
#endif // IRESEARCH_DEBUG

    ::simdunpack(reinterpret_cast<const __m128i*>(encoded), decoded, bits);
  }
}

uint32_t write_block_simd(
    data_output& out,
    const uint32_t* RESTRICT decoded,
    uint32_t* RESTRICT encoded) {
  assert(encoded);
  assert(decoded);

  if (all_equal(decoded, decoded + SIMDBlockSize)) {
    out.write_vint(ALL_EQUAL);
    out.write_vint(*decoded);
    return ALL_EQUAL;
  }

  const auto bits = ::maxbits_length(decoded, SIMDBlockSize);
  std::memset(encoded, 0, sizeof(uint32_t) * SIMDBlockSize); // FIXME do we need memset???
  ::simdpackwithoutmask(decoded, reinterpret_cast<__m128i*>(encoded), bits);

  out.write_vint(bits);
  out.write_bytes(reinterpret_cast<const byte_type*>(encoded), 16*bits);

  return bits;
}

} // encode
} // bitpack
} // ROOT
