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

#include "shared.hpp"
#include "error/error.hpp"
#include "compression.hpp"
#include "utils/string_utils.hpp"
#include "utils/type_limits.hpp"

NS_ROOT

static_assert(
  sizeof(char) == sizeof(byte_type),
  "sizeof(char) != sizeof(byte_type)"
);

bytes_ref lz4compressor::compress(const byte_type* src, size_t size, bstring& out) {
  assert(size <= integer_traits<int>::const_max); // LZ4 API uses int
  auto src_size = static_cast<int>(size);
  auto src_data = reinterpret_cast<const char*>(src);

  // ensure LZ4 dictionary from the previous run is at the start of buf_
  {
    auto* dict_store = dict_size_ ? reinterpret_cast<char*>(&out[0]) : nullptr;

    // move the LZ4 dictionary from the previous run to the start of buf_
    if (dict_store) {
      dict_size_ = LZ4_saveDict(stream(), dict_store, dict_size_);
      assert(dict_size_ >= 0);
    }

    string_utils::oversize(out, LZ4_compressBound(src_size) + dict_size_);

    // reload the LZ4 dictionary if buf_ has changed
    if (reinterpret_cast<char*>(&out[0]) != dict_store) {
      dict_size_ = LZ4_loadDict(stream(), reinterpret_cast<char*>(&out[0]), dict_size_);
      assert(dict_size_ >= 0);
    }
  }

  auto* buf = reinterpret_cast<char*>(&out[dict_size_]);
  auto buf_size = static_cast<int>(std::min(
    out.size() - dict_size_,
    static_cast<size_t>(integer_traits<int>::const_max)) // LZ4 API uses int
  );

  #if defined(LZ4_VERSION_NUMBER) && (LZ4_VERSION_NUMBER >= 10700)
    auto lz4_size = LZ4_compress_fast_continue(stream(), src_data, buf, src_size, buf_size, 0); // 0 == use default acceleration
  #else
    auto lz4_size = LZ4_compress_limitedOutput_continue(stream(), src_data, buf, src_size, buf_size); // use for LZ4 <= v1.6.0
  #endif

  if (lz4_size < 0) {
    throw index_error("while compressing, error: LZ4 returned negative size");
  }

  return bytes_ref(reinterpret_cast<const byte_type*>(buf), lz4_size);
}

size_t lz4decompressor::decompress(
    const byte_type* src,  size_t src_size,
    byte_type* dst,  size_t dst_size) {
  assert(src_size <= integer_traits<int>::const_max); // LZ4 API uses int
  
  const auto lz4_size = LZ4_decompress_safe_continue(
    stream(),
    reinterpret_cast<const char*>(src),
    reinterpret_cast<char*>(dst),
    static_cast<int>(src_size),  // LZ4 API uses int
    static_cast<int>(std::min(dst_size, static_cast<size_t>(integer_traits<int>::const_max))) // LZ4 API uses int
  );

  return lz4_size < 0 
    ? type_limits<type_t::address_t>::invalid() // corrupted index
    : lz4_size;
}

NS_END
