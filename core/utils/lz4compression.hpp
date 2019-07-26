////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_LZ4COMPRESSION_H
#define IRESEARCH_LZ4COMPRESSION_H

#include "string.hpp"
#include "noncopyable.hpp"

#include <memory>
#include <lz4.h>

NS_ROOT

class IRESEARCH_API lz4compressor : private util::noncopyable {
 public:
#ifdef IRESEARCH_DLL
  lz4compressor() : stream_(LZ4_createStream()) {
    if (!stream_) {
      throw std::bad_alloc();
    }
  }
  ~lz4compressor() { LZ4_freeStream(stream_); }
#else
  lz4compressor() { LZ4_resetStream(&stream_); }
#endif

  bytes_ref compress(const byte_type* src, size_t size, bstring& out);

  template<typename Source>
  bytes_ref compress(const Source& src, bstring& out) {
    return compress(src.c_str(), src.size(), out);
  }

 private:
#ifdef IRESEARCH_DLL
  LZ4_stream_t* stream() NOEXCEPT { return stream_; }
  LZ4_stream_t* stream_;
#else
  LZ4_stream_t* stream() NOEXCEPT { return &stream_; }
  LZ4_stream_t stream_;
#endif
  int dict_size_{}; // the size of the LZ4 dictionary from the previous call
}; // compressor

class IRESEARCH_API lz4decompressor : private util::noncopyable {
 public:
#ifdef IRESEARCH_DLL
  lz4decompressor() : stream_(LZ4_createStreamDecode()) {
    if (!stream_) {
      throw std::bad_alloc();
    }
  }
  ~lz4decompressor() { LZ4_freeStreamDecode(stream_); }
#else
  lz4decompressor() NOEXCEPT {
    std::memset(&stream_, 0, sizeof(stream_));
  }
#endif

  // returns number of decompressed bytes,
  // or integer_traits<size_t>::const_max in case of error
  size_t decompress(const byte_type* src, size_t src_size,
                    byte_type* dst, size_t dst_size);

 private:
#ifdef IRESEARCH_DLL
  LZ4_streamDecode_t* stream() NOEXCEPT { return stream_; }
  LZ4_streamDecode_t* stream_;
#else
  LZ4_streamDecode_t* stream() NOEXCEPT { return &stream_; }
  LZ4_streamDecode_t stream_;
#endif
}; // decompressor

NS_END // NS_ROOT

#endif
