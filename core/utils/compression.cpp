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

#include "shared.hpp"
#include "error/error.hpp"
#include "compression.hpp"

#include <lz4.h>

NS_ROOT

compressor::compressor(unsigned int chunk_size):
  stream_(LZ4_createStream(), [](void* ptr)->void { LZ4_freeStream(reinterpret_cast<LZ4_stream_t*>(ptr)); }) {
  oversize(buf_, LZ4_COMPRESSBOUND(chunk_size));
}

void compressor::compress(const char* src, size_t size) {
  assert(size <= std::numeric_limits<int>::max()); // LZ4 API uses int
  auto src_size = static_cast<int>(size);
  auto& stream = *reinterpret_cast<LZ4_stream_t*>(stream_.get());

  oversize(buf_, LZ4_compressBound(src_size));
  LZ4_loadDict(&stream, nullptr, 0); // reset dictionary because buf_ may have changed

  auto buf_size = static_cast<int>(std::min(buf_.size(), static_cast<size_t>(std::numeric_limits<int>::max()))); // LZ4 API uses int

  #if defined(LZ4_VERSION_NUMBER) && (LZ4_VERSION_NUMBER >= 10700)
    auto lz4_size = LZ4_compress_fast_continue(&stream, src, &(buf_[0]), src_size, buf_size, 0); // 0 == use default acceleration
  #else
    auto lz4_size = LZ4_compress_limitedOutput_continue(&stream, src, &(buf_[0]), src_size, buf_size); // use for LZ4 <= v1.6.0
  #endif

  if (lz4_size < 0) {
    this->size_ = 0;
    throw index_error(); // corrupted index
  }

  this->data_ = reinterpret_cast<const byte_type*>(buf_.data());
  this->size_ = lz4_size;
}

decompressor::decompressor():
  stream_(LZ4_createStreamDecode(), [](void* ptr)->void { LZ4_freeStreamDecode(reinterpret_cast<LZ4_streamDecode_t*>(ptr)); }) {
  oversize(buf_);
}

decompressor::decompressor(unsigned int chunk_size):
  stream_(LZ4_createStreamDecode(), [](void* ptr)->void { LZ4_freeStreamDecode(reinterpret_cast<LZ4_streamDecode_t*>(ptr)); }) {
  oversize(buf_, chunk_size);
}

void decompressor::block_size(size_t size) {
  oversize(buf_, size);
}

void decompressor::decompress(const char* src, size_t size) {
  assert(size <= std::numeric_limits<int>::max()); // LZ4 API uses int
  auto src_size = static_cast<int>(size);
  auto& stream = *reinterpret_cast<LZ4_streamDecode_t*>(stream_.get());
  auto buf_size = static_cast<int>(std::min(buf_.size(), static_cast<size_t>(std::numeric_limits<int>::max()))); // LZ4 API uses int
  auto lz4_size = LZ4_decompress_safe_continue(&stream, src, &(buf_[0]), src_size, buf_size);

  if (lz4_size < 0) {
    this->size_ = 0;
    throw index_error(); // corrupted index
  }

  this->data_ = reinterpret_cast<const byte_type*>(buf_.data());
  this->size_ = lz4_size;
}

NS_END