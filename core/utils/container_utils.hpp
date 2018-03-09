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

#ifndef IRESEARCH_CONTAINER_UTILS_H
#define IRESEARCH_CONTAINER_UTILS_H

#include <memory>
#include <array>

#include "shared.hpp"
#include "math_utils.hpp"
#include "memory.hpp"
#include "noncopyable.hpp"

NS_ROOT
NS_BEGIN(container_utils)

struct bucket_size_t {
  bucket_size_t* next; // next bucket
  size_t offset; // sum of bucket sizes up to but excluding this bucket
  size_t size; // size of this bucket
};

//////////////////////////////////////////////////////////////////////////////
/// @brief compute individual sizes and offsets of exponentially sized buckets
/// @param num_buckets the number of bucket descriptions to generate
/// @param skip_bits 2^skip_bits is the size of the first bucket, consequently
///        the number of bits from a 'position' value to place into 1st bucket
//////////////////////////////////////////////////////////////////////////////
MSVC_ONLY(__pragma(warning(push)))
MSVC_ONLY(__pragma(warning(disable:4127))) // constexp conditionals are intended to be optimized out
template<size_t num_buckets, size_t skip_bits>
class bucket_meta {
 public:
  static const std::array<bucket_size_t, num_buckets>& get() NOEXCEPT {
    static const bucket_meta buckets;
    return buckets.buckets_;
  }

 private:
  bucket_meta() NOEXCEPT {
    if (!num_buckets) {
      return;
    }

    buckets_[0].size = 1 << skip_bits;
    buckets_[0].offset = 0;

    for (size_t i = 1; i < num_buckets; ++i) {
      buckets_[i - 1].next = &buckets_[i];
      buckets_[i].offset = buckets_[i - 1].offset + buckets_[i - 1].size;
      buckets_[i].size = buckets_[i - 1].size << 1;
    }

    // all subsequent buckets_ should have the same meta as the last bucket
    buckets_.back().next = &(buckets_.back());
  }

  std::array<bucket_size_t, num_buckets> buckets_;
};
MSVC_ONLY(__pragma(warning(pop)))

//////////////////////////////////////////////////////////////////////////////
/// @brief a function to calculate the bucket offset of the reqested position
///        for exponentially sized buckets, e.g. as per compute_bucket_meta()
/// @param skip_bits 2^skip_bits is the size of the first bucket, consequently
///        the number of bits from a 'position' value to place into 1st bucket
//////////////////////////////////////////////////////////////////////////////
template<size_t skip_bits>
size_t compute_bucket_offset(size_t position) NOEXCEPT {
  // 63 == 64 bits per size_t - 1 for allignment, +1 == align first value to start of bucket
  return 63 - math::clz64((position>>skip_bits) + 1);
}

class raw_block_vector_base : util::noncopyable {
 public:
  struct buffer_t {
    byte_type* data; // pointer at the actual data
    size_t offset; // sum of bucket sizes up to but excluding this buffer
    size_t size; // total buffer size
  };

  raw_block_vector_base() = default;

  raw_block_vector_base(raw_block_vector_base&& rhs) NOEXCEPT
    : buffers_(std::move(rhs.buffers_)) {
  }

  FORCE_INLINE size_t buffer_count() const NOEXCEPT {
    return buffers_.size();
  }

  FORCE_INLINE void clear() NOEXCEPT {
    buffers_.clear();
  }

  FORCE_INLINE const buffer_t& get_buffer(size_t i) const NOEXCEPT {
    return buffers_[i];
  }

  FORCE_INLINE buffer_t& get_buffer(size_t i) NOEXCEPT {
    return buffers_[i];
  }

 protected:
  struct buffer_entry_t: buffer_t, util::noncopyable {
    buffer_entry_t(size_t bucket_offset, size_t bucket_size)
      : ptr(memory::make_unique<byte_type[]>(bucket_size)) {
      buffer_t::data = ptr.get();
      buffer_t::offset = bucket_offset;
      buffer_t::size = bucket_size;
    }

    buffer_entry_t(buffer_entry_t&& other) NOEXCEPT
      : buffer_t(std::move(other)), ptr(std::move(other.ptr)) {
    }

    std::unique_ptr<byte_type[]> ptr;
  };

  buffer_t& push_buffer(size_t offset, size_t size) {
    buffers_.emplace_back(offset, size);
    return buffers_.back();
  }

  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  std::vector<buffer_entry_t> buffers_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
};

//////////////////////////////////////////////////////////////////////////////
/// @brief a container allowing raw access to internal storage, and
///        using an allocation strategy similar to an std::deque
//////////////////////////////////////////////////////////////////////////////
template<size_t num_buckets, size_t skip_bits>
class IRESEARCH_API_TEMPLATE raw_block_vector : public raw_block_vector_base {
 public:
  raw_block_vector() = default;

  raw_block_vector(raw_block_vector&& other) NOEXCEPT
    : raw_block_vector_base(std::move(other)) {
  }

  FORCE_INLINE size_t buffer_offset(size_t position) const NOEXCEPT {
    // non-precomputed bucket size is the same as the last precomputed bucket size
    return position < last_buffer_.offset
      ? compute_bucket_offset<skip_bits>(position)
      : (last_buffer_id_ + (position - last_buffer_.offset) / last_buffer_.size);
  }

  buffer_t& push_buffer() {
    if (buffers_.size() < meta_.size()) { // one of the precomputed buckets
      const auto& bucket = meta_[buffers_.size()];
      return raw_block_vector_base::push_buffer(bucket.offset, bucket.size);
    }

    // non-precomputed buckets, offset is the sum of previous buckets
    assert(!buffers_.empty()); // otherwise do not know what size buckets to create
    const auto& bucket = buffers_.back(); // most of the meta from last computed bucket
    return raw_block_vector_base::push_buffer(
      bucket.offset + bucket.size, bucket.size
    );
  }

 private:
  static const std::array<bucket_size_t, num_buckets>& meta_;
  static const bucket_size_t& last_buffer_;
  static const size_t last_buffer_id_;
};

template<size_t num_buckets, size_t skip_bits>
/*static*/ const std::array<bucket_size_t, num_buckets>& raw_block_vector<num_buckets, skip_bits>::meta_
  = bucket_meta<num_buckets, skip_bits>::get();

template<size_t num_buckets, size_t skip_bits>
/*static*/ const bucket_size_t&  raw_block_vector<num_buckets, skip_bits>::last_buffer_
  = bucket_meta<num_buckets, skip_bits>::get().back();

template<size_t num_buckets, size_t skip_bits>
/*static*/ const size_t raw_block_vector<num_buckets, skip_bits>::last_buffer_id_
  = bucket_meta<num_buckets, skip_bits>::get().size() - 1;

NS_END // container_utils
NS_END // NS_ROOT

#endif
