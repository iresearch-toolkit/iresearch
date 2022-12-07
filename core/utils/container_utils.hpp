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

#pragma once

#include <array>
#include <memory>

#include "shared.hpp"
#include "utils/ebo_ref.hpp"
#include "utils/math_utils.hpp"
#include "utils/memory.hpp"
#include "utils/noncopyable.hpp"
#include "utils/object_pool.hpp"

namespace irs {
namespace container_utils {

// Ssame as 'std::array' but this implementation capable of storing
// objects without default constructor.
template<typename T, size_t Size>
class array
  : private irs::memory::aligned_storage<sizeof(T) * Size, alignof(T)>,
    private util::noncopyable {
 private:
  typedef irs::memory::aligned_storage<sizeof(T) * Size, alignof(T)> buffer_t;

 public:
  typedef T value_type;
  typedef T& reference;
  typedef const T& const_reference;
  typedef T* iterator;
  typedef const T* const_iterator;
  typedef std::reverse_iterator<iterator> reverse_iterator;
  typedef std::reverse_iterator<const_iterator> const_reverse_iterator;

  static const size_t SIZE = Size;

  template<typename... Args>
  explicit array(Args&&... args) {
    auto begin = this->begin();
    auto end = this->end();

    for (; begin != end; ++begin) {
      new (begin) T(std::forward<Args>(args)...);
    }
  }

  ~array() noexcept {
    auto begin = this->begin();
    auto end = this->end();

    for (; begin != end; ++begin) {
      begin->~T();
    }
  }

  constexpr reference operator[](size_t i) noexcept {
    IRS_ASSERT(i < Size);
    return *(begin() + i);
  }

  constexpr const_reference operator[](size_t i) const noexcept {
    return const_cast<array&>(*this)[i];
  }

  constexpr reference back() noexcept { return *(end() - 1); }

  constexpr const_reference back() const noexcept {
    return const_cast<array*>(this)->back();
  }

  constexpr reference front() noexcept { return *begin(); }

  constexpr const_reference front() const noexcept {
    return const_cast<array*>(this)->front();
  }

  constexpr iterator begin() noexcept {
    return reinterpret_cast<T*>(buffer_t::data);
  }

  constexpr iterator end() noexcept { return this->begin() + Size; }

  constexpr const_iterator begin() const noexcept {
    return const_cast<array*>(this)->begin();
  }

  constexpr const_iterator end() const noexcept {
    return const_cast<array*>(this)->end();
  }

  constexpr reverse_iterator rbegin() noexcept {
    return reverse_iterator(end());
  }

  constexpr reverse_iterator rend() noexcept {
    return reverse_iterator(begin());
  }

  constexpr const_reverse_iterator rbegin() const noexcept {
    return const_reverse_iterator(end());
  }

  constexpr const_reverse_iterator rend() const noexcept {
    return const_reverse_iterator(begin());
  }

  constexpr size_t size() const noexcept { return Size; }

  constexpr bool empty() const noexcept { return 0 == size(); }
};

struct bucket_size_t {
  bucket_size_t* next;  // next bucket
  size_t offset;        // sum of bucket sizes up to but excluding this bucket
  size_t size;          // size of this bucket
  size_t index;         // bucket index
};

//////////////////////////////////////////////////////////////////////////////
/// @brief compute individual sizes and offsets of exponentially sized buckets
/// @param NumBuckets the number of bucket descriptions to generate
/// @param SkipBits 2^SkipBits is the size of the first bucket, consequently
///        the number of bits from a 'position' value to place into 1st bucket
//////////////////////////////////////////////////////////////////////////////

MSVC_ONLY(__pragma(warning(push)))  // cppcheck-suppress unknownMacro
MSVC_ONLY(__pragma(warning(
  disable : 4127)))  // constexp conditionals are intended to be optimized out
template<size_t NumBuckets, size_t SkipBits>
class bucket_meta {
 public:
  static const std::array<bucket_size_t, NumBuckets>& get() noexcept {
    static const bucket_meta buckets;
    return buckets.buckets_;
  }

 private:
  bucket_meta() noexcept {
    if (buckets_.empty()) {
      return;
    }

    buckets_[0].size = 1 << SkipBits;
    buckets_[0].offset = 0;
    buckets_[0].index = 0;

    for (size_t i = 1; i < NumBuckets; ++i) {
      buckets_[i - 1].next = &buckets_[i];
      buckets_[i].offset = buckets_[i - 1].offset + buckets_[i - 1].size;
      buckets_[i].size = buckets_[i - 1].size << 1;
      buckets_[i].index = i;
    }

    // all subsequent buckets_ should have the same meta as the last bucket
    buckets_.back().next = &(buckets_.back());
  }

  std::array<bucket_size_t, NumBuckets> buckets_;
};
MSVC_ONLY(__pragma(warning(pop)))

namespace memory {

template<typename BucketFactory, size_t Size>
class bucket_allocator : private util::noncopyable {
 public:
  // number of pools
  static const size_t SIZE = Size;

  using pool_type = unbounded_object_pool<BucketFactory>;
  using value_type = typename pool_type::ptr;

  explicit bucket_allocator(size_t pool_size) : pools_(pool_size) {}

  value_type allocate(const bucket_size_t& bucket) {
    IRS_ASSERT(bucket.index < pools_.size());
    return pools_[bucket.index].emplace(bucket.size);
  }

  void clear() {
    for (auto& pool : pools_) {
      pool.clear();
    }
  }

 private:
  array<pool_type, Size> pools_;
};

// default stateless allocator
struct default_allocator {
  typedef std::unique_ptr<byte_type[]> value_type;

  value_type allocate(const bucket_size_t& bucket) {
    return std::make_unique<byte_type[]>(bucket.size);
  }
};

}  // namespace memory

//////////////////////////////////////////////////////////////////////////////
/// @brief a function to calculate the bucket offset of the reqested position
///        for exponentially sized buckets, e.g. as per compute_bucket_meta()
/// @param SkipBits 2^SkipBits is the size of the first bucket, consequently
///        the number of bits from a 'position' value to place into 1st bucket
//////////////////////////////////////////////////////////////////////////////
template<size_t SkipBits>
size_t compute_bucket_offset(size_t position) noexcept {
  // 63 == 64 bits per size_t - 1 for allignment, +1 == align first value to
  // start of bucket
  return 63 - std::countl_zero((position >> SkipBits) + 1);
}

template<typename Allocator>
class raw_block_vector_base : private util::noncopyable {
 public:
  using allocator_type = Allocator;

  struct buffer_t {
    byte_type* data;  // pointer at the actual data
    size_t offset;    // sum of bucket sizes up to but excluding this buffer
    size_t size;      // total buffer size
  };

  explicit raw_block_vector_base(const allocator_type& alloc) noexcept
    : alloc_{alloc} {}

  raw_block_vector_base(raw_block_vector_base&& rhs) noexcept
    : alloc_{std::move(rhs.alloc_)}, buffers_(std::move(rhs.buffers_)) {}

  IRS_FORCE_INLINE size_t buffer_count() const noexcept {
    return buffers_.size();
  }

  IRS_FORCE_INLINE bool empty() const noexcept { return buffers_.empty(); }

  IRS_FORCE_INLINE void clear() noexcept { buffers_.clear(); }

  IRS_FORCE_INLINE const buffer_t& get_buffer(size_t i) const noexcept {
    return buffers_[i];
  }

  IRS_FORCE_INLINE buffer_t& get_buffer(size_t i) noexcept {
    return buffers_[i];
  }

  IRS_FORCE_INLINE void pop_buffer() { buffers_.pop_back(); }

 protected:
  struct buffer_entry_t : buffer_t, util::noncopyable {
    buffer_entry_t(size_t bucket_offset, size_t bucket_size,
                   typename allocator_type::value_type&& ptr) noexcept
      : ptr{std::move(ptr)} {
      buffer_t::data = this->ptr.get();
      buffer_t::offset = bucket_offset;
      buffer_t::size = bucket_size;
    }

    buffer_entry_t(buffer_entry_t&& other) noexcept
      : buffer_t{std::move(other)}, ptr{std::move(other.ptr)} {}

    typename Allocator::value_type ptr;
  };

  static_assert(std::is_nothrow_move_constructible_v<buffer_entry_t>,
                "default move constructor expected");

  buffer_t& push_buffer(size_t offset, const bucket_size_t& bucket) {
    buffers_.emplace_back(offset, bucket.size, alloc_.get().allocate(bucket));
    return buffers_.back();
  }

  IRS_NO_UNIQUE_ADDRESS EboRef<allocator_type> alloc_;
  std::vector<buffer_entry_t> buffers_;
};

//////////////////////////////////////////////////////////////////////////////
/// @brief a container allowing raw access to internal storage, and
///        using an allocation strategy similar to an std::deque
//////////////////////////////////////////////////////////////////////////////
template<size_t NumBuckets, size_t SkipBits,
         typename Allocator = memory::default_allocator>
class raw_block_vector : public raw_block_vector_base<Allocator> {
 public:
  static const size_t NUM_BUCKETS = NumBuckets;  // total number of buckets
  static const size_t FIRST_BUCKET_SIZE = 1 << SkipBits;

  typedef raw_block_vector_base<Allocator> base_t;
  typedef typename base_t::allocator_type allocator_type;

  explicit raw_block_vector(const Allocator& alloc /*= Allocator()*/) noexcept
    : base_t{alloc} {}

  raw_block_vector(raw_block_vector&& other) noexcept
    : base_t{std::move(other)} {}

  IRS_FORCE_INLINE size_t buffer_offset(size_t position) const noexcept {
    // non-precomputed bucket size is the same as the last precomputed bucket
    // size
    return position < LAST_BUFFER.offset
             ? compute_bucket_offset<SkipBits>(position)
             : (LAST_BUFFER_ID +
                (position - LAST_BUFFER.offset) / LAST_BUFFER.size);
  }

  typename base_t::buffer_t& push_buffer() {
    if (base_t::buffers_.size() <
        META.size()) {  // one of the precomputed buckets
      const auto& bucket = META[base_t::buffers_.size()];
      return base_t::push_buffer(bucket.offset, bucket);
    }

    // non-precomputed buckets, offset is the sum of previous buckets
    IRS_ASSERT(
      !base_t::buffers_
         .empty());  // otherwise do not know what size buckets to create
    const auto& bucket =
      base_t::buffers_.back();  // most of the meta from last computed bucket
    return base_t::push_buffer(bucket.offset + bucket.size, META.back());
  }

 private:
  static const std::array<bucket_size_t, NumBuckets>& META;
  static const bucket_size_t& LAST_BUFFER;
  static const size_t LAST_BUFFER_ID;
};

template<size_t NumBuckets, size_t SkipBits, typename Allocator>
/*static*/ const std::array<bucket_size_t, NumBuckets>&
  raw_block_vector<NumBuckets, SkipBits, Allocator>::META =
    bucket_meta<NumBuckets, SkipBits>::get();

template<size_t NumBuckets, size_t SkipBits, typename Allocator>
/*static*/ const bucket_size_t&
  raw_block_vector<NumBuckets, SkipBits, Allocator>::LAST_BUFFER =
    bucket_meta<NumBuckets, SkipBits>::get().back();

template<size_t NumBuckets, size_t SkipBits, typename Allocator>
/*static*/ const size_t
  raw_block_vector<NumBuckets, SkipBits, Allocator>::LAST_BUFFER_ID =
    bucket_meta<NumBuckets, SkipBits>::get().size() - 1;

}  // namespace container_utils
}  // namespace irs
