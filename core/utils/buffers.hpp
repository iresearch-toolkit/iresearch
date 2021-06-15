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
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_BUFFERS_H
#define IRESEARCH_BUFFERS_H

#include "shared.hpp"
#include "string.hpp"
#include "utils/math_utils.hpp"

namespace iresearch {

inline size_t oversize(
    size_t chunk_size, size_t size, size_t min_size) noexcept {
  assert(chunk_size);
  assert(min_size > size);

  typedef math::math_traits<size_t> math_traits;

  return size + math_traits::ceil(min_size-size, chunk_size);
}

// -------------------------------------------------------------------
// basic_str_builder
// -------------------------------------------------------------------

template<
  typename Elem,
  typename Traits = std::char_traits<Elem>,
  typename Alloc = std::allocator<Elem>
> class basic_str_builder : public basic_string_ref<Elem, Traits>,
                            private Alloc {
 public:
  typedef basic_string_ref<Elem, Traits> ref_type;
  typedef Alloc allocator_type;
  typedef typename ref_type::traits_type traits_type;
  typedef typename traits_type::char_type char_type;

  static constexpr size_t DEF_ALIGN = 8;

  explicit basic_str_builder(
      const allocator_type& alloc = allocator_type())
    : Alloc{alloc},
      capacity_{0} {
  }

  explicit basic_str_builder(
      size_t capacity,
      const allocator_type& alloc = allocator_type())
    : Alloc{alloc},
      capacity_{capacity} {
    if (capacity) {
      this->data_ = allocator().allocate(capacity);
    }
  }

  basic_str_builder(basic_str_builder&& rhs) noexcept
    : ref_type{rhs.data_, rhs.size_},
      allocator_type{std::move(static_cast<allocator_type&&>(rhs))},
      capacity_{rhs.capacity_} {
    rhs.data_ = nullptr;
    rhs.size_ = 0;
    rhs.capacity_ = 0;
  }

  basic_str_builder(const basic_str_builder& rhs)
    : ref_type{nullptr, rhs.size_},
      allocator_type{static_cast<const allocator_type&>(rhs)} {
    if (capacity_) {
      this->data_ = allocator().allocate(capacity_);
      traits_type::copy(data(), rhs.data_, rhs.size_);
    }
  }

  basic_str_builder& operator=(const basic_str_builder& rhs) {
    if (this != &rhs) {
      reserve(rhs.capacity_);

      if (capacity_) {
        traits_type::copy(data(), rhs.data_, this->size_ = rhs.size_);
      }
    }

    return *this;
  }

  basic_str_builder& operator=(basic_str_builder&& rhs) noexcept {
    if (this != &rhs) {
      this->data_ = rhs.data_;
      rhs.data_ = nullptr;
      this->size_ = rhs.size_;
      rhs.size_ = 0;
      this->capacity_ = rhs.capacity_;
      rhs.capacity_ = 0;
      static_cast<allocator_type&&>(*this) = static_cast<allocator_type&&>(rhs);
    }

    return *this;
  }

  virtual ~basic_str_builder() {
    destroy();
  }

  char_type* data() noexcept {
    return const_cast<char_type*>(this->data_);
  }

  void reset(size_t size) noexcept {
    assert(size <= capacity_);
    this->size_ = size;
  }

  void reset() noexcept {
    this->size_ = 0;
  }

  void append(char_type b, size_t chunk = DEF_ALIGN) {
    if (this->size() == capacity_) {
      reserve<true>(irs::oversize(chunk, capacity_, this->size() + 1));
    }
    data()[this->size_++] = b;
  }

  void assign(const char_type* b, size_t size, size_t chunk = DEF_ALIGN) {
    oversize<false>(this->size() + size, chunk);
    traits_type::copy(data(), b, size);
    this->size_ = size;
  }

  template<bool PreserveContent = true>
  void oversize(size_t minsize, size_t chunk = DEF_ALIGN) {
    if (minsize > capacity_) {
      reserve<PreserveContent>(irs::oversize(chunk, capacity_, minsize));
    }
  }

  template<bool PreserveContent = true>
  void reserve(size_t size) {
    assert(this->capacity_ >= this->size());

    if (size > capacity_) {
      char_type* newdata = allocator().allocate(size);
      if constexpr (PreserveContent) {
        traits_type::copy(newdata, this->data_, this->size());
      }
      destroy();
      this->data_ = newdata;
      this->capacity_ = size;
    }
  }

 private:
  allocator_type& allocator() noexcept {
    return static_cast<allocator_type&>(*this);
  }

  void destroy() noexcept {
    allocator().deallocate(data(), capacity_);
  }

  size_t capacity_;
};

typedef basic_str_builder<byte_type> bytes_builder;

}

#endif
