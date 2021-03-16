////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_SPARSE_BITMAP_H
#define IRESEARCH_SPARSE_BITMAP_H

#include <cassert>

#include "shared.hpp"

#include "analysis/token_attributes.hpp"

#include "search/cost.hpp"
#include "index/iterators.hpp"

#include "utils/frozen_attributes.hpp"
#include "utils/bit_utils.hpp"
#include "utils/bitset.hpp"
#include "utils/math_utils.hpp"
#include "utils/type_limits.hpp"

namespace iresearch {

struct index_output;

///
/// @class sparse_bitmap_writer
///
/// union {
///   doc_id_t doc;
///   struct {
///     uint16_t block;
///     uint16_t block_offset
///   }
/// };
class sparse_bitmap_writer {
 public:
  using value_type = doc_id_t; // for compatibility with back_inserter

  static constexpr uint32_t BLOCK_SIZE = 1 << 16;
  static constexpr uint32_t NUM_BLOCKS = BLOCK_SIZE / bits_required<size_t>();

  explicit sparse_bitmap_writer(index_output& out) noexcept
    : out_(&out) {
  }

  void push_back(doc_id_t doc) {
    static_assert(math::is_power2(BLOCK_SIZE));
    assert(doc_limits::valid(doc));
    assert(!doc_limits::eof(doc));

    const uint32_t block = doc / BLOCK_SIZE;

    if (block != block_) {
      flush();
      block_ = block;
    }

    set(doc % BLOCK_SIZE);
  }

  void finish();

 private:
  void flush() {
    const uint32_t popcnt = static_cast<uint32_t>(
      math::math_traits<size_t>::pop(std::begin(bits_), std::end(bits_)));
    if (popcnt) {
      flush(popcnt);
      popcnt_ += popcnt;
      std::memset(bits_, 0, sizeof bits_);
    }
  }

  FORCE_INLINE void set(doc_id_t value) noexcept {
    irs::set_bit(bits_[value / bits_required<size_t>()],
                 value % bits_required<size_t>());
  }

  void flush(uint32_t popcnt);

  index_output* out_;
  size_t bits_[NUM_BLOCKS]{};
  uint32_t popcnt_{};
  uint32_t block_{}; // last flushed block
};

struct value_index : document {
  static constexpr string_ref type_name() noexcept {
    return "value_index";
  }
};

class sparse_bitmap_iterator final : public doc_iterator {
 public:
  explicit sparse_bitmap_iterator(index_input& in) noexcept;

  virtual attribute* get_mutable(irs::type_info::type_id type) noexcept override final {
    return irs::get_mutable(attrs_, type);
  }

  virtual doc_id_t seek(doc_id_t target) override final;

  virtual bool next() override final {
    return !doc_limits::eof(seek(value() + 1));
  }

  virtual doc_id_t value() const noexcept override final {
    return std::get<document>(attrs_).value;
  }

  doc_id_t index() const noexcept {
    return std::get<value_index>(attrs_).value;
  }

 private:
  using block_seek_f = bool(*)(sparse_bitmap_iterator*, doc_id_t);

  template<uint32_t>
  friend struct block_seek_helper;

  void seek_to_block(doc_id_t block);
  void read_block_header();

  struct block_ctx {
    union {
      struct {
        const uint16_t* u16data;
      } sparse;
      struct {
        const size_t* u64data;
        doc_id_t popcnt;
        uint32_t word_idx;
        size_t word;
      } dense;
      struct {
        doc_id_t missing;
      } all;
      const byte_type* u8data;
    };
  } ctx;

  index_input* in_;
  std::tuple<document, cost, value_index> attrs_;
  block_seek_f seek_func_;
  size_t block_end_;
  doc_id_t index_;
  doc_id_t index_max_;
  doc_id_t block_;
};

} // iresearch

#endif // IRESEARCH_SPARSE_BITMAP_H
