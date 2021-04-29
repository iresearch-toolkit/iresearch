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

#include "shared.hpp"

#include "analysis/token_attributes.hpp"

#include "search/cost.hpp"
#include "index/iterators.hpp"

#include "utils/bit_utils.hpp"
#include "utils/bitset.hpp"
#include "utils/frozen_attributes.hpp"
#include "utils/math_utils.hpp"
#include "utils/range.hpp"
#include "utils/type_limits.hpp"

namespace iresearch {

//////////////////////////////////////////////////////////////////////////////
/// @class sparse_bitmap_writer
/// @note
/// union {
///   doc_id_t doc;
///   struct {
///     uint16_t block;
///     uint16_t block_offset
///   }
/// };
//////////////////////////////////////////////////////////////////////////////
class sparse_bitmap_writer {
 public:
  using value_type = doc_id_t; // for compatibility with back_inserter

  static constexpr uint32_t BLOCK_SIZE = 1 << 16;
  static constexpr uint32_t NUM_BLOCKS = BLOCK_SIZE / bits_required<size_t>();

  static_assert(math::is_power2(BLOCK_SIZE));

  struct block {
    doc_id_t index;
    uint32_t offset;
  };

  explicit sparse_bitmap_writer(index_output& out) noexcept(noexcept(out.file_pointer()))
    : out_{&out},
      origin_{out.file_pointer()} {
  }

  void push_back(doc_id_t doc) {
    static_assert(math::is_power2(BLOCK_SIZE));
    assert(doc_limits::valid(doc));
    assert(!doc_limits::eof(doc));
    assert(doc > prev_);

    const uint32_t block = doc / BLOCK_SIZE;

    if (block != block_) {
      flush(block_);
      block_ = block;
    }

    set(doc % BLOCK_SIZE);
    prev_ = doc;
  }

  void pop_back() noexcept {
    irs::set_bit(bits_[prev_ / bits_required<size_t>()],
                 prev_ % bits_required<size_t>());
  }

  doc_id_t back() const noexcept {
    return prev_;
  }

  void finish();

  void visit_index(const std::function<void(const block&, size_t)>& callback);

 private:
  void flush(uint32_t next_block) {
    const uint32_t popcnt = static_cast<uint32_t>(
      math::math_traits<size_t>::pop(std::begin(bits_), std::end(bits_)));
    if (popcnt) {
      add_block(next_block);
      do_flush(popcnt);
      popcnt_ += popcnt;
      std::memset(bits_, 0, sizeof bits_);
    }
  }

  FORCE_INLINE void set(doc_id_t value) noexcept {
    irs::set_bit(bits_[value / bits_required<size_t>()],
                 value % bits_required<size_t>());
  }

  FORCE_INLINE void add_block(uint32_t block_id) {
    const uint64_t offset = out_->file_pointer() - origin_;
    assert(offset <= std::numeric_limits<uint32_t>::max());
    block_index_.emplace_back(block{popcnt_, static_cast<uint32_t>(offset)}, block_id);
  }

  void do_flush(uint32_t popcnt);

  index_output* out_;
  uint64_t origin_;
  size_t bits_[NUM_BLOCKS]{};
  std::vector<std::pair<block, uint32_t>> block_index_;
  doc_id_t prev_{};
  uint32_t popcnt_{};
  uint32_t block_{}; // last flushed block
}; // sparse_bitmap_writer

//////////////////////////////////////////////////////////////////////////////
/// @struct value_index
/// @brief denotes a position of a value associated with a document
//////////////////////////////////////////////////////////////////////////////
struct value_index : document {
  static constexpr string_ref type_name() noexcept {
    return "value_index";
  }
}; // value_index

//////////////////////////////////////////////////////////////////////////////
/// @class sparse_bitmap_iterator
//////////////////////////////////////////////////////////////////////////////
class sparse_bitmap_iterator final : public doc_iterator {
 public:
  using block_index_t = range<const sparse_bitmap_writer::block>;

  explicit sparse_bitmap_iterator(index_input::ptr&& in, const block_index_t& block_index = {})
    : sparse_bitmap_iterator(memory::to_managed<index_input>(std::move(in)), block_index) {
  }
  explicit sparse_bitmap_iterator(index_input* in, const block_index_t& block_index = {})
    : sparse_bitmap_iterator(memory::to_managed<index_input, false>(in), block_index) {
  }

  template<typename Cost>
  sparse_bitmap_iterator(index_input::ptr&& in, const block_index_t& block_index, Cost&& est)
    : sparse_bitmap_iterator(std::move(in), block_index) {
    std::get<cost>(attrs_).reset(std::forward<Cost>(est));
  }

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

  /// @note the value is undefined for
  ///       doc_limits::invalid() and doc_limits::eof()
  doc_id_t index() const noexcept {
    return std::get<value_index>(attrs_).value;
  }

 private:
  using block_seek_f = bool(*)(sparse_bitmap_iterator*, doc_id_t);

  template<uint32_t>
  friend struct container_iterator;

  struct container_iterator_context {
    union {
      struct {
        const uint16_t* u16data;
      } sparse;
      struct {
        const size_t* u64data;
        doc_id_t popcnt;
        int32_t word_idx;
        size_t word;
      } dense;
      struct {
        const void* ignore;
        doc_id_t missing;
      } all;
      const byte_type* u8data;
    };
  };

  explicit sparse_bitmap_iterator(
    memory::managed_ptr<index_input>&& in,
    const block_index_t& block_index);

  void seek_to_block(doc_id_t block);
  void read_block_header();

  container_iterator_context ctx_;
  std::tuple<document, cost, value_index> attrs_;
  memory::managed_ptr<index_input> in_;
  block_seek_f seek_func_;
  block_index_t block_index_;
  uint64_t cont_begin_;
  uint64_t origin_;
  doc_id_t index_{};
  doc_id_t index_max_{};
  doc_id_t block_{};
}; // sparse_bitmap_iterator

} // iresearch

#endif // IRESEARCH_SPARSE_BITMAP_H
