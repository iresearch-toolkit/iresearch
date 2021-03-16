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


#include "sparse_bitmap.hpp"

#include <cstring>

#include "search/bitset_doc_iterator.hpp"

namespace {

constexpr uint32_t BITSET_THRESHOLD = (1 << 12) - 1;

enum BlockType : uint32_t {
  BT_DENSE = 0,
  BT_SPARSE
};

}

namespace iresearch {

// -----------------------------------------------------------------------------
// --SECTION--                                              sparse_bitmap_writer
// -----------------------------------------------------------------------------

void sparse_bitmap_writer::finish() {
  flush();

  // create a sentinel block to issue doc_limits::eof() automatically
  block_ = doc_limits::eof() / BLOCK_SIZE;
  set(doc_limits::eof() % BLOCK_SIZE);
  flush(1);
}

void sparse_bitmap_writer::flush(uint32_t popcnt) {
  assert(popcnt);
  assert(block_ < BLOCK_SIZE);
  assert(popcnt <= BLOCK_SIZE);

  out_->write_short(static_cast<uint16_t>(block_));
  out_->write_short(static_cast<uint16_t>(popcnt - 1)); // -1 to fit uint16_t

  if (popcnt > BITSET_THRESHOLD) {
    if (popcnt != BLOCK_SIZE) {
      if constexpr (!is_big_endian()) {
        std::for_each(
          std::begin(bits_), std::end(bits_),
          [](auto& v){ v = numeric_utils::numeric_traits<size_t>::hton(v); });
      }

      out_->write_bytes(
        reinterpret_cast<const byte_type*>(bits_),
        sizeof bits_);
    }
  } else {
    bitset_doc_iterator it(std::begin(bits_), std::end(bits_));

    while (it.next()) {
      out_->write_short(static_cast<uint16_t>(it.value()));
    }
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                                 block_seek_helper
// -----------------------------------------------------------------------------

template<uint32_t>
struct block_seek_helper;

template<>
struct block_seek_helper<BT_SPARSE> {
  template<bool Direct>
  static bool seek(sparse_bitmap_iterator* self, doc_id_t target) {
    target &= 0x0000FFFF;

    auto& ctx = self->ctx.sparse;
    const doc_id_t index_max = self->index_max_;

    for (; self->index_ < index_max; ++self->index_) {
      doc_id_t doc;
      if constexpr (!Direct) {
        doc = self->in_->read_short();
      } else if constexpr (is_big_endian()) {
        std::memcpy(&doc, ctx.u16data, sizeof(uint16_t));
        ++ctx.u16data;
      } else {
        doc = irs::read<uint16_t>(self->ctx.u8data);
      }

      if (doc >= target) {
        std::get<document>(self->attrs_).value = self->block_ | doc;
        std::get<value_index>(self->attrs_).value = self->index_++;
        return true;
      }
    }

    return false;
  }
};

template<>
struct block_seek_helper<BT_DENSE> {
  template<bool Direct>
  static bool seek(sparse_bitmap_iterator* self, doc_id_t target) {
    auto& ctx = self->ctx.dense;

    const uint32_t target_word_idx
      = (target & 0x0000FFFF) / bits_required<size_t>();
    assert(target_word_idx >= ctx.word_idx);
    auto word_delta = target_word_idx - ctx.word_idx + 1;

    if constexpr (Direct) {
      if (word_delta) {
        // FIMXE consider using SSE/avx256/avx512 extensions for large skips
        // FIXME consider align data first to avoid calling memcpy

        const size_t* end = ctx.u64data + word_delta;
        for (; ctx.u64data <= end; ++ctx.u64data) {
          std::memcpy(&ctx.word, ctx.u64data, sizeof(size_t));
          ctx.popcnt += math::math_traits<size_t>::pop(ctx.word);
        }
        ctx.word_idx = target_word_idx;
      }
    } else {
      for (; word_delta; --word_delta) {
        ctx.word = self->in_->read_long();
        ctx.popcnt += math::math_traits<size_t>::pop(ctx.word);
      }
      ctx.word_idx = target_word_idx;
    }

    const doc_id_t left = is_big_endian() || Direct // constexpr
      ? ctx.word >> (target % bits_required<size_t>())
      : ctx.word << (target % bits_required<size_t>());

    if (left) {
      const doc_id_t offset = math::math_traits<decltype(left)>::ctz(left);
      std::get<document>(self->attrs_).value = target + offset;
      std::get<value_index>(self->attrs_).value
        = ctx.popcnt - math::math_traits<decltype(left)>::pop(left);
      return true;
    }

    ++ctx.word_idx;
    for (; ctx.word_idx < sparse_bitmap_writer::NUM_BLOCKS; ++ctx.word_idx) {
      if constexpr (Direct) {
        std::memcpy(&ctx.word, ctx.u64data, sizeof(size_t));
        ++ctx.u64data;
      } else {
        ctx.word = self->in_->read_long();
      }

      if (ctx.word) {
        const doc_id_t offset = math::math_traits<size_t>::ctz(ctx.word);

        std::get<document>(self->attrs_).value
          = self->block_ + ctx.word_idx * bits_required<size_t>() + offset;
        std::get<value_index>(self->attrs_).value = ctx.popcnt;
        ctx.popcnt += math::math_traits<size_t>::pop(ctx.word);

        return true;
      }
    }

    return false;
  }
};

// -----------------------------------------------------------------------------
// --SECTION--                                            sparse_bitmap_iterator
// -----------------------------------------------------------------------------

sparse_bitmap_iterator::sparse_bitmap_iterator(index_input& in) noexcept
  : in_(&in),
    seek_func_([](sparse_bitmap_iterator* self, doc_id_t target) {
      assert(!doc_limits::valid(self->value()));
      assert(0 == (target & 0xFFFF0000));

      // we can get there iff the very
      // first block is not yet read
      self->read_block_header();
      self->seek(target);
      return true;
    }) {
}

void sparse_bitmap_iterator::read_block_header() {
  block_ = (in_->read_short() << 16);
  const uint32_t popcnt = 1 + static_cast<uint16_t>(in_->read_short());
  index_ = index_max_;
  index_max_ += popcnt;
  if (popcnt == sparse_bitmap_writer::BLOCK_SIZE) {
    ctx.all.missing = block_ - index_;
    block_end_ = in_->file_pointer();

    seek_func_ = [](sparse_bitmap_iterator* self, doc_id_t target) {
      std::get<document>(self->attrs_).value = target;
      std::get<value_index>(self->attrs_).value = target - self->ctx.all.missing;
      return true;
    };
  } else if (popcnt <= BITSET_THRESHOLD) {
    constexpr BlockType type = BT_SPARSE;
    const size_t block_size = 2*popcnt;
    ctx.u8data = in_->read_buffer(block_size, BufferHint::NORMAL);
    block_end_ = in_->file_pointer() + block_size;

    seek_func_ = ctx.u8data
      ? &block_seek_helper<type>::seek<true>
      : &block_seek_helper<type>::seek<false>;
  } else {
    constexpr BlockType type = BT_DENSE;
    constexpr size_t block_size
      = sparse_bitmap_writer::BLOCK_SIZE / bits_required<byte_type>();

    ctx.u8data = in_->read_buffer(block_size, BufferHint::NORMAL);
    block_end_ = in_->file_pointer() + block_size;

    seek_func_ = ctx.u8data
      ? block_seek_helper<type>::seek<true>
      : block_seek_helper<type>::seek<false>;
  }
}

void sparse_bitmap_iterator::seek_to_block(doc_id_t target) {
  do {
    in_->seek(block_end_);
    read_block_header();
  } while (block_ < target);
}

doc_id_t sparse_bitmap_iterator::seek(doc_id_t target) {
  const doc_id_t target_block = target & 0xFFFF0000;
  if (block_ < target_block) {
    seek_to_block(target_block);
  }

  if (block_ == target_block) {
    assert(seek_func_);
    if (seek_func_(this, target)) {
      return value();
    }
    read_block_header();
  }

  assert(seek_func_);
  seek_func_(this, block_);

  return value();
}

} // iresearch
