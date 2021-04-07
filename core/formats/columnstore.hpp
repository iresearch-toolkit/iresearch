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
//
#ifndef IRESEARCH_COLUMNSTORE_H
#define IRESEARCH_COLUMNSTORE_H

#include "shared.hpp"

#include "formats/formats.hpp"
#include "formats/sparse_bitmap.hpp"

#include "store/memory_directory.hpp"

#include "utils/bitpack.hpp"
#include "utils/encryption.hpp"
#include "utils/simd_utils.hpp"
#include "utils/math_utils.hpp"

namespace iresearch {
namespace columns {

template<size_t Size>
class offset_block {
 public:
  static constexpr size_t SIZE = Size;

  void push_back(uint64_t offset) noexcept {
    assert(offset_ >= offsets_);
    assert(offset_ < offsets_ + Size);

    if (full()) {

    }

    *offset_++ = offset;
    assert(offset >= offset_[-1]);
  }

  void pop_back() noexcept {
    assert(offset_ > offsets_);
    offset_--;
  }

  // returns number of items to be flushed
  uint32_t size() const noexcept {
    assert(offset_ >= offsets_);
    return uint32_t(offset_ - offsets_);
  }

  bool empty() const noexcept {
    return offset_ == offsets_;
  }

  bool full() const noexcept {
    return offset_ == std::end(offsets_);
  }

  uint64_t max_offset() const noexcept {
    assert(offset_ > offsets_);
    return *(offset_-1);
  }

  bool flush(data_output& out, uint32_t* buf);
  void flush();

 private:
  HWY_ALIGN uint64_t offsets_[Size]{};
  uint64_t* offset_{ offsets_ };
}; // offset_block

template<size_t Size>
void offset_block<Size>::flush() {
  assert(std::is_sorted(offsets_, offset_));
  const auto stats = simd::avg_encode<Size, true>(offsets_);

  bitpack::write_block64(
    &packed::p
  )



}


template<size_t Size>
bool offset_block<Size>::flush(data_output& out, uint32_t* buf) {
  if (empty()) {
    return true;
  }

  const auto size = this->size();

  bool fixed_length = false;

  // adjust number of elements to pack to the nearest value
  // that is multiple of the block size
  const auto block_size = math::ceil64(size, packed::BLOCK_SIZE_64);
  assert(block_size >= size);

  assert(std::is_sorted(offsets_, offset_));
  const auto stats = encode::avg::encode(offsets_, offset_);
  const auto bits = encode::avg::write_block(
    out, stats.first, stats.second,
    offsets_, block_size, buf);

  if (0 == offsets_[0] && bitpack::rl(bits)) {
    fixed_length = true;
  }

  // reset pointers and clear data
  offset_ = offsets_;
  std::memset(offsets_, 0, sizeof offsets_);

  return fixed_length;
}

class columns_writer {
 public:
  static size_t constexpr BLOCK_SIZE = 65536;

 private:
  class column final : public irs::columnstore_writer::column_output {
   public:
    explicit column(columns_writer& ctx, const irs::type_info& type,
                    const compression::compressor::ptr& compressor,
                    encryption::stream* cipher)
      : ctx_(&ctx),
        comp_type_(type),
        comp_(compressor),
        cipher_(cipher),
        data_(*ctx.alloc_) {
      assert(comp_); // ensured by `push_column'
      data_offs_ = ctx_->index_out_->file_pointer();
    }

    void prepare(doc_id_t key) {
      if (IRS_LIKELY(key > pending_key_)) {
        if (block_.full()) {
          flush();
        }

        docs_writer_.push_back(pending_key_);
        block_.push_back(data_.stream.file_pointer()); // start offset
      }
    }

    bool empty() const noexcept {
      return block_.empty();
    }

    void flush() {
       auto& index_out = *ctx_->index_out_;
       auto& data_out = *ctx_->data_out_;

       const uint64_t data_offs = data_out.file_pointer();
       data_.file >> data_out; // FIXME write directly???
       const uint64_t index_offs = data_out.file_pointer();
       block_.flush(); // FIXME flush block addr table, optimize cases with equal lengths

       // FIXME we don't need to track data_offs/index_offs per block after merge
       index_out.write_long(data_offs);
       index_out.write_long(index_offs);

       data_.stream.seek(0);
    }

    void finish() {
      docs_writer_.finish();

      auto& out = *ctx_->data_out_;

//       // evaluate overall column properties
//      auto column_props = blocks_props_;
//      if (0 != (column_props_ & CP_DENSE)) { column_props |= CP_COLUMN_DENSE; }
//      if (cipher_) { column_props |= CP_COLUMN_ENCRYPT; }
//
//      write_enum(out, column_props);
//      if (ctx_->version_ > FORMAT_MIN) {
//        write_string(out, comp_type_.name());
//        comp_->flush(out); // flush compression dependent data
//      }
//      out.write_vint(block_index_.total()); // total number of items
//      out.write_vint(max_); // max column key
//      out.write_vint(avg_block_size_); // avg data block size
//      out.write_vint(avg_block_count_); // avg number of elements per block
//      out.write_vint(column_index_.total()); // total number of index blocks
//      blocks_index_.file >> out; // column blocks index
    }

    void flush() { const auto minmax_length = simd::maxmin<IRESEARCH_COUNTOF(lengths_)>(lengths_, IRESEARCH_COUNTOF(lengths_));

      auto& index_out = *ctx_->index_out_;
      auto& data_out = *ctx_->data_out_;

      blocks_.stream.write_int(minmax_length.first);
      blocks_.stream.write_int(minmax_length.second);

      if (minmax_length.second >= minmax_length.first) {
        // write block address map
        blocks_.stream.write_long(data_out.file_pointer()); // data offset
        data_out << this->data_; // FIXME write directly???

        if (minmax_length.second > minmax_length.first) {
          blocks_.stream.write_long(data_out.file_pointer()); // lengths offset
          const auto stats = simd::avg_encode32<IRESEARCH_COUNTOF(lengths_)>(lengths_);
          data_out.write_int(stats.first);
          data_out.write_int(stats.second);
        }
      }

//      // do not take into account last block
//      const auto blocks_count = std::max(1U, column_index_.total());
//      avg_block_count_ = block_index_.flushed() / blocks_count;
//      avg_block_size_ = length_ / blocks_count;
//
//      // commit and flush remain blocks
//      flush_block();
//
//      // finish column blocks index
//      assert(ctx_->buf_.size() >= INDEX_BLOCK_SIZE*sizeof(uint64_t));
//      auto* buf = reinterpret_cast<uint64_t*>(&ctx_->buf_[0]);
//      column_index_.flush(blocks_index_.stream, buf);
//      blocks_index_.stream.flush();
    }

    virtual void close() override {
      // NOOP
    }

    virtual void write_byte(byte_type b) override {
      data_.stream.write_byte(b);
    }

    virtual void write_bytes(const byte_type* b, size_t size) override {
      data_.stream.write_bytes(b, size);
    }

    virtual void reset() override {
      if (empty()) {
        return;
      }

      // reset to previous offset
      docs_writer_.pop_back();
      block_.pop_back();
      data_.stream.seek(block_.max_offset());
    }

   private:
    columns_writer* ctx_;
    irs::type_info comp_type_;
    compression::compressor::ptr comp_;
    encryption::stream* cipher_;
    memory_output blocks_;
    memory_output data_;
    memory_output docs_;
    sparse_bitmap_writer docs_writer_{docs_.stream};
    offset_block<BLOCK_SIZE> block_;
    offset_block<BLOCK_SIZE> column_;
    doc_id_t pending_key_;
  }; // column

  memory_allocator* alloc_{ &memory_allocator::global() };
  index_output* data_out_;
  index_output* index_out_;
  std::deque<column> columns_; // pointers remain valid
  bool consolidation_;
};

class writer final : public columnstore_writer {
 public:
  static constexpr int32_t FORMAT_MIN = 0;
  static constexpr int32_t FORMAT_MAX = 0;

  static constexpr string_ref FORMAT_NAME = "iresearch_11_columnstore";
  static constexpr string_ref INDEX_FORMAT_EXT = "csi";
  static constexpr string_ref DATA_FORMAT_EXT = "csd";

  virtual void prepare(directory& dir, const segment_meta& meta) override;
  virtual column_t push_column(const column_info& info) override;
  virtual bool commit() override;
  virtual void rollback() noexcept override;

 private:
  column_writer writer_;
};

} // columns
} // iresearch

#endif // IRESEARCH_COLUMNSTORE_H
