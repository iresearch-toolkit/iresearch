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

#include "utils/encryption.hpp"
#include "utils/simd_utils.hpp"
#include "utils/math_utils.hpp"

namespace iresearch {
namespace columns {

template<typename Key, Key Invalid, size_t BlockSize>
class columns_writer {
 public:
  static_assert (math::is_power2(BlockSize));

  static_assert(std::is_same_v<Key, uint32_t> ||
                std::is_same_v<Key, uint64_t>);

  using key_type = Key;

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
    }

    void prepare(key_type key) {
      if (IRS_LIKELY(key > pending_key_)) {
        if (IRS_LIKELY(pending_key_ != Invalid)) {
          docs_writer_.add(pending_key_);

          const uint64_t offset = data_.stream.file_pointer();
          *length_++ = offset - pending_offset_;

          if (std::end(lengths_) == length_) {
            flush();
          }

          last_key_ = pending_key_;
          pending_offset_ = offset;
        }

        pending_key_ = key;
      }
    }

    bool empty() const noexcept {
      return std::begin(lengths_) == length_;
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

    void flush() {
      const auto minmax_length = simd::maxmin(lengths_, IRESEARCH_COUNTOF(lengths_));

      auto& index_out = *ctx_->index_out_;
      auto& data_out = *ctx_->data_out_;


      index_out.write_int(minmax_length.first);
      index_out.write_int(minmax_length.second);

      if (minmax_length.second > minmax_length.first) {
        // write block address map

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
        // nothing to reset
        return;
      }

      // reset to previous offset
      pending_key_ = last_key_;
      data_.stream.seek(pending_offset_);
    }

   private:
    columns_writer* ctx_;
    irs::type_info comp_type_;
    compression::compressor::ptr comp_;
    encryption::stream* cipher_;
    memory_output data_;
    memory_output docs_;
    sparse_bitmap_writer docs_writer_{docs_.stream};
    uint32_t lengths_[BlockSize]{};
    uint32_t* length_{lengths_};
    uint64_t pending_offset_{};
    key_type pending_key_{Invalid};
    key_type last_key_{ Invalid };
  }; // column

  memory_allocator* alloc_{ &memory_allocator::global() };
  index_output* data_out_;
  index_output* index_out_;
  std::deque<column> columns_; // pointers remain valid
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
