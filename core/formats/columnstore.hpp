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

#ifndef IRESEARCH_COLUMNSTORE_H
#define IRESEARCH_COLUMNSTORE_H

#include "shared.hpp"

#include "formats/formats.hpp"
#include "formats/sparse_bitmap.hpp"

#include "store/memory_directory.hpp"
#include "store/store_utils.hpp"

#include "utils/bitpack.hpp"
#include "utils/encryption.hpp"
#include "utils/math_utils.hpp"
#include "utils/simd_utils.hpp"

namespace iresearch {
namespace columns {

enum class ColumnProperty : uint16_t {
  NORMAL = 0,
  DENSE = 1,        // data blocks have no gaps
  FIXED = 1 << 1,   // fixed length columns
  ENCRYPT = 1 << 2  // column contains encrypted data
};

ENABLE_BITMASK_ENUM(ColumnProperty);

////////////////////////////////////////////////////////////////////////////////
/// @class column
////////////////////////////////////////////////////////////////////////////////
class column final : public irs::columnstore_writer::column_output {
 public:
  static constexpr size_t BLOCK_SIZE = 65536;

 private:
  class offset_block {
   public:
    uint64_t back() const noexcept {
      assert(offset_ > offsets_);
      return *(offset_-1);
    }

    void push_back(uint64_t offset) noexcept {
      assert(offset_ >= offsets_);
      assert(offset_ < offsets_ + BLOCK_SIZE);
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

    void reset() noexcept {
      std::memset(offsets_, 0, sizeof offsets_);
      offset_ = std::begin(offsets_);
    }

    uint64_t* begin() noexcept { return std::begin(offsets_); }
    uint64_t* current() noexcept { return offset_; }
    uint64_t* end() noexcept { return std::end(offsets_); }

   private:
    uint64_t offsets_[BLOCK_SIZE]{};
    uint64_t* offset_{offsets_};
  };

 public:
  struct context {
    memory_allocator* alloc;
    index_output* data_out;
    encryption::stream* cipher;
    union {
      byte_type* u8buf;
      uint64_t* u64buf;
    };
    bool consolidation;
  };

  explicit column(
      const context& ctx,
      const irs::type_info& type,
      const compression::compressor::ptr& deflater)
    : ctx_(ctx),
      comp_type_(type),
      deflater_(deflater) {
  }

  void prepare(doc_id_t key) {
    if (IRS_LIKELY(key > docs_writer_.back())) {
      if (block_.full()) {
        flush_block();
      }

      docs_writer_.push_back(key);
      block_.push_back(data_.stream.file_pointer());
    }
  }

  bool empty() const noexcept {
    return block_.empty();
  }

  void finish(index_output& index_out);

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
    data_.stream.seek(block_.back());
  }

 private:
  void flush_block();

  context ctx_;
  irs::type_info comp_type_;
  compression::compressor::ptr deflater_;
  memory_output blocks_{*ctx_.alloc};
  memory_output data_{*ctx_.alloc};
  memory_output docs_{*ctx_.alloc};
  sparse_bitmap_writer docs_writer_{docs_.stream};
  offset_block block_;
  uint16_t num_blocks_{};
  bool fixed_length_{true};
}; // column

////////////////////////////////////////////////////////////////////////////////
/// @class writer
////////////////////////////////////////////////////////////////////////////////
class writer final : public columnstore_writer {
 public:
  static constexpr int32_t FORMAT_MIN = 0;
  static constexpr int32_t FORMAT_MAX = 0;

  static constexpr string_ref DATA_FORMAT_NAME = "iresearch_11_columnstore_data";
  static constexpr string_ref INDEX_FORMAT_NAME = "iresearch_11_columnstore_index";
  static constexpr string_ref DATA_FORMAT_EXT = "csd";
  static constexpr string_ref INDEX_FORMAT_EXT = "csi";

  explicit writer(bool consolidation);

  virtual void prepare(directory& dir, const segment_meta& meta) override;
  virtual column_t push_column(const column_info& info) override;
  virtual bool commit() override;
  virtual void rollback() noexcept override;

 private:
  directory* dir_;
  std::string data_filename_;
  memory_allocator* alloc_;
  std::deque<column> columns_; // pointers remain valid
  index_output::ptr data_out_;
  encryption::stream::ptr data_cipher_;
  std::unique_ptr<byte_type[]> buf_;
  bool consolidation_;
}; // writer

struct column_block {
  uint64_t addr_offset;
  uint64_t avg;
  uint64_t base;
  uint64_t data_offset;
  uint64_t rl_value;
  uint32_t bits;
};

////////////////////////////////////////////////////////////////////////////////
/// @class reader
////////////////////////////////////////////////////////////////////////////////
class reader final : public columnstore_reader {
 public:
  virtual bool prepare(
    const directory& dir,
    const segment_meta& meta) override;

  virtual const column_reader* column(
    field_id field) const override;

  virtual size_t size() const override {
    return columns_.size();
  }

 private:
  struct column_entry final : public column_reader {
    explicit column_entry(
        size_t num_blocks,
        uint64_t doc_index_offset,
        index_input* data_in,
        compression::decompressor::ptr&& inflater,
        ColumnProperty props)
      : blocks(num_blocks),
        inflater(std::move(inflater)),
        doc_index_offset(doc_index_offset),
        data_in(data_in),
        props(props) {
    }

    virtual columnstore_reader::values_reader_f values() const override;

    virtual doc_iterator::ptr iterator() const override;

    virtual bool visit(
      const columnstore_reader::values_visitor_f& reader) const override;

    virtual size_t size() const noexcept override {
      return blocks.size();
    }

    std::vector<column_block> blocks;
    compression::decompressor::ptr inflater;
    uint64_t doc_index_offset;
    index_input* data_in;
    ColumnProperty props;
  };

  void prepare_data(const directory& dir, const std::string& filename);
  void prepare_index(const directory& dir, const std::string& filename);

  std::vector<column_entry> columns_;
  encryption::stream::ptr data_cipher_;
  index_input::ptr data_in_;
}; // reader

} // columns
} // iresearch

#endif // IRESEARCH_COLUMNSTORE_H
