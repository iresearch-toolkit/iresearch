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

template<size_t Size>
class offset_block {
 public:
  static constexpr size_t SIZE = Size;

  void push_back(uint64_t offset) noexcept {
    assert(offset_ >= offsets_);
    assert(offset_ < offsets_ + Size);
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

  bool flush(data_output& out, uint64_t* buf);

 private:
  uint64_t offsets_[Size]{};
  uint64_t* offset_{ offsets_ };
}; // offset_block

template<size_t Size>
bool offset_block<Size>::flush(data_output& out, uint64_t* buf) {
  if (empty()) {
    return true;
  }

  const auto size = this->size();

  // adjust number of elements to pack to the nearest value
  // that is multiple of the block size
  const auto block_size = math::ceil64(size, packed::BLOCK_SIZE_64);
  assert(block_size >= size);

  assert(std::is_sorted(offsets_, offset_));
  const auto stats = encode::avg::encode(offsets_, offset_);
  const auto bits = encode::avg::write_block(
    [](const uint64_t* RESTRICT decoded,
       uint64_t* RESTRICT encoded,
       size_t size,
       const uint32_t bits) noexcept {
      assert(encoded);
      assert(decoded);
      assert(size);
      packed::pack(decoded, decoded + size, encoded, bits);
    },
    out, stats.first, stats.second,
    offsets_, block_size, buf);

  // reset pointers and clear data
  offset_ = offsets_;
  std::memset(offsets_, 0, sizeof offsets_);

  const bool is_fixed_length = (0 == offsets_[0] && bitpack::rl(bits));
  return is_fixed_length;
}

class column final : public irs::columnstore_writer::column_output {
 public:
  static constexpr size_t BLOCK_SIZE = 65536;

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

  struct properties {
    bool fixed_length{false};
  };

  explicit column(
      const context& ctx,
      const irs::type_info& type,
      const compression::compressor::ptr& compressor)
    : ctx_(ctx),
      comp_type_(type),
      comp_(compressor) {
  }

  void prepare(doc_id_t key) {
    if (IRS_LIKELY(key > pending_key_)) {
      if (block_.full()) {
        flush_block();
      }

      docs_writer_.push_back(key);
      block_.push_back(data_.stream.file_pointer());
      pending_key_ = key;
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
    data_.stream.seek(block_.max_offset());
  }

 private:
  void flush_block();

  context ctx_;
  irs::type_info comp_type_;
  compression::compressor::ptr comp_;
  memory_output blocks_{*ctx_.alloc};
  memory_output data_{*ctx_.alloc};
  memory_output docs_{*ctx_.alloc};
  sparse_bitmap_writer docs_writer_{docs_.stream};
  offset_block<BLOCK_SIZE> block_;
  offset_block<BLOCK_SIZE> column_;
  doc_id_t pending_key_{doc_limits::invalid()};
}; // column

class writer final : columnstore_writer {
 private:
  static constexpr int32_t FORMAT_MIN = 0;
  static constexpr int32_t FORMAT_MAX = 0;

  static constexpr string_ref DATA_FORMAT_NAME = "iresearch_11_columnstore_data";
  static constexpr string_ref INDEX_FORMAT_NAME = "iresearch_11_columnstore_index";
  static constexpr string_ref INDEX_FORMAT_EXT = "csi";
  static constexpr string_ref DATA_FORMAT_EXT = "csd";

 public:
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
};

} // columns
} // iresearch

#endif // IRESEARCH_COLUMNSTORE_H
