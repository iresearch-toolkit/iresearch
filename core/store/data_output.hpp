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

#pragma once

#include "utils/bytes_utils.hpp"
#include "utils/io_utils.hpp"
#include "utils/noncopyable.hpp"
#include "utils/string.hpp"

namespace irs {

#define IRS_UNREACHABLE() \
  do {                    \
    IRS_ASSERT(false);    \
    ABSL_UNREACHABLE();   \
  } while (false)

template<typename T>
T byteswap(T value) {
  return value;
}

template<typename N, typename Assign>
IRS_FORCE_INLINE void WriteVarBytes(N n, Assign&& assign) {
  static constexpr N kMax = 0x80;
  // TODO(MBkkt) Maybe unrolled cycle is better?
  while (n >= kMax) {
    assign(static_cast<byte_type>(n | kMax));
    n >>= 7;
  }
  assign(static_cast<byte_type>(n));
}

class DataOutput {
 public:
  virtual ~DataOutput() = default;

  // According to C++ standard it's safe to convert signed to unsigned
  // But not opposite!
  // So we should read signed but write unsigned or duplicate code.
  // Duplicate code potentially worse for perfomance:
  //  more virtual functions in vtable, symbols, etc -- code colder

  virtual void WriteByte(byte_type b) = 0;
  virtual void WriteBytes(const byte_type* b, size_t len) = 0;

  virtual void WriteU16(uint16_t n) = 0;
  virtual void WriteU32(uint32_t n) = 0;
  virtual void WriteU64(uint64_t n) = 0;
  virtual void WriteV32(uint32_t n) = 0;
  virtual void WriteV64(uint64_t n) = 0;

 protected:
  template<typename N>
  IRS_FORCE_INLINE void WriteFixImpl(N n) {
    // TODO(MBkkt) change to little in new version
    n = absl::big_endian::FromHost(n);
    WriteBytes(reinterpret_cast<byte_type*>(&n), sizeof(n));
  }

  template<typename N>
  IRS_FORCE_INLINE void WriteVarImpl(N n) {
    WriteVarBytes(n, [&](byte_type b) { WriteByte(b); });
  }
};

// Instead of default implementation to avoid double virtual call
#define IRS_DATA_OUTPUT_MEMBERS                        \
  void WriteU16(uint16_t n) final { WriteFixImpl(n); } \
  void WriteU32(uint32_t n) final { WriteFixImpl(n); } \
  void WriteU64(uint64_t n) final { WriteFixImpl(n); } \
  void WriteV32(uint32_t n) final { WriteVarImpl(n); } \
  void WriteV64(uint64_t n) final { WriteVarImpl(n); }

class BufferedOutput : public DataOutput {
 public:
  void WriteByte(byte_type b) final {
    if (IRS_UNLIKELY(pos_ == end_)) {
      FlushBuffer();
    }
    *pos_++ = b;
  }

  void WriteBytes(const byte_type* b, size_t len) override;

  void WriteU16(uint16_t n) final { WriteFixImpl(n); }
  void WriteU32(uint32_t n) final { WriteFixImpl(n); }
  void WriteU64(uint64_t n) final { WriteFixImpl(n); }

  void WriteV32(uint32_t n) final { WriteVarImpl(n); }
  void WriteV64(uint64_t n) final { WriteVarImpl(n); }

 protected:
  BufferedOutput(byte_type* buf, byte_type* end)
    : buf_{buf}, pos_{buf}, end_{end} {}

  IRS_FORCE_INLINE size_t Length() const noexcept { return pos_ - buf_; }
  IRS_FORCE_INLINE size_t Remain() const noexcept { return end_ - pos_; }

  IRS_FORCE_INLINE void FlushBuffer() {
    WriteDirect(buf_, Length());
    pos_ = buf_;
  }

  virtual void WriteDirect(const byte_type* b, size_t len) = 0;

  IRS_FORCE_INLINE void WriteBuffer(const byte_type* b, size_t len) {
    std::memcpy(pos_, b, len);
    pos_ += len;
  }

  byte_type* buf_{};
  byte_type* pos_{};
  byte_type* end_{};

 private:
  template<typename N>
  IRS_FORCE_INLINE void WriteFixImpl(N n) {
    static constexpr size_t Needed = sizeof(n);
    // TODO(MBkkt) change to little in new version
    n = absl::big_endian::FromHost(n);
    if (const auto remain = Remain(); IRS_UNLIKELY(remain < Needed)) {
      return WriteFixFlush(n, remain);
    }
    std::memcpy(buf_, &n, Needed);
    pos_ += Needed;
  }

  template<typename N>
  IRS_FORCE_INLINE void WriteVarImpl(N n) {
    static constexpr size_t Needed = (sizeof(n) * 8 + 6) / 7;
    if (const auto remain = Remain(); IRS_UNLIKELY(remain < Needed)) {
      return WriteVarFlush(n, remain);
    }
    WriteVarBytes(n, [&](byte_type b) { *pos_++ = b; });
  }

  template<typename N>
  IRS_NO_INLINE void WriteFixFlush(N n, size_t size);

  template<typename N>
  IRS_NO_INLINE void WriteVarFlush(N n, size_t size);
};

class IndexOutput : public BufferedOutput {
 public:
  using BufferedOutput::BufferedOutput;

  IRS_USING_IO_PTR(IndexOutput, Close);

  virtual size_t Position() const noexcept = 0;

  // Flush output
  virtual void Flush() = 0;

  // Flush and compute checksum output
  virtual uint32_t Checksum() = 0;

  // Flush and close output
  virtual void Close() = 0;
};

}  // namespace irs
