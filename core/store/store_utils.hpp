////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///

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

#include "data_input.hpp"
#include "data_output.hpp"
#include "directory.hpp"
#include "shared.hpp"
#include "utils/attributes.hpp"
#include "utils/bit_utils.hpp"
#include "utils/bytes_utils.hpp"
#include "utils/numeric_utils.hpp"
#include "utils/std.hpp"
#include "utils/string.hpp"

namespace irs {

template<typename StringType,
         typename TraitsType = typename StringType::traits_type>
StringType to_string(const byte_type* begin) {
  typedef typename TraitsType::char_type char_type;

  const auto size = irs::vread<uint32_t>(begin);

  return StringType(reinterpret_cast<const char_type*>(begin), size);
}

struct enum_hash {
  template<typename T>
  size_t operator()(T value) const {
    static_assert(std::is_enum_v<T>);
    return static_cast<std::underlying_type_t<T>>(value);
  }
};

void write_zvfloat(DataOutput& out, float_t v);

float_t read_zvfloat(data_input& in);

void write_zvdouble(DataOutput& out, double_t v);

double_t read_zvdouble(data_input& in);

inline void write_zvint(DataOutput& out, int32_t v) {
  out.WriteV32(zig_zag_encode32(v));
}

inline int32_t read_zvint(data_input& in) {
  return zig_zag_decode32(in.read_vint());
}

inline void write_zvlong(DataOutput& out, int64_t v) {
  out.WriteV64(zig_zag_encode64(v));
}

inline int64_t read_zvlong(data_input& in) {
  return zig_zag_decode64(in.read_vlong());
}

inline void WriteStr(DataOutput& out, const char* s, size_t len) {
  IRS_ASSERT(len < std::numeric_limits<uint32_t>::max());
  out.WriteV32(uint32_t(len));
  out.WriteBytes(reinterpret_cast<const byte_type*>(s), len);
}

inline void WriteStr(DataOutput& out, const byte_type* s, size_t len) {
  IRS_ASSERT(len < std::numeric_limits<uint32_t>::max());
  out.WriteV32(static_cast<uint32_t>(len));
  out.WriteBytes(s, len);
}

template<typename StringType>
inline void WriteStr(DataOutput& out, const StringType& str) {
  WriteStr(out, str.data(), str.size());
}

template<typename StringType>
inline StringType read_string(data_input& in) {
  const size_t len = in.read_vint();

  StringType str(len, 0);
  [[maybe_unused]] const auto read =
    in.read_bytes(reinterpret_cast<byte_type*>(str.data()), str.size());
  IRS_ASSERT(read == str.size());
  return str;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief write to 'out' array of data pointed by 'value' of length 'size'
/// @return bytes written
////////////////////////////////////////////////////////////////////////////////
template<typename OutputIterator, typename T>
size_t WriteBytes(OutputIterator& out, const T* value, size_t size) {
  auto* data = reinterpret_cast<const byte_type*>(value);

  size = sizeof(T) * size;

  // write data out byte-by-byte
  for (auto i = size; i; --i) {
    *out = *data;
    ++out;
    ++data;
  }

  return size;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief write to 'out' raw byte representation of data in 'value'
/// @return bytes written
////////////////////////////////////////////////////////////////////////////////
template<typename OutputIterator, typename T>
size_t WriteBytes(OutputIterator& out, const T& value) {
  return WriteBytes(out, &value, 1);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief read a value of the specified type from 'in'
////////////////////////////////////////////////////////////////////////////////
template<typename T>
T& read_ref(const byte_type*& in) {
  auto& data = reinterpret_cast<T&>(*in);

  in += sizeof(T);  // increment past value

  return data;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief read an array of the specified type and length of 'size' from 'in'
////////////////////////////////////////////////////////////////////////////////
template<typename T>
T* read_ref(const byte_type*& in, size_t size) {
  auto* data = reinterpret_cast<T*>(&(*in));

  in += sizeof(T) * size;  // increment past value

  return data;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief write to 'out' size + data pointed by 'value' of length 'size'
////////////////////////////////////////////////////////////////////////////////
template<typename OutputIterator, typename CharType>
void vwrite_string(OutputIterator& out, const CharType* value, size_t size) {
  vwrite<uint64_t>(out, size);
  WriteBytes(out, value, size);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief write to 'out' data in 'value'
////////////////////////////////////////////////////////////////////////////////
template<typename OutputIterator, typename StringType>
void vwrite_string(OutputIterator& out, const StringType& value) {
  vwrite_string(out, value.data(), value.size());
}

////////////////////////////////////////////////////////////////////////////////
/// @brief read a string + size into a value of type 'StringType' from 'in'
////////////////////////////////////////////////////////////////////////////////
template<typename StringType,
         typename TraitsType = typename StringType::traits_type>
StringType vread_string(const byte_type*& in) {
  typedef typename TraitsType::char_type char_type;
  const auto size = vread<uint64_t>(in);

  return StringType(read_ref<const char_type>(in, size), size);
}

IRS_FORCE_INLINE uint64_t shift_pack_64(uint64_t val, bool b) noexcept {
  IRS_ASSERT(val <= UINT64_C(0x7FFFFFFFFFFFFFFF));
  return (val << 1) | uint64_t(b);
}

IRS_FORCE_INLINE uint32_t shift_pack_32(uint32_t val, bool b) noexcept {
  IRS_ASSERT(val <= UINT32_C(0x7FFFFFFF));
  return (val << 1) | uint32_t(b);
}

template<typename T = bool, typename U = uint64_t>
IRS_FORCE_INLINE T shift_unpack_64(uint64_t in, U& out) noexcept {
  out = static_cast<U>(in >> 1);
  return static_cast<T>(in & 1);
}

template<typename T = bool, typename U = uint32_t>
IRS_FORCE_INLINE T shift_unpack_32(uint32_t in, U& out) noexcept {
  out = static_cast<U>(in >> 1);
  return static_cast<T>(in & 1);
}

class bytes_view_input : public index_input {
 public:
  bytes_view_input() = default;
  explicit bytes_view_input(bytes_view data) noexcept
    : data_(data), pos_(data_.data()) {}

  void skip(size_t size) noexcept final {
    IRS_ASSERT(pos_ + size <= data_.data() + data_.size());
    pos_ += size;
  }

  void seek(size_t pos) noexcept override {
    IRS_ASSERT(data_.data() + pos <= data_.data() + data_.size());
    pos_ = data_.data() + pos;
  }

  size_t Position() const noexcept override {
    return std::distance(data_.data(), pos_);
  }

  size_t length() const noexcept final { return data_.size(); }

  bool eof() const noexcept final {
    return pos_ >= data_.data() + data_.size();
  }

  byte_type read_byte() noexcept final {
    IRS_ASSERT(pos_ < data_.data() + data_.size());
    return *pos_++;
  }

  const byte_type* read_buffer(size_t offset, size_t size,
                               BufferHint /*hint*/) noexcept override {
    const auto begin = data_.data() + offset;
    const auto end = begin + size;

    if (end <= data_.data() + data_.size()) {
      pos_ = end;
      return begin;
    }

    return nullptr;
  }

  const byte_type* read_buffer(size_t size,
                               BufferHint /*hint*/) noexcept final {
    const auto* pos = pos_ + size;

    if (pos <= data_.data() + data_.size()) {
      std::swap(pos, pos_);
      return pos;
    }

    return nullptr;
  }

  size_t read_bytes(byte_type* b, size_t size) noexcept final;

  size_t read_bytes(size_t offset, byte_type* b, size_t size) noexcept override;

  // append to buf
  void read_bytes(bstring& buf, size_t size);

  void reset(const byte_type* data, size_t size) noexcept {
    data_ = bytes_view(data, size);
    pos_ = data;
  }

  void reset(bytes_view ref) noexcept { reset(ref.data(), ref.size()); }

  ptr dup() const override { return std::make_unique<bytes_view_input>(*this); }

  ptr reopen() const override { return dup(); }

  int16_t read_short() noexcept final { return irs::read<uint16_t>(pos_); }

  int32_t read_int() noexcept final { return irs::read<uint32_t>(pos_); }

  int64_t read_long() noexcept final { return irs::read<uint64_t>(pos_); }

  uint64_t read_vlong() noexcept final { return irs::vread<uint64_t>(pos_); }

  uint32_t read_vint() noexcept final { return irs::vread<uint32_t>(pos_); }

  uint32_t checksum(size_t offset) const override;

 private:
  bytes_view data_;
  const byte_type* pos_{data_.data()};
};

// same as bytes_view_input but with support of adress remapping
// usable when original data offses needs to be persistent
// NOTE: remapped data blocks may have gaps but should not overlap!
class remapped_bytes_view_input : public bytes_view_input {
 public:
  using mapping_value = std::pair<size_t, size_t>;
  using mapping = std::vector<mapping_value>;

  explicit remapped_bytes_view_input(const bytes_view& data, mapping&& mapping)
    : bytes_view_input(data), mapping_{std::move(mapping)} {
    std::sort(
      mapping_.begin(), mapping_.end(),
      [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
  }

  remapped_bytes_view_input(const remapped_bytes_view_input& other)
    : bytes_view_input(other), mapping_{other.mapping_} {}

  uint32_t checksum(size_t offset) const final {
    return bytes_view_input::checksum(src_to_internal(offset));
  }

  void seek(size_t pos) noexcept final {
    bytes_view_input::seek(src_to_internal(pos));
  }

  size_t Position() const noexcept final;

  ptr dup() const final {
    return std::make_unique<remapped_bytes_view_input>(*this);
  }

  const byte_type* read_buffer(size_t offset, size_t size,
                               BufferHint hint) noexcept final {
    return bytes_view_input::read_buffer(src_to_internal(offset), size, hint);
  }

  using bytes_view_input::read_buffer;
  using bytes_view_input::read_bytes;

  size_t read_bytes(size_t offset, byte_type* b, size_t size) noexcept final {
    return bytes_view_input::read_bytes(src_to_internal(offset), b, size);
  }

 private:
  size_t src_to_internal(size_t t) const noexcept;

  mapping mapping_;
};

namespace encode::delta {

template<typename Iterator>
inline void decode(Iterator begin, Iterator end) {
  IRS_ASSERT(std::distance(begin, end) > 0);

  typedef typename std::iterator_traits<Iterator>::value_type value_type;
  const auto second = begin + 1;

  std::transform(second, end, begin, second, std::plus<value_type>());
}

template<typename Iterator>
inline void encode(Iterator begin, Iterator end) {
  IRS_ASSERT(std::distance(begin, end) > 0);

  typedef typename std::iterator_traits<Iterator>::value_type value_type;
  const auto rend = irstd::make_reverse_iterator(begin);
  const auto rbegin = irstd::make_reverse_iterator(end);

  std::transform(
    rbegin + 1, rend, rbegin, rbegin,
    [](const value_type& lhs, const value_type& rhs) { return rhs - lhs; });
}

}  // namespace encode::delta
}  // namespace irs
