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

#include "store_utils.hpp"

#include "shared.hpp"
#include "utils/crc.hpp"
#include "utils/memory.hpp"
#include "utils/std.hpp"

namespace irs {

// TODO(MBkkt) Remove zv with columnstore

void write_zvfloat(DataOutput& out, float_t v) {
  const int32_t iv = numeric_utils::ftoi32(v);

  if (iv >= -1 && iv <= 125) {
    // small signed values between [-1 and 125]
    out.WriteByte(static_cast<byte_type>(0x80 | (1 + iv)));
  } else if (!std::signbit(v)) {
    // positive value
    out.WriteU32(iv);
  } else {
    // negative value
    out.WriteByte(0xFF);
    out.WriteU32(iv);
  }
};

float_t read_zvfloat(data_input& in) {
  const uint32_t b = in.read_byte();

  if (0xFF == b) {
    // negative value
    return numeric_utils::i32tof(in.read_int());
  } else if (0 != (b & 0x80)) {
    // small signed value
    return float_t((b & 0x7F) - 1);
  }

  // positive float (ensure read order)
  const auto part = uint32_t(uint16_t(in.read_short())) << 8;

  return numeric_utils::i32tof((b << 24) | part | uint32_t(in.read_byte()));
}

void write_zvdouble(DataOutput& out, double_t v) {
  const int64_t lv = numeric_utils::dtoi64(v);

  if (lv > -1 && lv <= 124) {
    // small signed values between [-1 and 124]
    out.WriteByte(static_cast<byte_type>(0x80 | (1 + lv)));
  } else {
    const float_t fv = static_cast<float_t>(v);

    if (fv == static_cast<double_t>(v)) {
      out.WriteByte(0xFE);
      out.WriteU32(numeric_utils::ftoi32(fv));
    } else if (!std::signbit(v)) {
      // positive value
      out.WriteU64(lv);
    } else {
      // negative value
      out.WriteByte(0xFF);
      out.WriteU64(lv);
    }
  }
}

double_t read_zvdouble(data_input& in) {
  const uint64_t b = in.read_byte();

  if (0xFF == b) {
    // negative value
    return numeric_utils::i64tod(in.read_long());
  } else if (0xFE == b) {
    // float value
    return numeric_utils::i32tof(in.read_int());
  } else if (0 != (b & 0x80)) {
    // small signed value
    return double_t((b & 0x7F) - 1);
  }

  // positive double (ensure read order)
  const auto part1 = uint64_t(uint32_t(in.read_int())) << 24;
  const auto part2 = uint64_t(uint16_t(in.read_short())) << 8;

  return numeric_utils::i64tod((b << 56) | part1 | part2 |
                               uint64_t(in.read_byte()));
}

// ----------------------------------------------------------------------------
// --SECTION--                                   bytes_view_input implementation
// ----------------------------------------------------------------------------

size_t bytes_view_input::read_bytes(byte_type* b, size_t size) noexcept {
  size =
    std::min(size, size_t(std::distance(pos_, data_.data() + data_.size())));
  std::memcpy(b, pos_, sizeof(byte_type) * size);
  pos_ += size;
  return size;
}

size_t bytes_view_input::read_bytes(size_t offset, byte_type* b,
                                    size_t size) noexcept {
  if (offset < data_.size()) {
    size = std::min(size, size_t(data_.size() - offset));
    std::memcpy(b, data_.data() + offset, sizeof(byte_type) * size);
    pos_ = data_.data() + offset + size;
    return size;
  }

  pos_ = data_.data() + data_.size();
  return 0;
}

// append to buf
void bytes_view_input::read_bytes(bstring& buf, size_t size) {
  auto used = buf.size();

  buf.resize(used + size);

  [[maybe_unused]] const auto read = read_bytes(buf.data() + used, size);
  IRS_ASSERT(read == size);
}

uint32_t bytes_view_input::checksum(size_t offset) const {
  crc32c crc;

  crc.process_block(pos_, std::min(pos_ + offset, data_.data() + data_.size()));

  return crc.checksum();
}

size_t remapped_bytes_view_input::src_to_internal(size_t t) const noexcept {
  IRS_ASSERT(!mapping_.empty());
  auto it =
    std::lower_bound(mapping_.begin(), mapping_.end(), t,
                     [](const auto& l, const auto& r) { return l.first < r; });
  if (it == mapping_.end()) {
    --it;
  } else if (it->first > t) {
    IRS_ASSERT(it != mapping_.begin());
    --it;
  }
  return it->second + (t - it->first);
}

size_t remapped_bytes_view_input::Position() const noexcept {
  const auto addr = bytes_view_input::Position();
  auto diff = std::numeric_limits<size_t>::max();
  IRS_ASSERT(!mapping_.empty());
  mapping_value src = mapping_.front();
  for (const auto& m : mapping_) {
    if (m.second < addr) {
      if (addr - m.second < diff) {
        diff = addr - m.second;
        src = m;
      }
    }
  }
  if (IRS_UNLIKELY(diff == std::numeric_limits<size_t>::max())) {
    IRS_ASSERT(false);
    return 0;
  }
  return src.first + (addr - src.second);
}

}  // namespace irs
