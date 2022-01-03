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

#ifndef IRESEARCH_NORM_H
#define IRESEARCH_NORM_H

#include "shared.hpp"
#include "analysis/token_attributes.hpp"
#include "utils/lz4compression.hpp"

namespace iresearch {

class norm_base {
 public:
  bool reset(const sub_reader& segment, field_id column, const document& doc);
  bool empty() const noexcept;

 protected:
  norm_base() noexcept;

  doc_iterator::ptr column_it_;
  const payload* payload_;
  const document* doc_;
};

static_assert(std::is_nothrow_move_constructible_v<norm_base>);
static_assert(std::is_nothrow_move_assignable_v<norm_base>);

class IRESEARCH_API norm : public norm_base {
 public:
  // DO NOT CHANGE NAME
  static constexpr string_ref type_name() noexcept {
    return "norm";
  }

  FORCE_INLINE static constexpr float_t DEFAULT() noexcept {
    return 1.f;
  }

  static feature_writer::ptr make_writer(range<bytes_ref> payload);

  float_t read() const;
};

static_assert(std::is_nothrow_move_constructible_v<norm>);
static_assert(std::is_nothrow_move_assignable_v<norm>);

enum class Norm2Version : uint32_t {
  kMin = 0
};

class IRESEARCH_API norm2_header final {
 public:
  explicit norm2_header(Norm2Version ver) noexcept
    : ver_{ver} {
  }

  void update(uint32_t value) noexcept {
    min_ = std::min(min_, value);
    max_ = std::max(max_, value);
  }

  bool reset(bytes_ref payload) noexcept;

  size_t num_bytes() const noexcept {
    if (max_ <= std::numeric_limits<byte_type>::max()) {
      return sizeof(byte_type);
    } else if (max_ <= std::numeric_limits<uint16_t>::max()) {
      return sizeof(uint16_t);
    } else {
      return sizeof(uint32_t);
    }
  }

  void write(bstring& out) const {
    out.resize(byte_size());

    auto* p = out.data();
    irs::write(p, static_cast<uint32_t>(ver_));
    irs::write(p, min_);
    irs::write(p, max_);
  }

 private:
  constexpr static size_t byte_size() noexcept {
    return sizeof(ver_) + sizeof(min_) + sizeof(max_);
  }

  Norm2Version ver_;
  uint32_t min_{std::numeric_limits<uint32_t>::max()};
  uint32_t max_{std::numeric_limits<uint32_t>::min()};
};

template<size_t NumBytes>
class norm2_writer final : public feature_writer {
  static_assert(NumBytes == sizeof(byte_type) ||
                NumBytes == sizeof(uint16_t) ||
                NumBytes == sizeof(uint32_t));

 public:
  explicit norm2_writer(norm2_header hdr) noexcept
    : hdr_{hdr} {
  }

  virtual void write(
      const field_stats& stats,
      doc_id_t doc,
      // cppcheck-suppress constParameter
      columnstore_writer::values_writer_f& writer) final {
    hdr_.update(stats.len);
    writer(doc).write_int(stats.len);

//    if constexpr (NumBytes == sizeof(byte_type)) {
//      writer(doc).write_byte(static_cast<byte_type>(stats.len & 0xFF));
//    }
//
//    if constexpr (NumBytes == sizeof(uint16_t)) {
//      writer(doc).write_short(static_cast<uint16_t>(stats.len & 0xFFFF));
//    }
//
//    if constexpr (NumBytes == sizeof(uint32_t)) {
//      writer(doc).write_int(stats.len);
//    }
  }

  virtual void write(
      data_output& out,
      bytes_ref payload) {
    if (payload.size() == sizeof(uint32_t)) {
      auto* p = payload.c_str();
      hdr_.update(irs::read<uint32_t>(p));

      out.write_bytes(payload.c_str(), sizeof(uint32_t));
    }
  }

  virtual void finish(bstring& out) final {
    hdr_.write(out);
  }

 private:
  norm2_header hdr_;
};

class IRESEARCH_API norm2 : public norm_base {
 public:
  // DO NOT CHANGE NAME
  static constexpr string_ref type_name() noexcept {
    return "iresearch::norm2";
  }

  static feature_writer::ptr make_writer(range<bytes_ref> payload);

  uint32_t read() const {
    assert(column_it_);
    assert(payload_);

    if (IRS_LIKELY(doc_->value == column_it_->seek(doc_->value))) {
      assert(sizeof(uint32_t) == payload_->value.size());
      const auto* value = payload_->value.c_str();
      return irs::read<uint32_t>(value);
    }

    // we should investigate why we failed to find a norm2 value for doc
    assert(false);

    return 1;
  }
};

static_assert(std::is_nothrow_move_constructible_v<norm2>);
static_assert(std::is_nothrow_move_assignable_v<norm2>);

} // iresearch

#endif // IRESEARCH_NORM_H
