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
#include "store/store_utils.hpp"
#include "utils/lz4compression.hpp"

namespace iresearch {

struct NormReaderContext {
  bool Reset(const sub_reader& segment,
             field_id column,
             const document& doc);
  bool Valid() const noexcept {
    return doc != nullptr;
  }

  bytes_ref header;
  doc_iterator::ptr it;
  const irs::payload* payload{};
  const document* doc{};
};

static_assert(std::is_nothrow_move_constructible_v<NormReaderContext>);
static_assert(std::is_nothrow_move_assignable_v<NormReaderContext>);

class IRESEARCH_API Norm : public attribute {
 public:
  // DO NOT CHANGE NAME
  static constexpr string_ref type_name() noexcept {
    return "norm";
  }

  FORCE_INLINE static constexpr float_t DEFAULT() noexcept {
    return 1.f;
  }

  static feature_writer::ptr MakeWriter(range<bytes_ref> payload);

  static auto MakeReader(NormReaderContext&& ctx) {
    assert(ctx.it);
    assert(ctx.payload);
    assert(ctx.doc);

    return [ctx = std::move(ctx)]() mutable -> float_t {
      if (const auto doc = ctx.doc->value;
          doc != ctx.it->seek(doc)) {
        return Norm::DEFAULT();
      }
      bytes_ref_input in{ctx.payload->value};
      return read_zvfloat(in);
    };
  }
};

static_assert(std::is_nothrow_move_constructible_v<Norm>);
static_assert(std::is_nothrow_move_assignable_v<Norm>);

enum class Norm2Version : uint32_t {
  kMin = 0
};

class IRESEARCH_API Norm2Header final {
 public:
  explicit Norm2Header(Norm2Version ver) noexcept
    : ver_{ver} {
  }

  void Reset(uint32_t value) noexcept {
    min_ = std::min(min_, value);
    max_ = std::max(max_, value);
  }

  bool Reset(bytes_ref payload) noexcept;

  size_t NumBytes() const noexcept {
    if (max_ <= std::numeric_limits<byte_type>::max()) {
      return sizeof(byte_type);
    } else if (max_ <= std::numeric_limits<uint16_t>::max()) {
      return sizeof(uint16_t);
    } else {
      return sizeof(uint32_t);
    }
  }

  void Write(bstring& out) const {
    out.resize(ByteSize());

    auto* p = out.data();
    irs::write(p, static_cast<uint32_t>(ver_));
    irs::write(p, min_);
    irs::write(p, max_);
  }

 private:
  constexpr static size_t ByteSize() noexcept {
    return sizeof(ver_) + sizeof(min_) + sizeof(max_);
  }

  Norm2Version ver_;
  uint32_t min_{std::numeric_limits<uint32_t>::max()};
  uint32_t max_{std::numeric_limits<uint32_t>::min()};
};

template<size_t NumBytes>
class Norm2Writer final : public feature_writer {
 public:
  explicit Norm2Writer(Norm2Version ver) noexcept
    : hdr_{ver} {
  }

  virtual void write(
      const field_stats& stats,
      doc_id_t doc,
      // cppcheck-suppress constParameter
      columnstore_writer::values_writer_f& writer) final {
    hdr_.Reset(stats.len);

    auto& stream = writer(doc);
    WriteValue(stream, stats.len);
  }

  virtual void write(
      data_output& out,
      bytes_ref payload) {
    uint32_t value;

    switch (payload.size()) {
      case sizeof(irs::byte_type): {
        value = payload.front();
      } break;
      case sizeof(uint16_t): {
        auto* p = payload.c_str();
        value = irs::read<uint16_t>(p);
      } break;
      case sizeof(uint32_t): {
        auto* p = payload.c_str();
        value = irs::read<uint32_t>(p);
      } break;
      default:
        return;
    }

    hdr_.Reset(value);
    WriteValue(out, value);
  }

  virtual void finish(bstring& out) final {
    hdr_.Write(out);
  }

 private:
  static void WriteValue(data_output& out, uint32_t value) {
    static_assert(NumBytes == sizeof(byte_type) ||
                  NumBytes == sizeof(uint16_t) ||
                  NumBytes == sizeof(uint32_t));

    if constexpr (NumBytes == sizeof(byte_type)) {
      out.write_byte(static_cast<byte_type>(value & 0xFF));
    }

    if constexpr (NumBytes == sizeof(uint16_t)) {
      out.write_short(static_cast<uint16_t>(value & 0xFFFF));
    }

    if constexpr (NumBytes == sizeof(uint32_t)) {
      out.write_int(value);
    }
  }

  Norm2Header hdr_;
};


struct Norm2ReaderContext : NormReaderContext {
  bool Reset(const sub_reader& segment,
             field_id column,
             const document& doc);
  bool Valid() const noexcept {
    return NormReaderContext::Valid() && num_bytes;
  }

  uint32_t num_bytes{};
};

class IRESEARCH_API Norm2 : public attribute {
 public:
  // DO NOT CHANGE NAME
  static constexpr string_ref type_name() noexcept {
    return "iresearch::norm2";
  }

  static feature_writer::ptr MakeWriter(range<bytes_ref> payload);

  template<size_t NumBytes>
  static auto MakeReader(NormReaderContext&& ctx) {
    static_assert(NumBytes == sizeof(byte_type) ||
                  NumBytes == sizeof(uint16_t) ||
                  NumBytes == sizeof(uint32_t));
    assert(ctx.it);
    assert(ctx.payload);
    assert(ctx.doc);

    return [ctx = std::move(ctx)]() mutable -> uint32_t {
      const doc_id_t doc = ctx.doc->value;

      if (IRS_LIKELY(doc == ctx.it->seek(doc))) {
        assert(NumBytes == ctx.payload->value.size());
        const auto* value = ctx.payload->value.c_str();

        if constexpr (NumBytes == sizeof(irs::byte_type)) {
          return *value;
        }

        if constexpr (NumBytes == sizeof(uint16_t)) {
          return irs::read<uint16_t>(value);
        }

        if constexpr (NumBytes == sizeof(uint32_t)) {
          return irs::read<uint32_t>(value);
        }
      }

      // we should investigate why we failed to find a norm2 value for doc
      assert(false);

      return 1;
    };
  }

  template<typename Func>
  static auto MakeReader(Norm2ReaderContext&& ctx, Func&& func) {
    switch (ctx.num_bytes) {
      case sizeof(uint8_t):
        return func(MakeReader<sizeof(uint8_t)>(std::move(ctx)));
      case sizeof(uint16_t):
        return func(MakeReader<sizeof(uint16_t)>(std::move(ctx)));
      default:
        return func(MakeReader<sizeof(uint32_t)>(std::move(ctx)));
    }
  }
};

static_assert(std::is_nothrow_move_constructible_v<Norm2>);
static_assert(std::is_nothrow_move_assignable_v<Norm2>);

} // iresearch

#endif // IRESEARCH_NORM_H
