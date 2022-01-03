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

#include "norm.hpp"
#include "store/store_utils.hpp"
#include "utils/bytes_utils.hpp"

namespace {

using namespace irs;

class NormWriter final : public feature_writer {
 public:
  virtual void write(
      const field_stats& stats,
      doc_id_t doc,
      // cppcheck-suppress constParameter
      columnstore_writer::values_writer_f& writer) final {
    if (stats.len > 0) {
      const float_t value = 1.f / float_t(std::sqrt(double_t(stats.len)));
      if (value != Norm::DEFAULT()) {
        auto& stream = writer(doc);
        write_zvfloat(stream, value);
      }
    }
  }

  virtual void write(
      data_output& out,
      bytes_ref payload) {
    if (!payload.empty()) {
      out.write_bytes(payload.c_str(), payload.size());
    }
  }

  virtual void finish(bstring& /*out*/) final { }
};

NormWriter kNormWriter;

}

namespace iresearch {

bool NormReaderContext::Reset(
    const sub_reader& reader,
    field_id column_id,
    const document& doc) {
  const auto* column = reader.column(column_id);

  if (column) {
    auto it = column->iterator(false);
    if (IRS_LIKELY(it)) {
      auto* payload = irs::get<irs::payload>(*it);
      if (IRS_LIKELY(payload)) {
        this->header = column->payload();
        this->it = std::move(it);
        this->payload = payload;
        this->doc = &doc;
        return true;
      }
    }
  }

  return false;
}

bool Norm2ReaderContext::Reset(
    const sub_reader& reader,
    field_id column_id,
    const document& doc) {
  if (NormReaderContext::Reset(reader, column_id, doc)) {
    auto hdr = Norm2Header::Read(header);
    if (hdr.has_value()) {
      num_bytes = hdr.value().NumBytes();
      return true;
    }
  }

  return false;
}


/*static*/ feature_writer::ptr Norm::MakeWriter(range<bytes_ref> /*payload*/) {
  return memory::to_managed<feature_writer, false>(&kNormWriter);
}

bool Norm2Header::Reset(const Norm2Header& hdr) noexcept {
  if (hdr.ver_ != ver_) {
    IR_FRMT_ERROR("'norm2' header version mismatch, expected '%u', got '%u'",
                  static_cast<uint32_t>(ver_), static_cast<uint32_t>(hdr.ver_));
    return false;
  }

  min_ = std::min(hdr.min_, min_);
  max_ = std::max(hdr.max_, max_);
  return true;
}

/*static*/ void Norm2Header::Write(const Norm2Header& hdr, bstring& out) {
  out.resize(ByteSize());

  auto* p = out.data();
  irs::write(p, static_cast<byte_type>(hdr.ver_));
  irs::write(p, hdr.min_);
  irs::write(p, hdr.max_);
}

/*static*/ std::optional<Norm2Header> Norm2Header::Read(bytes_ref payload) noexcept {
  if (IRS_LIKELY(payload.size() == ByteSize())) {
    auto* p = payload.c_str();

    Norm2Header hdr{Norm2Version{irs::read<byte_type>(p)}};
    hdr.min_ = irs::read<decltype(min_)>(p);
    hdr.max_ = irs::read<decltype(min_)>(p);

    return hdr;
  }

  IR_FRMT_ERROR("Invalid 'norm2' header size " IR_SIZE_T_SPECIFIER "",
                payload.size());
  return std::nullopt;
}

/*static*/ feature_writer::ptr Norm2::MakeWriter(range<bytes_ref> headers) {
  constexpr Norm2Version kVersion{Norm2Version::kMin};

  size_t num_bytes{sizeof(value_type)};

  if (!headers.empty()) {
    Norm2Header acc{kVersion};
    for (auto header : headers) {
      auto hdr = Norm2Header::Read(header);
      if (!hdr.has_value() || acc.Reset(hdr.value())) {
        return nullptr;
      }
    }
    num_bytes = acc.NumBytes();
  }

  switch (num_bytes) {
    case sizeof(byte_type):
      return memory::make_managed<Norm2Writer<byte_type>>(kVersion);
    case sizeof(uint16_t):
      return memory::make_managed<Norm2Writer<uint16_t>>(kVersion);
    default:
      assert(num_bytes == sizeof(uint32_t));
      return memory::make_managed<Norm2Writer<uint32_t>>(kVersion);
  }

  return nullptr;
}

REGISTER_ATTRIBUTE(Norm);
REGISTER_ATTRIBUTE(Norm2);

} // iresearch
