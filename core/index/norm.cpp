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

class norm_writer final : public feature_writer {
 public:
  virtual void write(
      const field_stats& stats,
      doc_id_t doc,
      // cppcheck-suppress constParameter
      columnstore_writer::values_writer_f& writer) final {
    if (stats.len > 0) {
      const float_t value = 1.f / float_t(std::sqrt(double_t(stats.len)));
      if (value != norm::DEFAULT()) {
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

const document kInvalidDocument;
norm_writer kNormWriter;

}

namespace iresearch {

norm_base::norm_base() noexcept
  : payload_{nullptr},
    doc_{&kInvalidDocument} {
}

bool norm_base::empty() const noexcept {
  return doc_ == &kInvalidDocument;
}

bool norm_base::reset(
    const sub_reader& reader,
    field_id column_id,
    const document& doc) {
  const auto* column = reader.column(column_id);

  if (column) {
    column_it_ = column->iterator(false);
    if (IRS_LIKELY(column_it_)) {
      payload_ = irs::get<irs::payload>(*column_it_);
      if (IRS_LIKELY(payload_)) {
        doc_ = &doc;
        return true;
      }
    }
  }

  return false;
}

/*static*/ feature_writer::ptr norm::make_writer(range<bytes_ref> /*payload*/) {
  return memory::to_managed<feature_writer, false>(&kNormWriter);
}

float_t norm::read() const {
  assert(column_it_);
  if (doc_->value != column_it_->seek(doc_->value)) {
    return DEFAULT();
  }
  assert(payload_);
  bytes_ref_input in{payload_->value};
  return read_zvfloat(in);
}

bool norm2_header::reset(bytes_ref payload) noexcept {
  if (IRS_LIKELY(payload.size() == byte_size())) {
    auto* p = payload.c_str();

    const Norm2Version ver{irs::read<uint32_t>(p)};
    if (ver != ver_) {
      IR_FRMT_ERROR("'norm2' header version mismatch, expected '%u', got '%u'",
                    static_cast<uint32_t>(ver_), static_cast<uint32_t>(ver));
      return false;
    }

    min_ = std::min(irs::read<decltype(min_)>(p), min_);
    max_ = std::max(irs::read<decltype(max_)>(p), max_);
    return true;
  }

  IR_FRMT_ERROR("Invalid 'norm2' header size " IR_SIZE_T_SPECIFIER "",
                payload.size());
  return false;
}

/*static*/ feature_writer::ptr norm2::make_writer(range<bytes_ref> headers) {
  norm2_header hdr{Norm2Version::kMin};

  if (headers.empty()) {
    return memory::make_managed<norm2_writer<sizeof(uint32_t)>>(hdr);
  }

  for (auto header : headers) {
    if (!hdr.reset(header)) {
      return nullptr;
    }
  }

  switch (hdr.num_bytes()) {
    case sizeof(byte_type):
      return memory::make_managed<norm2_writer<sizeof(byte_type)>>(hdr);
    case sizeof(uint16_t):
      return memory::make_managed<norm2_writer<sizeof(uint16_t)>>(hdr);
    default:
      return memory::make_managed<norm2_writer<sizeof(uint32_t)>>(hdr);
  }

  return nullptr;
}

REGISTER_ATTRIBUTE(norm);
REGISTER_ATTRIBUTE(norm2);

} // iresearch
