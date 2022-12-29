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

#include "format_utils.hpp"

#include "formats/formats.hpp"
#include "index/index_meta.hpp"
#include "shared.hpp"
#include "store/store_utils.hpp"

namespace irs {

void validate_footer(index_input& in) {
  const int64_t remain = in.length() - in.file_pointer();

  if (remain != format_utils::kFooterLen) {
    throw index_error{absl::StrCat(
      "While validating footer, error: invalid position '", remain, "'")};
  }

  const int32_t magic = in.read_int();

  if (magic != format_utils::kFooterMagic) {
    throw index_error{absl::StrCat(
      "While validating footer, error: invalid magic number '", magic, "'")};
  }

  const int32_t alg_id = in.read_int();

  if (alg_id != 0) {
    throw index_error{absl::StrCat(
      "While validating footer, error: invalid algorithm '", alg_id, "'")};
  }
}

namespace format_utils {

void write_header(index_output& out, std::string_view format, int32_t ver) {
  out.write_int(kFormatMagic);
  write_string(out, format);
  out.write_int(ver);
}

void write_footer(index_output& out) {
  out.write_int(kFooterMagic);
  out.write_int(0);
  out.write_long(out.checksum());
}

size_t header_length(std::string_view format) noexcept {
  return sizeof(int32_t) * 2 + bytes_io<uint64_t>::vsize(format.size()) +
         format.size();
}

int32_t check_header(index_input& in, std::string_view req_format,
                     int32_t min_ver, int32_t max_ver) {
  const ptrdiff_t left = in.length() - in.file_pointer();

  if (left < 0) {
    throw illegal_state{"Header has invalid length."};
  }

  const size_t expected = header_length(req_format);

  if (static_cast<size_t>(left) < expected) {
    throw index_error{absl::StrCat("While checking header, error: only '", left,
                                   "' bytes left out of '", expected, "'")};
  }

  const int32_t magic = in.read_int();

  if (kFormatMagic != magic) {
    throw index_error{absl::StrCat(
      "While checking header, error: invalid magic '", magic, "'")};
  }

  const auto format = read_string<std::string>(in);

  if (req_format != format) {
    throw index_error{
      absl::StrCat("While checking header, error: format mismatch '", format,
                   "' != '", req_format, "'")};
  }

  const int32_t ver = in.read_int();

  if (ver < min_ver || ver > max_ver) {
    throw index_error{absl::StrCat(
      "While checking header, error: invalid version '", ver, "'")};
  }

  return ver;
}

int64_t checksum(const index_input& in) {
  auto* stream = &in;

  const auto length = stream->length();

  if (length < sizeof(uint64_t)) {
    throw index_error{
      absl::StrCat("failed to read checksum from a file of size ", length)};
  }

  index_input::ptr dup;
  if (0 != in.file_pointer()) {
    dup = in.dup();

    if (!dup) {
      IR_FRMT_ERROR("Failed to duplicate input in: %s", __FUNCTION__);

      throw io_error("failed to duplicate input");
    }

    dup->seek(0);
    stream = dup.get();
  }

  IRS_ASSERT(0 == stream->file_pointer());
  return stream->checksum(length - sizeof(uint64_t));
}

}  // namespace format_utils
}  // namespace irs
