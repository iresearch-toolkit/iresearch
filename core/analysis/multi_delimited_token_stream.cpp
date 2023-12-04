////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include "multi_delimited_token_stream.hpp"

#include <string_view>

#include "utils/vpack_utils.hpp"
#include "velocypack/Builder.h"
#include "velocypack/Parser.h"
#include "velocypack/Slice.h"
#include "velocypack/velocypack-aliases.h"

using namespace irs;
using namespace irs::analysis;

namespace {

template<typename Derived>
class multi_delimited_token_stream_single_chars_base
  : public multi_delimited_token_stream {
 public:
  bool next() override {
    while (true) {
      if (data_.begin() == data_.end()) {
        return false;
      }

      auto next = static_cast<Derived*>(this)->find_next_delim();

      if (next == data_.begin()) {
        // skip empty terms
        data_ = bytes_view{next + 1, data_.end()};
        continue;
      }

      auto& term = std::get<term_attribute>(attrs_);
      term.value = bytes_view{data_.begin(), next};

      if (next == data_.end()) {
        data_ = {};
      } else {
        data_ = bytes_view{next + 1, data_.end()};
      }

      return true;
    }
  }

  bool reset(std::string_view data) override {
    data_ = ViewCast<byte_type>(data);
    return true;
  }
};

template<std::size_t N>
class multi_delimited_token_stream_single_chars final
  : public multi_delimited_token_stream_single_chars_base<
      multi_delimited_token_stream_single_chars<N>> {
 public:
  explicit multi_delimited_token_stream_single_chars(
    const multi_delimited_token_stream::Options& opts) {
    IRS_ASSERT(opts.delimiters.size() == N);
    std::size_t k = 0;
    for (const auto& delim : opts.delimiters) {
      IRS_ASSERT(delim.size() == 1);
      bytes_[k++] = delim[0];
    }
  }

  auto find_next_delim() {
    return std::search(this->data_.begin(), this->data_.end(), bytes_.begin(),
                       bytes_.end());
  }

  std::array<byte_type, N> bytes_;
};

class multi_delimited_token_stream_generic_single_chars final
  : public multi_delimited_token_stream_single_chars_base<
      multi_delimited_token_stream_generic_single_chars> {
 public:
  explicit multi_delimited_token_stream_generic_single_chars(
    const Options& opts) {
    for (const auto& delim : opts.delimiters) {
      IRS_ASSERT(delim.size() == 1);
      bytes_[delim[0]] = true;
    }
  }

  auto find_next_delim() {
    return std::find_if(data_.begin(), data_.end(),
                        [&](auto c) { return bytes_[c]; });
  }
  // TODO maybe use a bitset instead?
  std::array<bool, CHAR_MAX + 1> bytes_;
};

class multi_delimited_token_stream_generic final
  : public multi_delimited_token_stream {
 public:
  explicit multi_delimited_token_stream_generic(Options&& opts)
    : options_(std::move(opts)) {}

  bool next() override { return false; }

  bool reset(std::string_view data) override {
    data_ = ViewCast<byte_type>(data);
    return true;
  }

  Options options_;
};

template<std::size_t N>
irs::analysis::analyzer::ptr make_single_char(
  multi_delimited_token_stream::Options&& opts) {
  if constexpr (N >= 4) {
    return std::make_unique<multi_delimited_token_stream_generic_single_chars>(
      std::move(opts));
  } else if (opts.delimiters.size() == N) {
    return std::make_unique<multi_delimited_token_stream_single_chars<N>>(
      std::move(opts));
  } else {
    return make_single_char<N + 1>(std::move(opts));
  }
}

irs::analysis::analyzer::ptr make(
  multi_delimited_token_stream::Options&& opts) {
  const bool single_character_case =
    std::all_of(opts.delimiters.begin(), opts.delimiters.end(),
                [](const auto& delim) { return delim.size() == 1; });
  if (single_character_case) {
    return make_single_char<0>(std::move(opts));
  } else {
    return std::make_unique<multi_delimited_token_stream_generic>(
      std::move(opts));
  }
}
/*
constexpr std::string_view DELIMITER_PARAM_NAME{"delimiter"};

bool parse_vpack_options(const VPackSlice slice, std::string& delimiter) {
  if (!slice.isObject() && !slice.isString()) {
    IRS_LOG_ERROR(
      "Slice for multi_delimited_token_stream is not an object or string");
    return false;
  }

  switch (slice.type()) {
    case VPackValueType::String:
      delimiter = slice.stringView();
      return true;
    case VPackValueType::Object:
      if (auto delim_type_slice = slice.get(DELIMITER_PARAM_NAME);
          !delim_type_slice.isNone()) {
        if (!delim_type_slice.isString()) {
          IRS_LOG_WARN(absl::StrCat(
            "Invalid type '", DELIMITER_PARAM_NAME,
            "' (string expected) for multi_delimited_token_stream from "
            "VPack arguments"));
          return false;
        }
        delimiter = delim_type_slice.stringView();
        return true;
      }
    default: {
    }  // fall through
  }

  IRS_LOG_ERROR(absl::StrCat(
    "Missing '", DELIMITER_PARAM_NAME,
    "' while constructing multi_delimited_token_stream from VPack arguments"));

  return false;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief args is a jSON encoded object with the following attributes:
///        "delimiter"(string): the delimiter to use for tokenization <required>
////////////////////////////////////////////////////////////////////////////////
irs::analysis::analyzer::ptr make_vpack(const VPackSlice slice) {
  std::string delimiter;
  if (parse_vpack_options(slice, delimiter)) {
    return irs::analysis::multi_delimited_token_stream::make(delimiter);
  } else {
    return nullptr;
  }
}

irs::analysis::analyzer::ptr make_vpack(std::string_view args) {
  VPackSlice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return make_vpack(slice);
}
///////////////////////////////////////////////////////////////////////////////
/// @brief builds analyzer config from internal options in json format
/// @param delimiter reference to analyzer options storage
/// @param definition string for storing json document with config
///////////////////////////////////////////////////////////////////////////////
bool make_vpack_config(std::string_view delimiter,
                       VPackBuilder* vpack_builder) {
  VPackObjectBuilder object(vpack_builder);
  {
    // delimiter
    vpack_builder->add(DELIMITER_PARAM_NAME, VPackValue(delimiter));
  }

  return true;
}

bool normalize_vpack_config(const VPackSlice slice,
                            VPackBuilder* vpack_builder) {
  std::string delimiter;
  if (parse_vpack_options(slice, delimiter)) {
    return make_vpack_config(delimiter, vpack_builder);
  } else {
    return false;
  }
}

bool normalize_vpack_config(std::string_view args, std::string& definition) {
  VPackSlice slice(reinterpret_cast<const uint8_t*>(args.data()));
  VPackBuilder builder;
  bool res = normalize_vpack_config(slice, &builder);
  if (res) {
    definition.assign(builder.slice().startAs<char>(),
                      builder.slice().byteSize());
  }
  return res;
}

irs::analysis::analyzer::ptr make_json(std::string_view args) {
  try {
    if (irs::IsNull(args)) {
      IRS_LOG_ERROR(
        "Null arguments while constructing multi_delimited_token_stream");
      return nullptr;
    }
    auto vpack = VPackParser::fromJson(args.data(), args.size());
    return make_vpack(vpack->slice());
  } catch (const VPackException& ex) {
    IRS_LOG_ERROR(absl::StrCat(
      "Caught error '", ex.what(),
      "' while constructing multi_delimited_token_stream from JSON"));
  } catch (...) {
    IRS_LOG_ERROR(
      "Caught error while constructing multi_delimited_token_stream from JSON");
  }
  return nullptr;
}

bool normalize_json_config(std::string_view args, std::string& definition) {
  try {
    if (irs::IsNull(args)) {
      IRS_LOG_ERROR(
        "Null arguments while normalizing multi_delimited_token_stream");
      return false;
    }
    auto vpack = VPackParser::fromJson(args.data(), args.size());
    VPackBuilder vpack_builder;
    if (normalize_vpack_config(vpack->slice(), &vpack_builder)) {
      definition = vpack_builder.toString();
      return !definition.empty();
    }
  } catch (const VPackException& ex) {
    IRS_LOG_ERROR(absl::StrCat(
      "Caught error '", ex.what(),
      "' while normalizing multi_delimited_token_stream from JSON"));
  } catch (...) {
    IRS_LOG_ERROR(
      "Caught error while normalizing multi_delimited_token_stream from JSON");
  }
  return false;
}

REGISTER_ANALYZER_VPACK(irs::analysis::multi_delimited_token_stream, make_vpack,
                        normalize_vpack_config);
REGISTER_ANALYZER_JSON(irs::analysis::multi_delimited_token_stream, make_json,
                       normalize_json_config);
*/
}  // namespace

namespace irs {
namespace analysis {
/*
void multi_delimited_token_stream::init() {
  REGISTER_ANALYZER_VPACK(multi_delimited_token_stream, make_vpack,
                          normalize_vpack_config);  // match registration above
  REGISTER_ANALYZER_JSON(multi_delimited_token_stream, make_json,
                         normalize_json_config);  // match registration above
}
*/
analyzer::ptr multi_delimited_token_stream::make(
  multi_delimited_token_stream::Options&& opts) {
  return ::make(std::move(opts));
}

}  // namespace analysis
}  // namespace irs
