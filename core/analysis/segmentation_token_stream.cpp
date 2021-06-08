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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#include "segmentation_token_stream.hpp"

#include <frozen/unordered_map.h>

#include "unicorn/segment.hpp"
#include "unicorn/string.hpp"
#include "velocypack/Slice.h"
#include "velocypack/Builder.h"
#include "velocypack/Parser.h"
#include "velocypack/velocypack-aliases.h"
#include "utils/vpack_utils.hpp"
#include "utils/hash_utils.hpp"


namespace {

constexpr VPackStringRef CASE_CONVERT_PARAM_NAME  = VPackStringRef("case");
constexpr VPackStringRef BREAK_PARAM_NAME         = VPackStringRef("break");

const frozen::unordered_map<
    irs::string_ref,
    irs::analysis::segmentation_token_stream::options_t::case_convert_t, 3> CASE_CONVERT_MAP = {
  { "lower", irs::analysis::segmentation_token_stream::options_t::case_convert_t::LOWER },
  { "none", irs::analysis::segmentation_token_stream::options_t::case_convert_t::NONE },
  { "upper", irs::analysis::segmentation_token_stream::options_t::case_convert_t::UPPER },
};

const frozen::unordered_map<
    irs::string_ref,
    irs::analysis::segmentation_token_stream::options_t::word_break_t, 3> BREAK_CONVERT_MAP = {
  { "all", irs::analysis::segmentation_token_stream::options_t::word_break_t::ALL },
  { "alpha", irs::analysis::segmentation_token_stream::options_t::word_break_t::ALPHA },
  { "graphic", irs::analysis::segmentation_token_stream::options_t::word_break_t::GRAPHIC },
};

bool parse_vpack_options(const VPackSlice slice,
                         irs::analysis::segmentation_token_stream::options_t& options) {
  if (!slice.isObject()) {
    IR_FRMT_ERROR(
      "Slice for segmentation_token_stream is not an object");
    return false;
  }
  if (slice.hasKey(CASE_CONVERT_PARAM_NAME)) {
    auto case_convert_slice = slice.get(CASE_CONVERT_PARAM_NAME);
    if (!case_convert_slice.isString()) {
      
      IR_FRMT_WARN(
        "Invalid type '%s' (string expected) for segmentation_token_stream from"
        " VPack arguments",
        CASE_CONVERT_PARAM_NAME.data());
      return false;
    }
    auto case_convert = case_convert_slice.stringRef();
    auto itr = CASE_CONVERT_MAP.find(irs::string_ref(case_convert.data(),
                                     case_convert.size()));

    if (itr == CASE_CONVERT_MAP.end()) {
      
      IR_FRMT_WARN(
        "Invalid value in '%s' for segmentation_token_stream from"
        " VPack arguments",
        CASE_CONVERT_PARAM_NAME.data());
      return false;
    }
    options.case_convert = itr->second;
  }
  if (slice.hasKey(BREAK_PARAM_NAME)) {
    auto break_type_slice = slice.get(BREAK_PARAM_NAME);
    if (!break_type_slice.isString()) {
      
      IR_FRMT_WARN(
        "Invalid type '%s' (string expected) for segmentation_token_stream from "
        "VPack arguments",
        BREAK_PARAM_NAME.data());
      return false;
    }
    auto break_type = break_type_slice.stringRef();
    auto itr = BREAK_CONVERT_MAP.find(irs::string_ref(break_type.data(),
                                                      break_type.size()));

    if (itr == BREAK_CONVERT_MAP.end()) {
      
      IR_FRMT_WARN(
        "Invalid value in '%s' for segmentation_token_stream from "
        "VPack arguments",
        BREAK_PARAM_NAME.data());
      return false;
    }
    options.word_break = itr->second;
  }
  return true;
}

bool make_vpack_config(
    const irs::analysis::segmentation_token_stream::options_t& options,
    VPackBuilder* builder) {

  VPackObjectBuilder object(builder);
  {
    auto it = std::find_if(
        CASE_CONVERT_MAP.begin(), CASE_CONVERT_MAP.end(),
        [v = options.case_convert](const decltype(CASE_CONVERT_MAP)::value_type& m) {
          return m.second == v;
        });
    if (it != CASE_CONVERT_MAP.end()) {
      builder->add(CASE_CONVERT_PARAM_NAME, VPackValue(it->first));
    } else {
       IR_FRMT_WARN(
          "Invalid value in '%s' for normalizing segmentation_token_stream from "
          "Value is : %d",
          CASE_CONVERT_PARAM_NAME.data(), options.case_convert);
       return false;
    }
  }
  {
    auto it = std::find_if(
        BREAK_CONVERT_MAP.begin(), BREAK_CONVERT_MAP.end(),
        [v = options.word_break](const decltype(BREAK_CONVERT_MAP)::value_type& m) {
          return m.second == v;
        });
    if (it != BREAK_CONVERT_MAP.end()) {
      builder->add(BREAK_PARAM_NAME, VPackValue(it->first));
    } else {
       IR_FRMT_WARN(
          "Invalid value in '%s' for normalizing segmentation_token_stream from "
          "Value is : %d",
          BREAK_PARAM_NAME.data(), options.word_break);
       return false;
    }
  }
  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief args is a Vpack slice object with the following attributes:
///        "case"(string enum): modify token case
///        "break"(string enum): word breaking method
////////////////////////////////////////////////////////////////////////////////
irs::analysis::analyzer::ptr make_vpack(const VPackSlice slice) {
  try {
    irs::analysis::segmentation_token_stream::options_t options;
    if (!parse_vpack_options(slice, options)) {
      return nullptr;
    }
    return irs::memory::make_unique<irs::analysis::segmentation_token_stream>(
        std::move(options));
  } catch(const VPackException& ex) {
    IR_FRMT_ERROR(
      "Caught error '%s' while constructing segmentation_token_stream from VPack arguments",
      ex.what());
  } catch (...) {
    IR_FRMT_ERROR(
      "Caught error while constructing segmentation_token_stream from VPack arguments");
  }
  return nullptr;
}

irs::analysis::analyzer::ptr make_vpack(const irs::string_ref& args) {
  VPackSlice slice(reinterpret_cast<const uint8_t*>(args.c_str()));
  return make_vpack(slice);
}

bool normalize_vpack_config(const VPackSlice slice, VPackBuilder* vpack_builder) {
  irs::analysis::segmentation_token_stream::options_t options;
  try {
    if (parse_vpack_options(slice, options)) {
      return make_vpack_config(options, vpack_builder);
    } else {
      return false;
    }
  } catch(const VPackException& ex) {
    IR_FRMT_ERROR(
      "Caught error '%s' while normalizing segmentation_token_stream from VPack arguments",
      ex.what());
  } catch (...) {
    IR_FRMT_ERROR(
      "Caught error while normalizing segmentation_token_stream from VPack arguments");
  }
  return false;
}

bool normalize_vpack_config(const irs::string_ref& args, std::string& config) {
  VPackSlice slice(reinterpret_cast<const uint8_t*>(args.c_str()));
  VPackBuilder builder;
  if (normalize_vpack_config(slice, &builder)) {
    config.assign(builder.slice().startAs<char>(), builder.slice().byteSize());
    return true;
  }
  return false;
}

irs::analysis::analyzer::ptr make_json(const irs::string_ref& args) {
  try {
    if (args.null()) {
      IR_FRMT_ERROR("Null arguments while constructing segmentation_token_stream");
      return nullptr;
    }
    auto vpack = VPackParser::fromJson(args.c_str(), args.size());
    return make_vpack(vpack->slice());
  } catch(const VPackException& ex) {
    IR_FRMT_ERROR(
      "Caught error '%s' while constructing segmentation_token_stream from VPack",
      ex.what());
  } catch (...) {
    IR_FRMT_ERROR(
      "Caught error while constructing segmentation_token_stream from VPack");
  }
  return nullptr;
}

bool normalize_json_config(const irs::string_ref& args, std::string& definition) {
  try {
    if (args.null()) {
      IR_FRMT_ERROR("Null arguments while normalizing segmentation_token_stream");
      return false;
    }
    auto vpack = VPackParser::fromJson(args.c_str(), args.size());
    VPackBuilder builder;
    if (normalize_vpack_config(vpack->slice(), &builder)) {
      definition = builder.toString();
      return !definition.empty();
    }
  } catch(const VPackException& ex) {
    IR_FRMT_ERROR("Caught error '%s' while normalizing segmentation_token_stream from VPack",
                  ex.what());
  } catch (...) {
    IR_FRMT_ERROR("Caught error while normalizing segmentation_token_stream from VPack");
  }
  return false;
}

} // namespace

namespace iresearch {
namespace analysis {

struct segmentation_token_stream::state_t {
  RS::Unicorn::WordIterator<char> begin;
  RS::Unicorn::WordIterator<char> end;
};

REGISTER_ANALYZER_VPACK(segmentation_token_stream, make_vpack,
                        normalize_vpack_config);
REGISTER_ANALYZER_JSON(segmentation_token_stream, make_json,
                        normalize_json_config);


/*static*/ void segmentation_token_stream::init() {
  REGISTER_ANALYZER_VPACK(segmentation_token_stream, make_vpack,
                          normalize_vpack_config);  // match registration above
  REGISTER_ANALYZER_JSON(segmentation_token_stream, make_json,
                         normalize_json_config);  // match registration above
}

segmentation_token_stream::segmentation_token_stream(
    segmentation_token_stream::options_t&& options)
  : analyzer{ irs::type<segmentation_token_stream>::get() },
    state_(memory::make_unique<state_t>()), options_(options) {
}

bool segmentation_token_stream::next() {
  if (state_->begin != state_->end) {
    assert(state_->begin->first.offset() <=
           std::numeric_limits<uint32_t>::max());
    assert(state_->begin->second.offset() <=
           std::numeric_limits<uint32_t>::max());
    std::get<1>(attrs_).start =
        static_cast<uint32_t>(state_->begin->first.offset());
    std::get<1>(attrs_).end =
        static_cast<uint32_t>(state_->begin->second.offset());
    switch (options_.case_convert) {
      case options_t::case_convert_t::NONE:
        std::get<2>(attrs_).value =
            irs::ref_cast<irs::byte_type>(RS::Unicorn::u_view(*state_->begin++));
        break;
      case options_t::case_convert_t::LOWER:
        term_buf_ =
            RS::Unicorn::str_lowercase(RS::Unicorn::u_view(*state_->begin++));
        std::get<2>(attrs_).value =
            irs::ref_cast<irs::byte_type>(irs::string_ref(term_buf_));
        break;
      case options_t::case_convert_t::UPPER:
        term_buf_ =
            RS::Unicorn::str_uppercase(RS::Unicorn::u_view(*state_->begin++));
        std::get<2>(attrs_).value =
            irs::ref_cast<irs::byte_type>(irs::string_ref(term_buf_));
        break;
    }
    return true;
  }
  return false;
}

bool segmentation_token_stream::reset(const string_ref& data) {
  auto flags = RS::Unicorn::Segment::alpha;
  switch (options_.word_break) {
    case options_t::word_break_t::ALL:
      flags = RS::Unicorn::Segment::unicode;
      break;
    case options_t::word_break_t::GRAPHIC:
      flags = RS::Unicorn::Segment::graphic;
      break;
    case options_t::word_break_t::ALPHA:
    [[fallthrough]];
    default:
      assert(options_.word_break == options_t::word_break_t::ALPHA);
      break;
  }
  auto range =  RS::Unicorn::word_range(static_cast<std::string_view>(data), flags);
  state_->begin = range.begin();
  state_->end = range.end();
  return true;
}

} // namespace analysis
} // namespace iresearch
