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

#include <rapidjson/rapidjson/document.h> // for rapidjson::Document
#include <rapidjson/rapidjson/writer.h> // for rapidjson::Writer
#include <rapidjson/rapidjson/stringbuffer.h> // for rapidjson::StringBuffer

#include "unicorn/segment.hpp"
#include "unicorn/string.hpp"

namespace iresearch {
namespace analysis {

struct segmentation_token_stream::state_t {
  RS::Unicorn::WordIterator<char> begin;
  RS::Unicorn::WordIterator<char> end;
};

/*static*/ void segmentation_token_stream::init() {
 // REGISTER_ANALYZER_JSON(pipeline_token_stream, make_json,
 //   normalize_json_config);  // match registration above
}

segmentation_token_stream::segmentation_token_stream(segmentation_token_stream::options_t&& options)
  : analyzer{ irs::type<segmentation_token_stream>::get() },
    state_(memory::make_unique<state_t>()), options_(options) {
}

bool segmentation_token_stream::next() {
  if (state_->begin != state_->end) {
    std::get<1>(attrs_).start = state_->begin->first.offset();
    std::get<1>(attrs_).end = state_->begin->second.offset();
    switch (options_.case_convert) {
      case options_t::case_convert_t::NONE:
        std::get<2>(attrs_).value = irs::ref_cast<irs::byte_type>(RS::Unicorn::u_view(*state_->begin++));
        break;
      case options_t::case_convert_t::LOWER:
        term_buf_ = RS::Unicorn::str_lowercase(RS::Unicorn::u_view(*state_->begin++));
        std::get<2>(attrs_).value = irs::ref_cast<irs::byte_type>(irs::string_ref(term_buf_));
        break;
      case options_t::case_convert_t::UPPER:
        term_buf_ = RS::Unicorn::str_uppercase(RS::Unicorn::u_view(*state_->begin++));
        std::get<2>(attrs_).value = irs::ref_cast<irs::byte_type>(irs::string_ref(term_buf_));
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
    case options_t::word_break_t::ALPHA:
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