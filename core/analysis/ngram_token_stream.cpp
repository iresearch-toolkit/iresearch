////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <rapidjson/rapidjson/document.h> // for rapidjson::Document
#include <rapidjson/rapidjson/writer.h> // for rapidjson::Writer
#include <rapidjson/rapidjson/stringbuffer.h> // for rapidjson::StringBuffer
#include <utfcpp/utf8.h>

#include "ngram_token_stream.hpp"
#include "utils/json_utils.hpp"

NS_LOCAL

const irs::string_ref MIN_PARAM_NAME               = "min";
const irs::string_ref MAX_PARAM_NAME               = "max";
const irs::string_ref PRESERVE_ORIGINAL_PARAM_NAME = "preserveOriginal";
const irs::string_ref STREAM_TYPE_PARAM_NAME       = "streamType";
const irs::string_ref START_MARKER_PARAM_NAME      = "startMarker";
const irs::string_ref END_MARKER_PARAM_NAME        = "endMarker";

const std::unordered_map<
    std::string,
    irs::analysis::ngram_token_stream::options_t::stream_bytes_t> STREAM_TYPE_CONVERT_MAP = {
  { "binary", irs::analysis::ngram_token_stream::options_t::BinaryStream },
  { "utf8", irs::analysis::ngram_token_stream::options_t::Ut8Stream }
};

bool parse_json_config(const irs::string_ref& args,
                        irs::analysis::ngram_token_stream::options_t& options) {
  rapidjson::Document json;
  if (json.Parse(args.c_str(), args.size()).HasParseError()) {
    IR_FRMT_ERROR(
        "Invalid jSON arguments passed while constructing ngram_token_stream, "
        "arguments: %s",
        args.c_str());

    return false;
  }

  if (rapidjson::kObjectType != json.GetType()) {
    IR_FRMT_ERROR(
        "Not a jSON object passed while constructing ngram_token_stream, "
        "arguments: %s",
        args.c_str());

    return false;
  }

  uint64_t min, max;
  bool preserve_original;
  auto stream_bytes_type = irs::analysis::ngram_token_stream::options_t::BinaryStream;
  irs::bstring start_marker, end_marker;

  if (!get_uint64(json, MIN_PARAM_NAME, min)) {
    IR_FRMT_ERROR(
        "Failed to read '%s' attribute as number while constructing "
        "ngram_token_stream from jSON arguments: %s",
        MIN_PARAM_NAME.c_str(), args.c_str());
    return false;
  }

  if (!get_uint64(json, MAX_PARAM_NAME, max)) {
    IR_FRMT_ERROR(
        "Failed to read '%s' attribute as number while constructing "
        "ngram_token_stream from jSON arguments: %s",
        MAX_PARAM_NAME.c_str(), args.c_str());
    return false;
  }

  if (!get_bool(json, PRESERVE_ORIGINAL_PARAM_NAME, preserve_original)) {
    IR_FRMT_ERROR(
        "Failed to read '%s' attribute as boolean while constructing "
        "ngram_token_stream from jSON arguments: %s",
        PRESERVE_ORIGINAL_PARAM_NAME.c_str(), args.c_str());
    return false;
  }

  if (json.HasMember(START_MARKER_PARAM_NAME.c_str())) {
    auto& start_marker_json = json[START_MARKER_PARAM_NAME.c_str()];
    if (start_marker_json.IsString()) {
      static_assert(sizeof(irs::byte_type) == sizeof(char), "sizeof(irs::byte_type) == sizeof(char)");
      start_marker.assign(reinterpret_cast<const irs::byte_type*>(start_marker_json.GetString()),
                          start_marker_json.GetStringLength());
    } else {
      IR_FRMT_ERROR(
          "Failed to read '%s' attribute as string while constructing "
          "ngram_token_stream from jSON arguments: %s",
          START_MARKER_PARAM_NAME.c_str(), args.c_str());
      return false;
    }
  }

  if (json.HasMember(END_MARKER_PARAM_NAME.c_str())) {
    auto& end_marker_json = json[END_MARKER_PARAM_NAME.c_str()];
    if (end_marker_json.IsString()) {
      static_assert(sizeof(irs::byte_type) == sizeof(char), "sizeof(irs::byte_type) == sizeof(char)");
      end_marker.assign(reinterpret_cast<const irs::byte_type*>(end_marker_json.GetString()),
                          end_marker_json.GetStringLength());
    } else {
      IR_FRMT_ERROR(
          "Failed to read '%s' attribute as string while constructing "
          "ngram_token_stream from jSON arguments: %s",
          END_MARKER_PARAM_NAME.c_str(), args.c_str());
      return false;
    }
  }

  if (json.HasMember(STREAM_TYPE_PARAM_NAME.c_str())) {
      auto& stream_type_json =
          json[STREAM_TYPE_PARAM_NAME.c_str()];

      if (!stream_type_json.IsString()) {
        IR_FRMT_WARN(
            "Non-string value in '%s' while constructing ngram_token_stream "
            "from jSON arguments: %s",
            STREAM_TYPE_PARAM_NAME.c_str(), args.c_str());
        return false;
      }
      auto itr = STREAM_TYPE_CONVERT_MAP.find(stream_type_json.GetString());
      if (itr == STREAM_TYPE_CONVERT_MAP.end()) {
        IR_FRMT_WARN(
            "Invalid value in '%s' while constructing ngram_token_stream from "
            "jSON arguments: %s",
            STREAM_TYPE_PARAM_NAME.c_str(), args.c_str());
        return false;
      }
      stream_bytes_type = itr->second;
  }


  options.min_gram = min;
  options.max_gram = max;
  options.preserve_original = preserve_original;
  options.start_marker = start_marker;
  options.end_marker = end_marker;
  options.stream_bytes_type = stream_bytes_type;
  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief args is a jSON encoded object with the following attributes:
///        "min" (number): minimum ngram size
///        "max" (number): maximum ngram size
///        "preserveOriginal" (boolean): preserve or not the original term
////////////////////////////////////////////////////////////////////////////////
irs::analysis::analyzer::ptr make_json(const irs::string_ref& args) {
  irs::analysis::ngram_token_stream::options_t options;
  if (parse_json_config(args, options)) {
    return irs::analysis::ngram_token_stream::make(options);
  } else {
    return nullptr;
  }
}

///////////////////////////////////////////////////////////////////////////////
/// @brief builds analyzer config from internal options in json format
///////////////////////////////////////////////////////////////////////////////
bool make_json_config(const irs::analysis::ngram_token_stream::options_t& options,
                      std::string& definition) {
  rapidjson::Document json;
  json.SetObject();
  rapidjson::Document::AllocatorType& allocator = json.GetAllocator();

  // ensure disambiguating casts below are safe. Casts required for clang compiler on Mac
  static_assert(sizeof(uint64_t) >= sizeof(size_t), "sizeof(uint64_t) >= sizeof(size_t)");
  //min_gram
  json.AddMember(
    rapidjson::StringRef(MIN_PARAM_NAME.c_str(), MIN_PARAM_NAME.size()),
    rapidjson::Value(static_cast<uint64_t>(options.min_gram)),
    allocator);

  //max_gram
  json.AddMember(
    rapidjson::StringRef(MAX_PARAM_NAME.c_str(), MAX_PARAM_NAME.size()),
    rapidjson::Value(static_cast<uint64_t>(options.max_gram)),
    allocator);

  //preserve_original
  json.AddMember(
    rapidjson::StringRef(PRESERVE_ORIGINAL_PARAM_NAME.c_str(), PRESERVE_ORIGINAL_PARAM_NAME.size()),
    rapidjson::Value(options.preserve_original),
    allocator);

  // stream type
  {
    auto stream_type_value = std::find_if(STREAM_TYPE_CONVERT_MAP.begin(), STREAM_TYPE_CONVERT_MAP.end(),
      [&options](const decltype(STREAM_TYPE_CONVERT_MAP)::value_type& v) {
        return v.second == options.stream_bytes_type;
      });

    if (stream_type_value != STREAM_TYPE_CONVERT_MAP.end()) {
      json.AddMember(
        rapidjson::StringRef(STREAM_TYPE_PARAM_NAME.c_str(), STREAM_TYPE_PARAM_NAME.size()),
        rapidjson::StringRef(stream_type_value->first.c_str(), stream_type_value->first.length()),
        allocator);
    } else {
      IR_FRMT_ERROR(
        "Invalid %s value in ngram analyzer options: %d",
        STREAM_TYPE_PARAM_NAME.c_str(),
        static_cast<int>(options.stream_bytes_type));
      return false;
    }
  }

  // start_marker
  if (!options.start_marker.empty()) {
    json.AddMember(
      rapidjson::StringRef(START_MARKER_PARAM_NAME.c_str(), START_MARKER_PARAM_NAME.size()),
      rapidjson::StringRef(reinterpret_cast<const char*>(options.start_marker.c_str()), options.start_marker.length()),
      allocator);
  }

  // end_marker
  if (!options.end_marker.empty()) {
    json.AddMember(
      rapidjson::StringRef(END_MARKER_PARAM_NAME.c_str(), END_MARKER_PARAM_NAME.size()),
      rapidjson::StringRef(reinterpret_cast<const char*>(options.end_marker.c_str()), options.end_marker.length()),
      allocator);
  }

  //output json to string
  rapidjson::StringBuffer buffer;
  rapidjson::Writer< rapidjson::StringBuffer> writer(buffer);
  json.Accept(writer);
  definition = buffer.GetString();
  return true;
}

bool normalize_json_config(const irs::string_ref& args, std::string& config) {
  irs::analysis::ngram_token_stream::options_t options;
  if (parse_json_config(args, options)) {
    return make_json_config(options, config);
  } else {
    return false;
  }
}

REGISTER_ANALYZER_JSON(irs::analysis::ngram_token_stream, make_json,
                       normalize_json_config);

NS_END

NS_ROOT
NS_BEGIN(analysis)

/*static*/ analyzer::ptr ngram_token_stream::make(
    const options_t& options
) {
  return std::make_shared<ngram_token_stream>(options);
}

/*static*/ void ngram_token_stream::init() {
  REGISTER_ANALYZER_JSON(ngram_token_stream, make_json,
                         normalize_json_config); // match registration above
}

ngram_token_stream::ngram_token_stream(
    const options_t& options
) : analyzer(ngram_token_stream::type()),
    options_(options) {
  options_.min_gram = std::max(options_.min_gram, size_t(1));
  options_.max_gram = std::max(options_.max_gram, options_.min_gram);

  attrs_.emplace(offset_);
  attrs_.emplace(inc_);
  attrs_.emplace(term_);
  const size_t max_marker_size = std::max(options_.start_marker.size(), options_.end_marker.size());
  if (max_marker_size > 0) {
    // we have at least one marker. As we need to append marker to ngtram and provide term 
    // value as continious buffer, we can`t return pointer to some byte inside input stream
    // but rather we return pointer to buffer with copied values of ngram and marker
    // For sake of performance we allocate requested memory right now
    size_t buffer_size = options_.preserve_original ? data_.size() : std::min(data_.size(), options_.max_gram);
    buffer_size += max_marker_size;
    marked_term_buffer_.reserve(buffer_size);
  }
}

bool ngram_token_stream::next_symbol(const byte_type*& it) noexcept {
  if (it != nullptr) {
    if (it >= data_.end()) {
      return false;
    }
    switch (options_.stream_bytes_type) {
    case options_t::BinaryStream:
      it++;
      break;
    case options_t::Ut8Stream:
      utf8::unchecked::next(it);
      break;
    default:
      IRS_ASSERT(false);
      IR_FRMT_ERROR(
        "Invalid stream type value %d",
        static_cast<int>(options_.stream_bytes_type));
      return false;
    }
  } else {
    // special case - stream entry
    it = data_.begin();
  }
  return it <= data_.end();
}

void ngram_token_stream::emit_ngram() noexcept {
  const auto ngram_byte_len = std::distance(begin_, ngram_end_);
  if (emit_original_ == None || begin_ != data_.begin() || ngram_byte_len < data_.size()) {
    offset_.start = std::distance(data_.begin(), begin_);
    offset_.end = offset_.start + ngram_byte_len;
    inc_.value = next_inc_val_;
    next_inc_val_ = 0;
    keep_ngram_position_ = false;
    if (0 == offset_.start && !options_.start_marker.empty()) {
      marked_term_buffer_.clear();
      IRS_ASSERT(marked_term_buffer_.capacity() >= (options_.start_marker.size() + ngram_byte_len));
      marked_term_buffer_.append(options_.start_marker.begin(), options_.start_marker.end());
      marked_term_buffer_.append(begin_, ngram_byte_len);
      term_.value(marked_term_buffer_);
      assert(marked_term_buffer_.size() <= integer_traits<uint32_t>::const_max);
    } else if (ngram_end_ == data_.end()) {
      marked_term_buffer_.clear();
      IRS_ASSERT(marked_term_buffer_.capacity() >= (options_.end_marker.size() + ngram_byte_len));
      marked_term_buffer_.append(begin_, ngram_byte_len);
      marked_term_buffer_.append(options_.end_marker.begin(), options_.end_marker.end());
      term_.value(marked_term_buffer_);
    } else {
      assert(ngram_byte_len <= integer_traits<uint32_t>::const_max);
      term_.value(irs::bytes_ref(begin_, ngram_byte_len));
    }
  } else {
    emit_original();
  }
}

void ngram_token_stream::emit_original() noexcept {
  switch (emit_original_) {
    case WithoutMarkers:
      term_.value(data_);
      assert(data_.size() <= integer_traits<uint32_t>::const_max);
      offset_.start = 0;
      offset_.end = uint32_t(data_.size());
      emit_original_ = None;
      inc_.value = next_inc_val_;
      next_inc_val_ = 1;
      keep_ngram_position_ = false;
      break;
    case WithEndMarker:
      marked_term_buffer_.clear();
      IRS_ASSERT(marked_term_buffer_.capacity() >= (options_.end_marker.size() + data_.size()));
      marked_term_buffer_.append(data_.begin(), data_.end());
      marked_term_buffer_.append(options_.end_marker.begin(), options_.end_marker.end());
      term_.value(marked_term_buffer_);
      assert(marked_term_buffer_.size() <= integer_traits<uint32_t>::const_max);
      offset_.start = 0;
      offset_.end = uint32_t(data_.size());
      emit_original_ = None; // end marker is emitted last, so we are done emitting original
      keep_ngram_position_ = false;
      inc_.value = next_inc_val_;
      next_inc_val_ = 1;
      break;
    case WithStartMarker:
      marked_term_buffer_.clear();
      IRS_ASSERT(marked_term_buffer_.capacity() >= (options_.start_marker.size() + data_.size()));
      marked_term_buffer_.append(options_.start_marker.begin(), options_.start_marker.end());
      marked_term_buffer_.append(data_.begin(), data_.end());
      term_.value(marked_term_buffer_);
      assert(marked_term_buffer_.size() <= integer_traits<uint32_t>::const_max);
      offset_.start = 0;
      offset_.end = uint32_t(data_.size());
      emit_original_ = options_.end_marker.empty()? None : WithEndMarker;
      inc_.value = next_inc_val_;
      next_inc_val_ = emit_original_ != None ? 0 : 1;
      keep_ngram_position_ = emit_original_ != None;
      break;
  }
}


bool ngram_token_stream::next() noexcept {
  while (begin_ < data_.end()) {
    if (!keep_ngram_position_ && length_ < options_.max_gram && next_symbol(ngram_end_)) {
      // we have next ngram from current position
      length_++;
      if (length_ >= options_.min_gram) {
        emit_ngram();
        return true;
      }
    } else {
      // need to move to next position
      if (emit_original_ == None ) {
        if (next_symbol(begin_)) {
          next_inc_val_ = 1;
          length_ = 0;
          ngram_end_ = begin_;
        } else {
          return false; // stream exhausted
        }
      } else {
        // as stream has unsigned incremet attribute
        // we cannot go back, so we must emit original 
        // as token(s) from pos = 0 if needed (as it starts from pos=0 in stream)
        emit_original();
        return true;
      }
    }
  }
  return false;
}

bool ngram_token_stream::reset(const irs::string_ref& value) noexcept {
  if (value.size() > integer_traits<uint32_t>::const_max) {
    // can't handle data which is longer than integer_traits<uint32_t>::const_max
    return false;
  }

  // reset term attribute
  term_.value(bytes_ref::NIL);

  // reset offset attribute
  offset_.start = integer_traits<uint32_t>::const_max;
  offset_.end = integer_traits<uint32_t>::const_max;

  // reset stream
  data_ = ref_cast<byte_type>(value);
  begin_ = data_.begin();
  ngram_end_ = data_.begin();
  length_ = 0;
  if (options_.preserve_original) {
    if (!options_.start_marker.empty()) {
      emit_original_ = WithStartMarker;
    } else if (!options_.end_marker.empty()) {
      emit_original_ = WithEndMarker;
    } else {
      emit_original_ = WithoutMarkers;
    }
  } else {
    emit_original_ = None;
  }
  next_inc_val_ = 1;
  keep_ngram_position_ = false;
  assert(length_ < options_.min_gram);

  return true;
}

DEFINE_ANALYZER_TYPE_NAMED(ngram_token_stream, "ngram")

NS_END // analysis
NS_END // ROOT
