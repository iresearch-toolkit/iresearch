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
/// @author Alex Geenen
////////////////////////////////////////////////////////////////////////////////


#include <functional>
#include <sstream>
#include <store/store_utils.hpp>
#include "classification_stream.hpp"

#include "velocypack/Parser.h"
#include "velocypack/Slice.h"
#include "utils/vpack_utils.hpp"

namespace {

constexpr VPackStringRef MODEL_LOCATION_PARAM_NAME {"model_location"};

bool parse_vpack_options(const VPackSlice slice, irs::analysis::classification_stream::Options& options, const char* action) {
  switch (slice.type()) {
    case VPackValueType::Object: {
      auto model_location_slice = slice.get(MODEL_LOCATION_PARAM_NAME);
      if (!model_location_slice.isString() && !model_location_slice.isNone()) {
        IR_FRMT_ERROR(
          "Invalid vpack while %s classification_stream from VPack arguments. %s value should be a string.",
          action,
          MODEL_LOCATION_PARAM_NAME.data());
        return false;
      }
      options.model_location = iresearch::get_string<std::string>(model_location_slice);
      return true;
    }
    default: {
      IR_FRMT_ERROR(
        "Invalid vpack while %s classification_stream from VPack arguments. Object was expected.",
        action);
    }
  }
  return false;
}

irs::analysis::analyzer::ptr construct(irs::analysis::classification_stream::Options& options) {
  auto load_model= [&options]() {
    auto ft = std::make_shared<fasttext::FastText>();
    ft->loadModel(options.model_location);
    return ft;
  };
  return irs::memory::make_unique<irs::analysis::classification_stream>(load_model);
}

irs::analysis::analyzer::ptr make_vpack(const VPackSlice slice) {
  irs::analysis::classification_stream::Options options{};
  if (parse_vpack_options(slice, options, "constructing")) {
    return construct(options);
  }
  return nullptr;
}

irs::analysis::analyzer::ptr make_vpack(const irs::string_ref& args) {
  VPackSlice slice{reinterpret_cast<const uint8_t*>(args.c_str())};
  return make_vpack(slice);
}

irs::analysis::analyzer::ptr make_json(const irs::string_ref& args) {
  try {
    if (args.null()) {
      IR_FRMT_ERROR("Null arguments while constructing embeddings_classification_stream ");
      return nullptr;
    }
    auto vpack = VPackParser::fromJson(args.c_str());
    return make_vpack(vpack->slice());
  } catch (const VPackException &ex) {
    IR_FRMT_ERROR(
      "Caught error '%s' while constructing embeddings_classification_stream from JSON",
      ex.what());
  } catch (...) {
    IR_FRMT_ERROR(
      "Caught error while constructing embeddings_classification_stream from JSON");
  }
  return nullptr;
}

bool make_vpack_config(
  const irs::analysis::classification_stream::Options& options,
  VPackBuilder* builder) {
  VPackObjectBuilder object{builder};
  {
    builder->add(MODEL_LOCATION_PARAM_NAME, VPackValue(options.model_location));
  }
  return true;
}

bool normalize_vpack_config(const VPackSlice slice, VPackBuilder* builder) {
  irs::analysis::classification_stream::Options options{};
  if (parse_vpack_options(slice, options, "normalizing")) {
    return make_vpack_config(options, builder);
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

bool normalize_json_config(const irs::string_ref& args, std::string& definition) {
  try {
    if (args.null()) {
      IR_FRMT_ERROR("Null arguments while normalizing embeddings_classification_stream ");
      return false;
    }
    auto vpack = VPackParser::fromJson(args.c_str());
    VPackBuilder builder;
    if (normalize_vpack_config(vpack->slice(), &builder)) {
      definition = builder.toString();
      return !definition.empty();
    }
  } catch(const VPackException& ex) {
    IR_FRMT_ERROR(
      "Caught error '%s' while normalizing text_token_normalizing_stream from JSON",
      ex.what());
  } catch (...) {
    IR_FRMT_ERROR(
      "Caught error while normalizing text_token_normalizing_stream from JSON");
  }
  return false;
}

REGISTER_ANALYZER_VPACK(irs::analysis::classification_stream, make_vpack, normalize_vpack_config);
REGISTER_ANALYZER_JSON(irs::analysis::classification_stream, make_json, normalize_json_config);

} // namespace

namespace iresearch {
namespace analysis {

classification_stream::classification_stream(std::function<std::shared_ptr<fasttext::FastText>()> model_provider)
: analyzer{irs::type<classification_stream>::get()},
    model_container_{model_provider()},
    predictions_{},
    predictions_it_{predictions_.end()} {}

void classification_stream::init() {
  REGISTER_ANALYZER_JSON(classification_stream, make_json, normalize_json_config);
  REGISTER_ANALYZER_VPACK(classification_stream, make_vpack, normalize_vpack_config);
}

bool classification_stream::next() {
  if (predictions_it_ == predictions_.end()) {
    return false;
  }

  auto& term = std::get<term_attribute>(attrs_);
  term.value = bytes_ref::NIL; // clear the term value
  term.value = bytes_ref(reinterpret_cast<const byte_type *>(predictions_it_->second.c_str()), predictions_it_->second.size());

  ++predictions_it_;

  return true;
}

bool classification_stream::reset(const string_ref& data) {
  auto& offset = std::get<irs::offset>(attrs_);
  offset.start = 0;
  offset.end = static_cast<uint32_t>(data.size());

  predictions_.clear();

  std::stringstream ss{};
  ss << data;
  model_container_->predictLine(ss, predictions_, 1, 0.0);
  predictions_it_ = predictions_.begin();

  return true;
}

}
}