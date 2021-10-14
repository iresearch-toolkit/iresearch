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
#include "nearest_neighbors_stream.hpp"

#include "velocypack/Parser.h"
#include "velocypack/Slice.h"
#include "utils/vpack_utils.hpp"

namespace {

constexpr VPackStringRef MODEL_LOCATION_PARAM_NAME {"model_location"};
constexpr VPackStringRef TOP_K_PARAM_NAME {"top_k"};

bool parse_vpack_options(const VPackSlice slice, irs::analysis::nearest_neighbors_stream::Options& options, const char* action) {
  switch (slice.type()) {
    case VPackValueType::Object: {
      auto model_location_slice = slice.get(MODEL_LOCATION_PARAM_NAME);
      if (!model_location_slice.isString() && !model_location_slice.isNone()) {
        IR_FRMT_ERROR(
          "Invalid vpack while %s nearest_neighbors_stream from VPack arguments. %s value should be a string.",
          action,
          MODEL_LOCATION_PARAM_NAME.data());
        return false;
      }
      options.model_location = iresearch::get_string<std::string>(model_location_slice);
      auto top_k_slice = slice.get(TOP_K_PARAM_NAME);
      if (!top_k_slice.isNone()) {
        if (!top_k_slice.isInteger()) {
          IR_FRMT_ERROR(
            "Invalid vpack while %s nearest_neighbors_stream from VPack arguments. %s value should be an integer.",
            action,
            TOP_K_PARAM_NAME.data());
          return false;
        }
        auto top_k_wide = top_k_slice.getInt();
        int32_t top_k_narrow = static_cast<int32_t>(top_k_wide);
        if (top_k_wide != top_k_narrow) {
          IR_FRMT_ERROR(
            "Invalid value provided while %s nearest_neighbors_stream from VPack arguments. %s value should be an int32_t.",
            action,
            TOP_K_PARAM_NAME.data());
          return false;
        }
        options.top_k = top_k_narrow;
      }

      return true;
    }
    default: {
      IR_FRMT_ERROR(
        "Invalid vpack while %s nearest_neighbors_stream from VPack arguments. Object was expected.",
        action);
    }
  }
  return false;
}

irs::analysis::analyzer::ptr construct(irs::analysis::nearest_neighbors_stream::Options& options) {
  auto load_model= [&options]() {
    auto ft = std::make_shared<fasttext::FastText>();
    ft->loadModel(options.model_location);
    return ft;
  };
  return irs::memory::make_unique<irs::analysis::nearest_neighbors_stream>(options, load_model);
}

irs::analysis::analyzer::ptr make_vpack(const VPackSlice slice) {
  irs::analysis::nearest_neighbors_stream::Options options{};
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
      IR_FRMT_ERROR("Null arguments while constructing nearest_neighbors_stream ");
      return nullptr;
    }
    auto vpack = VPackParser::fromJson(args.c_str());
    return make_vpack(vpack->slice());
  } catch (const VPackException &ex) {
    IR_FRMT_ERROR(
      "Caught error '%s' while constructing nearest_neighbors_stream from JSON",
      ex.what());
  } catch (...) {
    IR_FRMT_ERROR(
      "Caught error while constructing nearest_neighbors_stream from JSON");
  }
  return nullptr;
}

bool make_vpack_config(
  const irs::analysis::nearest_neighbors_stream::Options& options,
  VPackBuilder* builder) {
  VPackObjectBuilder object{builder};
  {
    builder->add(MODEL_LOCATION_PARAM_NAME, VPackValue(options.model_location));
    builder->add(TOP_K_PARAM_NAME, VPackValue(options.top_k));
  }
  return true;
}

bool normalize_vpack_config(const VPackSlice slice, VPackBuilder* builder) {
  irs::analysis::nearest_neighbors_stream::Options options{};
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
      IR_FRMT_ERROR("Null arguments while normalizing nearest_neighbors_stream ");
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
      "Caught error '%s' while normalizing nearest_neighbors_stream from JSON",
      ex.what());
  } catch (...) {
    IR_FRMT_ERROR(
      "Caught error while normalizing nearest_neighbors_stream from JSON");
  }
  return false;
}

REGISTER_ANALYZER_VPACK(irs::analysis::nearest_neighbors_stream, make_vpack, normalize_vpack_config);
REGISTER_ANALYZER_JSON(irs::analysis::nearest_neighbors_stream, make_json, normalize_json_config);

} // namespace

namespace iresearch {
namespace analysis {

nearest_neighbors_stream::nearest_neighbors_stream(Options& options, std::function<std::shared_ptr<fasttext::FastText>()> model_provider)
  : analyzer{irs::type<nearest_neighbors_stream>::get()},
    model_container_{model_provider()},
    top_k_{options.top_k},
    neighbors_{},
    neighbors_it_{neighbors_.end()} {}

void nearest_neighbors_stream::init() {
  REGISTER_ANALYZER_JSON(nearest_neighbors_stream, make_json, normalize_json_config);
  REGISTER_ANALYZER_VPACK(nearest_neighbors_stream, make_vpack, normalize_vpack_config);
}

bool nearest_neighbors_stream::next() {
  if (neighbors_it_ == neighbors_.end()) {
    return false;
  }

  auto& term = std::get<term_attribute>(attrs_);
  term.value = bytes_ref::NIL; // clear the term value
  term.value = bytes_ref(reinterpret_cast<const byte_type *>(neighbors_it_->second.c_str()), neighbors_it_->second.size());

  ++neighbors_it_;

  return true;
}

bool nearest_neighbors_stream::reset(const string_ref& data) {
  auto& offset = std::get<irs::offset>(attrs_);
  offset.start = 0;
  offset.end = static_cast<uint32_t>(data.size());

  neighbors_.clear();

  bytes_ref_input s_input{ref_cast<byte_type>(data)};
  input_buf buf{&s_input};
  std::istream ss{&buf};

  std::vector<int32_t> words, labels;
  auto len{model_container_->getDictionary()->getLine(ss, words, labels)};
  for (auto ind = 0; ind < len; ++ind) {
    const auto word_neighbors{model_container_->getNN(model_container_->getDictionary()->getWord(words[ind]), top_k_)};
    neighbors_.insert(neighbors_.end(), word_neighbors.begin(), word_neighbors.end());
  }
  neighbors_it_ = neighbors_.begin();

  return true;
}

}
}
