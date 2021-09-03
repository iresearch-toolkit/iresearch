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


#include "embedding_classification_stream.hpp"

#include "velocypack/Parser.h"
#include "velocypack/Slice.h"
#include "utils/vpack_utils.hpp"

namespace {

constexpr VPackStringRef MODEL_LOCATION_PARAM_NAME {"model_location"};

bool parse_vpack_options(const VPackSlice slice, irs::analysis::embedding_classification_stream::Options& options, const char* action) {
  switch (slice.type()) {
    case VPackValueType::Object: {
      auto model_location_slice = slice.get(MODEL_LOCATION_PARAM_NAME);
      if (!model_location_slice.isString() && !model_location_slice.isNone()) {
        IR_FRMT_ERROR(
          "Invalid vpack while %s embedding_classification_stream from VPack arguments. %s value should be a string.",
          action,
          MODEL_LOCATION_PARAM_NAME.data());
        return false;
      }
      auto ref = model_location_slice.stringRef();
      std::string model_location = std::string{ref.data(), ref.length()};
      options.model_location = model_location;
      return true;
    }
    default: {
      IR_FRMT_ERROR(
        "Invalid vpack while %s embedding_classification_stream from VPack arguments. Object was expected.",
        action);
    }
  }
  return false;
}

irs::analysis::analyzer::ptr construct(irs::analysis::embedding_classification_stream::Options& options) {
  // TODO: create the model here & put it into a singleton.
  // TODO: This will require modification of options
  return irs::memory::make_shared<irs::analysis::embedding_classification_stream>(std::move(options));
}

irs::analysis::analyzer::ptr make_vpack(const VPackSlice slice) {
  irs::analysis::embedding_classification_stream::Options options{};
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
  const irs::analysis::embedding_classification_stream::Options& options,
  VPackBuilder* builder) {
  VPackObjectBuilder object{builder};
  {
    builder->add(MODEL_LOCATION_PARAM_NAME, VPackValue(options.model_location));
  }
  return true;
}

bool normalize_vpack_config(const VPackSlice slice, VPackBuilder* builder) {
  irs::analysis::embedding_classification_stream::Options options{};
  if (parse_vpack_options(slice, options, "normalizing")) {
    return make_vpack_config(options, builder);
  }
  return false;
}


bool normalize_vpack_config(const irs::string_ref& args, std::string& config) {
  VPackSlice slice{reinterpret_cast<const uint8_t*>(args.c_str())};
  VPackBuilder builder;
  if (normalize_vpack_config(slice, &builder)) {
    config.assign(builder.slice().startAs<char>(), builder.slice().byteSize());
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
      return true;
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

REGISTER_ANALYZER_VPACK(irs::analysis::embedding_classification_stream, make_vpack, normalize_vpack_config);
REGISTER_ANALYZER_JSON(irs::analysis::embedding_classification_stream, make_json, normalize_json_config);

class EmbeddingsModelLoader {
  public:
    static std::shared_ptr<fasttext::FastText> getModel(std::string model_location) {
      std::unique_lock<std::mutex> lock(map_Mutex);
      if (modelMap.find(model_location) == modelMap.end()) {
        auto ft = iresearch::memory::make_shared<fasttext::FastText>();
        ft->loadModel(model_location);
        modelMap[model_location] = std::move(ft);
      }
      return modelMap.at(model_location);
    }

  private:
    EmbeddingsModelLoader();
    ~EmbeddingsModelLoader();
    EmbeddingsModelLoader(const EmbeddingsModelLoader &embeddingsModelLoader);
    const EmbeddingsModelLoader &operator=(const EmbeddingsModelLoader &embeddingsModelLoader);

    static std::unordered_map<std::string, std::shared_ptr<fasttext::FastText>> modelMap;
    static std::mutex map_Mutex;
};

} // namespace

namespace iresearch {
namespace analysis {

embedding_classification_stream::embedding_classification_stream(const Options &options)
  : analyzer{irs::type<embedding_classification_stream>::get()},
  modelContainer{EmbeddingsModelLoader::getModel(options.model_location)} {}

void embedding_classification_stream::init() {
  REGISTER_ANALYZER_VPACK(embedding_classification_stream, make_vpack, normalize_vpack_config);
  REGISTER_ANALYZER_JSON(embedding_classification_stream, make_json, normalize_json_config);
}

bool embedding_classification_stream::next() {
  if (term_eof_) {
    return false;
  }

  term_eof_ = true;

  return true;
}



}
}