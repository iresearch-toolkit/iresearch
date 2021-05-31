////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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

#include "pipeline_token_stream.hpp"

#include "velocypack/Slice.h"
#include "velocypack/Builder.h"
#include "velocypack/Parser.h"
#include "velocypack/velocypack-aliases.h"
#include "velocypack/vpack.h"

#include "utils/vpack_utils.hpp"

#include <rapidjson/rapidjson/document.h> // for rapidjson::Document
#include <rapidjson/rapidjson/writer.h> // for rapidjson::Writer
#include <rapidjson/rapidjson/stringbuffer.h> // for rapidjson::StringBuffer

namespace {

constexpr irs::string_ref PIPELINE_PARAM_NAME = "pipeline";
constexpr irs::string_ref TYPE_PARAM_NAME = "type";
constexpr irs::string_ref PROPERTIES_PARAM_NAME = "properties";

const irs::offset NO_OFFSET;

class empty_analyzer final
  : public irs::analysis::analyzer, private irs::util::noncopyable {
 public:
  empty_analyzer() : analyzer(irs::type<empty_analyzer>::get()) {}
  virtual irs::attribute* get_mutable(irs::type_info::type_id) override { return nullptr;  }
  static constexpr irs::string_ref type_name() noexcept { return "empty_analyzer"; }
  virtual bool next() override { return false; }
  virtual bool reset(const irs::string_ref&) override { return false; }
};

empty_analyzer EMPTY_ANALYZER;

using  options_normalize_t = std::vector<std::pair<std::string, std::string>>;

template<typename T>
bool parse_vpack_options(const irs::string_ref& args, T& options) {
  if constexpr (std::is_same_v<T, irs::analysis::pipeline_token_stream::options_t>) {
    assert(options.empty());
  }

  VPackSlice slice(reinterpret_cast<const uint8_t*>(args.c_str()));

  if (!slice.isObject()) {
    std::string slice_as_str = iresearch::get_string(slice);
    IR_FRMT_ERROR("Slice for ngram_token_stream is not an object: %s",
                  slice_as_str.c_str());
    return false;
  }

  if (VPackValueType::Object != slice.type()) {
      IR_FRMT_ERROR(
        "Not a jSON object passed while constructing pipeline_token_stream, "
        "arguments: %s",
        args.c_str());
      return false;
  }
  auto tmp = slice.toJson();

  if (slice.hasKey(PIPELINE_PARAM_NAME.c_str())) {
    auto pipeline_slice = slice.get(PIPELINE_PARAM_NAME);
    if (pipeline_slice.isArray()) {
      auto tmp1 = pipeline_slice.toJson();
      for (auto const& pipe : VPackArrayIterator(pipeline_slice)) {
        if (pipe.isObject()) {
          auto tmp2 = pipe.toJson();
          std::string type;
          // TYPE_PARAM_NAME
          if (pipe.hasKey(TYPE_PARAM_NAME.c_str())) {
            auto type_atr_slice = pipe.get(TYPE_PARAM_NAME);
            auto tmp3 = type_atr_slice.toJson();
            if (type_atr_slice.isString()) {
              //VPackValueLength length;
              //const char* ptr = type_atr.getString(length);
              //type = irs::string_ref(ptr, length);
              type = type_atr_slice.toString();
            } else {
              IR_FRMT_ERROR(
                "Failed to read '%s' attribute of  '%s' member as string while constructing "
                "pipeline_token_stream from jSON arguments: %s",
                TYPE_PARAM_NAME.c_str(), PIPELINE_PARAM_NAME.c_str(), args.c_str());
              return false;
            }
          } else {
            IR_FRMT_ERROR(
              "Failed to get '%s' attribute of  '%s' member while constructing "
              "pipeline_token_stream from jSON arguments: %s",
              TYPE_PARAM_NAME.c_str(), PIPELINE_PARAM_NAME.c_str(), args.c_str());
            return false;
          }
          // PROPERTIES_PARAM_NAME
          if (pipe.hasKey(PROPERTIES_PARAM_NAME.c_str())) {
            auto properties_slice = pipe.get(PROPERTIES_PARAM_NAME);
            auto tmp3 = properties_slice.toJson();
            if (!properties_slice.isObject()) {
              return false;
            }
            if (!properties_slice.hasKey(type.c_str())) {
              return false;
            }
            auto properties_value_slice = properties_slice.get(type);

            if (!properties_value_slice.isString()) {
              return false;
            }

            std::string properties_ref = properties_value_slice.toString();
            //VPackValueLength length;
            //char const* ptr = properties_slice.getString(length);
            //std::string properties_ref(ptr, length);

            if constexpr (std::is_same_v<T, irs::analysis::pipeline_token_stream::options_t>) {
              auto analyzer = irs::analysis::analyzers::get(
                type,
                irs::type<irs::text_format::json>::get(),
                properties_ref);
              if (analyzer) {
                options.push_back(std::move(analyzer));
              } else {
                IR_FRMT_ERROR(
                  "Failed to create pipeline member of type '%s' with properties '%s' while constructing "
                  "pipeline_token_stream from jSON arguments: %s",
                  type.c_str(), properties_ref.c_str(), args.c_str());
                return false;
              }
            } else {
              std::string normalized;
              if (!irs::analysis::analyzers::normalize(normalized, type,
                                                       irs::type<irs::text_format::json>::get(),
                properties_ref)) {
                IR_FRMT_ERROR(
                  "Failed to normalize pipeline member of type '%s' with properties '%s' while constructing "
                  "pipeline_token_stream from jSON arguments: %s",
                  type.c_str(), properties_ref.c_str(), args.c_str());
                return false;
              }
              options.emplace_back(type, normalized);
            }
          } else {
            IR_FRMT_ERROR(
              "Failed to get '%s' attribute of  '%s' member while constructing "
              "pipeline_token_stream from jSON arguments: %s",
              PROPERTIES_PARAM_NAME.c_str(), PIPELINE_PARAM_NAME.c_str(), args.c_str());
            return false;
          }
        } else {
          IR_FRMT_ERROR(
            "Failed to read '%s' member as object while constructing "
            "pipeline_token_stream from jSON arguments: %s",
            PIPELINE_PARAM_NAME.c_str(), args.c_str());
          return false;
        }
      }
    } else {
      IR_FRMT_ERROR(
        "Failed to read '%s' attribute as array while constructing "
        "pipeline_token_stream from jSON arguments: %s",
        PIPELINE_PARAM_NAME.c_str(), args.c_str());
      return false;
    }
  } else {
    IR_FRMT_ERROR(
      "Not found parameter '%s' while constructing pipeline_token_stream, "
      "arguments: %s",
      PIPELINE_PARAM_NAME.c_str(),
      args.c_str());
    return false;
  }

  if (options.empty()) {
    IR_FRMT_ERROR(
      "Empty pipeline found while constructing pipeline_token_stream, "
      "arguments: %s", args.c_str());
    return false;
  }
  return true;
}

bool make_vpack_config(const options_normalize_t& options,
                      std::string& definition) {
  VPackBuilder builder;
  {
    VPackObjectBuilder object(&builder);
    {
      VPackArrayBuilder array(&builder, PIPELINE_PARAM_NAME.c_str());
      {
        for (auto analyzer : options) {
          VPackObjectBuilder sub_object(&builder);
          {
            builder.add(TYPE_PARAM_NAME.c_str(), VPackValue(analyzer.first));
            builder.add(PROPERTIES_PARAM_NAME.c_str(), VPackValue(analyzer.second));
          }
        }
      }
    }
  }
  definition.clear();
  definition.assign(builder.slice().startAs<char>(), builder.slice().byteSize());
  return true;
}

bool normalize_vpack_config(const irs::string_ref& args, std::string& definition) {
  options_normalize_t options;
  if (parse_vpack_options(args, options)) {
    return make_vpack_config(options, definition);
  } else {
    return false;
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief args is a jSON encoded object with the following attributes:
/// pipeline: Array of objects containing analyzers definition inside pipeline.
/// Each definition is an object with the following attributes:
/// type: analyzer type name (one of registered analyzers type)
/// properties: object with properties for corresponding analyzer
////////////////////////////////////////////////////////////////////////////////
irs::analysis::analyzer::ptr make_vpack(const irs::string_ref& args) {
  irs::analysis::pipeline_token_stream::options_t options;
  if (parse_vpack_options(args, options)) {
    return std::make_shared<irs::analysis::pipeline_token_stream>(std::move(options));
  } else {
    return nullptr;
  }
}

irs::analysis::analyzer::ptr make_json(const irs::string_ref& args) {
  try {
    if (args.null()) {
      IR_FRMT_ERROR("Null arguments while constructing ngram_token_stream");
      return nullptr;
    }
    auto vpack = VPackParser::fromJson(args.c_str());
    return make_vpack(
        irs::string_ref(reinterpret_cast<const char*>(vpack->data()), vpack->size()));
  } catch(const VPackException& ex) {
    IR_FRMT_ERROR("Caught error '%s' while constructing ngram_token_stream from json: %s",
                  ex.what(), args.c_str());
  } catch (...) {
    IR_FRMT_ERROR("Caught error while constructing ngram_token_stream from json: %s",
                  args.c_str());
  }
  return nullptr;
}

bool normalize_json_config(const irs::string_ref& args, std::string& definition) {
  try {
    if (args.null()) {
      IR_FRMT_ERROR("Null arguments while normalizing ngram_token_stream");
      return false;
    }
    auto vpack = VPackParser::fromJson(args.c_str());
    std::string vpack_container;
    if (normalize_vpack_config(
        irs::string_ref(reinterpret_cast<const char*>(vpack->data()), vpack->size()),
        vpack_container)) {
      VPackSlice slice(
          reinterpret_cast<const uint8_t*>(vpack_container.c_str()));
      definition = iresearch::get_string(slice);
      if (definition.empty()) {
          return false;
      }
      return true;
    }
  } catch(const VPackException& ex) {
    IR_FRMT_ERROR("Caught error '%s' while normalizing ngram_token_stream from json: %s",
                  ex.what(), args.c_str());
  } catch (...) {
    IR_FRMT_ERROR("Caught error while normalizing ngram_token_stream from json: %s",
                  args.c_str());
  }
  return false;
}

REGISTER_ANALYZER_JSON(irs::analysis::pipeline_token_stream, make_json,
  normalize_json_config);

REGISTER_ANALYZER_VPACK(irs::analysis::pipeline_token_stream, make_vpack,
  normalize_vpack_config);

irs::payload* find_payload(const std::vector<irs::analysis::analyzer::ptr>& pipeline) {
  for (auto it = pipeline.rbegin(); it != pipeline.rend(); ++it) {
    auto payload = irs::get_mutable<irs::payload>(it->get());
    if (payload) {
      return payload;
    }
  }
  return nullptr;
}

bool all_have_offset(const std::vector<irs::analysis::analyzer::ptr>& pipeline) {
  return std::all_of(
      pipeline.begin(), pipeline.end(),
      [](const irs::analysis::analyzer::ptr& v) {
    return nullptr != irs::get<irs::offset>(*v);
  });
}

}

namespace iresearch {
namespace analysis {

pipeline_token_stream::pipeline_token_stream(pipeline_token_stream::options_t&& options)
  : analyzer{irs::type<pipeline_token_stream>::get()},
    attrs_{
      {},
      options.empty()
        ? nullptr
        : irs::get_mutable<term_attribute>(options.back().get()),
      all_have_offset(options)
        ? &offs_
        : attribute_ptr<offset>{},
      find_payload(options)
    } {
  const auto track_offset = irs::get<offset>(*this) != nullptr;
  pipeline_.reserve(options.size());
  for (const auto& p : options) {
    assert(p);
    pipeline_.emplace_back(std::move(p), track_offset);
  }
  options.clear(); // mimic move semantic
  if (pipeline_.empty()) {
    pipeline_.emplace_back();
  }
  top_ = pipeline_.begin();
  bottom_ = --pipeline_.end();
}

/// Moves pipeline to next token.
/// Term is taken from last analyzer in pipeline
/// Offset is recalculated accordingly (only if ALL analyzers in pipeline )
/// Payload is taken from lowest analyzer having this attribute
/// Increment is calculated according to following position change rules
///  - If none of pipeline members changes position - whole pipeline holds position
///  - If one or more pipeline member moves - pipeline moves( change from max->0 is not move, see rules below!).
///    All position gaps are accumulated (e.g. if one member has inc 2(1 pos gap) and other has inc 3(2 pos gap)  - pipeline has inc 4 (1+2 pos gap))
///  - All position changes caused by parent analyzer move next (e.g. transfer from max->0 by first next after reset) are collapsed.
///    e.g if parent moves after next, all its children are resetted to new token and also moves step froward - this whole operation
///    is just one step for pipeline (if any of children has moved more than 1 step - gaps are preserved!)
///  - If parent after next is NOT moved (inc == 0) than pipeline makes one step forward if at least one child changes
///    position from any positive value back to 0 due to reset (additional gaps also preserved!) as this is
///    not change max->0 and position is indeed changed.
bool pipeline_token_stream::next() {
  uint32_t pipeline_inc;
  bool step_for_rollback{ false };
  do {
    while (!current_->next()) {
      if (current_ == top_) { // reached pipeline top and next has failed - we are done
        return false;
      }
      --current_;
    }
    pipeline_inc = current_->inc->value;
    const auto top_holds_position = current_->inc->value == 0;
    // go down to lowest pipe to get actual tokens
    while (current_ != bottom_) {
      const auto prev_term = current_->term->value;
      const auto prev_start = current_->start();
      const auto prev_end = current_->end();
      ++current_;
      // check do we need to do step forward due to rollback to 0.
      step_for_rollback |= top_holds_position && current_->pos != 0 &&
        current_->pos != std::numeric_limits<uint32_t>::max();
      if (!current_->reset(prev_start, prev_end, irs::ref_cast<char>(prev_term))) {
        return false;
      }
      if (!current_->next()) { // empty one found. Another round from the very beginning.
        assert(current_ != top_);
        --current_;
        break;
      }
      pipeline_inc += current_->inc->value;
      assert(current_->inc->value > 0); // first increment after reset should be positive to give 0 or next pos!
      assert(pipeline_inc > 0);
      pipeline_inc--; // compensate placing sub_analyzer from max to 0 due to reset
                      // as this step actually does not move whole pipeline
                      // sub analyzer just stays same pos as it`s parent (step for rollback to 0 will be done below if necessary!)
    }
  } while (current_ != bottom_);
  if (step_for_rollback) {
    pipeline_inc++;
  }
  std::get<increment>(attrs_).value = pipeline_inc;
  offs_.start = current_->start();
  offs_.end = current_->end();
  return true;
}

bool pipeline_token_stream::reset(const string_ref& data) {
  current_ = top_;
  return pipeline_.front().reset(0, static_cast<uint32_t>(data.size()), data);
}

/*static*/ void pipeline_token_stream::init() {
  REGISTER_ANALYZER_JSON(pipeline_token_stream, make_json,
    normalize_json_config);  // match registration above
  REGISTER_ANALYZER_VPACK(pipeline_token_stream, make_vpack,
    normalize_vpack_config);  // match registration above
}

pipeline_token_stream::sub_analyzer_t::sub_analyzer_t(
    const irs::analysis::analyzer::ptr& a,
    bool track_offset)
  : term(irs::get<irs::term_attribute>(*a)),
    inc(irs::get<irs::increment>(*a)),
    offs(track_offset ? irs::get<irs::offset>(*a) : &NO_OFFSET),
    analyzer(a) {
  assert(inc);
  assert(term);
}

pipeline_token_stream::sub_analyzer_t::sub_analyzer_t()
  : term(nullptr), inc(nullptr), offs(nullptr),
    analyzer(irs::analysis::analyzer::ptr(), &EMPTY_ANALYZER) { }

}
}
