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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////
#include "analysis/analyzers.hpp"

#include "analysis/classification_stream.hpp"
#include "analysis/collation_token_stream.hpp"
#include "analysis/delimited_token_stream.hpp"
#include "analysis/nearest_neighbors_stream.hpp"
#include "analysis/ngram_token_stream.hpp"
#include "analysis/pipeline_token_stream.hpp"
#include "analysis/segmentation_token_stream.hpp"
#include "analysis/text_token_normalizing_stream.hpp"
#include "analysis/text_token_stemming_stream.hpp"
#include "analysis/text_token_stream.hpp"
#include "analysis/token_stopwords_stream.hpp"
#include "utils/hash_utils.hpp"
#include "utils/register.hpp"

namespace {

using namespace irs;

struct key {
  key(string_ref type, const irs::type_info& args_format)
      : type{type}, args_format{args_format} {}

  bool operator==(const key& other) const noexcept {
    return args_format == other.args_format && type == other.type;
  }

  bool operator!=(const key& other) const noexcept { return !(*this == other); }

  string_ref type;
  irs::type_info args_format;
};

struct value {
  explicit value(analysis::factory_f factory = nullptr,
                 analysis::normalizer_f normalizer = nullptr)
      : factory(factory), normalizer(normalizer) {}

  bool empty() const noexcept { return nullptr == factory; }

  bool operator==(const value& other) const noexcept {
    return factory == other.factory && normalizer == other.normalizer;
  }

  bool operator!=(const value& other) const noexcept {
    return !(*this == other);
  }

  const analysis::factory_f factory;
  const analysis::normalizer_f normalizer;
};

}  // namespace
namespace std {

template<>
struct hash<::key> {
  size_t operator()(const ::key& value) const noexcept {
    return irs::hash_combine(
        std::hash<irs::type_info::type_id>()(value.args_format.id()),
        value.type);
  }
};

}  // namespace std
namespace {

constexpr std::string_view kFileNamePrefix = "libanalyzer-";

class analyzer_register
    : public irs::tagged_generic_register<::key, ::value, irs::string_ref,
                                          analyzer_register> {
 protected:
  std::string key_to_filename(const key_type& key) const override {
    const auto& name = key.type;
    std::string filename(kFileNamePrefix.size() + name.size(), 0);

    std::memcpy(&filename[0], kFileNamePrefix.data(), kFileNamePrefix.size());

    irs::string_ref::traits_type::copy(&filename[0] + kFileNamePrefix.size(),
                                       name.c_str(), name.size());

    return filename;
  }
};

}  // namespace

namespace iresearch::analysis {
namespace analyzers {
bool exists(string_ref name, const type_info& args_format,
            bool load_library /*= true*/) {
  return !analyzer_register::instance()
              .get(::key(name, args_format), load_library)
              .empty();
}

bool normalize(std::string& out, string_ref name, const type_info& args_format,
               string_ref args, bool load_library /*= true*/) noexcept {
  try {
    auto* normalizer = analyzer_register::instance()
                           .get(::key(name, args_format), load_library)
                           .normalizer;

    return normalizer ? normalizer(args, out) : false;
  } catch (...) {
    IR_FRMT_ERROR("Caught exception while normalizing analyzer '%s' arguments",
                  static_cast<std::string>(name).c_str());
  }

  return false;
}

result get(analyzer::ptr& analyzer, string_ref name,
           const type_info& args_format, string_ref args,
           bool load_library /*= true*/) noexcept {
  try {
    auto* factory = analyzer_register::instance()
                        .get(::key(name, args_format), load_library)
                        .factory;

    if (!factory) {
      return result::make<result::NOT_FOUND>();
    }

    analyzer = factory(args);
  } catch (const std::exception& e) {
    return result::make<result::INVALID_ARGUMENT>(
        "Caught exception while getting an analyzer instance", e.what());
  } catch (...) {
    return result::make<result::INVALID_ARGUMENT>(
        "Caught exception while getting an analyzer instance");
  }

  return {};
}

analyzer::ptr get(string_ref name, const type_info& args_format,
                  string_ref args, bool load_library /*= true*/) noexcept {
  try {
    auto* factory = analyzer_register::instance()
                        .get(::key(name, args_format), load_library)
                        .factory;

    return factory ? factory(args) : nullptr;
  } catch (...) {
    IR_FRMT_ERROR("Caught exception while getting an analyzer instance");
  }

  return nullptr;
}

void init() {
  irs::analysis::collation_token_stream::init();
  irs::analysis::delimited_token_stream::init();
  irs::analysis::ngram_token_stream_base::init();
  irs::analysis::normalizing_token_stream::init();
  irs::analysis::pipeline_token_stream::init();
  irs::analysis::segmentation_token_stream::init();
  irs::analysis::stemming_token_stream::init();
  irs::analysis::text_token_stream::init();
  irs::analysis::token_stopwords_stream::init();
}

void init_with_model() {
  irs::analysis::classification_stream::init();
  irs::analysis::nearest_neighbors_stream::init();
}

void load_all(std::string_view path) {
  load_libraries(path, kFileNamePrefix, "");
}

bool visit(const std::function<bool(string_ref, const type_info&)>& visitor) {
  analyzer_register::visitor_t wrapper = [&visitor](const ::key& key) -> bool {
    return visitor(key.type, key.args_format);
  };

  return analyzer_register::instance().visit(wrapper);
}

}  // namespace analyzers

analyzer_registrar::analyzer_registrar(const type_info& type,
                                       const type_info& args_format,
                                       factory_f factory,
                                       normalizer_f normalizer,
                                       const char* source /*= nullptr*/) {
  const string_ref source_ref(source);
  const auto new_entry = ::value(factory, normalizer);
  auto entry = analyzer_register::instance().set(
      ::key(type.name(), args_format), new_entry,
      source_ref.null() ? nullptr : &source_ref);

  registered_ = entry.second;

  if (!registered_ && new_entry != entry.first) {
    auto* registered_source =
        analyzer_register::instance().tag(::key(type.name(), args_format));

    if (source && registered_source) {
      IR_FRMT_WARN(
          "type name collision detected while registering analyzer, ignoring: "
          "type '%s' from %s, previously from %s",
          type.name().c_str(), source, registered_source->c_str());
    } else if (source) {
      IR_FRMT_WARN(
          "type name collision detected while registering analyzer, ignoring: "
          "type '%s' from %s",
          type.name().c_str(), source);
    } else if (registered_source) {
      IR_FRMT_WARN(
          "type name collision detected while registering analyzer, ignoring: "
          "type '%s', previously from %s",
          type.name().c_str(), registered_source->c_str());
    } else {
      IR_FRMT_WARN(
          "type name collision detected while registering analyzer, ignoring: "
          "type '%s'",
          type.name().c_str());
    }
  }
}

}  // namespace iresearch::analysis
