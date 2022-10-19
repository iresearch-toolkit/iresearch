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

#include "utils/hash_utils.hpp"
#include "utils/register.hpp"

namespace {

using namespace irs;

struct key {
  key(std::string_view type, const irs::type_info& args_format)
    : type{type}, args_format{args_format} {}

  bool operator==(const key& other) const noexcept {
    return args_format == other.args_format && type == other.type;
  }

  bool operator!=(const key& other) const noexcept { return !(*this == other); }

  std::string_view type;
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
      std::hash<irs::type_info::type_id>()(value.args_format.id()), value.type);
  }
};

}  // namespace std

namespace {

constexpr std::string_view kFileNamePrefix{"libanalyzer-"};

class analyzer_register final
  : public irs::tagged_generic_register<::key, ::value, std::string_view,
                                        analyzer_register> {
 protected:
  virtual std::string key_to_filename(const key_type& key) const override {
    const auto& name = key.type;
    std::string filename(kFileNamePrefix.size() + name.size(), 0);

    std::memcpy(filename.data(), kFileNamePrefix.data(),
                kFileNamePrefix.size());

    std::memcpy(filename.data() + kFileNamePrefix.size(), name.data(),
                name.size());

    return filename;
  }
};

}  // namespace

namespace iresearch::analysis {

analyzer_registrar::analyzer_registrar(
  const type_info& type, const type_info& args_format,
  analyzer::ptr (*factory)(std::string_view args),
  bool (*normalizer)(std::string_view args, std::string& config),
  const char* source /*= nullptr*/) {
  const auto source_ref =
    source ? std::string_view{source} : std::string_view{};
  const auto new_entry = ::value(factory, normalizer);
  auto entry = analyzer_register::instance().set(
    ::key(type.name(), args_format), new_entry,
    IsNull(source_ref) ? nullptr : &source_ref);

  registered_ = entry.second;

  if (!registered_ && new_entry != entry.first) {
    auto* registered_source =
      analyzer_register::instance().tag(::key(type.name(), args_format));

    if (source && registered_source) {
      IR_FRMT_WARN(
        "type name collision detected while registering analyzer, ignoring: "
        "type '%s' from %s, previously from %s",
        type.name().data(), source, registered_source->data());
    } else if (source) {
      IR_FRMT_WARN(
        "type name collision detected while registering analyzer, ignoring: "
        "type '%s' from %s",
        type.name().data(), source);
    } else if (registered_source) {
      IR_FRMT_WARN(
        "type name collision detected while registering analyzer, ignoring: "
        "type '%s', previously from %s",
        type.name().data(), registered_source->data());
    } else {
      IR_FRMT_WARN(
        "type name collision detected while registering analyzer, ignoring: "
        "type '%s'",
        type.name().data());
    }
  }
}

namespace analyzers {

bool exists(std::string_view name, const type_info& args_format,
            bool load_library /*= true*/) {
  return !analyzer_register::instance()
            .get(::key(name, args_format), load_library)
            .empty();
}

bool normalize(std::string& out, std::string_view name,
               const type_info& args_format, std::string_view args,
               bool load_library /*= true*/) noexcept {
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

result get(analyzer::ptr& analyzer, std::string_view name,
           const type_info& args_format, std::string_view args,
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

analyzer::ptr get(std::string_view name, const type_info& args_format,
                  std::string_view args,
                  bool load_library /*= true*/) noexcept {
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

void load_all(std::string_view path) {
  load_libraries(path, kFileNamePrefix, "");
}

bool visit(
  const std::function<bool(std::string_view, const type_info&)>& visitor) {
  analyzer_register::visitor_t wrapper = [&visitor](const ::key& key) -> bool {
    return visitor(key.type, key.args_format);
  };

  return analyzer_register::instance().visit(wrapper);
}

}  // namespace analyzers
}  // namespace iresearch::analysis
