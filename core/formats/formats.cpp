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
////////////////////////////////////////////////////////////////////////////////

#include "formats.hpp"

// list of statically loaded formats via init()
#ifndef IRESEARCH_DLL
#include "formats_10.hpp"
#endif

#include "analysis/token_attributes.hpp"
#include "utils/hash_utils.hpp"
#include "utils/register.hpp"
#include "utils/type_limits.hpp"

namespace {

constexpr std::string_view kFileNamePrefix{"libformat-"};

// first - format name
// second - module name, nullptr => matches format name
typedef std::pair<std::string_view, std::string_view> key_t;

struct equal_to {
  bool operator()(const key_t& lhs, const key_t& rhs) const noexcept {
    return lhs.first == rhs.first;
  }
};

struct hash {
  size_t operator()(const key_t& lhs) const noexcept {
    return irs::hash_utils::Hash(lhs.first);
  }
};

class format_register
  : public irs::tagged_generic_register<key_t, irs::format::ptr (*)(),
                                        std::string_view, format_register, hash,
                                        equal_to> {
 protected:
  virtual std::string key_to_filename(const key_type& key) const override {
    auto const& module = irs::IsNull(key.second) ? key.first : key.second;

    std::string filename(kFileNamePrefix.size() + module.size(), 0);

    std::memcpy(filename.data(), kFileNamePrefix.data(),
                kFileNamePrefix.size());

    std::memcpy(filename.data() + kFileNamePrefix.size(), module.data(),
                module.size());

    return filename;
  }
};  // format_register

}  // namespace

namespace iresearch {

/* static */ void index_meta_writer::complete(index_meta& meta) noexcept {
  meta.last_gen_ = meta.gen_;
}
/* static */ void index_meta_writer::prepare(index_meta& meta) noexcept {
  meta.gen_ = meta.next_generation();
}

/* static */ void index_meta_reader::complete(
  index_meta& meta, uint64_t generation, uint64_t counter,
  index_meta::index_segments_t&& segments, bstring* payload) {
  meta.gen_ = generation;
  meta.last_gen_ = generation;
  meta.seg_counter_ = counter;
  meta.segments_ = std::move(segments);
  if (payload) {
    meta.payload(std::move(*payload));
  } else {
    meta.payload(bytes_view{});
  }
}

/*static*/ bool formats::exists(std::string_view name, bool load_library /*= true*/) {
  auto const key = std::make_pair(name, std::string_view{});
  return nullptr != format_register::instance().get(key, load_library);
}

/*static*/ format::ptr formats::get(std::string_view name,
                                    std::string_view module /*= {} */,
                                    bool load_library /*= true*/) noexcept {
  try {
    auto const key = std::make_pair(name, module);
    auto* factory = format_register::instance().get(key, load_library);

    return factory ? factory() : nullptr;
  } catch (...) {
    IR_FRMT_ERROR("Caught exception while getting a format instance");
  }

  return nullptr;
}

/*static*/ void formats::init() {
#ifndef IRESEARCH_DLL
  irs::version10::init();
#endif
}

/*static*/ void formats::load_all(std::string_view path) {
  load_libraries(path, kFileNamePrefix, "");
}

/*static*/ bool formats::visit(const std::function<bool(std::string_view)>& visitor) {
  auto visit_all = [&visitor](const format_register::key_type& key) {
    if (!visitor(key.first)) {
      return false;
    }
    return true;
  };

  return format_register::instance().visit(visit_all);
}

// -----------------------------------------------------------------------------
// --SECTION--                                               format registration
// -----------------------------------------------------------------------------

format_registrar::format_registrar(const type_info& type, std::string_view module,
                                   format::ptr (*factory)(),
                                   const char* source /*= nullptr*/) {
  std::string_view source_ref(source);

  auto entry = format_register::instance().set(
    std::make_pair(type.name(), module), factory,
    IsNull(source_ref) ? nullptr : &source_ref);

  registered_ = entry.second;

  if (!registered_ && factory != entry.first) {
    const auto key = std::make_pair(type.name(), std::string_view{});
    auto* registered_source = format_register::instance().tag(key);

    if (source && registered_source) {
      IR_FRMT_WARN(
        "type name collision detected while registering format, ignoring: type "
        "'%s' from %s, previously from %s",
        type.name().data(), source, registered_source->data());
    } else if (source) {
      IR_FRMT_WARN(
        "type name collision detected while registering format, ignoring: type "
        "'%s' from %s",
        type.name().data(), source);
    } else if (registered_source) {
      IR_FRMT_WARN(
        "type name collision detected while registering format, ignoring: type "
        "'%s', previously from %s",
        type.name().data(), registered_source->data());
    } else {
      IR_FRMT_WARN(
        "type name collision detected while registering format, ignoring: type "
        "'%s'",
        type.name().data());
    }
  }
}

}  // namespace iresearch
