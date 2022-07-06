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

#ifndef IRESEARCH_ANALYZERS_H
#define IRESEARCH_ANALYZERS_H

#include <functional>

#include "shared.hpp"
#include "analyzer.hpp"
#include "utils/text_format.hpp"
#include "utils/result.hpp"

namespace iresearch {
namespace analysis {

// -----------------------------------------------------------------------------
// --SECTION--                                             analyzer registration
// -----------------------------------------------------------------------------

typedef analysis::analyzer::ptr(*factory_f)(string_ref args);
typedef bool(*normalizer_f)(string_ref args, std::string& config);

class analyzer_registrar {
 public:
   analyzer_registrar(
    const type_info& type,
    const type_info& args_format,
    factory_f factory,
    normalizer_f normalizer,
    const char* source = nullptr);

  operator bool() const noexcept {
    return registered_;
  }

 private:
  bool registered_;
};

#define REGISTER_ANALYZER__(analyzer_name, args_format, factory, normalizer, line, source) \
    static ::iresearch::analysis::analyzer_registrar analyzer_registrar ## _ ## line(::iresearch::type<analyzer_name>::get(), ::iresearch::type<args_format>::get(), &factory, &normalizer, source)
#define REGISTER_ANALYZER_EXPANDER__(analyzer_name, args_format, factory, normalizer, file, line) REGISTER_ANALYZER__(analyzer_name, args_format, factory, normalizer, line, file ":" TOSTRING(line))
#define REGISTER_ANALYZER(analyzer_name, args_format, factory, normalizer) REGISTER_ANALYZER_EXPANDER__(analyzer_name, args_format, factory, normalizer, __FILE__, __LINE__)
#define REGISTER_ANALYZER_JSON(analyzer_name, factory, normalizer) REGISTER_ANALYZER(analyzer_name, ::iresearch::text_format::json, factory, normalizer)
#define REGISTER_ANALYZER_TEXT(analyzer_name, factory, normalizer) REGISTER_ANALYZER(analyzer_name, ::iresearch::text_format::text, factory, normalizer)
#define REGISTER_ANALYZER_TYPED(analyzer_name, args_format) REGISTER_ANALYZER(analyzer_name, args_format, analyzer_name::make)
#define REGISTER_ANALYZER_VPACK(analyzer_name, factory, normalizer) REGISTER_ANALYZER(analyzer_name, ::iresearch::text_format::vpack, factory, normalizer)

// -----------------------------------------------------------------------------
// --SECTION--                                               convinience methods
// -----------------------------------------------------------------------------

class analyzers {
 public:
  ////////////////////////////////////////////////////////////////////////////////
  /// @brief checks whether an analyzer with the specified name is registered
  ////////////////////////////////////////////////////////////////////////////////
  static bool exists(
    string_ref name,
    const type_info& args_format,
    bool load_library = true);

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief normalized arguments for an analyzer specified by name and store them
  ///        in 'out' argument
  /// @returns true on success, false - otherwise
  ////////////////////////////////////////////////////////////////////////////////
  static bool normalize(
    std::string& out,
    string_ref name,
    const type_info& args_format,
    string_ref args,
    bool load_library = true) noexcept;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief find an analyzer by name, or nullptr if not found
  ///        indirect call to <class>::make(...)
  ///        NOTE: make(...) MUST be defined in CPP to ensire proper code scope
  ////////////////////////////////////////////////////////////////////////////////
  static result get(
    analyzer::ptr& analyzer,
    string_ref name,
    const type_info& args_format,
    string_ref args,
    bool load_library = true) noexcept;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief find an analyzer by name, or nullptr if not found
  ///        indirect call to <class>::make(...)
  ///        NOTE: make(...) MUST be defined in CPP to ensire proper code scope
  ////////////////////////////////////////////////////////////////////////////////
  static analyzer::ptr get(
    string_ref name,
    const type_info& args_format,
    string_ref args,
    bool load_library = true) noexcept;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief load all analyzers from plugins directory
  ////////////////////////////////////////////////////////////////////////////////
  static void load_all(std::string_view path);

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief visit all loaded analyzers, terminate early if visitor returns false
  ////////////////////////////////////////////////////////////////////////////////
  static bool visit(
    const std::function<bool(string_ref, const type_info&)>& visitor);

 private:
  analyzers() = delete;
};

} // namespace analysis {
}

#endif
