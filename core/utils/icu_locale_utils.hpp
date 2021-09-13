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
/// @author Alexey Bakharew
////////////////////////////////////////////////////////////////////////////////
///
#ifndef ICU_LOCALE_UTILS_HPP
#define ICU_LOCALE_UTILS_HPP

#include <set>
#include <unicode/locid.h>
#include "string.hpp"
#include "velocypack/Slice.h"
#include "velocypack/velocypack-aliases.h"

namespace iresearch {
namespace icu_locale_utils {

class LocaleChecker {
public:
  LocaleChecker() : is_init_(false) {}
  void init();
  bool is_locale_correct(const std::string& locale_name);
private:
  bool is_init_;
  std::set<std::string> locales_;
};

extern std::shared_ptr<LocaleChecker> locale_checker;

enum class unicode_t { NONE, UTF7, UTF8, UTF16, UTF32 };

constexpr VPackStringRef LANGUAGE_PARAM_NAME {"language"};
constexpr VPackStringRef COUNTRY_PARAM_NAME  {"country"};
constexpr VPackStringRef VARIANT_PARAM_NAME  {"variant"};
constexpr VPackStringRef ENCODING_PARAM_NAME {"encoding"};

bool get_locale_from_vpack(const VPackSlice slice, icu::Locale& locale);

bool verify_icu_locale(const icu::Locale& locale);

} // icu_locale_utils
} // iresearch


#endif // ICU_LOCALE_UTILS_HPP
