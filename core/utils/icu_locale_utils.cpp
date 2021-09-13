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
#include "icu_locale_utils.hpp"
#include "utils/string_utils.hpp"
#include "utils/vpack_utils.hpp"
#include "utils/locale_utils.hpp"
#include "utils/log.hpp"

namespace iresearch {
namespace icu_locale_utils {

void LocaleChecker::init() {

  int32_t count;
  const icu::Locale* locales = icu::Locale::getAvailableLocales(count);

  for (int i = 0; i < count; ++i) {
    locales_.insert(locales[i].getName());
  }

  is_init_ = true;
}

bool LocaleChecker::is_locale_correct(const std::string &locale_name) {
  if (!is_init_) {
    init();
  }
  return (locales_.find(locale_name) != locales_.end());
}

std::shared_ptr<LocaleChecker> locale_checker = std::make_shared<LocaleChecker>();

bool get_locale_from_vpack(const VPackSlice slice, icu::Locale& locale) {

  if (!slice.isObject()) {
    return false;
  }

  string_ref language;
  string_ref country;
  string_ref variant;

  auto lang_slice = slice.get(LANGUAGE_PARAM_NAME);
  if (lang_slice.isNone() || !lang_slice.isString()) {
    IR_FRMT_WARN(
      "Language parameter is not specified or it is not a string",
      LANGUAGE_PARAM_NAME.data());
    return false;
  }
  language = get_string<string_ref>(lang_slice);

  if (slice.hasKey(COUNTRY_PARAM_NAME)) {
    auto country_slice = slice.get(COUNTRY_PARAM_NAME);
    if (!country_slice.isString()) {
      return false;
    }
    country = get_string<string_ref>(country_slice);
  }

  if (slice.hasKey(VARIANT_PARAM_NAME)) {
    auto variant_slice = slice.get(VARIANT_PARAM_NAME);
    if (!variant_slice.isString()) {
      return false;
    }
    variant = get_string<string_ref>(variant_slice);
  }

  std::string locale_name(language.c_str(), language.size());

  if (country.size() > 0) {
    locale_name.append(1, '_').append(country.c_str(), country.size());
  }
  if (variant.size() > 0) {
    locale_name.append(1, '_').append(variant.c_str(), variant.size());
  }

  locale = icu::Locale::createFromName(locale_name.c_str());

  return true;
}

bool verify_icu_locale(const icu::Locale& locale) {

  // check locale by a standard way
  if (locale.isBogus()) {
    return false;
  }

  auto language = std::string(locale.getLanguage());
  auto country = std::string(locale.getCountry());

  std::string name_to_check = language;
  if (!country.empty()) {
    name_to_check.append(1, '_').append(country);
  }

  return locale_checker->is_locale_correct(name_to_check);
}

} // icu_locale_utils
} // iresearch
