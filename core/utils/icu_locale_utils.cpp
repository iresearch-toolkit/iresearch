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
/// @author Alexey Bakharew
////////////////////////////////////////////////////////////////////////////////
#include "icu_locale_utils.hpp"

#include <absl/container/flat_hash_set.h>

#include "utils/string_utils.hpp"
#include "utils/vpack_utils.hpp"
#include "utils/locale_utils.hpp"
#include "utils/log.hpp"

namespace iresearch {
namespace icu_locale_utils {

constexpr VPackStringRef LOCALE_PARAM_NAME   {"locale"};
constexpr VPackStringRef LANGUAGE_PARAM_NAME {"language"};
constexpr VPackStringRef COUNTRY_PARAM_NAME  {"country"};
constexpr VPackStringRef VARIANT_PARAM_NAME  {"variant"};
constexpr VPackStringRef ENCODING_PARAM_NAME {"encoding"};

bool get_locale_from_vpack(const VPackSlice locale_slice, icu::Locale& locale) {

  if (!locale_slice.isObject()) {
    return false;
  }

  string_ref language;
  string_ref country;
  string_ref variant;

  auto lang_slice = locale_slice.get(LANGUAGE_PARAM_NAME);
  if (!lang_slice.isString()) {
    IR_FRMT_ERROR(
      "Language parameter '%s' is not specified or it is not a string",
      LANGUAGE_PARAM_NAME.data());
    return false;
  }
  language = get_string<string_ref>(lang_slice);

  if (locale_slice.hasKey(COUNTRY_PARAM_NAME)) {
    auto country_slice = locale_slice.get(COUNTRY_PARAM_NAME);
    if (!country_slice.isString()) {
      IR_FRMT_ERROR(
        "'%s' parameter name should be string", COUNTRY_PARAM_NAME.data());
      return false;
    }
    country = get_string<string_ref>(country_slice);
  }

  if (locale_slice.hasKey(VARIANT_PARAM_NAME)) {
    auto variant_slice = locale_slice.get(VARIANT_PARAM_NAME);
    if (!variant_slice.isString()) {
      IR_FRMT_ERROR(
        "'%s' parameter name should be string", VARIANT_PARAM_NAME.data());
      return false;
    }
    variant = get_string<string_ref>(variant_slice);
  }

  std::string locale_name(language.c_str(), language.size());

  if (country.size() > 0) {
    locale_name.append(1, '_').append(country.c_str(), country.size());
  }
  if (variant.size() > 0) {
    locale_name.append(variant.c_str(), variant.size());
  }

  locale = icu::Locale::createFromName(locale_name.c_str());

  return true;
}

bool locale_to_vpack(const icu::Locale& locale, VPackBuilder* const builder) {
  {
    VPackObjectBuilder object(builder);

    const auto language = locale.getLanguage();
    builder->add(LANGUAGE_PARAM_NAME, VPackValue(language));

    const auto country = locale.getCountry();
    if (strlen(country)) {
      builder->add(COUNTRY_PARAM_NAME, VPackValue(country));
    }

    const auto variant = locale.getVariant();
    if (strlen(variant)) {
      builder->add(VARIANT_PARAM_NAME, VPackValue(variant));
    }

    builder->add(ENCODING_PARAM_NAME, VPackValue("utf-8"));
  }

  return true;
}

} // icu_locale_utils
} // iresearch
