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
#include "utils/log.hpp"

namespace  {

using irs::icu_locale_utils::Unicode;

constexpr VPackStringRef LANGUAGE_PARAM_NAME {"language"};
constexpr VPackStringRef COUNTRY_PARAM_NAME  {"country"};
constexpr VPackStringRef VARIANT_PARAM_NAME  {"variant"};
constexpr VPackStringRef ENCODING_PARAM_NAME {"encoding"};

// function that removes all unnecessary chars from encoding name
void normalize_encoding(std::string& encoding) {
  auto* str = encoding.data();

  auto end = std::remove_if(
    str, str + encoding.size(),
    [](char x){ return !(('0' <= x && '9' >= x) || ('a' <= x && 'z' >= x)); });

  encoding.assign(str, std::distance(str, end));
}

constexpr std::string_view to_string(Unicode v) noexcept {
  switch (v) {
    case Unicode::UTF8:
      return "utf8";
    case Unicode::UTF16:
      return "utf16";
    case Unicode::UTF32:
      return "utf32";
  }

  assert(false);
  return "";
}

constexpr Unicode from_string(std::string_view str, Unicode fallback = Unicode::UTF8) noexcept {
  if (to_string(Unicode::UTF8) == str) {
    return Unicode::UTF8;
  }

  if (to_string(Unicode::UTF16) == str) {
    return Unicode::UTF16;
  }

  if (to_string(Unicode::UTF32) == str) {
    return Unicode::UTF32;
  }

  return fallback;
}

}

namespace iresearch {
namespace icu_locale_utils {

bool get_locale_from_vpack(const VPackSlice locale_slice,
                           icu::Locale& locale,
                           Unicode& unicode) {
  if (!locale_slice.isObject()) {
    IR_FRMT_ERROR(
      "Locale value is not specified or it is not an object");
    return false;
  }

  string_ref language;
  string_ref country;
  string_ref variant;
  std::string encoding;

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

  if (locale_slice.hasKey(ENCODING_PARAM_NAME)) { // encoding exists
    auto encoding_slice = locale_slice.get(ENCODING_PARAM_NAME);
    if (!encoding_slice.isString()) {
      IR_FRMT_ERROR(
        "'%s' parameter name should be string", ENCODING_PARAM_NAME.data());
      return false;
    }
    encoding = get_string<string_ref>(encoding_slice);

    // transform encoding to lower case
    std::transform(encoding.begin(), encoding.end(), encoding.begin(), ::tolower);

    // remove unwanted chars from encoding name
    normalize_encoding(encoding);
  }

  std::string locale_name(language.c_str(), language.size());

  if (country.size() > 0) {
    locale_name.append(1, '_').append(country.c_str(), country.size());
  }
  if (variant.size() > 0) {
    locale_name.append(variant.c_str(), variant.size());
  }

  unicode = from_string(encoding);
  locale = icu::Locale::createFromName(locale_name.c_str());

  return !locale.isBogus();
}

bool get_locale_from_str(string_ref locale_str,
                         icu::Locale& locale,
                         bool is_new_format,
                         Unicode& unicode) {
  std::string locale_name;
  std::string encoding;

  const char* at_pos = std::find(locale_str.begin(), locale_str.end(), '@'); // find pos of '@' symbol
  const char* dot_pos = std::find(locale_str.begin(), locale_str.end(), '.'); // find pos of '.' symbol

  // extract locale name
  // new format accept locale string including '@' and following items
  if (is_new_format || at_pos == locale_str.end()) {
    locale_name.assign(locale_str.begin(), locale_str.end());
  } else { // include items only before '@'
    locale_name.assign(locale_str.begin(), at_pos);
  }

  // extract encoding
  if (dot_pos != locale_str.end()) { // encoding is specified
    encoding.clear();
    if (at_pos != locale_str.end() && dot_pos < at_pos) { // '.' should be before '@'
      encoding.assign(dot_pos + 1, at_pos); // encoding is located between '.' and '@' symbols
    } else {
      encoding.assign(dot_pos + 1, locale_str.end()); // encoding is located between '.' and end of string
    }

    // trasnform encoding to lower case
    std::transform(encoding.begin(), encoding.end(), encoding.begin(), ::tolower);

    normalize_encoding(encoding);
  }

  unicode = from_string(encoding);
  locale = icu::Locale::createFromName(locale_name.c_str());

  return !locale.isBogus();
}

bool get_locale_from_vpack(VPackSlice slice,
                           icu::Locale& locale,
                           bool is_new_format,
                           Unicode& unicode) {
  switch (slice.type()) {
    case VPackValueType::String:
      return icu_locale_utils::get_locale_from_str(
        get_string<string_ref>(slice), locale, is_new_format, unicode);
    case VPackValueType::Object:
      return icu_locale_utils::get_locale_from_vpack(slice, locale, unicode);
    default:
      return false;
  }
}

void locale_to_vpack(const icu::Locale& locale,
                     VPackBuilder* const builder,
                     const Unicode* unicode) {
  VPackObjectBuilder object(builder);
  {
    const auto language = locale.getLanguage();
    builder->add(LANGUAGE_PARAM_NAME, VPackValue(language));

    const auto country = locale.getCountry();
    if (*country) {
      builder->add(COUNTRY_PARAM_NAME, VPackValue(country));
    }

    const auto variant = locale.getVariant();
    if (*variant) {
      builder->add(VARIANT_PARAM_NAME, VPackValue(variant));
    }

    if (unicode) {
      builder->add(ENCODING_PARAM_NAME, VPackValue(to_string(*unicode)));
    }
  }
}

bool to_unicode(Unicode unicode, string_ref data, icu::UnicodeString& out) {
  switch(unicode) {
    case Unicode::UTF8: {
      constexpr size_t MAX = std::numeric_limits<int32_t>::max();

      if (data.size() > MAX) {
        return false;
      }

      out = icu::UnicodeString::fromUTF8(
        icu::StringPiece(data.c_str(),
        static_cast<int32_t>(data.size())));
      break;
    }

    case Unicode::UTF16: {
      constexpr size_t MAX = std::numeric_limits<int32_t>::max() * sizeof(UChar);

      if (data.size() > MAX) {
        return false;
      }

      // utf-16 is base encoding for icu::UnicodeString
      out = icu::UnicodeString(
        reinterpret_cast<const UChar*>(data.c_str()),
        static_cast<int32_t>(data.size() / sizeof(UChar)));
      break;
    }

    case Unicode::UTF32: {
      constexpr size_t MAX = std::numeric_limits<int32_t>::max() * sizeof(UChar32);

      if (data.size() > MAX) {
        return false;
      }

      out = icu::UnicodeString::fromUTF32(
        reinterpret_cast<const UChar32*>(data.c_str()),
        static_cast<int32_t>(data.size() / sizeof(UChar32)));
      break;
    }
  }

  return true;
}

} // icu_locale_utils
} // iresearch
