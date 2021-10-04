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

namespace iresearch {
namespace icu_locale_utils {

constexpr VPackStringRef LANGUAGE_PARAM_NAME {"language"};
constexpr VPackStringRef COUNTRY_PARAM_NAME  {"country"};
constexpr VPackStringRef VARIANT_PARAM_NAME  {"variant"};
constexpr VPackStringRef ENCODING_PARAM_NAME {"encoding"};

bool get_locale_from_vpack(const VPackSlice locale_slice,
                           icu::Locale& locale,
                           Unicode* unicode) {

  if (!locale_slice.isObject()) {
    IR_FRMT_ERROR(
      "Locale value is not specified or it is not an object");
    return false;
  }

  string_ref language;
  string_ref country;
  string_ref variant;
  std::string encoding = "ascii"; // default encoding

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
    std::transform(encoding.begin(), encoding.end(), encoding.begin(),
        [](unsigned char c){ return std::tolower(c); });

    if (encoding != "utf-8" && 
        encoding != "utf8") {
      IR_FRMT_ERROR(
        "Unsupported encoding parameter '%s'", encoding.c_str());
      return false;
    }
  }

  if (unicode) {
    if (encoding == "utf-8" ||
        encoding == "utf8") {
      *unicode = Unicode::UTF8;
    } else {
      *unicode = Unicode::NON_UTF8;
    }
  }

  std::string locale_name(language.c_str(), language.size());

  if (country.size() > 0) {
    locale_name.append(1, '_').append(country.c_str(), country.size());
  }
  if (variant.size() > 0) {
    locale_name.append(variant.c_str(), variant.size());
  }

  locale = icu::Locale::createFromName(locale_name.c_str());

  return !locale.isBogus();
}

bool get_locale_from_str(string_ref locale_str,
                         icu::Locale& locale,
                         bool is_new_format,
                         Unicode* unicode,
                         std::string* encoding) {

  std::string locale_name;
  std::string encoding_name = "ascii"; // default encoding
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
    encoding_name.clear();
    if (at_pos != locale_str.end() && dot_pos < at_pos) { // '.' should be before '@'
      encoding_name.assign(dot_pos + 1, at_pos); // encoding is located between '.' and '@' symbols
    } else {
      encoding_name.assign(dot_pos + 1, locale_str.end()); // encoding is located between '.' and end of string
    }

    // trasnform encoding to lower case
    std::transform(encoding_name.begin(), encoding_name.end(), encoding_name.begin(),
        [](unsigned char c){ return std::tolower(c); });
  }

  // set 'encoding' value
  if (encoding) {
    // assume that 'encoding_name' has enough memory for copying name of locale
    *encoding = encoding_name;
  }

  // set 'unicode' value
  if (unicode) {
    if (encoding_name == "utf-8" ||
        encoding_name == "utf8") {
      *unicode = Unicode::UTF8;
    } else {
      *unicode = Unicode::NON_UTF8;
    }
  }

  locale = icu::Locale::createFromName(locale_name.c_str());
  return !locale.isBogus();
}

bool locale_to_vpack(const icu::Locale& locale,
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

    if (unicode && *unicode == Unicode::UTF8) {
      builder->add(ENCODING_PARAM_NAME, VPackValue("utf-8"));
    }
  }

  return true;
}

bool convert_to_utf16(string_ref from_encoding,
                      const string_ref& from, // other encoding
                      std::basic_string<char16_t>& to, // utf16
                      locale_utils::converter_pool** cvt) {

  if (from_encoding == "utf16") { // attempt to convert from utf16 to utf16
    to.assign(reinterpret_cast<const char16_t*>(from.c_str()), from.size() / 2);
    return true;
  }

  UErrorCode err_code = UErrorCode::U_ZERO_ERROR;
  auto new_size = from.size() * 2;
  to.resize(new_size);

  //// there is a special case for utf32 encoding, because 'ucnv_toUChars'
  //// working uncorrectly with such data. Same implementation is currently
  //// in 'locale_utils.cpp' file
  if (from_encoding == "utf32") {
    int32_t dest_length;
    u_strFromUTF32(to.data(),
                   to.capacity(),
                   &dest_length,
                   reinterpret_cast<const UChar32*>(from.c_str()),
                   from.size() / 4, &err_code);

    if (!U_SUCCESS(err_code)) {
      return false;
    }

    // resize to the actual size
    to.resize(dest_length);
    return true;
  }

  irs::locale_utils::converter_pool* curr_cvt = nullptr;

  if ((cvt && !*cvt) || !cvt) {
    // try to get converter for specified locale
    curr_cvt = &locale_utils::get_converter(from_encoding.c_str());

    if (!curr_cvt) {
      // no such converter
      return false;
    }

    if (cvt && !*cvt) {
      *cvt = curr_cvt;
    } else {
      cvt = &curr_cvt;
    }
  }

  size_t actual_size = ucnv_toUChars((*cvt)->get().get(),
                                     to.data(),
                                     new_size,
                                     from.c_str(),
                                     from.size(),
                                     &err_code);

  if (!U_SUCCESS(err_code)) {
    return false;
  }
  // resize to the actual size
  to.resize(actual_size);

  return true;
}

bool create_unicode_string(string_ref from_encoding,
                           const string_ref& from,
                           icu::UnicodeString& unicode_str,
                           locale_utils::converter_pool** cvt) {


  std::u16string to_str;
  bool res = convert_to_utf16(from_encoding,
                              from,
                              to_str,
                              cvt);

  unicode_str = icu::UnicodeString(to_str.data(), to_str.size());

  return res;
}

} // icu_locale_utils
} // iresearch
