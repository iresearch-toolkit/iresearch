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

#ifndef ICU_LOCALE_UTILS_HPP
#define ICU_LOCALE_UTILS_HPP

#include <unicode/locid.h>
#include <unicode/ucnv.h> // for UConverter
#include <unicode/ustring.h> // for u_strToUTF32, u_strToUTF8

#include "string.hpp"
#include "velocypack/Slice.h"
#include "velocypack/Builder.h"
#include "velocypack/velocypack-aliases.h"
#include "utils/locale_utils.hpp"

namespace iresearch {
namespace icu_locale_utils {

enum class Unicode { UTF8, NON_UTF8 };

bool get_locale_from_vpack(const VPackSlice slice,
                           icu::Locale& locale,
                           Unicode* unicode = nullptr);

bool get_locale_from_str(irs::string_ref locale_str,
                         icu::Locale& locale,
                         bool is_new_format,
                         Unicode* unicode = nullptr,
                         std::string* encoding = nullptr);

bool locale_to_vpack(const icu::Locale& locale,
                     VPackBuilder* const builder,
                     const Unicode* unicode = nullptr);

template <typename From, typename To>
bool convert_to_utf16(string_ref from_encoding,
                      const From& from, // other encoding
                      To& to, // utf16
                      locale_utils::converter_pool** cvt = nullptr) {

  if (from_encoding == "utf16") { // attempt to convert from utf16 to utf16
    to.assign((UChar*)from.c_str(), from.size());
    return true;
  }

  UErrorCode err_code = UErrorCode::U_ZERO_ERROR;
  auto new_size = from.size() * sizeof(*from.c_str());
  to.resize(new_size);

  //// there is a special case for utf32 encoding, because 'ucnv_toUChars'
  //// working uncorrectly with such data. Same implementation is currently
  //// in 'locale_utils.cpp' file
  if (from_encoding == "utf32") {
    int32_t dest_length;
    u_strFromUTF32(to.data(),
                   to.capacity(),
                   &dest_length,
                   (const UChar32*)from.c_str(),
                   from.size(), &err_code);

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
    curr_cvt = &locale_utils::get_converter(std::string(from_encoding.c_str(), from_encoding.size()).c_str());
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
                                     (const char*)from.c_str(),
                                     from.size(),
                                     &err_code);

  if (!U_SUCCESS(err_code)) {
    return false;
  }
  // resize to the actual size
  to.resize(actual_size);

  return true;
}

template <typename From, typename To>
bool convert_from_utf16(string_ref from_encoding,
                        const From& from, // utf16
                        To& to, // another encoding
                        locale_utils::converter_pool** cvt = nullptr) {


  UErrorCode err_code = UErrorCode::U_ZERO_ERROR;

  //// there is a special case for utf32 encoding, because 'ucnv_fromUChars'
  //// working uncorrectly with such data. Same implementation is currently
  //// in 'locale_utils.cpp' file
  if (from_encoding == "utf32") {
    auto new_size = from.size() * 2;
    to.resize(new_size);

    int32_t dest_length; // length of actual written symbols to 'to' str
    u_strToUTF32((UChar32*)to.data(),
                 to.capacity(),
                 &dest_length,
                 (const UChar*)from.c_str(),
                 from.size(), &err_code);

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
    curr_cvt = &locale_utils::get_converter(std::string(from_encoding.c_str(), from_encoding.size()).c_str());
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

  auto new_size = UCNV_GET_MAX_BYTES_FOR_STRING(from.size(), ucnv_getMaxCharSize((*cvt)->get().get()));
  to.resize(new_size);
  auto actual_size = ucnv_fromUChars((*cvt)->get().get(),
                                     (char*)to.data(),
                                     new_size,
                                     (UChar*)from.c_str(),
                                     from.size(),
                                     &err_code);

  if (!U_SUCCESS(err_code)) {
    return false;
  }
  // resize to the actual size
  to.resize(actual_size);

  return true;
}

template <typename From>
bool create_unicode_string(string_ref from_encoding,
                           const From& from,
                           icu::UnicodeString& unicode_str,
                           locale_utils::converter_pool** cvt = nullptr) {


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


#endif // ICU_LOCALE_UTILS_HPP
