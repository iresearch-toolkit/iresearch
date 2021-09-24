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

#include "string.hpp"
#include "velocypack/Slice.h"
#include "velocypack/Builder.h"
#include "velocypack/velocypack-aliases.h"
#include "utils/locale_utils.hpp"

namespace  {
  using sting16_t = std::basic_string<UChar>;
}

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
bool convert_to_utf16(string_ref orig_encoding,
                      const From& from, // other encoding
                      To& to, // utf16
                      locale_utils::converter_pool* cvt = nullptr) {

  if (!cvt) {
    cvt = &locale_utils::get_converter(orig_encoding.c_str());
  }

  to.resize(from.size());

  UErrorCode err_code = UErrorCode::U_ZERO_ERROR;
  ucnv_toUChars(cvt->get().get(), (UChar*)to.data(), to.size(), (char*)from.c_str(), from.size(), &err_code);

  if (!U_SUCCESS(err_code)) {
    return false;
  }

  return true;
}

template <typename From, typename To>
bool convert_from_utf16(string_ref orig_encoding,
                        const From& from, // utf16
                        To& to, // another encoding
                        locale_utils::converter_pool* cvt = nullptr) {
  if (!cvt) {
    cvt = &locale_utils::get_converter(orig_encoding.c_str());
  }

  // may be assert that 'from' str is utf-16?
UCNV_GET_MAX_BYTES_FOR_STRING

  size_t cu_size = sizeof(*to.c_str()); // size of code unit
  if (cu_size == 1) {
    cu_size *= 2; // from 2bytes to 1bytes. Require more space
  }

  to.resize(from.size() * cu_size);

  UErrorCode err_code = UErrorCode::U_ZERO_ERROR;
  ucnv_fromUChars(cvt->get().get(), (char*)to.data(), to.size(),(UChar*)from.c_str(), from.size(), &err_code);

  return U_SUCCESS(err_code);
}

template <typename From>
bool create_unicode_string(string_ref orig_encoding,
                           const From& from,
                           icu::UnicodeString& unicode_str,
                           locale_utils::converter_pool* cvt = nullptr) {

  sting16_t to_str;
  bool res = convert_to_utf16(orig_encoding,
                              from,
                              to_str,
                              cvt);

  unicode_str = icu::UnicodeString(to_str.c_str(), to_str.size(), to_str.capacity());

  return res;
}

} // icu_locale_utils
} // iresearch


#endif // ICU_LOCALE_UTILS_HPP
