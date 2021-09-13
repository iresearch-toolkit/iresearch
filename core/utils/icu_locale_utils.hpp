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

#include <unicode/locid.h>
#include "string.hpp"
#include "velocypack/Slice.h"
#include "velocypack/velocypack-aliases.h"

namespace iresearch {
namespace icu_locale_utils {

constexpr VPackStringRef LANGUAGE_PARAM_NAME {"language"};
constexpr VPackStringRef COUNTRY_PARAM_NAME  {"country"};
constexpr VPackStringRef VARIANT_PARAM_NAME  {"variant"};

std::string get_locale_name(const string_ref language,
                            const string_ref country = string_ref::EMPTY,
                            const string_ref encodingOverride = string_ref::EMPTY,
                            const string_ref variant = string_ref::EMPTY);

bool get_locale_from_vpack(const VPackSlice slice, icu::Locale& locale);

bool verify_icu_locale(const icu::Locale& locale);

} // icu_locale_utils
} // iresearch


#endif // ICU_LOCALE_UTILS_HPP
