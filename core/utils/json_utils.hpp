////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_JSON_UTILS_H
#define IRESEARCH_JSON_UTILS_H

#include <rapidjson/rapidjson/document.h>
#include "string.hpp"

NS_ROOT

bool get_uint64(rapidjson::Document const& json,
     const irs::string_ref& name,
     uint64_t& value);

bool get_bool(
    rapidjson::Document const& json,
    const irs::string_ref& name,
    bool& value
);

NS_END

#endif // IRESEARCH_JSON_UTILS_H
