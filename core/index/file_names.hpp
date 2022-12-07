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

#ifndef IRESEARCH_FILE_NAMES_H
#define IRESEARCH_FILE_NAMES_H

#include <memory>

#include "utils/string.hpp"

namespace iresearch {

// returns string in the following format : _{gen}
std::string file_name(uint64_t gen);

// returns string in the following format : {prefix}{gen}
std::string file_name(std::string_view prefix, uint64_t gen);

// returns string in the following format : {name}.{ext}
std::string file_name(std::string_view name, std::string_view ext);
void file_name(std::string& out, std::string_view name, std::string_view ext);

// returns string in the following format : {name}.{gen}.{ext}
std::string file_name(std::string_view name, uint64_t gen,
                      std::string_view ext);

}  // namespace iresearch

#endif
