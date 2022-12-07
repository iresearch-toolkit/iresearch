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

#include "file_names.hpp"

#include "absl/strings/internal/resize_uninitialized.h"
#include "absl/strings/str_cat.h"
#include "shared.hpp"

namespace irs {
namespace {
template<typename T>
char* Append(char* out, const T& x) noexcept {
  assert(x.size() != 0);
  char* after = out + x.size();
  std::memcpy(out, x.data(), x.size());
  return after;
}
}  // namespace

std::string file_name(std::string_view prefix, uint64_t gen) {
  const absl::AlphaNum gen_str{gen};

  std::string result;
  absl::strings_internal::STLStringResizeUninitialized(
    &result, gen_str.size() + prefix.size());

  char* out = result.data();
  out = Append(out, prefix);
  Append(out, gen_str);

  return result;
}

void file_name(std::string& result, std::string_view name,
               std::string_view ext) {
  absl::strings_internal::STLStringResizeUninitialized(
    &result, 1 + name.size() + ext.size());

  char* out = result.data();
  out = Append(out, name);
  *out++ = '.';
  Append(out, ext);
}

std::string file_name(std::string_view name, uint64_t gen,
                      std::string_view ext) {
  const absl::AlphaNum gen_str{gen};

  std::string result;
  absl::strings_internal::STLStringResizeUninitialized(
    &result, 2 + name.size() + gen_str.size() + ext.size());

  char* out = result.data();
  out = Append(out, name);
  *out++ = '.';
  out = Append(out, gen_str);
  *out++ = '.';
  Append(out, ext);

  return result;
}

}  // namespace irs
