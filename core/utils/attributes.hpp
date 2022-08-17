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

#ifndef IRESEARCH_ATTRIBUTES_H
#define IRESEARCH_ATTRIBUTES_H

#include "shared.hpp"
#include "type_id.hpp"

namespace iresearch {

struct attributes {
  static bool exists(string_ref name, bool load_library = true);

  static type_info get(string_ref name, bool load_library = true) noexcept;

  attributes() = delete;
};

class attribute_registrar {
 public:
  explicit attribute_registrar(const type_info& type,
                               const char* source = nullptr);
  operator bool() const noexcept;

 private:
  bool registered_;
};

#define REGISTER_ATTRIBUTE__(attribute_name, line, source)              \
  static ::iresearch::attribute_registrar attribute_registrar##_##line( \
    ::iresearch::type<attribute_name>::get(), source)
#define REGISTER_ATTRIBUTE_EXPANDER__(attribute_name, file, line) \
  REGISTER_ATTRIBUTE__(attribute_name, line, file ":" TOSTRING(line))
#define REGISTER_ATTRIBUTE(attribute_name) \
  REGISTER_ATTRIBUTE_EXPANDER__(attribute_name, __FILE__, __LINE__)

}  // namespace iresearch

#endif  // IRESEARCH_ATTRIBUTES_H
