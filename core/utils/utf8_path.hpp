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

#ifndef IRESEARCH_UTF8_PATH_H
#define IRESEARCH_UTF8_PATH_H

#include "string.hpp"

#include <functional>

namespace iresearch {

class IRESEARCH_API utf8_path {
 public:
  #ifdef _WIN32
    using native_char_t = wchar_t;
  #else
    using native_char_t = char;
  #endif

  using directory_visitor = std::function<bool(const native_char_t* name)> ;
  using native_str_t = std::basic_string<native_char_t> ;

  explicit utf8_path(bool current_working_path = false);
  explicit utf8_path(basic_string_ref<native_char_t> path);
  utf8_path& operator+=(basic_string_ref<native_char_t> name);
  utf8_path& operator/=(basic_string_ref<native_char_t> name);

  bool absolute(bool& result) const noexcept;
  bool chdir() const noexcept;
  bool exists(bool& result) const noexcept;
  bool exists_directory(bool& result) const noexcept;
  bool exists_file(bool& result) const noexcept;
  bool file_size(uint64_t& result) const noexcept;
  /// @brief creates path 
  /// @param createNew requires at least last segment of path to be actually created or error will be detected
  bool mkdir(bool createNew  = true) const noexcept;
  bool mtime(time_t& result) const noexcept;
  bool remove() const noexcept;
  bool rename(const utf8_path& destination) const noexcept;
  bool visit_directory(
    const directory_visitor& visitor,
    bool include_dot_dir = true);

  const native_char_t* c_str() const noexcept;
  const native_str_t& native() const noexcept;
  std::string utf8() const;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief IFF absolute(): same as utf8()
  ///        IFF !absolute(): same as utf8_path(true) /= utf8()
  //////////////////////////////////////////////////////////////////////////////
  std::string utf8_absolute() const;

  void clear();

 private:
  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  native_str_t path_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
};

}

#endif
