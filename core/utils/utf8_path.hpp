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
    typedef wchar_t native_char_t;
  #else
    typedef char native_char_t;
  #endif

  typedef std::function<bool(const native_char_t* name)> directory_visitor;
  typedef std::basic_string<native_char_t> native_str_t;
  typedef std::basic_string<native_char_t> string_type; // to simmplify move to std::filesystem::path
  typedef native_char_t value_type; // to simmplify move to std::filesystem::path

  utf8_path(bool current_working_path = false);
  utf8_path(const char* utf8_path);
  utf8_path(const std::string& utf8_path);
  utf8_path(const irs::string_ref& utf8_path);
  utf8_path(const wchar_t* utf8_path);
  utf8_path(const irs::basic_string_ref<wchar_t>& ucs2_path);
  utf8_path(const std::wstring& ucs2_path);
  utf8_path& operator+=(const char* utf8_name);
  utf8_path& operator+=(const std::string& utf8_name);
  utf8_path& operator+=(const irs::string_ref& utf8_name);
  utf8_path& operator+=(const wchar_t* ucs2_name);
  utf8_path& operator+=(const irs::basic_string_ref<wchar_t>& ucs2_name);
  utf8_path& operator+=(const std::wstring& ucs2_name);
  utf8_path& operator/=(const char* utf8_name);
  utf8_path& operator/=(const std::string& utf8_name);
  utf8_path& operator/=(const irs::string_ref& utf8_name);
  utf8_path& operator/=(const wchar_t* ucs2_name);
  utf8_path& operator/=(const irs::basic_string_ref<wchar_t>& ucs2_name);
  utf8_path& operator/=(const std::wstring& ucs2_name);

  bool is_absolute(bool& result) const noexcept;

  // std::filesystem::path compliant version
  bool utf8_path::is_absolute() const noexcept {
    bool result;
    return is_absolute(result) && result;
  }

  const native_char_t* c_str() const noexcept;
  const native_str_t& native() const noexcept;
  std::string u8string() const;

  template<typename T>
  utf8_path& assign(T&& source) {
    path_.clear();
    (*this) += source;
    return *this;
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief IFF absolute(): same as utf8()
  ///        IFF !absolute(): same as utf8_path(true) /= utf8()
  //////////////////////////////////////////////////////////////////////////////
  //std::string utf8_absolute() const;

  void clear();

 private:
  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  native_str_t path_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
};

utf8_path operator/( const utf8_path& lhs, const utf8_path& rhs );

}

#endif
