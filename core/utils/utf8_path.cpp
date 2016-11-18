//
// IResearch search engine 
// 
// Copyright © 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#include <boost/filesystem/operations.hpp>
#include <boost/locale/encoding.hpp>

#include "locale_utils.hpp"

#include "utf8_path.hpp"

NS_LOCAL

// in some situations codecvt returned from boost::filesystem::path seems to get corrupted
// always use UTF-8 locale for reading/writing filesystem paths
const boost::filesystem::path::codecvt_type& fs_codecvt() {
  static auto default_locale = iresearch::locale_utils::locale("", true);

  return std::use_facet<boost::filesystem::path::codecvt_type>(default_locale);
}

static auto& fs_cvt = fs_codecvt();

NS_END

NS_ROOT

utf8_path::utf8_path() {
}

utf8_path::utf8_path(const ::boost::filesystem::path& path): path_(path) {
}

utf8_path& utf8_path::operator/(const string_ref& utf8_name) {
  #ifdef _WIN32
    auto native_name = boost::locale::conv::utf_to_utf<wchar_t>(
      utf8_name.c_str(), utf8_name.c_str() + utf8_name.size()
    );

    path_.append(std::move(native_name), fs_cvt);
  #else
    path_.append(utf8_name.c_str(), utf8_name.c_str() + utf8_name.size(), fs_cvt);
  #endif

  return *this;
}

utf8_path& utf8_path::operator/(const std::string &utf8_name) {
  #ifdef _WIN32
    auto native_name = boost::locale::conv::utf_to_utf<wchar_t>(
      utf8_name.c_str(), utf8_name.c_str() + utf8_name.size()
    );

    path_.append(std::move(native_name), fs_cvt);
  #else
    path_.append(utf8_name.c_str(), utf8_name.c_str() + utf8_name.size(), fs_cvt);
  #endif

  return *this;
}

utf8_path& utf8_path::operator/(const std::wstring &ucs2_name) {
  #ifdef _WIN32
    path_.append(ucs2_name, fs_cvt);
  #else
    auto native_name = boost::locale::conv::utf_to_utf<char>(
      ucs2_name.c_str(), ucs2_name.c_str() + ucs2_name.size()
    );

    path_.append(std::move(native_name), fs_cvt);
  #endif

  return *this;
}

utf8_path& utf8_path::operator/(const wchar_t* ucs2_name) {
  return (*this) / iresearch::basic_string_ref<wchar_t>(ucs2_name);
}

utf8_path& utf8_path::operator/(const iresearch::basic_string_ref<wchar_t>& ucs2_name) {
  #ifdef _WIN32
    path_.append(ucs2_name.c_str(),ucs2_name.c_str() + ucs2_name.size() , fs_cvt);
  #else
    auto native_name = boost::locale::conv::utf_to_utf<char>(
      ucs2_name.c_str(), ucs2_name.c_str() + ucs2_name.size()
    );

    path_.append(std::move(native_name), fs_cvt);
  #endif

  return *this;
}
  
bool utf8_path::exists() const {
  boost::system::error_code code;

  return boost::filesystem::exists(path_, code) && boost::system::errc::success == code.value();
}

bool utf8_path::exists_file() const {
  boost::system::error_code code;

  return boost::filesystem::is_regular_file(path_, code) && boost::system::errc::success == code.value();
}

std::time_t utf8_path::file_mtime() const {
  boost::system::error_code code;

  return boost::filesystem::last_write_time(path_, code);
}

int64_t utf8_path::file_size() const {
  boost::system::error_code code;

  return boost::filesystem::file_size(path_, code);
}

const boost::filesystem::path::string_type& utf8_path::native() const {
  return path_.native();
}

const boost::filesystem::path::value_type* utf8_path::c_str() const {
  return path_.c_str();
}

bool utf8_path::mkdir() const {
  boost::system::error_code code;

  return boost::filesystem::create_directories(path_, code) && boost::system::errc::success == code.value();
}

bool utf8_path::remove() const {
  boost::system::error_code code;

  return boost::filesystem::remove_all(path_, code) > 0 && boost::system::errc::success == code.value();
}

void utf8_path::rename(const utf8_path& destination) const {
  boost::system::error_code code;

  boost::filesystem::rename(path_, destination.path_, code);

  if(code) {
    throw(detailed_io_error(code.message()));
  }
}

bool utf8_path::rmdir() const {
  boost::system::error_code code;

  boost::filesystem::remove_all(path_, code);

  return boost::system::errc::success == code.value();
}

void utf8_path::clear() {
  path_.clear();
}

std::string utf8_path::utf8() const {
  return path_.string(fs_cvt);
}

NS_END