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

#include "utils/utf8_path.hpp"
#include "utils/file_utils.hpp"
#include "utils/log.hpp"
#include "error/error.hpp"

namespace iresearch {

utf8_path::utf8_path(bool current_working_path /*= false*/) {
  if (current_working_path) {
    std::basic_string<native_char_t> buf;

    // emulate boost::filesystem behaviour by leaving path_ unset in case of error
    if (irs::file_utils::read_cwd(buf)) {
      *this += buf;
    }
  }
}

utf8_path::utf8_path(basic_string_ref<native_char_t> path) {
  if (path.null()) {
    std::basic_string<native_char_t> buf;

    // emulate boost::filesystem behaviour by leaving path_ unset in case of error
    if (irs::file_utils::read_cwd(buf)) {
      path_ = std::move(buf);
    }

    return;
  }

  if (!file_utils::append(path_, path)) {
    // emulate boost::filesystem behaviour by throwing an exception
    throw io_error("path conversion failure");
  }
}

utf8_path& utf8_path::operator+=(basic_string_ref<native_char_t> name) {
  if (!file_utils::append(path_, name)) {
    // emulate boost::filesystem behaviour by throwing an exception
    throw io_error("path conversion failure");
  }

  return *this;
}

utf8_path& utf8_path::operator/=(basic_string_ref<native_char_t> name) {
  if (!path_.empty()) {
    path_.append(1, file_path_delimiter);
  }

  if (!file_utils::append(path_, name)) {
    // emulate boost::filesystem behaviour by throwing an exception
    throw io_error("path conversion failure");
  }

  return *this;
}

bool utf8_path::absolute(bool& result) const noexcept {
  return irs::file_utils::absolute(result, c_str());
}

bool utf8_path::chdir() const noexcept {
  return irs::file_utils::set_cwd(c_str());
}

bool utf8_path::exists(bool& result) const noexcept {
  return irs::file_utils::exists(result, c_str());
}

bool utf8_path::exists_directory(bool& result) const noexcept {
  return irs::file_utils::exists_directory(result, c_str());
}

bool utf8_path::exists_file(bool& result) const noexcept {
  return irs::file_utils::exists_file(result, c_str());
}

bool utf8_path::file_size(uint64_t& result) const noexcept {
  return irs::file_utils::byte_size(result, c_str());
}

bool utf8_path::mtime(time_t& result) const noexcept {
  return irs::file_utils::mtime(result, c_str());
}


bool utf8_path::mkdir(bool createNew /* = true*/) const noexcept {
  return irs::file_utils::mkdir(c_str(), createNew); 
}

bool utf8_path::remove() const noexcept {
  return irs::file_utils::remove(c_str());
}

bool utf8_path::rename(const utf8_path& destination) const noexcept {
  return irs::file_utils::move(c_str(), destination.c_str());
}

bool utf8_path::visit_directory(
    const directory_visitor& visitor,
    bool include_dot_dir /*= true*/ ) {
  if (!irs::file_utils::visit_directory(c_str(), visitor, include_dot_dir)) {
    bool result;

    return !(!exists_directory(result) || !result); // check for FS error, treat non-directory as error
  }

  return true;
}

void utf8_path::clear() {
  path_.clear();
}

const utf8_path::native_char_t* utf8_path::c_str() const noexcept {
  return path_.c_str();
}

const utf8_path::native_str_t& utf8_path::native() const noexcept {
  return path_;
}

std::string utf8_path::utf8() const {
  #ifdef _WIN32
    std::string buf;

    if (!file_utils::append(buf, path_)) {
      // emulate boost::filesystem behaviour by throwing an exception
      throw io_error("Path conversion failure");
    }

    return buf;
  #else
    return path_;
  #endif
}

std::string utf8_path::utf8_absolute() const {
  bool abs;

  if (absolute(abs) && abs) {
    return utf8(); // already absolute (on failure assume relative path)
  }

  utf8_path path(true);

  path /= path_;

  return path.utf8();
}

}
