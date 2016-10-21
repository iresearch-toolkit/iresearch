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

#ifndef IRESEARCH_UTF8_PATH_H
#define IRESEARCH_UTF8_PATH_H

#include "boost/filesystem/path.hpp"
#include "error/error.hpp"

NS_ROOT

struct detailed_io_error: virtual iresearch::io_error {
  explicit detailed_io_error(const std::string& error): error_(error) {}
  explicit detailed_io_error(std::string&& error): error_(std::move(error)) {}
  virtual iresearch::ErrorCode code() const NOEXCEPT override{ return CODE; }
  virtual const char* what() const NOEXCEPT{ return error_.c_str(); }
 private:
   std::string error_;
};

class utf8_path: private ::boost::filesystem::path {
 public:
  utf8_path();
  utf8_path(const ::boost::filesystem::path& path);
  utf8_path& operator/(const std::string &utf8_name);
  utf8_path& operator/(const iresearch::string_ref &utf8_name);
  utf8_path& operator/(const std::wstring &ucs2_name);
  bool exists() const;
  bool exists_file() const;
  std::time_t file_mtime() const;
  int64_t file_size() const;
  const ::boost::filesystem::path::string_type& native() const;
  const ::boost::filesystem::path::value_type* c_str() const;
  bool mkdir() const;
  bool remove() const;
  void rename(const utf8_path& destination) const;
  bool rmdir() const;
  std::string utf8() const;

 private:
  ::boost::filesystem::path path_;
};

NS_END

#endif