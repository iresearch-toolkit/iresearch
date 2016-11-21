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

#ifndef IRESEARCH_FILE_SYSTEM_DIRECTORY_H
#define IRESEARCH_FILE_SYSTEM_DIRECTORY_H

#include "directory.hpp"
#include "utils/attributes.hpp"
#include "utils/file_utils.hpp"
#include "utils/string.hpp"
#include "utils/noncopyable.hpp"

NS_ROOT

//////////////////////////////////////////////////////////////////////////////
/// @class fs_directory
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API fs_directory : public directory {
 public:
  static bool create_directory(const string_ref& dir);

  static bool remove_directory(const string_ref& dir);

  explicit fs_directory(const std::string& dir);

  const std::string& directory() const;
  using directory::attributes;
  virtual iresearch::attributes& attributes() override;
  virtual void close() override;

  virtual bool visit(const visitor_f& visitor) const override;

  virtual bool exists(const std::string& name) const override;

  virtual std::time_t mtime(const std::string& name) const override;

  virtual bool remove(const std::string& name) override;

  virtual void rename(
    const std::string& src,
    const std::string& dst) override;

  virtual int64_t length(const std::string& name) const override;

  virtual void sync(const std::string& name) override;

  virtual index_lock::ptr make_lock(const std::string& name) override;

  virtual index_output::ptr create(const std::string& name) override;

  virtual index_input::ptr open(const std::string& name) const override;

 private:
  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  iresearch::attributes attributes_;
  std::string dir_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
};

NS_END

#endif