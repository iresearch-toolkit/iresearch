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

#ifndef IRESEARCH_DIRECTORY_H
#define IRESEARCH_DIRECTORY_H

#include "data_input.hpp"
#include "data_output.hpp"
#include "utils/attributes_provider.hpp"
#include "utils/memory.hpp"
#include "utils/noncopyable.hpp"
#include "utils/string.hpp"

#include <vector>

NS_ROOT

//////////////////////////////////////////////////////////////////////////////
/// @struct index_lock 
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API index_lock : private util::noncopyable {
  DECLARE_IO_PTR(index_lock, unlock);
  DECLARE_FACTORY(index_lock);

  static const size_t LOCK_POLL_INTERVAL = 1000;
  static const size_t LOCK_WAIT_FOREVER = integer_traits<size_t>::const_max;

  virtual ~index_lock();

  virtual bool lock() = 0;

  virtual bool is_locked() const = 0;

  virtual void unlock() NOEXCEPT = 0;

  static bool lock(index_lock& l, size_t wait_timeout = 1000);
}; // unique_lock

//////////////////////////////////////////////////////////////////////////////
/// @struct directory 
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API directory 
  : public util::attributes_provider, 
    private util::noncopyable {
  typedef std::vector<std::string> files;

  DECLARE_PTR(directory);
  DECLARE_FACTORY(directory);

  virtual ~directory();

  virtual void close() = 0;

  virtual bool list(files& names) const = 0;

  virtual bool exists(const std::string& name) const = 0;

  virtual std::time_t mtime(const std::string& name) const = 0;

  virtual bool remove(const std::string& name) = 0;
  
  virtual void rename(
    const std::string& src,
    const std::string& dst) = 0;
  
  virtual int64_t length(const std::string& name) const = 0;
  
  virtual void sync(const std::string& name) = 0;
  
  virtual index_lock::ptr make_lock(const std::string& name) = 0;

  virtual index_output::ptr create(const std::string& name) = 0;

  virtual index_input::ptr open(const std::string& name) const = 0;

}; // directory
  
NS_END

#endif
