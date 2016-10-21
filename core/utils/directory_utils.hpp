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

#ifndef IRESEARCH_DIRECTORY_UTILS_H
#define IRESEARCH_DIRECTORY_UTILS_H

#include "shared.hpp"
#include "store/data_input.hpp"
#include "store/data_output.hpp"
#include "store/directory.hpp"
#include "store/directory_cleaner.hpp"

NS_ROOT

class format;
class index_meta;
struct segment_meta;

NS_BEGIN(directory_utils)

// return a reference to a file or empty() if not found
IRESEARCH_API index_file_refs::ref_t reference(
  directory& dir,
  const std::string& name,
  bool include_missing = false
);

// return success, visitor gets passed references to files retrieved from source
IRESEARCH_API bool reference(
  directory& dir,
  const std::function<const std::string*()>& source,
  const std::function<bool(index_file_refs::ref_t&& ref)>& visitor,
  bool include_missing = false
);

// return success, visitor gets passed references to files registered with index_meta
IRESEARCH_API bool reference(
  directory& dir,
  const index_meta& meta,
  const std::function<bool(index_file_refs::ref_t&& ref)>& visitor,
  bool include_missing = false
);

// return success, visitor gets passed references to files registered with segment_meta
IRESEARCH_API bool reference(
  directory& dir,
  const segment_meta& meta,
  const std::function<bool(index_file_refs::ref_t&& ref)>& visitor,
  bool include_missing = false
);

// remove all (tracked and non-tracked) files if they are unreferenced
void remove_all_unreferenced(directory& dir);

// remove tracked files if they are unreferenced and not part of the latest segments
IRESEARCH_API directory_cleaner::removal_acceptor_t remove_except_current_segments(
  const directory& dir, format& codec
);

NS_END

//////////////////////////////////////////////////////////////////////////////
/// @class tracking_directory
/// @brief track files created/opened via file names
//////////////////////////////////////////////////////////////////////////////

struct IRESEARCH_API tracking_directory: public directory {
  typedef std::unordered_set<std::string> file_set;

  // @param track_open - track file refs for calls to open(...)
  tracking_directory(directory& impl, bool track_open = false);
  virtual ~tracking_directory();
  directory& operator*();
  using directory::attributes;
  virtual iresearch::attributes& attributes() override;
  virtual void close() override;
  virtual index_output::ptr create(const std::string& name) override;
  virtual bool exists(const std::string& name) const override;
  virtual int64_t length(const std::string& name) const override;
  virtual bool list(directory::files& names) const override;
  virtual index_lock::ptr make_lock(const std::string& name) override;
  virtual std::time_t mtime(const std::string& name) const override;
  virtual index_input::ptr open(const std::string& name) const override;
  virtual bool remove(const std::string& name) override;
  virtual void rename(const std::string& src, const std::string& dst) override;
  void swap_tracked(file_set& other);
  void swap_tracked(tracking_directory& other);
  virtual void sync(const std::string& name) override;

 private:
  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  mutable file_set files_;
  directory& impl_;
  bool track_open_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
};

//////////////////////////////////////////////////////////////////////////////
/// @class ref_tracking_directory
/// @brief track files created/opened via file refs instead of file names
//////////////////////////////////////////////////////////////////////////////

struct IRESEARCH_API ref_tracking_directory: public directory {
 public:
  DECLARE_PTR(ref_tracking_directory);
  // @param track_open - track file refs for calls to open(...)
  ref_tracking_directory(directory& impl, bool track_open = false);
  ref_tracking_directory(ref_tracking_directory&& other);
  virtual ~ref_tracking_directory();
  directory& operator*();
  using directory::attributes;
  virtual iresearch::attributes& attributes() override;
  void clear_refs() const;
  virtual void close() override;
  virtual index_output::ptr create(const std::string &name) override;
  virtual bool exists(const std::string& name) const override;
  virtual int64_t length(const std::string& name) const override;
  virtual bool list(directory::files& buf) const override;
  virtual index_lock::ptr make_lock(const std::string& name) override;
  virtual std::time_t mtime(const std::string& name) const override;
  virtual index_input::ptr open(const std::string& name) const override;
  virtual bool remove(const std::string& name) override;
  virtual void rename(const std::string& src, const std::string& dst) override;
  virtual void sync(const std::string& name) override;
  bool visit_refs(const std::function<bool(const index_file_refs::ref_t& ref)>& visitor) const;

 private:
  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  index_file_refs::attribute_t& attribute_;
  directory& impl_;
  mutable std::mutex mutex_; // for use with refs_
  mutable std::unordered_map<string_ref, index_file_refs::ref_t> refs_;
  bool track_open_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
};

NS_END

#endif