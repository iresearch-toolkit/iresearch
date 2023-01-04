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

#pragma once

#include "shared.hpp"
#include "store/data_input.hpp"
#include "store/data_output.hpp"
#include "store/directory.hpp"
#include "store/directory_cleaner.hpp"

namespace irs {

class IndexMeta;
struct SegmentMeta;

namespace directory_utils {

// FIXME(gnusi): remove reference functions

// return a reference to a file or empty() if not found
index_file_refs::ref_t reference(const directory& dir, std::string_view name,
                                 bool include_missing = false);

// return success, visitor gets passed references to files retrieved from source
bool reference(const directory& dir,
               const std::function<std::optional<std::string_view>()>& source,
               const std::function<bool(index_file_refs::ref_t&& ref)>& visitor,
               bool include_missing = false);

// return success, visitor gets passed references to files registered with
// IndexMeta
bool reference(const directory& dir, const IndexMeta& meta,
               const std::function<bool(index_file_refs::ref_t&& ref)>& visitor,
               bool include_missing = false);

// return success, visitor gets passed references to files registered with
// SegmentMeta
bool reference(const directory& dir, const SegmentMeta& meta,
               const std::function<bool(index_file_refs::ref_t&& ref)>& visitor,
               bool include_missing = false);

// remove all (tracked and non-tracked) files if they are unreferenced
// return success
bool remove_all_unreferenced(directory& dir);

}  // namespace directory_utils

//////////////////////////////////////////////////////////////////////////////
/// @class tracking_directory
/// @brief track files created/opened via file names
//////////////////////////////////////////////////////////////////////////////
struct tracking_directory final : public directory {
  using file_set = absl::flat_hash_set<std::string>;

  // @param track_open - track file refs for calls to open(...)
  explicit tracking_directory(directory& impl,
                              bool track_open = false) noexcept;

  directory& operator*() noexcept { return impl_; }

  directory_attributes& attributes() noexcept override {
    return impl_.attributes();
  }

  index_output::ptr create(std::string_view name) noexcept override;

  void clear_tracked() noexcept;

  bool exists(bool& result, std::string_view name) const noexcept override {
    return impl_.exists(result, name);
  }

  void flush_tracked(file_set& other) noexcept;

  bool length(uint64_t& result, std::string_view name) const noexcept override {
    return impl_.length(result, name);
  }

  index_lock::ptr make_lock(std::string_view name) noexcept override {
    return impl_.make_lock(name);
  }

  bool mtime(std::time_t& result,
             std::string_view name) const noexcept override {
    return impl_.mtime(result, name);
  }

  index_input::ptr open(std::string_view name,
                        IOAdvice advice) const noexcept override;

  bool remove(std::string_view name) noexcept override;

  bool rename(std::string_view src, std::string_view dst) noexcept override;

  bool sync(std::span<const std::string_view> files) noexcept override {
    return impl_.sync(files);
  }

  bool visit(const visitor_f& visitor) const override {
    return impl_.visit(visitor);
  }

 private:
  mutable file_set files_;
  directory& impl_;
  bool track_open_;
};  // tracking_directory

//////////////////////////////////////////////////////////////////////////////
/// @class ref_tracking_directory
/// @brief track files created/opened via file refs instead of file names
//////////////////////////////////////////////////////////////////////////////
struct ref_tracking_directory : public directory {
 public:
  using ptr = std::unique_ptr<ref_tracking_directory>;

  // @param track_open - track file refs for calls to open(...)
  explicit ref_tracking_directory(directory& impl, bool track_open = false);
  ref_tracking_directory(ref_tracking_directory&& other) noexcept;

  directory& operator*() noexcept { return impl_; }

  directory_attributes& attributes() noexcept override {
    return impl_.attributes();
  }

  void clear_refs() const;

  index_output::ptr create(std::string_view name) noexcept override;

  bool exists(bool& result, std::string_view name) const noexcept override {
    return impl_.exists(result, name);
  }

  bool length(uint64_t& result, std::string_view name) const noexcept override {
    return impl_.length(result, name);
  }

  index_lock::ptr make_lock(std::string_view name) noexcept override {
    return impl_.make_lock(name);
  }

  bool mtime(std::time_t& result,
             std::string_view name) const noexcept override {
    return impl_.mtime(result, name);
  }

  index_input::ptr open(std::string_view name,
                        IOAdvice advice) const noexcept override;

  bool remove(std::string_view name) noexcept override;

  bool rename(std::string_view src, std::string_view dst) noexcept override;

  bool sync(std::span<const std::string_view> names) noexcept override {
    return impl_.sync(names);
  }

  bool visit(const visitor_f& visitor) const override {
    return impl_.visit(visitor);
  }

  std::vector<index_file_refs::ref_t> GetRefs() const;

  // FIXME(gnusi): remove
  bool visit_refs(const std::function<bool(const index_file_refs::ref_t& ref)>&
                    visitor) const;

 private:
  using refs_t = absl::flat_hash_set<index_file_refs::ref_t,
                                     index_file_refs::counter_t::hash,
                                     index_file_refs::counter_t::equal_to>;

  index_file_refs& attribute_;
  directory& impl_;
  mutable std::mutex mutex_;  // for use with refs_
  mutable refs_t refs_;
  bool track_open_;
};  // ref_tracking_directory

}  // namespace irs
