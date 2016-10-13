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

#include "index/index_meta.hpp"
#include "formats/formats.hpp"
#include "utils/attributes.hpp"
#include "directory_utils.hpp"

NS_ROOT
NS_BEGIN(directory_utils)

// return a reference to a file or empty() if not found
index_file_refs::ref_t reference(
    directory& dir, 
    const std::string& name,
    bool include_missing /*= false*/
) {
  if (include_missing) {
    return dir.attributes().add<index_file_refs>()->add(name);
  }

  if (!dir.exists(name)) {
    return nullptr;
  }

  auto ref = dir.attributes().add<index_file_refs>()->add(name);

  return dir.exists(name) ? ref : index_file_refs::ref_t(nullptr);
}

#if defined(_MSC_VER)
  #pragma warning(disable : 4706)
#elif defined (__GNUC__)
  #pragma GCC diagnostic ignored "-Wparentheses"
#endif

// return success, visitor gets passed references to files retrieved from source
bool reference(
    directory& dir,
    const std::function<const std::string*()>& source,
    const std::function<bool(index_file_refs::ref_t&& ref)>& visitor,
    bool include_missing /*= false*/
) {
  auto& attribute = dir.attributes().add<index_file_refs>();

  for (const std::string* file; file = source();) {
    if (include_missing) {
      if (!visitor(attribute->add(*file))) {
        return false;
      }

      continue;
    }

    if (!dir.exists(*file)) {
      continue;
    }

    auto ref = attribute->add(*file);

    if (dir.exists(*file) && !visitor(std::move(ref))) {
      return false;
    }
  }

  return true;
}

#if defined(_MSC_VER)
  #pragma warning(default : 4706)
#elif defined (__GNUC__)
  #pragma GCC diagnostic pop
#endif

// return success, visitor gets passed references to files registered with index_meta
bool reference(
    directory& dir,
    const index_meta& meta,
    const std::function<bool(index_file_refs::ref_t&& ref)>& visitor,
    bool include_missing /*= false*/
) {
  if (meta.empty()) {
    return true;
  }

  auto& attribute = dir.attributes().add<index_file_refs>();
  return meta.visit_files([include_missing, &attribute, &dir, &visitor](const std::string& file) {
    if (include_missing) {
      return visitor(attribute->add(file));
    }

    if (!dir.exists(file)) {
      return true;
    }

    auto ref = attribute->add(file);

    if (dir.exists(file)) {
      return visitor(std::move(ref));
    }

    return true;
  });
}

// return success, visitor gets passed references to files registered with segment_meta
bool reference(
  directory& dir,
  const segment_meta& meta,
  const std::function<bool(index_file_refs::ref_t&& ref)>& visitor,
  bool include_missing /*= false*/
) {
  auto files = meta.files;

  if (files.empty()) {
    return true;
  }

  auto& attribute = dir.attributes().add<index_file_refs>();

  for (auto& file: files) {
    if (include_missing) {
      if (!visitor(attribute->add(file))) {
        return false;
      }

      continue;
    }

    if (!dir.exists(file)) {
      continue;
    }

    auto ref = attribute->add(file);

    if (dir.exists(file) && !visitor(std::move(ref))) {
      return false;
    }
  }

  return true;
}

void remove_all_unreferenced(directory& dir) {
  auto& attribute = dir.attributes().add<index_file_refs>();
  directory::files files;

  dir.list(files);

  for (auto& file: files) {
    attribute->add(std::move(file)); // ensure all files in dir are tracked
  }

  directory_cleaner::clean(dir);
}

directory_cleaner::removal_acceptor_t remove_except_current_segments(
  const directory& dir, format& codec
) {
  directory::files files;

  dir.list(files);

  static const auto acceptor = [](
    const std::string& filename, const std::unordered_set<std::string>& retain
  ) {
    return retain.find(filename) == retain.end();
  };
  index_meta meta;
  auto reader = codec.get_index_meta_reader();
  auto* segment_file = reader->last_segments_file(files);

  if (!segment_file) {
    return [](const std::string&)->bool { return true; };
  }

  reader->read(dir, meta, *segment_file);

  std::unordered_set<std::string> retain;
  retain.reserve(meta.size());

  meta.visit_files([&retain] (std::string& file) {
    retain.emplace(std::move(file));
    return true;
  });

  retain.emplace(std::move(*segment_file));

  return std::bind(acceptor, std::placeholders::_1, std::move(retain));
}

NS_END

// -----------------------------------------------------------------------------
// --SECTION--                                                tracking_directory
// -----------------------------------------------------------------------------

tracking_directory::tracking_directory(
  directory& impl, bool track_open /*= false*/):
  impl_(impl), track_open_(track_open) {
}

tracking_directory::~tracking_directory() {}

directory& tracking_directory::operator*() {
  return impl_;
}

attributes& tracking_directory::attributes() {
  return impl_.attributes();
}

void tracking_directory::close() {
  impl_.close();
}

index_output::ptr tracking_directory::create(const std::string& name) {
  files_.emplace(name);
  return impl_.create(name);
}

bool tracking_directory::exists(const std::string& name) const {
  return impl_.exists(name);
}

int64_t tracking_directory::length(const std::string& name) const {
  return impl_.length(name);
}

bool tracking_directory::list(directory::files& names) const {
  return impl_.list(names);
}

index_lock::ptr tracking_directory::make_lock(const std::string& name) {
  return impl_.make_lock(name);
}

std::time_t tracking_directory::mtime(const std::string& name) const {
  return impl_.mtime(name);
}

index_input::ptr tracking_directory::open(const std::string& name) const {
  if (track_open_) {
    files_.emplace(name);
  }

  return impl_.open(name);
}

bool tracking_directory::remove(const std::string& name) {
  bool result = impl_.remove(name);
  files_.erase(name);
  return result;
}

void tracking_directory::rename(
    const std::string& src, 
    const std::string& dst) {
  if (files_.emplace(dst).second) {
    files_.erase(src);
  }

  impl_.rename(src, dst);
}

void tracking_directory::swap_tracked(file_set& other) {
  files_.swap(other);
}

void tracking_directory::swap_tracked(tracking_directory& other) {
  swap_tracked(other.files_);
}

void tracking_directory::sync(const std::string& name) {
  impl_.sync(name);
}

// -----------------------------------------------------------------------------
// --SECTION--                                            ref_tracking_directory
// -----------------------------------------------------------------------------

ref_tracking_directory::ref_tracking_directory(
  directory& impl, bool track_open /*= false*/
):
  attribute_(impl.attributes().add<index_file_refs>()),
  impl_(impl),
  track_open_(track_open) {
}

ref_tracking_directory::ref_tracking_directory(ref_tracking_directory&& other):
  attribute_(other.attribute_), // references do not require std::move(...)
  impl_(other.impl_), // references do not require std::move(...)
  refs_(std::move(other.refs_)),
  track_open_(std::move(other.track_open_)) {
}

ref_tracking_directory::~ref_tracking_directory() {}

directory& ref_tracking_directory::operator*() {
  return impl_;
}

attributes& ref_tracking_directory::attributes() {
  return impl_.attributes();
}

void ref_tracking_directory::clear_refs() const {
  SCOPED_LOCK(mutex_);

  refs_.clear();
}

void ref_tracking_directory::close() {
  impl_.close();
}

index_output::ptr ref_tracking_directory::create(const std::string& name) {
  auto ref = attribute_->add(name);
  SCOPED_LOCK(mutex_);
  auto result = impl_.create(name);

  // only track ref on successful call to impl_
  if (result) {
    refs_.emplace(*ref, std::move(ref));
  }

  return result;
}

bool ref_tracking_directory::exists(const std::string& name) const {
  return impl_.exists(name);
}

int64_t ref_tracking_directory::length(const std::string& name) const {
  return impl_.length(name);
}

bool ref_tracking_directory::list(directory::files& buf) const {
  return impl_.list(buf);
}

index_lock::ptr ref_tracking_directory::make_lock(const std::string& name) {
  return impl_.make_lock(name);
}

std::time_t ref_tracking_directory::mtime(const std::string& name) const {
  return impl_.mtime(name);
}

index_input::ptr ref_tracking_directory::open(const std::string& name) const {
  if (!track_open_) {
    return impl_.open(name);
  }

  auto result = impl_.open(name);

  // only track ref on successful call to impl_
  if (result) {
    auto ref = attribute_->add(name);
    SCOPED_LOCK(mutex_);

    refs_.emplace(*ref, std::move(ref));
  }

  return result;
}

bool ref_tracking_directory::remove(const std::string& name) {
  bool result = impl_.remove(name);
  SCOPED_LOCK(mutex_);

  refs_.erase(name);

  return result;
}

void ref_tracking_directory::rename(
    const std::string& src, const std::string& dst
) {
  impl_.rename(src, dst);

  SCOPED_LOCK(mutex_);

  if (refs_.emplace(dst, attribute_->add(dst)).second) {
    refs_.erase(src);
  }
}

void ref_tracking_directory::sync(const std::string& name) {
  impl_.sync(name);
}

bool ref_tracking_directory::visit_refs(
  const std::function<bool(const index_file_refs::ref_t& ref)>& visitor
) const {
  SCOPED_LOCK(mutex_);

  for (const auto& ref: refs_) {
    if (!visitor(ref.second)) {
      return false;
    }
  }

  return true;
}

NS_END