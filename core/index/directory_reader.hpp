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

#include "index_reader.hpp"
#include "shared.hpp"
#include "utils/object_pool.hpp"

namespace irs {

// Representation of the metadata of a directory_reader
struct DirectoryMeta {
  std::string filename;
  IndexMeta meta;
};

class DirectoryReaderImpl;

// Interface for an index reader over a directory of segments
class directory_reader final : public index_reader {
 public:
  // Create an index reader over the specified directory
  // if codec == nullptr then use the latest file for all known codecs
  static directory_reader open(
    const directory& dir, format::ptr codec = nullptr,
    const index_reader_options& opts = index_reader_options{});

  directory_reader() = default;  // allow creation of an uninitialized ptr
  directory_reader(const directory_reader& other) noexcept;
  directory_reader& operator=(const directory_reader& other) noexcept;

  // Return the directory_meta this reader is based upon
  // Note that return value valid on an already open reader until call to
  // reopen()
  const DirectoryMeta& Meta() const;

  explicit operator bool() const noexcept { return nullptr != impl_; }

  bool operator==(std::nullptr_t) const noexcept { return !impl_; }

  bool operator==(const directory_reader& rhs) const noexcept {
    return impl_ == rhs.impl_;
  }

  directory_reader& operator*() noexcept { return *this; }
  const directory_reader& operator*() const noexcept { return *this; }
  directory_reader* operator->() noexcept { return this; }
  const directory_reader* operator->() const noexcept { return this; }

  const sub_reader& operator[](size_t i) const override;

  uint64_t docs_count() const override;

  uint64_t live_docs_count() const override;

  size_t size() const override;

  // FIXME(gnusi): remove codec arg
  //
  //  Open a new instance based on the latest file for the specified codec
  //         this call will atempt to reuse segments from the existing reader
  //         if codec == nullptr then use the latest file for all known codecs
  directory_reader reopen(format::ptr codec = nullptr) const;

  explicit operator index_reader::ptr() const noexcept;

 private:
  explicit directory_reader(
    std::shared_ptr<const DirectoryReaderImpl> impl) noexcept;

  std::shared_ptr<const DirectoryReaderImpl> impl_;
};

inline bool operator==(std::nullptr_t, const directory_reader& rhs) noexcept {
  return rhs == nullptr;
}

inline bool operator!=(std::nullptr_t, const directory_reader& rhs) noexcept {
  return rhs != nullptr;
}

}  // namespace irs
