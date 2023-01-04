////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <vector>

#include "index/composite_reader_impl.hpp"
#include "index/segment_reader.hpp"
#include "store/directory_attributes.hpp"

namespace irs {

// Representation of the metadata of a directory_reader
struct DirectoryMeta {
  std::string filename;
  IndexMeta index_meta;
};

class DirectoryReaderImpl
  : public CompositeReaderImpl<std::vector<SegmentReader>> {
 public:
  struct Init;

  // open a new directory reader
  // if codec == nullptr then use the latest file for all known codecs
  // if cached != nullptr then try to reuse its segments
  static std::shared_ptr<const DirectoryReaderImpl> Open(
    const directory& dir, const IndexReaderOptions& opts, const format* codec,
    const std::shared_ptr<const DirectoryReaderImpl>& cached);

  DirectoryReaderImpl(const directory& dir, const IndexReaderOptions& opts,
                      DirectoryMeta&& meta, ReadersType&& readers);

  const directory& Dir() const noexcept { return dir_; }

  const DirectoryMeta& Meta() const noexcept { return meta_; }

  const IndexReaderOptions& Options() const noexcept { return opts_; }

 private:
  DirectoryReaderImpl(Init&& init, const directory& dir,
                      const IndexReaderOptions& opts, DirectoryMeta&& meta,
                      ReadersType&& readers);

  using FileRefs = std::vector<index_file_refs::ref_t>;

  const directory& dir_;
  FileRefs file_refs_;
  DirectoryMeta meta_;
  IndexReaderOptions opts_;
};

}  // namespace irs
