////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_MMAP_DIRECTORY_H
#define IRESEARCH_MMAP_DIRECTORY_H

#include "fs_directory.hpp"

namespace iresearch {

class mmap_directory_attributes final : public fs_directory_attributes {
 public:
  explicit mmap_directory_attributes(
      size_t memory_pool_size = 0,
      std::unique_ptr<irs::encryption> enc = {})
    : fs_directory_attributes{0, memory_pool_size, std::move(enc)} {
  }

 private:
  using fs_directory_attributes::fd_pool_size;
};

//////////////////////////////////////////////////////////////////////////////
/// @class mmap_directory
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API mmap_directory : public fs_directory {
 public:
  explicit mmap_directory(
    std::string dir,
    mmap_directory_attributes attrs = mmap_directory_attributes{});

  virtual index_input::ptr open(
    const std::string& name,
    IOAdvice advice) const noexcept override final;
}; // mmap_directory

} // ROOT

#endif // IRESEARCH_MMAP_DIRECTORY_H
