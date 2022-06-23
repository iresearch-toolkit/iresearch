////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_ASYNC_DIRECTORY_H
#define IRESEARCH_ASYNC_DIRECTORY_H

#include "liburing.h"

#include "store/mmap_directory.hpp"
#include "utils/object_pool.hpp"

namespace iresearch {

class async_file;

struct async_file_deleter {
  void operator()(async_file*) noexcept;
};

struct async_file_builder {
  using ptr = std::unique_ptr<async_file, async_file_deleter>;

  static ptr make(size_t queue_size, unsigned flags);
};

using async_file_pool = unbounded_object_pool<async_file_builder>;
using async_file_ptr = async_file_pool::ptr;

//////////////////////////////////////////////////////////////////////////////
/// @class mmap_directory
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API async_directory : public mmap_directory {
 public:
  explicit async_directory(
    std::string dir,
    directory_attributes attrs = directory_attributes{},
    size_t pool_size = 16,
    size_t queue_size = 1024,
    unsigned flags = 0);

  virtual index_output::ptr create(std::string_view name) noexcept override;
  virtual bool sync(std::span<std::string_view> names) noexcept override;

  using mmap_directory::sync;

 private:
  async_file_pool async_pool_;
  size_t queue_size_;
  unsigned flags_;
}; // async_directory

}

#endif // IRESEARCH_ASYNC_DIRECTORY_H

