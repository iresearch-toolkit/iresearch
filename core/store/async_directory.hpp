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

#include "mmap_directory.hpp"

namespace iresearch {

//////////////////////////////////////////////////////////////////////////////
/// @class mmap_directory
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API async_directory : public mmap_directory {
 public:
  explicit async_directory(const std::string& dir);

  virtual index_output::ptr create(
    const std::string& name) const noexcept override final;

 private:
  mutable io_uring ring_;
}; // async_directory

}

#endif // IRESEARCH_ASYNC_DIRECTORY_H

