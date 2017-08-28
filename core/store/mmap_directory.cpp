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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "mmap_directory.hpp"
#include "store_utils.hpp"
#include "utils/utf8_path.hpp"
#include "utils/file_utils.hpp"

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

NS_LOCAL

struct mmap_handle {
  DECLARE_SPTR(mmap_handle);

  ~mmap_handle() {
    if (addr != MAP_FAILED) {
      ::munmap(addr, size);
    }

    if (fd >= 0 ) {
      ::close(fd);
    }
  }

  void* addr{MAP_FAILED};
  size_t size{}; // file size
  int fd{-1};
}; // mmap_handle

//////////////////////////////////////////////////////////////////////////////
/// @struct mmap_index_input
/// @brief input stream for memory mapped directory
//////////////////////////////////////////////////////////////////////////////
class mmap_index_input : public irs::bytes_ref_input {
 public:
  static irs::index_input::ptr open(const file_path_t file) NOEXCEPT {
    assert(file);

    mmap_handle::ptr handle;

    try {
      handle = std::make_shared<mmap_handle>();
    } catch (...) {
      IR_EXCEPTION();
      return nullptr;
    }

    const int fd = ::open(file, O_RDONLY);

    if (fd < 0) {
      IR_FRMT_ERROR("Failed to open input file, error: %d, path: %s", errno, file);
      return nullptr;
    }

    handle->fd = fd;

    const auto size = irs::file_utils::file_size(fd);

    if (size < 0) {
      IR_FRMT_ERROR("Failed to get stats for input file, error: %d, path: %s", errno, file);
      return nullptr;
    }

    if (size) {
      handle->size = size;

      void* addr = ::mmap(0, size, PROT_READ, MAP_PRIVATE, fd, 0);

      if (MAP_FAILED == addr) {
        IR_FRMT_ERROR("Failed to mmap input file, error: %d, path: %s", errno, file);
        return nullptr;
      }

      handle->addr = addr;
    }

    return std::unique_ptr<irs::index_input>(new mmap_index_input(std::move(handle)));
  }

  virtual ptr dup() const NOEXCEPT override {
    return std::unique_ptr<irs::index_input>(new mmap_index_input(*this));
  }

  virtual ptr reopen() const NOEXCEPT override {
    return dup();
  }

 private:
  explicit mmap_index_input(mmap_handle::ptr&& handle) NOEXCEPT
    : handle_(std::move(handle)) {
    if (handle_) {
      const auto* begin = reinterpret_cast<irs::byte_type*>(handle_->addr);
      bytes_ref_input::reset(begin, handle_->size);
    }
  }

  mmap_index_input(const mmap_index_input& rhs) NOEXCEPT
    : bytes_ref_input(rhs),
      handle_(rhs.handle_) {
  }

  mmap_index_input& operator=(const mmap_index_input&) = delete;

  mmap_handle::ptr handle_;
}; // mmap_index_input

NS_END // LOCAL

NS_ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                     mmap_directory implementation
// -----------------------------------------------------------------------------

mmap_directory::mmap_directory(const std::string& path)
  : fs_directory(path) {
}

index_input::ptr mmap_directory::open(const std::string& name) const NOEXCEPT {
  try {
    utf8_path path;

    (path/=directory())/=name;

    return mmap_index_input::open(path.c_str());
  } catch(...) {
    IR_EXCEPTION();
  }

  return nullptr;
}

NS_END // ROOT
