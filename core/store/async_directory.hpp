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

//////////////////////////////////////////////////////////////////////////////
/// @class mmap_directory
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API async_directory : public mmap_directory {
 public:
  explicit async_directory(const std::string& dir);

  virtual index_output::ptr create(const std::string& name) noexcept override;
//  virtual void sync(const std::string& name) override;

  void drain();

 private:
  static constexpr size_t NUM_PAGES = 128;

  class uring {
   public:
    uring(size_t queue_size, unsigned flags = 0) {
      if (io_uring_queue_init(queue_size, &ring, flags)) {
        throw not_supported();
      }
    }

    ~uring() {
      io_uring_queue_exit(&ring);
    }

    io_uring ring;
  };

  struct buffer_deleter {
    void operator()(byte_type* b) const noexcept {
      ::free(b);
    }
  };

  using node_type = concurrent_stack<byte_type*>::node_type;
  friend class async_index_output;

  node_type* get_page() {
    return free_pages_.pop();
  }
  io_uring_sqe* get_sqe();
  void* deque(bool wait);
  node_type* get_buffer();
  void submit();

  mutable concurrent_stack<byte_type*> free_pages_;
  decltype(free_pages_)::node_type pages_[NUM_PAGES];
  std::unique_ptr<byte_type, buffer_deleter> buffer_;
  mutable uring ring_;
}; // async_directory

}

#endif // IRESEARCH_ASYNC_DIRECTORY_H

