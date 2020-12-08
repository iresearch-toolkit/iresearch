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

#include "async_directory.hpp"

#include "liburing.h"

#include "store_utils.hpp"
#include "utils/utf8_path.hpp"
#include "utils/mmap_utils.hpp"
#include "utils/memory.hpp"
#include "utils/locale_utils.hpp"
#include "utils/string_utils.hpp"
#include "utils/file_utils.hpp"
#include "utils/crc.hpp"

namespace {

using namespace irs;

constexpr size_t PAGE_SIZE = 4096;
constexpr size_t PAGE_ALIGNEMNT = 4096;
constexpr size_t QUEUE_SIZE = 1024;

//////////////////////////////////////////////////////////////////////////////
/// @class async_index_output
//////////////////////////////////////////////////////////////////////////////
class async_index_output : public buffered_index_output {
 public:
  DEFINE_FACTORY_INLINE(async_index_output);

  static index_output::ptr open(const file_path_t name, io_uring& ring, concurrent_stack<byte_type*>& pages) noexcept {
    assert(name);

    file_utils::handle_t handle(
      file_utils::open(name, file_utils::OpenMode::Write, IR_FADVICE_NORMAL));

    if (nullptr == handle) {
      typedef std::remove_pointer<file_path_t>::type char_t;
      auto locale = irs::locale_utils::locale(irs::string_ref::NIL, "utf8", true); // utf8 internal and external
      std::string path;

      irs::locale_utils::append_external<char_t>(path, name, locale);

#ifdef _WIN32
      IR_FRMT_ERROR("Failed to open output file, error: %d, path: %s", GetLastError(), path.c_str());
#else
      IR_FRMT_ERROR("Failed to open output file, error: %d, path: %s", errno, path.c_str());
#endif

      return nullptr;
    }

    auto* buf = pages.pop();

    if (!buf) {
      // FIXME
      return nullptr;
    }

    try {
      return async_index_output::make<async_index_output>(
        ring, std::move(handle), buf, pages);
    } catch(...) {
    }

    return nullptr;
  }

  virtual void close() override {
    buffered_index_output::close();
    pages_->push(*buf_);
    handle_.reset(nullptr);
  }

  virtual int64_t checksum() const override {
    const_cast<async_index_output*>(this)->flush();
    return crc_.checksum();
  }


  virtual void write_bytes(const byte_type* b, size_t length) override;

 protected:
  virtual void flush_buffer(const byte_type* b, size_t len) override;

 private:
  using node_type = concurrent_stack<byte_type*>::node_type;

  async_index_output(
      io_uring& ring, file_utils::handle_t&& handle,
      concurrent_stack<byte_type*>::node_type* buf,
      concurrent_stack<byte_type*>& pages) noexcept
    : ring_(&ring),
      buf_(buf),
      pages_(&pages),
      handle_(std::move(handle)) {
    buffered_index_output::reset(buf->value, PAGE_SIZE);
  }

  io_uring* ring_;
  node_type* buf_;
  concurrent_stack<byte_type*>* pages_;
  file_utils::handle_t handle_;
  crc32c crc_;
}; // async_index_output


void async_index_output::flush_buffer(const byte_type* b, size_t len) {
  assert(handle_);

  io_uring_sqe* sqe = io_uring_get_sqe(ring_);

  if (!sqe) {
    throw io_error("failed to get sqe");
  }

  io_uring_prep_write_fixed(sqe, handle_cast(handle_.get()), b, len, buffer_offset(), 0);
  sqe->user_data = reinterpret_cast<uint64_t>(b);

  int ret = io_uring_submit(ring_);
  if (ret < 0) {
    throw io_error(string_utils::to_string(
      "failed to submit write request of '" IR_SIZE_T_SPECIFIER "' bytes, error %d", len, -ret));
  }

  crc_.process_bytes(b, len);

  io_uring_cqe* cqe;
  ret = io_uring_wait_cqe(ring_, &cqe);

  if (ret < 0) {
    // FIXME get another page from stack,
    //   if no pages are available either allocate and
    //   register another chunk or wait for completion

    throw io_error(string_utils::to_string(
      "failed to retrieve a cqe, error %d", -ret));
  }

  if (cqe->res < 0) {
    throw io_error(string_utils::to_string(
      "async i/o operation failed, error %d", -cqe->res));
  }

  static_assert(sizeof(byte_type*) == sizeof(decltype(cqe->user_data)));
  auto* buf = reinterpret_cast<byte_type*>(cqe->user_data);
  buf_->value = buf;
  buffered_index_output::reset(buf, PAGE_SIZE);
  io_uring_cqe_seen(ring_, cqe);
}


void async_index_output::write_bytes(const byte_type* b, size_t length) {
  assert(pos_ <= end_);
  auto left = size_t(std::distance(pos_, end_));

  // is there enough space in the buffer?
  if (left > length) {
    // we add the data to the end of the buffer
    std::memcpy(pos_, b, length);
    pos_ += length;
  } else {
    // we fill/flush the buffer (until the input is written)
    size_t slice_pos_ = 0; // pos_ition in the input data

    while (slice_pos_ < length) {
      auto slice_len = std::min(length - slice_pos_, left);

      std::memcpy(pos_, b + slice_pos_, slice_len);
      slice_pos_ += slice_len;
      pos_ += slice_len;

      // if the buffer is full, flush it
      left -= slice_len;
      if (pos_ == end_) {
        flush();
        left = buf_size_;
      }
    }
  }
}

}

namespace iresearch {

// -----------------------------------------------------------------------------
// --SECTION--                                     mmap_directory implementation
// -----------------------------------------------------------------------------

async_directory::async_directory(const std::string& path)
  : mmap_directory(path) {
  // FIXME move out from ctor
  if (io_uring_queue_init(QUEUE_SIZE, &ring_, 0)) {
    throw not_supported();
  }

  void* mem = nullptr;
  constexpr size_t BUF_SIZE = NUM_PAGES*PAGE_SIZE;

  if (posix_memalign(&mem, PAGE_ALIGNEMNT, BUF_SIZE)) {
    throw std::bad_alloc();
  }

  buffer_.reset(static_cast<byte_type*>(mem));

  auto begin = buffer_.get();
  for (auto& page : pages_) {
    page.value = begin;
    free_pages_.push(page);
    begin += PAGE_SIZE;
  }

  struct iovec iovec;
  iovec.iov_base = mem;
  iovec.iov_len = BUF_SIZE;

  if (io_uring_register_buffers(&ring_, &iovec, 1)) {
    throw illegal_state();
  }
}

async_directory::~async_directory() {
  io_uring_queue_exit(&ring_);
}

index_output::ptr async_directory::create(
    const std::string& name) noexcept {
  utf8_path path;

  try {
    (path/=directory())/=name;
  } catch(...) {
    return nullptr;
  }

  return async_index_output::open(path.c_str(), ring_, free_pages_);
}

//bool async_directory::sync(const std::string& name) noexcept {
//  utf8_path path;
//
//  try {
//    (path/=directory())/=name;
//  } catch (...) {
//    return false;
//  }
//
//  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
//
//  if (!sqe) {
//    IR_FRMT_ERROR("Failed to get sqe, path: %s", path.utf8().c_str());
//  }
//
//  file_utils::handle_t handle(
//    file_utils::open(name, file_utils::OpenMode::Write, IR_FADVICE_NORMAL));
//
//  io_uring_prep_fsync()
//
//  return false;
//
//  io_uring_prep_write_fixed(sqe, handle_cast(handle_.get()), b, len, buffer_offset(), 0);
//  sqe->user_data = reinterpret_cast<uint64_t>(b);
//}

} // ROOT

