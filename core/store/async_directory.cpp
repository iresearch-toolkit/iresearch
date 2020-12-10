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

namespace iresearch {

constexpr size_t PAGE_SIZE = 4096;
constexpr size_t PAGE_ALIGNEMNT = 4096;
constexpr size_t QUEUE_SIZE = 1024;

//////////////////////////////////////////////////////////////////////////////
/// @class async_index_output
//////////////////////////////////////////////////////////////////////////////
class async_index_output final : public index_output {
 public:
  DEFINE_FACTORY_INLINE(async_index_output);

  static index_output::ptr open(
    const file_path_t name,
    async_directory& dir) noexcept;

  virtual void write_int(int32_t value) override;
  virtual void write_long(int64_t value) override;
  virtual void write_vint(uint32_t v) override;
  virtual void write_vlong(uint64_t v) override;
  virtual void write_byte(byte_type b) override;
  virtual void write_bytes(const byte_type* b, size_t length) override;
  virtual size_t file_pointer() const override {
    assert(buf_->value <= pos_);
    return start_ + size_t(std::distance(buf_->value, pos_));
  }
  virtual void flush() override;

  virtual void close() override {
    auto reset = irs::make_finally([this](){
      dir_->free_pages_.push(*buf_);
      handle_.reset(nullptr);
    });

    flush();
  }

  virtual int64_t checksum() const override {
    const_cast<async_index_output*>(this)->flush();
    return crc_.checksum();
  }

 private:
  using node_type = concurrent_stack<byte_type*>::node_type;

  async_index_output(
      async_directory& dir,
      file_utils::handle_t&& handle,
      concurrent_stack<byte_type*>::node_type* buf) noexcept
    : dir_(&dir),
      buf_(buf),
      handle_(std::move(handle)) {
    reset(buf->value);
  }

  // returns number of reamining bytes in the buffer
  FORCE_INLINE size_t remain() const {
    return std::distance(pos_, end_);
  }

  void reset(byte_type* buf) {
    pos_ = buf;
    end_ = buf + PAGE_SIZE;
  }

  async_directory* dir_;
  node_type* buf_;
  file_utils::handle_t handle_;
  crc32c crc_;

  byte_type* pos_{ }; // current position in the buffer
  byte_type* end_{ }; // end of the valid bytes in the buffer
  size_t start_{}; // position of the buffer in file
}; // async_index_output

/*static*/ index_output::ptr async_index_output::open(
    const file_path_t name, async_directory& dir) noexcept {
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

  auto* buf = dir.get_page();

  if (!buf) {
    // FIXME
    return nullptr;
  }

  try {
    return index_output::make<async_index_output>(dir, std::move(handle), buf);
  } catch(...) {
  }

  return nullptr;
}

void async_index_output::write_int(int32_t value) {
  if (remain() < sizeof(uint32_t)) {
    irs::write<uint32_t>(*this, value);
  } else {
    irs::write<uint32_t>(pos_, value);
  }
}

void async_index_output::write_long(int64_t value) {
  if (remain() < sizeof(uint64_t)) {
    irs::write<uint64_t>(*this, value);
  } else {
    irs::write<uint64_t>(pos_, value);
  }
}

void async_index_output::write_vint(uint32_t v) {
  if (remain() < bytes_io<uint32_t>::const_max_vsize) {
    irs::vwrite<uint32_t>(*this, v);
  } else {
    irs::vwrite<uint32_t>(pos_, v);
  }
}

void async_index_output::write_vlong(uint64_t v) {
  if (remain() < bytes_io<uint64_t>::const_max_vsize) {
    irs::vwrite<uint64_t>(*this, v);
  } else {
    irs::vwrite<uint64_t>(pos_, v);
  }
}

void async_index_output::write_byte(byte_type b) {
  if (pos_ >= end_) {
    flush();
  }

  *pos_++ = b;
}

void async_index_output::flush() {
  assert(handle_);

  auto* buf = buf_->value;
  const auto size = size_t(std::distance(buf, pos_));

  if (!size) {
    return;
  }

  io_uring_sqe* sqe = dir_->get_sqe();

  io_uring_prep_write_fixed(
    sqe, handle_cast(handle_.get()),
    buf, size, start_, 0);
  sqe->user_data = reinterpret_cast<uint64_t>(buf_);

  dir_->submit();

  start_ += size;
  crc_.process_bytes(buf_->value, size);

  buf_ = dir_->get_buffer();

  assert(buf_);
  reset(buf_->value);
}

void async_index_output::write_bytes(const byte_type* b, size_t length) {
  assert(pos_ <= end_);
  auto left = size_t(std::distance(pos_, end_));

  if (left > length) {
    std::memcpy(pos_, b, length);
    pos_ += length;
  } else {
    auto* buf = buf_->value;

    std::memcpy(pos_, b, left);
    pos_ += left;
    {
      auto* buf = buf_->value;
      io_uring_sqe* sqe = dir_->get_sqe();
      io_uring_prep_write_fixed(
        sqe, handle_cast(handle_.get()),
        buf, PAGE_SIZE, start_, 0);
      sqe->user_data = reinterpret_cast<uint64_t>(buf_);
    }
    length -= left;
    b += left;
    start_ += PAGE_SIZE;

    auto* borig = b;
    const size_t count = length / PAGE_SIZE;
    for (size_t i = count; i; --i) {
      buf_ = dir_->get_buffer();
      assert(buf_);
      pos_ = buf_->value;
      std::memcpy(pos_, b, PAGE_SIZE);
      {
        auto* buf = buf_->value;
        io_uring_sqe* sqe = dir_->get_sqe();
        io_uring_prep_write_fixed(
          sqe, handle_cast(handle_.get()),
          buf, PAGE_SIZE, start_, 0);
        sqe->user_data = reinterpret_cast<uint64_t>(buf_);
      }
      b += PAGE_SIZE;
      start_ += PAGE_SIZE;
    }

    dir_->submit();

    const size_t processed = PAGE_SIZE*count;
    crc_.process_bytes(buf, PAGE_SIZE);
    crc_.process_bytes(borig, count*PAGE_SIZE);

    buf_ = dir_->get_buffer();
    reset(buf_->value);

    length -= processed;
    std::memcpy(pos_, b, length);
    pos_ += length;
  }
}

}

namespace iresearch {

// -----------------------------------------------------------------------------
// --SECTION--                                     mmap_directory implementation
// -----------------------------------------------------------------------------

async_directory::async_directory(const std::string& path)
  : mmap_directory(path),
    ring_(QUEUE_SIZE, 0) {
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

  if (io_uring_register_buffers(&ring_.ring, &iovec, 1)) {
    throw illegal_state();
  }
}

index_output::ptr async_directory::create(
    const std::string& name) noexcept {
  utf8_path path;

  try {
    (path/=directory())/=name;
  } catch(...) {
    return nullptr;
  }

  return async_index_output::open(path.c_str(), *this);
}

io_uring_sqe* async_directory::get_sqe() {
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_.ring);

  while (!sqe) {
    void* data = deque(true);
    sqe = io_uring_get_sqe(&ring_.ring);

    if (data) {
      free_pages_.push(*reinterpret_cast<node_type*>(data));
    }
  }

  return sqe;
}

void* async_directory::deque(bool wait) {
  io_uring_cqe* cqe;
  const int ret = wait
    ? io_uring_wait_cqe(&ring_.ring, &cqe)
    : io_uring_peek_cqe(&ring_.ring, &cqe);

  if (ret < 0) {
    if (ret != -EAGAIN) {
      throw io_error(string_utils::to_string(
        "failed to peek a request, error %d", -ret));
    }

    return nullptr;
  }

  if (cqe->res < 0) {
    throw io_error(string_utils::to_string(
      "async i/o operation failed, error %d", -cqe->res));
  }

  void* data = io_uring_cqe_get_data(cqe);
  io_uring_cqe_seen(&ring_.ring, cqe);

  return data;
}

async_directory::node_type* async_directory::get_buffer() {
  void* data = deque(false);

  while (!data) {
    data = free_pages_.pop();

    if (data) {
      break;
    }

    data = deque(true);
  }

  return reinterpret_cast<node_type*>(data);
}

void async_directory::submit() {
  int ret = io_uring_submit(&ring_.ring);
  if (ret < 0) {
    throw io_error(string_utils::to_string(
      "failed to submit write request, error %d", -ret));
  }
}

void async_directory::drain() {
  io_uring_sqe* sqe = get_sqe();
  assert(sqe);

  io_uring_prep_nop(sqe);
  sqe->flags |= (IOSQE_IO_LINK | IOSQE_IO_DRAIN);
  sqe->user_data = 0;

  submit();

  // FIXME how to exit properly?
  for (;;) {
    void* data = deque(true);

    if (!data) {
      break;
    }

    free_pages_.push(*reinterpret_cast<node_type*>(data));
  }
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

