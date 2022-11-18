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
#include "utils/crc.hpp"
#include "utils/file_utils.hpp"
#include "utils/memory.hpp"
#include "utils/mmap_utils.hpp"
#include "utils/string_utils.hpp"

namespace {

using namespace irs;

constexpr size_t NUM_PAGES = 128;
constexpr size_t PAGE_SIZE = 4096;
constexpr size_t PAGE_ALIGNEMNT = 4096;

struct buffer_deleter {
  void operator()(byte_type* b) const noexcept { ::free(b); }
};

std::unique_ptr<byte_type, buffer_deleter> allocate() {
  constexpr size_t BUF_SIZE = NUM_PAGES * PAGE_SIZE;

  void* mem = nullptr;
  if (posix_memalign(&mem, PAGE_ALIGNEMNT, BUF_SIZE)) {
    throw std::bad_alloc();
  }

  return std::unique_ptr<byte_type, buffer_deleter>{
    static_cast<byte_type*>(mem)};
}

class segregated_buffer {
 public:
  using node_type = concurrent_stack<byte_type*>::node_type;

  segregated_buffer();

  node_type* pop() noexcept { return free_pages_.pop(); }

  void push(node_type& node) noexcept { free_pages_.push(node); }

  constexpr size_t size() const noexcept { return NUM_PAGES * PAGE_SIZE; }

  byte_type* data() const noexcept { return buffer_.get(); }

 private:
  std::unique_ptr<byte_type, buffer_deleter> buffer_;
  node_type pages_[NUM_PAGES];
  concurrent_stack<byte_type*> free_pages_;
};

segregated_buffer::segregated_buffer() : buffer_(allocate()) {
  auto begin = buffer_.get();
  for (auto& page : pages_) {
    page.value = begin;
    free_pages_.push(page);
    begin += PAGE_SIZE;
  }
}

class uring {
 public:
  uring(size_t queue_size, unsigned flags) {
    if (io_uring_queue_init(queue_size, &ring, flags)) {
      throw not_supported();
    }
  }

  ~uring() { io_uring_queue_exit(&ring); }

  void reg_buffer(byte_type* b, size_t size);

  size_t size() noexcept { return ring.sq.ring_sz; }

  io_uring_sqe* get_sqe() noexcept { return io_uring_get_sqe(&ring); }

  size_t sq_ready() noexcept { return io_uring_sq_ready(&ring); }

  void submit();
  bool deque(bool wait, uint64_t* data);

  io_uring ring;
};

void uring::reg_buffer(byte_type* b, size_t size) {
  struct iovec iovec;
  iovec.iov_base = b;
  iovec.iov_len = size;

  if (io_uring_register_buffers(&ring, &iovec, 1)) {
    throw illegal_state("failed to register buffer");
  }
}

void uring::submit() {
  int ret = io_uring_submit(&ring);
  if (ret < 0) {
    throw io_error(string_utils::to_string(
      "failed to submit write request, error %d", -ret));
  }
}

bool uring::deque(bool wait, uint64_t* data) {
  io_uring_cqe* cqe;
  const int ret =
    wait ? io_uring_wait_cqe(&ring, &cqe) : io_uring_peek_cqe(&ring, &cqe);

  if (ret < 0) {
    if (ret != -EAGAIN) {
      throw io_error(
        string_utils::to_string("failed to peek a request, error %d", -ret));
    }

    return false;
  }

  if (cqe->res < 0) {
    throw io_error(string_utils::to_string(
      "async i/o operation failed, error %d", -cqe->res));
  }

  *data = cqe->user_data;
  io_uring_cqe_seen(&ring, cqe);

  return true;
}

}  // namespace

namespace iresearch {

class async_file {
 public:
  async_file(size_t queue_size, unsigned flags) : ring_(queue_size, flags) {
    ring_.reg_buffer(buffer_.data(), buffer_.size());
  }

  io_uring_sqe* get_sqe();
  void submit() { return ring_.submit(); }
  void drain(bool wait);

  segregated_buffer::node_type* get_buffer();
  void release_buffer(segregated_buffer::node_type& node) noexcept {
    buffer_.push(node);
  }

 private:
  segregated_buffer buffer_;
  uring ring_;
};

async_file_builder::ptr async_file_builder::make(size_t queue_size,
                                                 unsigned flags) {
  return async_file_builder::ptr(new async_file(queue_size, flags));
}

void async_file_deleter::operator()(async_file* p) noexcept { delete p; }

segregated_buffer::node_type* async_file::get_buffer() {
  uint64_t data = 0;
  while (ring_.deque(false, &data) && !data)
    ;

  if (data) {
    return reinterpret_cast<segregated_buffer::node_type*>(data);
  }

  auto* node = buffer_.pop();

  if (node) {
    return node;
  }

  while (ring_.deque(true, &data) && !data)
    ;
  return reinterpret_cast<segregated_buffer::node_type*>(data);
}

io_uring_sqe* async_file::get_sqe() {
  io_uring_sqe* sqe = ring_.get_sqe();

  if (!sqe) {
    uint64_t data;

    if (!ring_.deque(false, &data)) {
      assert(ring_.sq_ready());
      ring_.submit();
      ring_.deque(true, &data);
    }

    sqe = ring_.get_sqe();

    if (data) {
      buffer_.push(*reinterpret_cast<segregated_buffer::node_type*>(&data));
    }
  }

  assert(sqe);
  return sqe;
}

void async_file::drain(bool wait) {
  io_uring_sqe* sqe = get_sqe();
  assert(sqe);

  io_uring_prep_nop(sqe);
  sqe->flags |= (IOSQE_IO_LINK | IOSQE_IO_DRAIN);
  sqe->user_data = 0;

  ring_.submit();

  if (!wait) {
    return;
  }

  uint64_t data;
  for (;;) {
    ring_.deque(true, &data);

    if (!data) {
      return;
    }

    buffer_.push(*reinterpret_cast<segregated_buffer::node_type*>(data));
  }
}

//////////////////////////////////////////////////////////////////////////////
/// @class async_index_output
//////////////////////////////////////////////////////////////////////////////
class async_index_output final : public index_output {
 public:
  DEFINE_FACTORY_INLINE(async_index_output);

  static index_output::ptr open(const file_path_t name,
                                async_file_ptr&& async) noexcept;

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
    auto reset = irs::make_finally([this]() noexcept {
      async_->release_buffer(*buf_);
      handle_.reset(nullptr);
    });

    flush();

    // FIXME(gnusi): we can avoid waiting here in case
    // if we'll keep track of all unsynced files
    async_->drain(true);
  }

  virtual int64_t checksum() const override {
    const_cast<async_index_output*>(this)->flush();
    return crc_.checksum();
  }

 private:
  using node_type = concurrent_stack<byte_type*>::node_type;

  async_index_output(async_file_ptr&& async,
                     file_utils::handle_t&& handle) noexcept
    : async_(std::move(async)),
      handle_(std::move(handle)),
      buf_(async_->get_buffer()) {
    reset(buf_->value);
  }

  // returns number of reamining bytes in the buffer
  FORCE_INLINE size_t remain() const { return std::distance(pos_, end_); }

  void reset(byte_type* buf) {
    pos_ = buf;
    end_ = buf + PAGE_SIZE;
  }

  async_file_ptr async_;
  file_utils::handle_t handle_;
  node_type* buf_;
  crc32c crc_;

  byte_type* pos_{};  // current position in the buffer
  byte_type* end_{};  // end of the valid bytes in the buffer
  size_t start_{};    // position of the buffer in file
};                    // async_index_output

/*static*/ index_output::ptr async_index_output::open(
  const file_path_t name, async_file_ptr&& async) noexcept {
  assert(name);

  if (!async) {
    return nullptr;
  }

  file_utils::handle_t handle(
    file_utils::open(name, file_utils::OpenMode::Write, IR_FADVICE_NORMAL));

  if (nullptr == handle) {
#ifdef _WIN32
    IR_FRMT_ERROR("Failed to open output file, error: %d, path: %s",
                  GetLastError(), std::filesystem::path{name}.c_str());
#else
    IR_FRMT_ERROR("Failed to open output file, error: %d, path: %s", errno,
                  std::filesystem::path{name}.c_str());
#endif

    return nullptr;
  }

  try {
    return index_output::make<async_index_output>(std::move(async),
                                                  std::move(handle));
  } catch (...) {
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

  io_uring_sqe* sqe = async_->get_sqe();

  io_uring_prep_write_fixed(sqe, handle_cast(handle_.get()), buf, size, start_,
                            0);
  sqe->user_data = reinterpret_cast<uint64_t>(buf_);

  async_->submit();

  start_ += size;
  crc_.process_bytes(buf_->value, size);

  buf_ = async_->get_buffer();

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
      io_uring_sqe* sqe = async_->get_sqe();
      io_uring_prep_write_fixed(sqe, handle_cast(handle_.get()), buf, PAGE_SIZE,
                                start_, 0);
      sqe->user_data = reinterpret_cast<uint64_t>(buf_);
    }
    length -= left;
    b += left;
    start_ += PAGE_SIZE;

    auto* borig = b;
    const size_t count = length / PAGE_SIZE;
    for (size_t i = count; i; --i) {
      buf_ = async_->get_buffer();
      assert(buf_);
      pos_ = buf_->value;
      std::memcpy(pos_, b, PAGE_SIZE);
      {
        auto* buf = buf_->value;
        io_uring_sqe* sqe = async_->get_sqe();
        io_uring_prep_write_fixed(sqe, handle_cast(handle_.get()), buf,
                                  PAGE_SIZE, start_, 0);
        sqe->user_data = reinterpret_cast<uint64_t>(buf_);
      }
      b += PAGE_SIZE;
      start_ += PAGE_SIZE;
    }

    async_->submit();

    const size_t processed = PAGE_SIZE * count;
    crc_.process_bytes(buf, PAGE_SIZE);
    crc_.process_bytes(borig, count * PAGE_SIZE);

    buf_ = async_->get_buffer();
    reset(buf_->value);

    length -= processed;
    std::memcpy(pos_, b, length);
    pos_ += length;
  }
}

}  // namespace iresearch

namespace iresearch {

// -----------------------------------------------------------------------------
// --SECTION--                                     mmap_directory implementation
// -----------------------------------------------------------------------------

async_directory::async_directory(std::string path, directory_attributes attrs,
                                 size_t pool_size, size_t queue_size,
                                 unsigned flags)
  : mmap_directory{std::move(path), std::move(attrs)},
    async_pool_{pool_size},
    queue_size_{queue_size},
    flags_{flags} {}

index_output::ptr async_directory::create(std::string_view name) noexcept {
  std::filesystem::path path;

  try {
    (path /= directory()) /= name;

    return async_index_output::open(path.c_str(),
                                    async_pool_.emplace(queue_size_, flags_));
  } catch (...) {
  }

  return nullptr;
}

bool async_directory::sync(std::span<std::string_view> names) noexcept {
  std::filesystem::path path;

  try {
    std::vector<file_utils::handle_t> handles(names.size());
    path /= directory();

    auto async = async_pool_.emplace(queue_size_, flags_);

    for (auto name = names.begin(); auto& handle : handles) {
      std::filesystem::path full_path(path);
      full_path /= (*name);
      ++name;

      const int fd = ::open(full_path.c_str(), O_WRONLY, S_IRWXU);

      if (fd < 0) {
        return false;
      }

      handle.reset(reinterpret_cast<void*>(fd));

      auto* sqe = async->get_sqe();

      while (!sqe) {
        async->submit();
        sqe = async->get_sqe();
      }

      io_uring_prep_fsync(sqe, fd, 0);
      sqe->user_data = 0;
    }

    // FIXME(gnusi): or submit one-by-one?
    async->submit();
    async->drain(true);
  } catch (...) {
    return false;
  }

  return true;
}

// bool async_directory::sync(std::string_view name) noexcept {
//   std::filesystem::path path;
//
//   try {
//     (path/=directory())/=name;
//   } catch (...) {
//     return false;
//   }
//
//   io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
//
//   if (!sqe) {
//     IR_FRMT_ERROR("Failed to get sqe, path: %s", path.utf8().c_str());
//   }
//
//   file_utils::handle_t handle(
//     file_utils::open(name, file_utils::OpenMode::Write, IR_FADVICE_NORMAL));
//
//   io_uring_prep_fsync()
//
//   return false;
//
//   io_uring_prep_write_fixed(sqe, handle_cast(handle_.get()), b, len,
//   buffer_offset(), 0); sqe->user_data = reinterpret_cast<uint64_t>(b);
// }

}  // namespace iresearch
