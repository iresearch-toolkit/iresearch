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

#include "memory_directory.hpp"

#include <algorithm>
#include <cstring>

#include "error/error.hpp"
#include "shared.hpp"
#include "utils/assert.hpp"
#include "utils/bytes_utils.hpp"
#include "utils/crc.hpp"
#include "utils/directory_utils.hpp"
#include "utils/log.hpp"
#include "utils/numeric_utils.hpp"
#include "utils/std.hpp"
#include "utils/string.hpp"
#include "utils/thread_utils.hpp"

namespace irs {

class SingleInstanceLock : public index_lock {
 public:
  SingleInstanceLock(std::string_view name, MemoryDirectory* parent)
    : name{name}, parent{parent} {
    IRS_ASSERT(parent);
  }

  bool lock() final {
    std::lock_guard lock{parent->llock_};
    return parent->locks_.insert(name).second;
  }

  bool is_locked(bool& result) const noexcept final {
    std::lock_guard lock{parent->llock_};
    result = parent->locks_.contains(name);
    return true;
  }

  bool unlock() noexcept final {
    std::lock_guard lock{parent->llock_};
    return parent->locks_.erase(name) != 0;
  }

 private:
  std::string name;
  MemoryDirectory* parent;
};

memory_index_input::memory_index_input(const MemoryFile& file) noexcept
  : file_{&file} {}

index_input::ptr memory_index_input::dup() const {
  return ptr(new memory_index_input(*this));
}

uint32_t memory_index_input::checksum(size_t offset) const {
  if (!file_->Length()) {
    return 0;
  }

  crc32c crc;

  auto buffer_idx = file_->buffer_offset(Position());
  size_t to_process;

  // process current buffer if exists
  if (begin_) {
    to_process = std::min(offset, remain());
    crc.process_bytes(begin_, to_process);
    offset -= to_process;
    ++buffer_idx;
  }

  // process intermediate buffers
  auto last_buffer_idx = file_->buffer_count();

  if (last_buffer_idx) {
    --last_buffer_idx;
  }

  for (; offset && buffer_idx < last_buffer_idx; ++buffer_idx) {
    auto& buf = file_->get_buffer(buffer_idx);
    to_process = std::min(offset, buf.size);
    crc.process_bytes(buf.data, to_process);
    offset -= to_process;
  }

  // process the last buffer
  if (offset && buffer_idx == last_buffer_idx) {
    auto& buf = file_->get_buffer(last_buffer_idx);
    to_process = std::min(offset, file_->Length() - buf.offset);
    crc.process_bytes(buf.data, to_process);
  }

  return crc.checksum();
}

bool memory_index_input::eof() const { return Position() >= file_->Length(); }

index_input::ptr memory_index_input::reopen() const {
  return dup();  // memory_file pointers are thread-safe
}

void memory_index_input::switch_buffer(size_t pos) {
  auto idx = file_->buffer_offset(pos);
  IRS_ASSERT(idx < file_->buffer_count());
  auto& buf = file_->get_buffer(idx);

  if (buf.data != buf_) {
    buf_ = buf.data;
    start_ = buf.offset;
    end_ = buf_ + std::min(buf.size, file_->Length() - start_);
  }

  IRS_ASSERT(start_ <= pos && pos < start_ + std::distance(buf_, end_));
  begin_ = buf_ + (pos - start_);
}

size_t memory_index_input::length() const { return file_->Length(); }

size_t memory_index_input::Position() const {
  return start_ + std::distance(buf_, begin_);
}

void memory_index_input::seek(size_t pos) {
  // allow seeking past eof(), set to eof
  if (pos >= file_->Length()) {
    buf_ = nullptr;
    begin_ = nullptr;
    start_ = file_->Length();
    return;
  }

  switch_buffer(pos);
}

const byte_type* memory_index_input::read_buffer(size_t offset, size_t size,
                                                 BufferHint /*hint*/) noexcept {
  const auto idx = file_->buffer_offset(offset);
  IRS_ASSERT(idx < file_->buffer_count());
  const auto& buf = file_->get_buffer(idx);
  const auto begin = buf.data + offset - buf.offset;
  const auto end = begin + size;
  const auto buf_end =
    buf.data + std::min(buf.size, file_->Length() - buf.offset);

  if (end <= buf_end) {
    buf_ = buf.data;
    begin_ = end;
    end_ = buf_end;
    start_ = buf.offset;
    return begin;
  }

  return nullptr;
}

const byte_type* memory_index_input::read_buffer(size_t size,
                                                 BufferHint /*hint*/) noexcept {
  const auto* begin = begin_ + size;

  if (begin > end_) {
    return nullptr;
  }

  std::swap(begin, begin_);
  return begin;
}

byte_type memory_index_input::read_byte() {
  if (begin_ >= end_) {
    switch_buffer(Position());
  }

  return *begin_++;
}

size_t memory_index_input::read_bytes(byte_type* b, size_t left) {
  const size_t bytes_left = left;  // initial length
  while (left) {
    if (begin_ >= end_) {
      if (eof()) {
        break;
      }
      switch_buffer(Position());
    }

    size_t copied = std::min(size_t(std::distance(begin_, end_)), left);
    std::memcpy(b, begin_, sizeof(byte_type) * copied);

    left -= copied;
    begin_ += copied;
    b += copied;
  }
  return bytes_left - left;
}

int16_t memory_index_input::read_short() {
  return remain() < sizeof(uint16_t) ? data_input::read_short()
                                     : irs::read<uint16_t>(begin_);
}

int32_t memory_index_input::read_int() {
  return remain() < sizeof(uint32_t) ? irs::read<uint32_t>(*this)
                                     : irs::read<uint32_t>(begin_);
}

int64_t memory_index_input::read_long() {
  return remain() < sizeof(uint64_t) ? irs::read<uint64_t>(*this)
                                     : irs::read<uint64_t>(begin_);
}

uint32_t memory_index_input::read_vint() {
  return remain() < bytes_io<uint32_t>::const_max_vsize
           ? irs::vread<uint32_t>(*this)
           : irs::vread<uint32_t>(begin_);
}

uint64_t memory_index_input::read_vlong() {
  return remain() < bytes_io<uint64_t>::const_max_vsize
           ? irs::vread<uint64_t>(*this)
           : irs::vread<uint64_t>(begin_);
}

void MemoryIndexOutput::NextBuffer() {
  IRS_ASSERT(Remain() == 0);
  auto idx = file_.buffer_offset(Position());
  auto buf =
    idx < file_.buffer_count() ? file_.get_buffer(idx) : file_.push_buffer();
  offset_ = buf.offset;
  buf_ = buf.data;
  pos_ = buf_;
  end_ = buf_ + buf.size;
}

template<typename Writer>
void MemoryIndexOutput::WriteBytesImpl(const byte_type* b, size_t len,
                                       const Writer& writer) {
  for (size_t to_copy = 0; len != 0; len -= to_copy) {
    if (Remain() == 0) {
      NextBuffer();
    }
    to_copy = std::min(Remain(), len);
    writer(b, to_copy);
    b += to_copy;
    pos_ += to_copy;
  }
}

MemoryIndexOutput::MemoryIndexOutput(MemoryFile& file) noexcept
  : IndexOutput{nullptr, nullptr}, file_{file} {}

void MemoryIndexOutput::Truncate(size_t pos) noexcept {
  auto idx = file_.buffer_offset(pos);
  IRS_ASSERT(idx < file_.buffer_count());
  auto buf = file_.get_buffer(idx);
  offset_ = buf.offset;
  buf_ = buf.data;
  pos_ = buf_ + pos - offset_;
  end_ = buf_ + buf.size;
}

void MemoryIndexOutput::WriteBytes(const byte_type* b, size_t len) {
  WriteBytesImpl(b, len, [pos = pos_](const byte_type* b, size_t len) {
    std::memcpy(pos, b, len);
  });
}

void MemoryIndexOutput::WriteDirect(const byte_type* /*b*/, size_t /*len*/) {
  IRS_ASSERT(Remain() == 0);
  NextBuffer();
}

class ChecksumMemoryIndexOutput final : public MemoryIndexOutput {
 public:
  explicit ChecksumMemoryIndexOutput(MemoryFile& file) noexcept
    : MemoryIndexOutput{file} {}

  void Flush() noexcept final {
    const auto prev = file_.Length();
    const auto curr = Position();
    file_.Length(curr);
    crc_.process_bytes(buf_ + (prev - offset_), curr - prev);
  }

  uint32_t Checksum() noexcept final {
    Flush();
    return crc_.checksum();
  }

  void Close() noexcept final { Flush(); }

 private:
  void WriteBytes(const byte_type* b, size_t len) final {
    WriteBytesImpl(b, len, [&](const byte_type* b, size_t len) {
      std::memcpy(pos_, b, len);
      crc_.process_bytes(b, len);
    });
  }

  void WriteDirect(const byte_type* b, size_t len) final {
    crc_.process_bytes(buf_, end_ - buf_);
    NextBuffer();
  }

 private:
  crc32c crc_;
};

MemoryDirectory::MemoryDirectory(directory_attributes attrs,
                                 const ResourceManagementOptions& rm)
  : attrs_{std::move(attrs)}, files_{FilesAllocator{*rm.readers}} {}

MemoryDirectory::~MemoryDirectory() noexcept {
  std::lock_guard lock{flock_};

  files_.clear();
}

bool MemoryDirectory::exists(bool& result,
                             std::string_view name) const noexcept {
  std::shared_lock lock{flock_};

  result = files_.contains(name);

  return true;
}

IndexOutput::ptr MemoryDirectory::create(std::string_view name) noexcept try {
  std::lock_guard lock{flock_};

  auto it = files_.lazy_emplace(name, [&](const auto& ctor) {
    ctor(new MemoryFile{files_.get_allocator().ResourceManager()});
  });

  it->second->Reset();

  return IndexOutput::ptr{new ChecksumMemoryIndexOutput{*it->second}};
} catch (...) {
  return nullptr;
}

bool MemoryDirectory::length(uint64_t& result,
                             std::string_view name) const noexcept {
  std::shared_lock lock{flock_};

  const auto it = files_.find(name);

  if (it == files_.end()) {
    return false;
  }

  result = it->second->Length();

  return true;
}

index_lock::ptr MemoryDirectory::make_lock(std::string_view name) noexcept try {
  return index_lock::ptr{new SingleInstanceLock{name, this}};
} catch (...) {
  IRS_ASSERT(false);
  return nullptr;
}

bool MemoryDirectory::mtime(std::time_t& result,
                            std::string_view name) const noexcept {
  std::shared_lock lock{flock_};
  const auto it = files_.find(name);
  if (it == files_.end()) {
    return false;
  }
  result = it->second->mtime();
  return true;
}

index_input::ptr MemoryDirectory::open(std::string_view name,
                                       IOAdvice /*advice*/) const noexcept try {
  std::shared_lock lock{flock_};
  const auto it = files_.find(name);
  if (it != files_.end()) {
    return std::make_unique<memory_index_input>(*it->second);
  }
  IRS_LOG_ERROR(absl::StrCat(
    "Failed to open input file, error: File not found, path: ", name));
  return nullptr;
} catch (...) {
  IRS_LOG_ERROR(absl::StrCat("Failed to open input file, path: ", name));
  return nullptr;
}

bool MemoryDirectory::remove(std::string_view name) noexcept {
  std::lock_guard lock{flock_};
  return files_.erase(name) != 0;
}

bool MemoryDirectory::rename(std::string_view src,
                             std::string_view dst) noexcept {
  try {
    std::lock_guard lock{flock_};

    const auto res = files_.try_emplace(dst);
    auto it = files_.find(src);

    if (IRS_LIKELY(it != files_.end())) {
      if (IRS_LIKELY(it != res.first)) {
        res.first->second = std::move(it->second);
        files_.erase(it);
      }
      return true;
    }

    if (res.second) {
      files_.erase(res.first);
    }
  } catch (...) {
  }

  return false;
}

bool MemoryDirectory::visit(const directory::visitor_f& visitor) const {
  std::vector<std::string> files;
  // take a snapshot of existing files in directory
  // to avoid potential recursive read locks in visitor
  std::shared_lock lock{flock_};
  files.reserve(files_.size());
  for (auto& entry : files_) {
    files.emplace_back(entry.first);  // cppcheck-suppress useStlAlgorithm
  }
  lock.unlock();
  for (auto& file : files) {
    if (!visitor(file)) {
      return false;
    }
  }
  return true;
}

}  // namespace irs
