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

#pragma once

#include <mutex>
#include <shared_mutex>

#include "directory.hpp"
#include "resource_manager.hpp"
#include "store/directory_attributes.hpp"
#include "utils/async_utils.hpp"
#include "utils/attributes.hpp"
#include "utils/string.hpp"

#include <absl/container/flat_hash_map.h>

namespace irs {

class MemoryFile : public container_utils::raw_block_vector<16, 8> {
  // total number of levels and size of the first level 2^8
  using Base = container_utils::raw_block_vector<16, 8>;

 public:
  explicit MemoryFile(IResourceManager& rm) noexcept : Base{rm} {
    meta_.mtime = now();
  }

  MemoryFile(MemoryFile&& rhs) noexcept
    : Base{std::move(rhs)},
      meta_{rhs.meta_},
      len_{std::exchange(rhs.len_, 0)} {}

  MemoryFile& operator>>(DataOutput& out) {
    Visit([&](const byte_type* b, size_t len) {
      out.WriteBytes(b, len);
      return true;
    });
    return *this;
  }

  size_t Length() const noexcept { return len_; }

  void Length(size_t length) noexcept {
    len_ = length;
    meta_.mtime = now();
  }

  std::time_t mtime() const noexcept { return meta_.mtime; }

  void Reset() noexcept { len_ = 0; }

  void Clear() noexcept {
    Base::clear();
    Reset();
  }

  template<typename Visitor>
  bool Visit(const Visitor& visitor) {
    auto len = len_;
    for (const auto& buffer : buffers_) {
      if (!visitor(buffer.data, std::min(len, buffer.size))) {
        return false;
      }
      len -= buffer.size;
    }
    return true;
  }

 private:
  // metadata for a memory_file
  struct meta {
    std::time_t mtime;
  };

  static std::time_t now() noexcept {
    return std::chrono::system_clock::to_time_t(
      std::chrono::system_clock::now());
  }

  meta meta_;
  size_t len_{};
};

class memory_index_input final : public index_input {
 public:
  explicit memory_index_input(const MemoryFile& file) noexcept;

  index_input::ptr dup() const final;
  uint32_t checksum(size_t offset) const final;
  bool eof() const final;
  byte_type read_byte() final;
  const byte_type* read_buffer(size_t size, BufferHint hint) noexcept final;
  const byte_type* read_buffer(size_t offset, size_t size,
                               BufferHint hint) noexcept final;
  size_t read_bytes(byte_type* b, size_t len) final;
  size_t read_bytes(size_t offset, byte_type* b, size_t len) final {
    seek(offset);
    return read_bytes(b, len);
  }
  index_input::ptr reopen() const final;
  size_t length() const final;

  size_t Position() const final;

  void seek(size_t pos) final;

  int16_t read_short() final;
  int32_t read_int() final;
  int64_t read_long() final;
  uint32_t read_vint() final;
  uint64_t read_vlong() final;

  byte_type operator*() { return read_byte(); }
  memory_index_input& operator++() noexcept { return *this; }
  memory_index_input& operator++(int) noexcept { return *this; }

 private:
  memory_index_input(const memory_index_input&) = default;

  void switch_buffer(size_t pos);

  // returns number of reamining bytes in the buffer
  IRS_FORCE_INLINE size_t remain() const { return std::distance(begin_, end_); }

  const MemoryFile* file_;        // underline file
  const byte_type* buf_{};        // current buffer
  const byte_type* begin_{buf_};  // current position
  const byte_type* end_{buf_};    // end of the valid bytes
  size_t start_{};                // buffer offset in file
};

class MemoryIndexOutput : public IndexOutput {
 public:
  explicit MemoryIndexOutput(MemoryFile& file) noexcept;

  void Reset() noexcept {
    buf_ = pos_ = end_ = nullptr;
    offset_ = 0;
  }

  void Truncate(size_t pos) noexcept;

  size_t Position() const noexcept final { return offset_ + Length(); }

  void Flush() noexcept override { file_.Length(Position()); }

  uint32_t Checksum() override { throw not_supported{}; }

  void Close() noexcept override { Flush(); }

  void WriteBytes(const byte_type* b, size_t len) override;

 protected:
  void WriteDirect(const byte_type* b, size_t len) override;

  template<typename Writer>
  void WriteBytesImpl(const byte_type* b, size_t len, const Writer& writer);

  void NextBuffer();

  MemoryFile& file_;
  size_t offset_ = 0;
};

class MemoryDirectory final : public directory {
 public:
  explicit MemoryDirectory(
    directory_attributes attributes = directory_attributes{},
    const ResourceManagementOptions& rm = ResourceManagementOptions::kDefault);

  ~MemoryDirectory() noexcept final;

  directory_attributes& attributes() noexcept final { return attrs_; }

  IndexOutput::ptr create(std::string_view name) noexcept final;

  bool exists(bool& result, std::string_view name) const noexcept final;

  bool length(uint64_t& result, std::string_view name) const noexcept final;

  index_lock::ptr make_lock(std::string_view name) noexcept final;

  bool mtime(std::time_t& result, std::string_view name) const noexcept final;

  index_input::ptr open(std::string_view name,
                        IOAdvice advice) const noexcept final;

  bool remove(std::string_view name) noexcept final;

  bool rename(std::string_view src, std::string_view dst) noexcept final;

  bool sync(std::span<const std::string_view>) noexcept final { return true; }

  bool visit(const visitor_f& visitor) const final;

 private:
  friend class SingleInstanceLock;

  using FilesAllocator = ManagedTypedAllocator<
    std::pair<const std::string, std::unique_ptr<MemoryFile>>>;
  using FileMap = absl::flat_hash_map<
    std::string, std::unique_ptr<MemoryFile>,
    absl::container_internal::hash_default_hash<std::string>,
    absl::container_internal::hash_default_eq<std::string>,
    FilesAllocator>;  // unique_ptr because of rename
  using LockMap = absl::flat_hash_set<std::string>;

  directory_attributes attrs_;
  mutable std::shared_mutex flock_;
  std::mutex llock_;
  FileMap files_;
  LockMap locks_;
};

struct MemoryOutput {
  explicit MemoryOutput(IResourceManager& rm) noexcept : file{rm} {}

  MemoryOutput(MemoryOutput&& rhs) noexcept : file{std::move(rhs.file)} {}

  void Reset() noexcept {
    file.Reset();
    stream.Reset();
  }

  MemoryFile file;
  MemoryIndexOutput stream{file};
};

}  // namespace irs
