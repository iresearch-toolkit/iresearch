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

#include "store/mmap_directory.hpp"

#include "store/store_utils.hpp"
#include "utils/memory.hpp"
#include "utils/mmap_utils.hpp"

namespace iresearch {
namespace {

using mmap_utils::mmap_handle;

// Converts the specified IOAdvice to corresponding posix madvice
inline int get_posix_madvice(IOAdvice advice) {
  switch (advice) {
    case IOAdvice::NORMAL:
    case IOAdvice::DIRECT_READ:
      return IR_MADVICE_NORMAL;
    case IOAdvice::SEQUENTIAL:
      return IR_MADVICE_SEQUENTIAL;
    case IOAdvice::RANDOM:
      return IR_MADVICE_RANDOM;
    case IOAdvice::READONCE:
      return IR_MADVICE_NORMAL;
    case IOAdvice::READONCE_SEQUENTIAL:
      return IR_MADVICE_SEQUENTIAL;
    case IOAdvice::READONCE_RANDOM:
      return IR_MADVICE_RANDOM;
  }

  IR_FRMT_ERROR(
    "madvice '%d' is not valid (RANDOM|SEQUENTIAL), fallback to NORMAL",
    uint32_t(advice));

  return IR_MADVICE_NORMAL;
}

std::shared_ptr<mmap_handle> OpenHandle(const file_path_t file,
                                        IOAdvice advice) noexcept {
  assert(file);

  std::shared_ptr<mmap_handle> handle;

  try {
    handle = std::make_shared<mmap_handle>();
  } catch (...) {
    return nullptr;
  }

  if (!handle->open(file)) {
    IR_FRMT_ERROR(
      "Failed to open mmapped input file, path: " IR_FILEPATH_SPECIFIER, file);
    return nullptr;
  }

  if (0 == handle->size()) {
    return handle;
  }

  const int padvice = get_posix_madvice(advice);

  if (IR_MADVICE_NORMAL != padvice && !handle->advise(padvice)) {
    IR_FRMT_ERROR("Failed to madvise input file, path: " IR_FILEPATH_SPECIFIER
                  ", error %d",
                  file, errno);
  }

  handle->dontneed(bool(advice & IOAdvice::READONCE));

  return handle;
}

std::shared_ptr<mmap_handle> OpenHandle(const std::filesystem::path& dir,
                                        std::string_view name,
                                        IOAdvice advice) noexcept {
  try {
    const auto path = dir / name;

    return OpenHandle(path.c_str(), advice);
  } catch (...) {
  }

  return nullptr;
}

// Input stream for memory mapped directory
class mmap_index_input final : public bytes_view_input {
 public:
  explicit mmap_index_input(std::shared_ptr<mmap_handle>&& handle) noexcept
    : handle_{std::move(handle)} {
    if (IRS_LIKELY(handle_ && handle_->size())) {
      assert(handle_->addr() != MAP_FAILED);
      const auto* begin = reinterpret_cast<byte_type*>(handle_->addr());
      bytes_view_input::reset(begin, handle_->size());
    } else {
      handle_.reset();
    }
  }

  mmap_index_input(const mmap_index_input& rhs) noexcept
    : bytes_view_input{rhs}, handle_{rhs.handle_} {}

  ptr dup() const override { return std::make_unique<mmap_index_input>(*this); }

  ptr reopen() const override { return dup(); }

 private:
  mmap_index_input& operator=(const mmap_index_input&) = delete;

  std::shared_ptr<mmap_utils::mmap_handle> handle_;
};

}  // namespace

MMapDirectory::MMapDirectory(std::filesystem::path path,
                             directory_attributes attrs)
  : FSDirectory{std::move(path), std::move(attrs)} {}

index_input::ptr MMapDirectory::open(std::string_view name,
                                     IOAdvice advice) const noexcept {
  if (IOAdvice::DIRECT_READ == (advice & IOAdvice::DIRECT_READ)) {
    return FSDirectory::open(name, advice);
  }

  auto handle = OpenHandle(directory(), name, advice);

  if (!handle) {
    return nullptr;
  }

  try {
    return std::make_unique<mmap_index_input>(std::move(handle));
  } catch (...) {
  }

  return nullptr;
}

bool CachingMMapDirectory::length(uint64_t& result,
                                  std::string_view name) const noexcept {
  if (cache_.Visit(name, [&](const auto& cached) noexcept {
        result = cached->size();
        return true;
      })) {
    return true;
  }

  return MMapDirectory::length(result, name);
}

index_input::ptr CachingMMapDirectory::open(std::string_view name,
                                            IOAdvice advice) const noexcept {
  if (IOAdvice::DIRECT_READ == (advice & IOAdvice::DIRECT_READ)) {
    return FSDirectory::open(name, advice);
  }

  std::shared_ptr<mmap_utils::mmap_handle> handle;

  auto make_stream = [&]() noexcept -> index_input::ptr {
    if (!handle) {
      return nullptr;
    }

    try {
      return std::make_unique<mmap_index_input>(std::move(handle));
    } catch (...) {
    }

    return nullptr;
  };

  if (cache_.Visit(name, [&](const auto& cached) noexcept {
        handle = cached;
        return handle != nullptr;
      })) {
    return make_stream();
  }

  if (handle = OpenHandle(directory(), name, advice); handle) {
    cache_.Put(name, advice, [&]() noexcept { return handle; });
  }

  return make_stream();
}

}  // namespace iresearch
