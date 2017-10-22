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
#include "utils/mmap_utils.hpp"

NS_LOCAL

using irs::mmap_utils::mmap_handle;

typedef std::shared_ptr<mmap_handle> mmap_handle_ptr;

//////////////////////////////////////////////////////////////////////////////
/// @brief converts the specified IOAdvice to corresponding posix madvice
//////////////////////////////////////////////////////////////////////////////
inline int get_posix_advice(irs::IOAdvice advice) {
  switch (uint32_t(advice)) {
    case uint32_t(irs::IOAdvice::NORMAL):
      return IR_MADVISE_NORMAL;
    case uint32_t(irs::IOAdvice::SEQUENTIAL):
      return IR_MADVISE_SEQUENTIAL;
    case uint32_t(irs::IOAdvice::RANDOM):
      return IR_MADVISE_RANDOM;
    case uint32_t(irs::IOAdvice::READONCE):
      return IR_MADVISE_NORMAL;
    case uint32_t(irs::IOAdvice::SEQUENTIAL | irs::IOAdvice::READONCE):
      return IR_MADVISE_SEQUENTIAL;
    case uint32_t(irs::IOAdvice::RANDOM | irs::IOAdvice::READONCE):
      return IR_MADVISE_RANDOM;
  }

  IR_FRMT_ERROR(
    "madvice '%d' is not valid (RANDOM|SEQUENTIAL), fallback to NORMAL",
    uint32_t(advice)
  );

  return IR_MADVISE_NORMAL;
}

//////////////////////////////////////////////////////////////////////////////
/// @struct mmap_index_input
/// @brief input stream for memory mapped directory
//////////////////////////////////////////////////////////////////////////////
class mmap_index_input : public irs::bytes_ref_input {
 public:
  static irs::index_input::ptr open(
      const file_path_t file,
      irs::IOAdvice advice) NOEXCEPT {
    assert(file);

    mmap_handle_ptr handle;

    try {
      handle = std::make_shared<mmap_handle>();
    } catch (...) {
      IR_EXCEPTION();
      return nullptr;
    }

    if (!handle->open(file)) {
      IR_FRMT_ERROR("Failed to open mmapped input file, path: " IR_FILEPATH_SPECIFIER, file);
      return nullptr;
    }

    const int padvice = get_posix_advice(advice);

    if (IR_MADVISE_NORMAL != padvice && !handle->advise(padvice)) {
      IR_FRMT_ERROR("Failed to madvise input file, path: " IR_FILEPATH_SPECIFIER ", error %d", file, errno);
    }

    return mmap_index_input::make<mmap_index_input>(
      std::move(handle), bool(advice & irs::IOAdvice::READONCE)
    );
  }

  virtual ptr dup() const NOEXCEPT override {
    return mmap_index_input::make<mmap_index_input>(*this);
  }

  virtual ptr reopen() const NOEXCEPT override {
    return dup();
  }

 private:
  DECLARE_FACTORY(index_input);

  mmap_index_input(mmap_handle_ptr&& handle, bool readonce) NOEXCEPT
    : handle_(std::move(handle)), readonce_(readonce) {
    if (handle_) {
      const auto* begin = reinterpret_cast<irs::byte_type*>(handle_->addr());
      bytes_ref_input::reset(begin, handle_->size());
    }
  }

  mmap_index_input(const mmap_index_input& rhs) NOEXCEPT
    : bytes_ref_input(rhs),
      handle_(rhs.handle_) {
  }

  mmap_index_input& operator=(const mmap_index_input&) = delete;

  ~mmap_index_input() {
    if (readonce_) {
      handle_->advise(IR_MADVICE_DONTNEED);
    }
  }

  mmap_handle_ptr handle_;
  bool readonce_;
}; // mmap_index_input

NS_END // LOCAL

NS_ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                     mmap_directory implementation
// -----------------------------------------------------------------------------

mmap_directory::mmap_directory(const std::string& path)
  : fs_directory(path) {
}

index_input::ptr mmap_directory::open(
    const std::string& name,
    IOAdvice advice) const NOEXCEPT {
  utf8_path path;

  try {
    (path/=directory())/=name;
  } catch(...) {
    IR_EXCEPTION();
    return nullptr;
  }

  return mmap_index_input::open(path.c_str(), advice);
}

NS_END // ROOT
