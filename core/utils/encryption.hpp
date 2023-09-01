////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#pragma once

#include "store/data_input.hpp"
#include "store/data_output.hpp"
#include "store/directory_attributes.hpp"
#include "utils/math_utils.hpp"
#include "utils/noncopyable.hpp"

namespace irs {

/// @brief initialize an encryption header and create corresponding cipher
/// stream
/// @returns true if cipher stream was initialized, false if encryption is not
///          appliclabe
/// @throws index_error in case of error on header or stream creation
bool encrypt(std::string_view filename, IndexOutput& out, encryption* enc,
             bstring& header, encryption::stream::ptr& cipher);

/// @brief create corresponding cipher stream from a specified encryption header
/// @returns true if cipher stream was initialized, false if encryption is not
///          appliclabe
/// @throws index_error in case of error on cipher stream creation
bool decrypt(std::string_view filename, index_input& in, encryption* enc,
             encryption::stream::ptr& cipher);

////////////////////////////////////////////////////////////////////////////////
///// @brief reasonable default value for a buffer serving encryption
////////////////////////////////////////////////////////////////////////////////
inline constexpr size_t kDefaultEncryptionBufferSize = 1024;

class EncryptedOutput final : public IndexOutput, util::noncopyable {
 public:
  EncryptedOutput(IndexOutput& out, encryption::stream& cipher,
                  size_t num_blocks);

  EncryptedOutput(IndexOutput::ptr&& out, encryption::stream& cipher,
                  size_t num_blocks);

  ~EncryptedOutput() final;

  IndexOutput::ptr Release() noexcept { return std::move(managed_out_); }

  size_t Position() const noexcept final { return offset_ + Length(); }

  void Flush() final { FlushBuffer(); }

  uint32_t Checksum() final {
    // FIXME do we need to calculate checksum over unencrypted data here?
    // That will slow down writes.
    // TODO(MBkkt) Flush?
    return out_->Checksum();
  }

  void Close() final {
    Flush();
    offset_ = 0;
    pos_ = buf_;
  }

 private:
  void WriteDirect(const byte_type* b, size_t len) final;

  IndexOutput::ptr managed_out_;
  IndexOutput* out_;
  encryption::stream* cipher_;
  size_t offset_{};
};

class encrypted_input : public buffered_index_input, util::noncopyable {
 public:
  encrypted_input(index_input& in, encryption::stream& cipher, size_t buf_size,
                  size_t padding = 0);

  encrypted_input(index_input::ptr&& in, encryption::stream& cipher,
                  size_t buf_size, size_t padding = 0);

  index_input::ptr dup() const final;

  index_input::ptr reopen() const final;

  size_t length() const noexcept final { return length_; }

  uint32_t checksum(size_t offset) const final;

  size_t buffer_size() const noexcept { return buf_size_; }

  const index_input& stream() const noexcept { return *in_; }

  index_input::ptr release() noexcept { return std::move(managed_in_); }

 protected:
  void seek_internal(size_t pos) final;

  size_t read_internal(byte_type* b, size_t count) final;

  encrypted_input(const encrypted_input& rhs, index_input::ptr&& in) noexcept;

 private:
  size_t buf_size_;
  std::unique_ptr<byte_type[]> buf_;
  index_input::ptr managed_in_;
  index_input* in_;
  encryption::stream* cipher_;
  const uint64_t start_;
  const size_t length_;
};

}  // namespace irs
