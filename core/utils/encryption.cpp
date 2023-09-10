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

#include "encryption.hpp"

#include "error/error.hpp"
#include "store/data_output.hpp"
#include "store/directory_attributes.hpp"
#include "store/store_utils.hpp"
#include "utils/bytes_utils.hpp"
#include "utils/crc.hpp"
#include "utils/misc.hpp"

#include <absl/strings/str_cat.h>

namespace irs {

bool encrypt(std::string_view filename, IndexOutput& out, encryption* enc,
             bstring& header, encryption::stream::ptr& cipher) {
  header.resize(enc ? enc->header_length() : 0);

  if (header.empty()) {
    // no encryption
    irs::WriteStr(out, header);
    return false;
  }

  IRS_ASSERT(enc);

  if (!enc->create_header(filename, header.data())) {
    throw index_error{absl::StrCat(
      "failed to initialize encryption header, path '", filename, "'")};
  }

  // header is encrypted here
  irs::WriteStr(out, header);

  cipher = enc->create_stream(filename, header.data());

  if (!cipher) {
    throw index_error{absl::StrCat(
      "Failed to instantiate encryption stream, path '", filename, "'")};
  }

  if (!cipher->block_size()) {
    throw index_error{absl::StrCat(
      "Failed to instantiate encryption stream with block of size 0, path '",
      filename, "'")};
  }

  // header is decrypted here, write checksum
  crc32c crc;
  crc.process_bytes(header.c_str(), header.size());
  out.WriteV64(crc.checksum());

  return true;
}

bool decrypt(std::string_view filename, index_input& in, encryption* enc,
             encryption::stream::ptr& cipher) {
  auto header = irs::read_string<bstring>(in);

  if (header.empty()) {
    // no encryption
    return false;
  }

  if (!enc) {
    throw index_error{absl::StrCat(
      "Failed to open encrypted file without cipher, path '", filename, "'")};
  }

  if (header.size() != enc->header_length()) {
    throw index_error{
      absl::StrCat("failed to open encrypted file, expect encryption header of "
                   "size ",
                   enc->header_length(), ", got ", header.size(), ", path '",
                   filename, "'")};
  }

  cipher = enc->create_stream(filename, header.data());

  if (!cipher) {
    throw index_error{
      absl::StrCat("Failed to open encrypted file, path '", filename, "'")};
  }

  const auto block_size = cipher->block_size();

  if (!block_size) {
    throw index_error{
      absl::StrCat("Invalid block size 0 specified for encrypted file, path '",
                   filename, "'")};
  }

  // header is decrypted here, check checksum
  crc32c crc;
  crc.process_bytes(header.c_str(), header.size());

  if (crc.checksum() != in.read_vlong()) {
    throw index_error{
      absl::StrCat("Invalid ecryption header, path '", filename, "'")};
  }

  return true;
}

EncryptedOutput::EncryptedOutput(IndexOutput& out, encryption::stream& cipher,
                                 size_t num_blocks)
  : IndexOutput{nullptr, nullptr}, out_{&out}, cipher_{&cipher} {
  IRS_ASSERT(num_blocks);
  const auto block_size = cipher.block_size();
  IRS_ASSERT(block_size);
  const auto buf_size = block_size * num_blocks;
  buf_ = std::allocator<byte_type>{}.allocate(buf_size);
  pos_ = buf_;
  end_ = buf_ + buf_size;
}

EncryptedOutput::EncryptedOutput(IndexOutput::ptr&& out,
                                 encryption::stream& cipher, size_t num_blocks)
  : EncryptedOutput{*out, cipher, num_blocks} {
  IRS_ASSERT(out);
  managed_out_ = std::move(out);
}

EncryptedOutput::~EncryptedOutput() {
  std::allocator<byte_type>{}.deallocate(buf_, end_ - buf_);
}

void EncryptedOutput::WriteDirect(const byte_type* b, size_t len) {
  IRS_ASSERT(b == buf_);
  if (!cipher_->encrypt(out_->Position(), pos_, Length())) {
    throw io_error{absl::StrCat("Buffer size ", len,
                                " is not multiple of cipher block size ",
                                cipher_->block_size())};
  }

  out_->WriteBytes(b, len);
  offset_ += len;
}

encrypted_input::encrypted_input(index_input& in, encryption::stream& cipher,
                                 size_t num_buffers, size_t padding /* = 0*/)
  : buf_size_(cipher.block_size() * std::max(size_t(1), num_buffers)),
    buf_(std::make_unique<byte_type[]>(buf_size_)),
    in_(&in),
    cipher_(&cipher),
    start_(in_->Position()),
    length_(in_->length() - start_ - padding) {
  IRS_ASSERT(cipher.block_size() && buf_size_);
  IRS_ASSERT(in_ && in_->length() >= in_->Position() + padding);
  buffered_index_input::reset(buf_.get(), buf_size_, 0);
}

encrypted_input::encrypted_input(index_input::ptr&& in,
                                 encryption::stream& cipher, size_t num_buffers,
                                 size_t padding /* = 0*/)
  : encrypted_input(*in, cipher, num_buffers, padding) {
  managed_in_ = std::move(in);
}

encrypted_input::encrypted_input(const encrypted_input& rhs,
                                 index_input::ptr&& in) noexcept
  : buf_size_(rhs.buf_size_),
    buf_(std::make_unique<byte_type[]>(buf_size_)),
    managed_in_(std::move(in)),
    in_(managed_in_.get()),
    cipher_(rhs.cipher_),
    start_(rhs.start_),
    length_(rhs.length_) {
  IRS_ASSERT(cipher_->block_size());
  buffered_index_input::reset(buf_.get(), buf_size_, rhs.Position());
}

uint32_t encrypted_input::checksum(size_t offset) const {
  const auto begin = Position();
  const auto end = (std::min)(begin + offset, this->length());

  Finally restore_position = [begin, this]() noexcept {
    // FIXME make me noexcept as I'm begin called from within ~finally()
    const_cast<encrypted_input*>(this)->seek_internal(begin);
  };

  const_cast<encrypted_input*>(this)->seek_internal(begin);

  crc32c crc;
  byte_type buf[kDefaultEncryptionBufferSize];

  for (auto pos = begin; pos < end;) {
    const auto to_read = (std::min)(end - pos, sizeof buf);
    pos += const_cast<encrypted_input*>(this)->read_internal(buf, to_read);
    crc.process_bytes(buf, to_read);
  }

  return crc.checksum();
}

index_input::ptr encrypted_input::dup() const {
  auto dup = in_->dup();

  if (!dup) {
    throw io_error{
      absl::StrCat("Failed to duplicate input file, error: ", errno)};
  }

  return index_input::ptr{new encrypted_input{*this, std::move(dup)}};
}

index_input::ptr encrypted_input::reopen() const {
  auto reopened = in_->reopen();

  if (!reopened) {
    throw io_error(absl::StrCat("Failed to reopen input file, error: ", errno));
  }

  return index_input::ptr{new encrypted_input{*this, std::move(reopened)}};
}

void encrypted_input::seek_internal(size_t pos) {
  pos += start_;

  if (pos != in_->Position()) {
    in_->seek(pos);
  }
}

size_t encrypted_input::read_internal(byte_type* b, size_t count) {
  const auto offset = in_->Position();

  const auto read = in_->read_bytes(b, count);

  if (!cipher_->decrypt(offset, b, read)) {
    throw io_error{absl::StrCat("Buffer size ", read,
                                " is not multiple of cipher block size ",
                                cipher_->block_size())};
  }

  return read;
}

}  // namespace irs
