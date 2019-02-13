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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_CIPHER_UTILS_H
#define IRESEARCH_CIPHER_UTILS_H

#include "store/data_output.hpp"
#include "store/data_input.hpp"
#include "utils/noncopyable.hpp"

NS_ROOT

struct cipher;

//////////////////////////////////////////////////////////////////////////////
/// @class encrypted_output
//////////////////////////////////////////////////////////////////////////////
class encrypted_output final : public irs::index_output, util::noncopyable {
 public:
  encrypted_output(
    irs::index_output& out,
    const irs::cipher& cipher,
    size_t buf_size
  );

  virtual void flush() override;

  virtual void close() override;

  virtual size_t file_pointer() const override;

  virtual void write_byte(byte_type b) override final;

  virtual void write_bytes(const byte_type* b, size_t length) override final;

  virtual void write_vint(uint32_t v) override final;

  virtual void write_vlong(uint64_t v) override final;

  virtual void write_int(int32_t v) override final;

  virtual void write_long(int64_t v) override final;

  virtual int64_t checksum() const override final {
    return out_->checksum();
  }

 private:
  // returns number of reamining bytes in the buffer
  FORCE_INLINE size_t remain() const {
    return std::distance(pos_, end_);
  }

  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  irs::index_output* out_;
  const irs::cipher* cipher_;
  const size_t buf_size_;
  std::unique_ptr<byte_type[]> buf_;
  size_t start_; // position of buffer in a file
  byte_type* pos_; // position in buffer
  byte_type* end_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
}; // encrypted_output

class encrypted_input final : public irs::buffered_index_input, util::noncopyable {
 public:
  encrypted_input(
    irs::index_input& in,
    const irs::cipher& cipher,
    size_t buf_size
  );

  virtual index_input::ptr dup() const override final;

  virtual index_input::ptr reopen() const override final {
    throw not_supported();
  }

  virtual size_t length() const override final {
    return in_->length();
  }

  virtual int64_t checksum(size_t offset) const override final {
    return in_->checksum(offset);
  }

 protected:
  virtual void seek_internal(size_t pos) override final {
    throw not_supported();
  }

  virtual size_t read_internal(byte_type* b, size_t count) override final;

 private:
  irs::index_input* in_;
  const irs::cipher* cipher_;
}; // encrypted_input

NS_END // ROOT

#endif
