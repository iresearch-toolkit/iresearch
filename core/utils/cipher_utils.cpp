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

#include "cipher_utils.hpp"
#include "string_utils.hpp"
#include "error/error.hpp"
#include "store/data_output.hpp"
#include "store/directory_attributes.hpp"

NS_ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                                           helpers
// -----------------------------------------------------------------------------

void append_padding(const cipher& cipher, index_output& out) {
  static const byte_type PADDING[16] { }; // FIXME
  auto pad = padding(cipher, out.file_pointer());

  while (pad) {
    const auto to_write = std::min(pad, sizeof(PADDING));
    out.write_bytes(PADDING, to_write);
    pad -= to_write;
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                   encrypted_output implementation
// -----------------------------------------------------------------------------

encrypted_output::encrypted_output(
    irs::index_output& out,
    const irs::cipher& cipher,
    size_t buf_size
) : out_(&out),
    cipher_(&cipher),
    buf_size_(cipher.block_size() * buf_size),
    buf_(memory::make_unique<byte_type[]>(buf_size_)),
    start_(0),
    pos_(buf_.get()),
    end_(pos_ + buf_size_) {
}

void encrypted_output::write_int(int32_t value) {
  if (remain() < sizeof(uint32_t)) {
    index_output::write_int(value);
  } else {
    irs::write<uint32_t>(pos_, value);
  }
}

void encrypted_output::write_long(int64_t value) {
  if (remain() < sizeof(uint64_t)) {
    index_output::write_long(value);
  } else {
    irs::write<uint64_t>(pos_, value);
  }
}

void encrypted_output::write_vint(uint32_t v) {
  if (remain() < bytes_io<uint32_t>::const_max_vsize) {
    index_output::write_vint(v);
  } else {
    irs::vwrite<uint32_t>(pos_, v);
  }
}

void encrypted_output::write_vlong(uint64_t v) {
  if (remain() < bytes_io<uint64_t>::const_max_vsize) {
    index_output::write_vlong(v);
  } else {
    irs::vwrite<uint64_t>(pos_, v);
  }
}

void encrypted_output::write_byte(byte_type b) {
  if (pos_ >= end_) {
    flush();
  }

  *pos_++ = b;
}

void encrypted_output::write_bytes(const byte_type* b, size_t length) {
  assert(pos_ <= end_);
  auto left = size_t(std::distance(pos_, end_));

  // is there enough space in the buffer?
  if (left >= length) {
    // we add the data to the end of the buffer
    std::memcpy(pos_, b, length);
    pos_ += length;

    // if the buffer is full, flush it
    if (end_ == pos_) {
      flush();
    }
  } else {
    // we fill/flush the buffer (until the input is written)
    size_t slice_pos = 0; // position in the input data

    while (slice_pos < length) {
      auto slice_len = std::min(length - slice_pos, left);

      std::memcpy(pos_, b + slice_pos, slice_len);
      slice_pos += slice_len;
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

size_t encrypted_output::file_pointer() const {
  assert(buf_.get() <= pos_);
  return start_ + size_t(std::distance(buf_.get(), pos_));
}

void encrypted_output::flush() {
  flush(false);
}

void encrypted_output::flush(bool append_padding) {
  assert(buf_.get() <= pos_);
  auto size = size_t(std::distance(buf_.get(), pos_));

  if (append_padding) {
    irs::append_padding(*cipher_, *this);
    size = size_t(std::distance(buf_.get(), pos_));
  }

  if (!cipher_->encrypt(buf_.get(), size)) {
    throw io_error(string_utils::to_string(
      "buffer size " IR_SIZE_T_SPECIFIER " is not multiple to cipher block size " IR_SIZE_T_SPECIFIER,
      size, cipher_->block_size()
    ));
  }

  out_->write_bytes(buf_.get(), size);
  start_ += size;
  pos_ = buf_.get();

  out_->flush();
}

void encrypted_output::close() {
  flush();
  start_ = 0;
  pos_ = buf_.get();

  out_->close();
}

// -----------------------------------------------------------------------------
// --SECTION--                                    encrypted_input implementation
// -----------------------------------------------------------------------------

encrypted_input::encrypted_input(
    irs::index_input& in,
    const irs::cipher& cipher,
    size_t buf_size
) : irs::buffered_index_input(cipher.block_size()*buf_size),
    in_(&in),
    cipher_(&cipher),
    length_(in.length() - in.file_pointer()) {
  assert(in.length() >= in.file_pointer());
}

index_input::ptr encrypted_input::dup() const {
  return index_input::make<encrypted_input>(
    *in_, *cipher_, buffer_size() / cipher_->block_size()
  );
}

size_t encrypted_input::read_internal(byte_type* b, size_t count) {
  const auto read = in_->read_bytes(b, count);

  if (!cipher_->decrypt(b, read)) {
    throw io_error(string_utils::to_string(
      "buffer size " IR_SIZE_T_SPECIFIER " is not multiple to cipher block size " IR_SIZE_T_SPECIFIER,
      read, cipher_->block_size()
    ));
  }

  return read;
}

NS_END
