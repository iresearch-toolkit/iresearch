////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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


#include "sparse_bitmap.hpp"

#include <cstring>

#include "search/bitset_doc_iterator.hpp"

namespace {

constexpr uint32_t BITSET_THRESHOLD = (1 << 12) - 1;

}

namespace iresearch {

void sparse_bitmap_writer::finish() {
  flush();

  // create a sentinel block to issue doc_limits::eof() automatically
  block_ = doc_limits::eof() / BLOCK_SIZE;
  set(doc_limits::eof() % BLOCK_SIZE);
  flush(1);
}

void sparse_bitmap_writer::flush(uint32_t popcnt) {
  assert(popcnt);
  assert(block_ < std::numeric_limits<uint16_t>::max());
  assert(popcnt <= std::numeric_limits<uint16_t>::max());

  out_->write_short(static_cast<uint16_t>(block_));
  out_->write_short(static_cast<uint16_t>(popcnt));

  if (popcnt > BITSET_THRESHOLD) {
    if (popcnt != BLOCK_SIZE) {
      if constexpr (!is_big_endian()) {
        std::for_each(
          std::begin(bits_), std::end(bits_),
          [](auto& v){ v = numeric_utils::hton64(v); });
      }

      out_->write_bytes(
        reinterpret_cast<const byte_type*>(bits_),
        sizeof bits_);
    }
  } else {
    bitset_doc_iterator it(std::begin(bits_), std::end(bits_));

    while (it.next()) {
      out_->write_short(static_cast<uint16_t>(it.value()));
    }
  }
}


void sparse_bitmap_iterator::seek_to_block(size_t block) {

}

doc_id_t sparse_bitmap_iterator::seek(doc_id_t target) {
  const size_t block = target / sparse_bitmap_writer::BLOCK_SIZE;
  if (block_ < block) {
    seek_to_block(block);
  }

  return doc_limits::eof();
}

} // iresearch
