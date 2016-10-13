//
// IResearch search engine 
// 
// Copyright © 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#include "shared.hpp"
#include "format_compress.hpp"
#include "format_utils.hpp"

#include "error/error.hpp"

#include "utils/bit_utils.hpp"
#include "utils/bit_packing.hpp"
#include "utils/type_limits.hpp"

#if defined(_MSC_VER)
  #pragma warning(disable : 4244)
  #pragma warning(disable : 4245)
#elif defined (__GNUC__)
  // NOOP
#endif

#include <boost/crc.hpp>

#if defined(_MSC_VER)
  #pragma warning(default: 4244)
  #pragma warning(default: 4245)
#elif defined (__GNUC__)
  // NOOP
#endif

NS_ROOT

/* -------------------------------------------------------------------
 * compressing_index_writer
 * ------------------------------------------------------------------*/

const string_ref compressing_index_writer::FORMAT_NAME = "iresearch_10_compressing_index";

compressing_index_writer::compressing_index_writer(size_t block_size)
  : doc_base_deltas_(new uint32_t[block_size]),
    doc_pos_deltas_(new uint64_t[block_size]),
    block_size_(block_size) {
}

void compressing_index_writer::prepare(index_output& out, uint64_t ptr) {
  out_ = &out;
  last_pos_ = ptr;
  first_pos_ = ptr;
  docs_ = 0;
  block_docs_ = 0;
  block_chunks_ = 0;
}
  
void compressing_index_writer::compute_stats(
    uint32_t& avg_chunk_docs,
    uint64_t& avg_chunk_size,
    uint32_t& doc_delta_bits,
    uint32_t& ptr_bits) {

  // compute medians
  avg_chunk_size = 0;
  avg_chunk_docs = 0;
  if (block_chunks_ > 1) {
    const size_t den = block_chunks_ - 1;
    avg_chunk_docs = std::lround(
      static_cast<float_t>(docs_ - doc_base_deltas_[den]) / den
    );
    avg_chunk_size = (last_pos_ - first_pos_) / den;
  }

  // compute number of bits to use in packing
  uint32_t max_doc_delta = 0;
  uint64_t max_start = 0;
  uint32_t doc_base = 0;
  uint64_t start = 0;
  for (size_t i = 0; i < block_chunks_; ++i) {
    start += doc_pos_deltas_[i];
    const int64_t start_delta = start - avg_chunk_size * i;
    //TODO: write start_delta
    max_start = std::max(max_start, zig_zag_encode64(start_delta));

    const int32_t doc_delta = doc_base - avg_chunk_docs * uint32_t(i);
    max_doc_delta = std::max(max_doc_delta, zig_zag_encode32(doc_delta));
    doc_base += doc_base_deltas_[i];
  }

  doc_delta_bits = packed::bits_required_32(max_doc_delta);
  ptr_bits = packed::bits_required_64(max_start);
}

void compressing_index_writer::flush() {
  assert(out_);
  assert(block_chunks_ > 0);

  // average chunk docs count and size;
  uint32_t avg_chunk_docs = 0;
  uint64_t avg_chunk_size = 0;
  // number of bits to encode doc deltas & start pointers
  uint32_t doc_delta_bits = 0;
  uint32_t ptr_bits = 0;

  compute_stats(avg_chunk_docs, avg_chunk_size, doc_delta_bits, ptr_bits);

  // convert docs to deltas
  doc_id_t delta, base = 0;

  for (size_t i = 0; i < block_chunks_; ++i) {
    delta = zig_zag_encode32(base - avg_chunk_docs*uint32_t(i));
    base += doc_base_deltas_[i];
    doc_base_deltas_[i] = delta;
  }

  size_t full_chunks = block_chunks_ - (block_chunks_ % packed::BLOCK_SIZE_32);

  out_->write_vint(uint32_t(block_chunks_));

  // write document bases : header
  out_->write_vint(docs_ - block_docs_);
  out_->write_vint(avg_chunk_docs);
  out_->write_vint(doc_delta_bits);

  // write document bases : full chunks 
  if (full_chunks) {
    packed_.clear();
    packed_.resize(full_chunks);

    packed::pack(
      doc_base_deltas_.get(),
      doc_base_deltas_.get() + full_chunks,
      &packed_[0],
      doc_delta_bits
    );

    out_->write_bytes(
      reinterpret_cast<const byte_type*>(&packed_[0]),
      sizeof(uint32_t)*packed::blocks_required_32(uint32_t(full_chunks), doc_delta_bits)
    );
  }

  // write document bases : tail
  for (; full_chunks < block_chunks_; ++full_chunks) {
    out_->write_vint(doc_base_deltas_[full_chunks]);
  }

  // write start pointers : header 
  out_->write_vlong(first_pos_);
  out_->write_vlong(avg_chunk_size);
  out_->write_vint(ptr_bits);

  //TODO: use packed ints
  // write start pointers : pointers 
  int64_t pos = 0;
  for (size_t i = 0; i < block_chunks_; ++i) {
    pos += doc_pos_deltas_[i];
    write_zvlong(*out_, pos - avg_chunk_size*i);
  }
}

void compressing_index_writer::write(uint32_t docs, uint64_t ptr) {
  assert(out_);

  if (block_chunks_ == block_size_) {
    flush();
    block_chunks_ = 0;
    block_docs_ = 0;
    first_pos_ = last_pos_ = ptr;
  }

  assert(ptr >= last_pos_);

  doc_base_deltas_[block_chunks_] = docs;
  doc_pos_deltas_[block_chunks_] = ptr - last_pos_;

  ++block_chunks_;
  block_docs_ += docs;
  docs_ += docs;
  last_pos_ = ptr;
}

void compressing_index_writer::finish() {
  assert(out_);

  if (block_chunks_ > 0) {
    flush();
  }
  
  out_->write_vint(0); // end marker
}

/* -------------------------------------------------------------------
 * compressing_index_reader
 * ------------------------------------------------------------------*/

block_chunk::block_chunk(
  uint64_t start, doc_id_t base, std::vector<block>&& blocks
): blocks(std::move(blocks)), start(start), base(base) {
}

block_chunk::block_chunk(block_chunk&& rhs)
  : blocks(std::move(rhs.blocks)), 
    start(rhs.start), 
    base(rhs.base) {
  rhs.start = type_limits<type_t::address_t>::invalid();
  rhs.base = type_limits<type_t::doc_id_t>::invalid();
}

bool compressing_index_reader::prepare(index_input& in, uint64_t docs_count) {
  size_t num_chunks = in.read_vint();

  std::vector<uint32_t> packed; 
  std::vector<uint32_t> unpacked;

  std::vector<block> blocks(num_chunks);
  for (; num_chunks; num_chunks = in.read_vint()) {
    blocks.resize(num_chunks);

    size_t full_chunks = num_chunks - (num_chunks % packed::BLOCK_SIZE_32);

    // read document bases : header
    const doc_id_t doc_base = in.read_vlong();
    const doc_id_t avg_delta = in.read_vlong();
    const uint32_t doc_bits = in.read_vint();
    if (doc_bits > bits_required<uint32_t>()) {
      // invalid number of bits per document
      throw index_error();
    }

    // read document bases : full chunks 
    if (full_chunks) {
      unpacked.resize(full_chunks);

      packed.resize(packed::blocks_required_32(uint32_t(full_chunks), doc_bits));
      
      in.read_bytes(
        reinterpret_cast<byte_type*>(&packed[0]),
        sizeof(uint32_t)*packed.size()
      );

      packed::unpack(
        &unpacked[0],
        &unpacked[0] + unpacked.size(),
        &packed[0],
        doc_bits
      );

      for (size_t i = 0; i < full_chunks; ++i) {
        blocks[i].base = zig_zag_decode32(unpacked[i]) + doc_id_t(i) * avg_delta;
      }
    }

    // read document bases : tail 
    for (; full_chunks < num_chunks; ++full_chunks) {
      blocks[full_chunks].base = read_zvint(in) + doc_id_t(full_chunks) * avg_delta;
    }

    // read start pointers : header
    const uint64_t start = in.read_vlong();
    const uint64_t avg_size = in.read_vlong();
    const uint32_t ptr_bits = in.read_vint();
    if (ptr_bits > bits_required<uint64_t>()) {
      // invalid number of bits per block size
      throw index_error();
    }

    //TODO: use packed ints here
    // read start pointers : pointers
    for (size_t i = 0; i < num_chunks; ++i) {
      blocks[i].start = read_zvlong(in) + i*avg_size;
    }

    data_.emplace_back(start, doc_base, std::move(blocks));
  }

  max_doc_ = docs_count;
  return true;
}

uint64_t compressing_index_reader::start_ptr(doc_id_t doc) const {
  assert(type_limits<type_t::doc_id_t>::valid(doc));

  if (doc - type_limits<type_t::doc_id_t>::min() >= max_doc_) {
    throw illegal_argument();
  }

  // find the right block chunk
  auto chunk = std::lower_bound(
    data_.rbegin(),
    data_.rend(),
    doc - type_limits<type_t::doc_id_t>::min(),
    [](const block_chunk& lhs, doc_id_t rhs) {
      return lhs.base > rhs;
  });
  assert(chunk != data_.rend());

  // find the right block in the chunk
  auto block = std::lower_bound(
    chunk->blocks.rbegin(),
    chunk->blocks.rend(),
    doc - chunk->base - type_limits<type_t::doc_id_t>::min(),
    [](const iresearch::block& lhs, doc_id_t rhs) {
      return lhs.base > rhs;
  });
  assert(block != chunk->blocks.rend());

  return chunk->start + block->start;
}

NS_END