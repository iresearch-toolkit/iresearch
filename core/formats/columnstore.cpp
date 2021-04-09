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

#include "columnstore.hpp"

#include "error/error.hpp"
#include "formats/format_utils.hpp"
#include "index/file_names.hpp"
#include "utils/compression.hpp"
#include "utils/directory_utils.hpp"
#include "utils/string_utils.hpp"

namespace iresearch {
namespace columns {

// -----------------------------------------------------------------------------
// --SECTION--                                             column implementation
// -----------------------------------------------------------------------------

void column::flush_block() {
  assert(ctx_.data_out);

  auto& data_out = *ctx_.data_out;

  const uint64_t index_offs = data_out.file_pointer();
  block_.flush(data_out, ctx_.u64buf); // FIXME flush block addr table, optimize cases with equal lengths
  const uint64_t data_offs = data_out.file_pointer();
  data_.file >> data_out; // FIXME write directly???

  // FIXME we don't need to track data_offs/index_offs per block after merge
  blocks_.stream.write_long(index_offs);
  blocks_.stream.write_long(data_offs);

  data_.stream.seek(0);
}

void column::finish(index_output& index_out) {
  docs_writer_.finish();

  if (!block_.empty()) {
    flush_block();
  }

  // FIXME
  // * write column properties
  // * write block index
}

/*
  void finish() {
    docs_writer_.finish();

    auto& out = *ctx_->data_out_;

       // evaluate overall column properties
      auto column_props = blocks_props_;
      if (0 != (column_props_ & CP_DENSE)) { column_props |= CP_COLUMN_DENSE; }
      if (cipher_) { column_props |= CP_COLUMN_ENCRYPT; }

      write_enum(out, column_props);
      if (ctx_->version_ > FORMAT_MIN) {
        write_string(out, comp_type_.name());
        comp_->flush(out); // flush compression dependent data
      }
      out.write_vint(block_index_.total()); // total number of items
      out.write_vint(max_); // max column key
      out.write_vint(avg_block_size_); // avg data block size
      out.write_vint(avg_block_count_); // avg number of elements per block
      out.write_vint(column_index_.total()); // total number of index blocks
      blocks_index_.file >> out; // column blocks index
  }
  */

/*
  void flush() {
    const auto minmax_length = simd::maxmin<IRESEARCH_COUNTOF(lengths_)>(lengths_, IRESEARCH_COUNTOF(lengths_));

    auto& index_out = *ctx_->index_out_;
    auto& data_out = *ctx_->data_out_;

    blocks_.stream.write_int(minmax_length.first);
    blocks_.stream.write_int(minmax_length.second);

    if (minmax_length.second >= minmax_length.first) {
      // write block address map
      blocks_.stream.write_long(data_out.file_pointer()); // data offset
      data_out << this->data_; // FIXME write directly???

      if (minmax_length.second > minmax_length.first) {
        blocks_.stream.write_long(data_out.file_pointer()); // lengths offset
        const auto stats = simd::avg_encode32<IRESEARCH_COUNTOF(lengths_)>(lengths_);
        data_out.write_int(stats.first);
        data_out.write_int(stats.second);
      }
    }

      // do not take into account last block
      const auto blocks_count = std::max(1U, column_index_.total());
      avg_block_count_ = block_index_.flushed() / blocks_count;
      avg_block_size_ = length_ / blocks_count;

      // commit and flush remain blocks
      flush_block();

      // finish column blocks index
      assert(ctx_->buf_.size() >= INDEX_BLOCK_SIZE*sizeof(uint64_t));
      auto* buf = reinterpret_cast<uint64_t*>(&ctx_->buf_[0]);
      column_index_.flush(blocks_index_.stream, buf);
      blocks_index_.stream.flush();
  }
  */

// -----------------------------------------------------------------------------
// --SECTION--                                             writer implementation
// -----------------------------------------------------------------------------

writer::writer(bool consolidation)
  : alloc_(&memory_allocator::global()),
    buf_(memory::make_unique<byte_type[]>(column::BLOCK_SIZE*sizeof(uint64_t))),
    consolidation_(consolidation) {
}

void writer::prepare(directory& dir, const segment_meta& meta) {
  columns_.clear();

  auto filename = file_name(meta.name, DATA_FORMAT_EXT);
  auto data_out = dir.create(filename);

  if (!data_out) {
    throw io_error(string_utils::to_string(
      "Failed to create file, path: %s",
      filename.c_str()));
  }

  format_utils::write_header(*data_out, DATA_FORMAT_NAME, FORMAT_MIN);

  encryption::stream::ptr data_cipher;
  bstring enc_header;
  auto* enc = get_encryption(dir.attributes());
  const auto encrypt = irs::encrypt(filename, *data_out, enc, enc_header, data_cipher);
  assert(!encrypt || (data_cipher && data_cipher->block_size()));
  UNUSED(encrypt);

  alloc_ = &directory_utils::get_allocator(dir);

  // noexcept block
  dir_ = &dir;
  data_filename_ = std::move(filename);
  data_out_ = std::move(data_out);
  data_cipher_ = std::move(data_cipher);
}

columnstore_writer::column_t writer::push_column(const column_info& info) {
  irs::type_info compression = info.compression();
  encryption::stream* cipher = info.encryption()
    ? data_cipher_.get()
    : nullptr;

  auto compressor = compression::get_compressor(compression, info.options());

  if (!compressor) {
    compressor = compression::compressor::identity();
  }

  const auto id = columns_.size();
  auto& column = columns_.emplace_back(
    column::context{
      alloc_,
      data_out_.get(),
      cipher, { buf_.get() },
      consolidation_ },
    info.compression(),
    compressor);

  return std::make_pair(id, [&column] (doc_id_t doc) -> column_output& {
    // to avoid extra (and useless in our case) check for block index
    // emptiness in 'writer::column::prepare', we disallow passing
    // doc <= doc_limits::invalid() || doc >= doc_limits::eof()
    assert(doc > doc_limits::invalid() && doc < doc_limits::eof());

    column.prepare(doc);
    return column;
  });
}

bool writer::commit() {
  assert(dir_);

  // remove all empty columns from tail
  while (!columns_.empty() && columns_.back().empty()) {
    columns_.pop_back();
  }

  // remove file if there is no data to write
  if (columns_.empty()) {
    data_out_.reset();

    if (!dir_->remove(data_filename_)) { // ignore error
      IR_FRMT_ERROR("Failed to remove file, path: %s", data_filename_.c_str());
    }

    return false; // nothing to flush
  }

  const irs::string_ref segment_name(data_filename_, data_filename_.size() - DATA_FORMAT_EXT.size());
  auto index_filename = file_name(segment_name, INDEX_FORMAT_EXT);
  auto index_out = dir_->create(index_filename);

  if (!index_out) {
    throw io_error(string_utils::to_string(
      "Failed to create file, path: %s",
      index_filename.c_str()));
  }

  format_utils::write_header(*index_out, INDEX_FORMAT_NAME, FORMAT_MIN);

  // flush all remain data including possible empty columns among filled columns
  for (auto& column : columns_) {
    // commit and flush remain blocks
    column.finish(*index_out);
  }

  // FIXME
  /*
  const auto block_index_ptr = data_out_->file_pointer(); // where blocks index start

  data_out_->write_vlong(columns_.size()); // number of columns

  for (auto& column : columns_) {
    column.finish(); // column blocks index
  }

  data_out_->write_long(block_index_ptr);
  format_utils::write_footer(*data_out_);
  */

  rollback();

  return true;
}

void writer::rollback() noexcept {
  data_filename_.clear();
  dir_ = nullptr;
  data_out_.reset(); // close output
  columns_.clear(); // ensure next flush (without prepare(...)) will use the section without 'data_out_'
}

}
}
