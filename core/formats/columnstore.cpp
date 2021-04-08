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

void column::flush() {
  assert(ctx_.index_out);
  assert(ctx_.data_out);

  auto& index_out = *ctx_.index_out;
  auto& data_out = *ctx_.data_out;

  const uint64_t data_offs = data_out.file_pointer();
  data_.file >> data_out; // FIXME write directly???
  const uint64_t index_offs = data_out.file_pointer();
  block_.flush(data_out, ctx); // FIXME flush block addr table, optimize cases with equal lengths

  // FIXME we don't need to track data_offs/index_offs per block after merge
  index_out.write_long(data_offs);
  index_out.write_long(index_offs);

  data_.stream.seek(0);
}

// -----------------------------------------------------------------------------
// --SECTION--                                             writer implementation
// -----------------------------------------------------------------------------

writer::writer(bool consolidation)
  : alloc_{&memory_allocator::global()},
    buf_{BLOCK_SIZE*sizeof(uint64_t), 0},
    consolidation_{consolidation} {
}

void writer::prepare(directory& dir, const segment_meta& meta) {
  columns_.clear();

  auto data_filename = file_name(meta.name, DATA_FORMAT_EXT);
  auto data_out = dir.create(data_filename);

  if (!data_out) {
    throw io_error(string_utils::to_string(
      "Failed to create file, path: %s",
      data_filename.c_str()));
  }

  auto index_filename = file_name(meta.name, INDEX_FORMAT_EXT);
  auto index_out = dir.create(index_filename);

  if (!data_out) {
    throw io_error(string_utils::to_string(
      "Failed to create file, path: %s",
      index_filename.c_str()));
  }

  format_utils::write_header(*data_out, DATA_FORMAT_NAME, FORMAT_MIN);
  format_utils::write_header(*index_out, INDEX_FORMAT_NAME, FORMAT_MIN);

  encryption::stream::ptr data_cipher;
  bstring enc_header;
  auto* enc = get_encryption(dir.attributes());
  const auto encrypt = irs::encrypt(data_filename, *data_out, enc, enc_header, data_cipher);
  assert(!encrypt || (data_cipher && data_cipher->block_size()));
  UNUSED(encrypt);

  alloc_ = &directory_utils::get_allocator(dir);

  // noexcept block
  dir_ = &dir;
  data_filename_ = std::move(data_filename);
  index_filename_ = std::move(index_filename);
  data_out_ = std::move(data_out);
  index_out_ = std::move(index_out);
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
    columnstore_context{
      alloc_,
      data_out_.get(),
      index_out_.get(),
      cipher, &buf_[0],
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
    index_out_.reset();

    if (!dir_->remove(data_filename_)) { // ignore error
      IR_FRMT_ERROR("Failed to remove file, path: %s", data_filename_.c_str());
    }
    if (!dir_->remove(index_filename_)) { // ignore error
      IR_FRMT_ERROR("Failed to remove file, path: %s", index_filename_.c_str());
    }

    return false; // nothing to flush
  }

  // flush all remain data including possible empty columns among filled columns
  for (auto& column : columns_) {
    // commit and flush remain blocks
    column.flush();
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
  index_filename_.clear();
  dir_ = nullptr;
  data_out_.reset(); // close output
  index_out_.reset(); // close output
  columns_.clear(); // ensure next flush (without prepare(...)) will use the section without 'data_out_'
}

}
}
