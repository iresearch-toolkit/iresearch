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
#include "search/score.hpp"
#include "utils/compression.hpp"
#include "utils/directory_utils.hpp"
#include "utils/string_utils.hpp"

namespace {

using namespace irs;
using namespace irs::columns;

std::string data_file_name(string_ref prefix) {
  return file_name(prefix, writer::DATA_FORMAT_EXT);
}

std::string index_file_name(string_ref prefix) {
  return file_name(prefix, writer::INDEX_FORMAT_EXT);
}

struct block_meta {

};

}

namespace iresearch {
namespace columns {

// -----------------------------------------------------------------------------
// --SECTION--                                             column implementation
// -----------------------------------------------------------------------------

void column::flush_block() {
  assert(ctx_.data_out);

  auto& data_out = *ctx_.data_out;

  // write block index
  // FIXME optimize cases with equal lengths
  {
    // adjust number of elements to pack to the nearest value
    // that is multiple of the block size
    const uint64_t block_size = math::ceil64(block_.size(), packed::BLOCK_SIZE_64);
    auto* begin = block_.begin();
    auto* end = begin + block_size;

    const uint64_t addr_table_offs = data_out.file_pointer();
    const auto [base, avg] = encode::avg::encode(begin, end);
    blocks_.stream.write_long(addr_table_offs);
    blocks_.stream.write_long(base);
    blocks_.stream.write_long(avg);

    if (simd::all_equal<false>(begin, end)) {
      blocks_.stream.write_byte(static_cast<byte_type>(bitpack::ALL_EQUAL));
      blocks_.stream.write_long(*begin);
      fixed_length_ &= (0 == *begin);
    } else {
      const uint32_t bits = packed::maxbits64(begin, end);
      const size_t buf_size = packed::bytes_required_64(block_size, bits);
      std::memset(ctx_.u64buf, 0, buf_size);
      packed::pack(begin, end, ctx_.u64buf, bits);

      blocks_.stream.write_byte(static_cast<byte_type>(bits));
      data_out.write_bytes(ctx_.u8buf, buf_size);
      fixed_length_ = false;
    }

    block_.reset();
  }

  // write block data
  // FIXME we don't need to track data_offs/index_offs per block after merge
  {
    const uint64_t data_offs = data_out.file_pointer();
    data_.stream.flush();
    data_.file >> data_out; // FIXME write directly???
    blocks_.stream.write_long(data_offs);
  }

  ++num_blocks_;
  data_.stream.seek(0);
}

void column::finish(index_output& index_out) {
  docs_writer_.finish();

  if (!block_.empty()) {
    flush_block();
  }

  const uint64_t doc_index_offset = ctx_.data_out->file_pointer();
  docs_.stream.flush();
  docs_.file >> *ctx_.data_out;

  // can have at most 65536 blocks
  blocks_.stream.flush();

  ColumnProperty props{ColumnProperty::NORMAL};
  if (ctx_.consolidation) props |= ColumnProperty::DENSE;
  if (ctx_.cipher)        props |= ColumnProperty::ENCRYPT;
  if (fixed_length_)      props |= ColumnProperty::FIXED;

  irs::write_string(index_out, comp_type_.name());
  index_out.write_short(static_cast<uint16_t>(props));
  index_out.write_short(num_blocks_);
  index_out.write_long(doc_index_offset);
  blocks_.file >> index_out;
}

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

  auto filename = data_file_name(meta.name);
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

  const irs::string_ref segment_name{
    data_filename_,
    data_filename_.size() - DATA_FORMAT_EXT.size() - 1 };
  auto index_filename = index_file_name(segment_name);
  auto index_out = dir_->create(index_filename);

  if (!index_out) {
    throw io_error(string_utils::to_string(
      "Failed to create file, path: %s",
      index_filename.c_str()));
  }

  format_utils::write_header(*index_out, INDEX_FORMAT_NAME, FORMAT_MIN);

  // flush all remain data including possible
  // empty columns among filled columns
  index_out->write_vlong(columns_.size()); // number of columns
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
  */

  format_utils::write_footer(*index_out);
  format_utils::write_footer(*data_out_);

  rollback();

  return true;
}

void writer::rollback() noexcept {
  data_filename_.clear();
  dir_ = nullptr;
  data_out_.reset(); // close output
  columns_.clear(); // ensure next flush (without prepare(...)) will use the section without 'data_out_'
}

// -----------------------------------------------------------------------------
// --SECTION--                               reader::column_entry implementation
// -----------------------------------------------------------------------------

class column_iterator final : public irs::doc_iterator {
 private:
  using attributes = std::tuple<document, cost, score, payload>;

 public:
  explicit column_iterator(
      index_input::ptr&& in,
      const column_block* blocks)
    : in_(std::move(in)),
      blocks_(blocks),
      bitmap_(*in_) {
  }

  virtual attribute* get_mutable(irs::type_info::type_id type) noexcept override {
    return irs::get_mutable(attrs_, type);
  }

  virtual doc_id_t value() const noexcept override {
    return std::get<document>(attrs_).value;
  }

  virtual doc_id_t seek(irs::doc_id_t doc) override;

  virtual bool next() override {
    return false;
  }

 private:
  index_input::ptr in_;
  index_input::ptr dup_; // FIXME
  const column_block* blocks_;
  sparse_bitmap_iterator bitmap_;
  attributes attrs_;
}; // column_iterator

doc_id_t column_iterator::seek(doc_id_t doc) {
  doc = bitmap_.seek(doc);

  if (!doc_limits::eof(doc)) {
    const size_t value_idx = bitmap_.index();
    const auto& block = blocks_[value_idx / column::BLOCK_SIZE];
    const size_t index = value_idx % column::BLOCK_SIZE;

    dup_->seek(block.base);
    auto* buf = dup_->read_buffer(packed::BLOCK_SIZE_64, BufferHint::NORMAL);
    assert(buf);
    auto offs = packed::at(reinterpret_cast<const uint64_t*>(buf), index, block.bits);
  }

  return doc;
}

columnstore_reader::values_reader_f reader::column_entry::values() const {
  return {};
}

doc_iterator::ptr reader::column_entry::iterator() const {
  index_input::ptr stream = data_in->reopen();

  if (!stream) {
    // implementation returned wrong pointer
    IR_FRMT_ERROR("Failed to reopen payload input in: %s", __FUNCTION__);

    throw io_error("failed to reopen payload input");
  }

  return memory::make_managed<column_iterator>(std::move(stream), blocks.data());
}

bool reader::column_entry::visit(
      const columnstore_reader::values_visitor_f& reader) const {
  return false;
}

// -----------------------------------------------------------------------------
// --SECTION--                                             reader implementation
// -----------------------------------------------------------------------------

void reader::prepare_data(const directory& dir, const std::string& filename) {
  auto data_in = dir.open(filename, irs::IOAdvice::RANDOM);

  if (!data_in) {
    throw io_error(string_utils::to_string(
      "Failed to open file, path: %s",
      filename.c_str()));
  }

  [[maybe_unused]] const auto version =
    format_utils::check_header(
      *data_in,
      writer::DATA_FORMAT_NAME,
      writer::FORMAT_MIN,
      writer::FORMAT_MAX);

  encryption::stream::ptr cipher;
  auto* enc = get_encryption(dir.attributes());
  if (irs::decrypt(filename, *data_in, enc, cipher)) {
    assert(cipher && cipher->block_size());
  }

  // since columns data are too large
  // it is too costly to verify checksum of
  // the entire file. here we perform cheap
  // error detection which could recognize
  // some forms of corruption
  [[maybe_unused]] const auto checksum = format_utils::read_checksum(*data_in);

  // noexcept
  data_cipher_ = std::move(cipher);
  data_in_ = std::move(data_in);
}

// FIXME return result???
void reader::prepare_index(const directory& dir, const std::string& filename) {
  // open columstore stream
  auto index_in = dir.open(filename, irs::IOAdvice::READONCE_SEQUENTIAL);

  if (!index_in) {
    throw io_error(string_utils::to_string(
      "Failed to open file, path: %s",
      filename.c_str()));
  }

  const auto checksum = format_utils::checksum(*index_in);

  [[maybe_unused]] const auto version =
    format_utils::check_header(
      *index_in,
      writer::INDEX_FORMAT_NAME,
      writer::FORMAT_MIN,
      writer::FORMAT_MAX);

  std::vector<column_entry> columns;
  columns.reserve(index_in->read_vlong());

  for (size_t i = 0, size = columns.capacity(); i < size; ++i) {
    const auto compression_id = read_string<std::string>(*index_in);
    auto inflater = compression::get_decompressor(compression_id);

    if (!inflater && !compression::exists(compression_id)) {
      throw index_error(string_utils::to_string(
        "Failed to load compression '%s' for column id=" IR_SIZE_T_SPECIFIER,
        compression_id.c_str(), i));
    }

    if (inflater && !inflater->prepare(*index_in)) { // FIXME or data_in???
      throw index_error(string_utils::to_string(
        "Failed to prepare compression '%s' for column id=" IR_SIZE_T_SPECIFIER,
        compression_id.c_str(), i));
    }

    const auto props = static_cast<ColumnProperty>(index_in->read_short());
    const size_t num_blocks = index_in->read_short();
    const uint64_t doc_index_offset = index_in->read_long();
    auto& column = columns.emplace_back(num_blocks, doc_index_offset, data_in_.get(), std::move(inflater), props);

    // FIXME optimize
    for (auto& block : column.blocks) {
      block.addr_offset = index_in->read_long();
      block.base = index_in->read_long();
      block.avg = index_in->read_long();
      block.bits = index_in->read_byte();
      if (!block.bits) {
        block.rl_value = index_in->read_long();
      }
      block.data_offset = index_in->read_long();
    }
  }

  format_utils::check_footer(*index_in, checksum);

  // noexcept block
  columns_ = std::move(columns);
}

bool reader::prepare(const directory& dir, const segment_meta& meta) {
  bool exists;
  const auto data_filename = data_file_name(meta.name);

  if (!dir.exists(exists, data_filename)) {
    throw io_error(string_utils::to_string(
      "failed to check existence of file, path: %s",
      data_filename.c_str()));
  }

  if (!exists) {
    // possible that the file does not exist since columnstore is optional
    return false;
  }

  prepare_data(dir, data_filename);
  assert(data_in_);

  const auto index_filename = index_file_name(meta.name);

  if (!dir.exists(exists, index_filename)) {
    throw io_error(string_utils::to_string(
      "failed to check existence of file, path: %s",
      index_filename.c_str()));
  }

  if (!exists) {
    // more likely index is currupted
    throw index_error(string_utils::to_string(
      "columnstore index file '%s' is missing",
      index_filename.c_str()));
  }

  prepare_index(dir, index_filename);

  return true;
}

const reader::column_reader* reader::column(field_id field) const {
  return field >= columns_.size()
    ? nullptr // can't find column with the specified identifier
    : &columns_[field];
}

}
}
