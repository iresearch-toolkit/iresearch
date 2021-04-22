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
#include "search/all_iterator.hpp"
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

struct column_header {
  /// @brief bitmap index offset, 0 if not present
  /// @note 0 - not preset, meaning dense column
  uint64_t docs_index{};

  /// @brief total number of docs in a column
  doc_id_t docs_count{};

  /// @brief column properties
  ColumnProperty props{ColumnProperty::NORMAL};
};

void write(index_output& out, const column_header& hdr) {
  out.write_long(hdr.docs_index);
  out.write_int(hdr.docs_count);
  write_enum(out, hdr.props);
}

void read(index_input& in, column_header& hdr) {
  hdr.docs_index = in.read_long();
  hdr.docs_count = in.read_int();
  hdr.props = read_enum<ColumnProperty>(in);
}

////////////////////////////////////////////////////////////////////////////////
/// @struct column_base
////////////////////////////////////////////////////////////////////////////////
class column_base : public columnstore_reader::column_reader {
 public:
  explicit column_base(const column_header& hdr)
    : hdr_(hdr) {
  }

  virtual columnstore_reader::values_reader_f values() const override {
    return [](auto, auto) { return false; };
  }

  virtual bool visit(const columnstore_reader::values_visitor_f&) const override {
    return true;
  }

  virtual doc_id_t size() const noexcept final {
    return hdr_.docs_count;
  }

  const column_header& header() const noexcept {
    return hdr_;
  }

 private:
  column_header hdr_;
}; // column_base

////////////////////////////////////////////////////////////////////////////////
/// @struct empty_column
////////////////////////////////////////////////////////////////////////////////
struct empty_column final : public columnstore_reader::column_reader {
  virtual doc_iterator::ptr iterator() const override {
    return doc_iterator::empty();
  }

  virtual columnstore_reader::values_reader_f values() const override {
    return [](auto, auto) { return false; };
  }

  virtual bool visit(const columnstore_reader::values_visitor_f&) const override {
    return true;
  }

  virtual doc_id_t size() const noexcept override {
    return 0;
  }
}; // empty_column

template<typename ValueReader>
class all_docs_column_iterator final : public irs::doc_iterator {
 private:
  using value_reader = ValueReader;

  using attributes = std::tuple<document, cost, score, payload>;

 public:
  all_docs_column_iterator(ValueReader&& reader, doc_id_t docs_count)
    : reader_(std::move(reader)),
      max_doc_{doc_limits::min() + docs_count - 1} {
    std::get<irs::cost>(attrs_).reset(docs_count);
  }

  virtual attribute* get_mutable(irs::type_info::type_id type) noexcept override {
    return irs::get_mutable(attrs_, type);
  }

  virtual doc_id_t value() const noexcept override {
    return std::get<document>(attrs_).value;
  }

  virtual doc_id_t seek(irs::doc_id_t doc) override {
    if (doc <= max_doc_) {
      std::get<document>(attrs_).value = doc;
      std::get<payload>(attrs_).value = reader_(doc - doc_limits::min());
      return doc;
    }

    std::get<document>(attrs_).value = doc_limits::eof();
    std::get<payload>(attrs_).value = bytes_ref::NIL;
    return doc_limits::eof();
  }

  virtual bool next() override {
    if (value() <= max_doc_) {
      ++std::get<document>(attrs_).value;
      std::get<payload>(attrs_).value = reader_(value() - doc_limits::min());
      return true;
    }

    std::get<document>(attrs_).value = doc_limits::eof();
    std::get<payload>(attrs_).value = bytes_ref::NIL;
    return false;
  }

 private:
  value_reader reader_;
  doc_id_t max_doc_;
  attributes attrs_;
}; // all_docs_column_iterator

template<typename ValueReader>
class bitmap_column_iterator final : public irs::doc_iterator {
 private:
  using value_reader = ValueReader;

  using attributes = std::tuple<
    attribute_ptr<document>,
    cost,
    score,
    payload>;

 public:
  bitmap_column_iterator(
      ValueReader&& reader,
      index_input::ptr&& bitmap_in,
      cost::cost_t cost)
    : reader_(std::move(reader)),
      bitmap_(std::move(bitmap_in)) {
    std::get<irs::cost>(attrs_).reset(cost);
    std::get<attribute_ptr<document>>(attrs_) = irs::get_mutable<document>(&bitmap_);
  }

  virtual attribute* get_mutable(irs::type_info::type_id type) noexcept override {
    return irs::get_mutable(attrs_, type);
  }

  virtual doc_id_t value() const noexcept override {
    return std::get<attribute_ptr<document>>(attrs_).ptr->value;
  }

  virtual doc_id_t seek(irs::doc_id_t doc) override {
    doc = bitmap_.seek(doc);

    if (!doc_limits::eof(doc)) {
       std::get<payload>(attrs_).value = reader_(bitmap_.index());
       return doc;
    }

    std::get<payload>(attrs_).value = bytes_ref::NIL;
    return doc_limits::eof();
  }

  virtual bool next() override {
    if (bitmap_.next()) {
      std::get<payload>(attrs_).value = reader_(bitmap_.index());
      return true;
    }

    std::get<payload>(attrs_).value = bytes_ref::NIL;
    return false;
  }

 private:
  value_reader reader_;
  sparse_bitmap_iterator bitmap_;
  attributes attrs_;
}; // bitmap_column_iterator

struct empty_value_reader {
  bytes_ref operator()(doc_id_t) noexcept {
    return bytes_ref::NIL;
  }
};

class dense_fixed_length_value_reader {
 public:
  dense_fixed_length_value_reader(
      index_input::ptr&& data_in,
      uint64_t data_offset,
      uint64_t length)
    : data_in_{std::move(data_in)},
      data_offset_{data_offset},
      length_{length} {
  }

  bytes_ref operator()(doc_id_t i) {
    const auto offs = data_offset_ + length_*i;

    data_in_->seek(offs);
    auto* buf = data_in_->read_buffer(length_, BufferHint::NORMAL);
    assert(buf);

    return { buf, length_ };
  }

 private:
  index_input::ptr data_in_;
  uint64_t data_offset_; // where data starts
  uint64_t length_;      // data entry length
}; // dense_fixed_length_value_reader

template<typename Block>
class fixed_length_value_reader {
 public:
  // Assume the following members:
  // uint64_t data; // where data starts
  // uint64_t avg;  // avg delta
  using block_type = Block;

  fixed_length_value_reader(index_input::ptr data_in, const block_type* blocks)
    : data_in_{std::move(data_in)},
      blocks_{blocks} {
  }

  bytes_ref operator()(doc_id_t i) {
    const auto block_idx = i / column::BLOCK_SIZE;
    const auto value_idx = i % column::BLOCK_SIZE;

    auto& block = blocks_[block_idx];
    const auto offs = block.data + block.avg*value_idx;

    data_in_->seek(offs);
    auto* buf = data_in_->read_buffer(block.avg, BufferHint::NORMAL);
    assert(buf);

    return { buf, block.avg };
  }

 private:
  index_input::ptr data_in_;
  const block_type* blocks_;
}; // fixed_length_value_reader

template<typename Block>
class sparse_value_reader {
 public:
  // Assume the following members:
  // uint64_t data; // where data starts
  // uint64_t addr; // where address table starts
  // uint64_t avg;  // avg delta
  // uint64_t size; // block size // FIXME don't need it for dense block
  // uint32_t bits; // bits used to encode the block
  using block_type = Block;

  sparse_value_reader(index_input::ptr data_in, const block_type* blocks)
    : data_in_{std::move(data_in)}, blocks_{blocks} {
  }

  bytes_ref operator()(doc_id_t i) {
    const auto& block = blocks_[i / column::BLOCK_SIZE];
    const size_t index = i % column::BLOCK_SIZE;

    if (bitpack::ALL_EQUAL == block.bits) {
      const size_t addr = block.data + block.avg*index;
      data_in_->seek(addr);
      auto* buf = data_in_->read_buffer(block.avg, BufferHint::NORMAL);
      assert(buf);

      return { buf, block.avg };
    }

    data_in_->seek(block.addr);
    auto* addr_buf = data_in_->read_buffer(block.bits*(column::BLOCK_SIZE/packed::BLOCK_SIZE_64), BufferHint::NORMAL);
    assert(addr_buf);

    const uint64_t start_delta = zig_zag_decode64(packed::at(reinterpret_cast<const uint64_t*>(addr_buf), index, block.bits));
    const uint64_t start = block.avg*index + start_delta;

    size_t length;
    if (index != (column::BLOCK_SIZE - 1)) {
      const uint64_t end_delta = zig_zag_decode64(packed::at(reinterpret_cast<const uint64_t*>(addr_buf), index + 1, block.bits));
      length = end_delta - start_delta + block.avg;
    } else {
      length = block.size - start;
    }

    data_in_->seek(block.data + start);
    auto* buf = data_in_->read_buffer(length, BufferHint::NORMAL);
    assert(buf);

    return { buf, length };
  }

 private:
  index_input::ptr data_in_;
  const block_type* blocks_;
}; // sparse_value_reader

// -----------------------------------------------------------------------------
// --SECTION--                                      sparse column implementation
// -----------------------------------------------------------------------------

struct sparse_column final : public column_base {
  struct column_block {
    uint64_t data;
    uint64_t addr;
    uint64_t avg;
    uint64_t size;
    uint32_t bits;
  };

  explicit sparse_column(
      const column_header& hdr,
      index_input* data_in,
      compression::decompressor::ptr&& inflater)
    : column_base(hdr),
      inflater(std::move(inflater)),
      data_in(data_in) {
  }

  virtual doc_iterator::ptr iterator() const override;

  std::vector<column_block> blocks;
  compression::decompressor::ptr inflater;
  index_input* data_in;
};

doc_iterator::ptr sparse_column::iterator() const {
  if (!header().docs_count) {
    assert(0 == header().docs_index);
    return doc_iterator::empty();
  }

  if (0 == header().docs_index) {
    if (ColumnProperty::MASK == (header().props & ColumnProperty::MASK)) {
      // FIXME all docs mask case
      return memory::make_managed<all_docs_column_iterator<empty_value_reader>>(
        empty_value_reader{}, header().docs_count);
    } else {
      // FIXME all docs case
      return memory::make_managed<all_docs_column_iterator<sparse_value_reader<column_block>>>(
        sparse_value_reader<column_block>{data_in->reopen(), blocks.data()},
        header().docs_count);
    }
  }

  index_input::ptr stream = data_in->reopen();

  if (!stream) {
    // implementation returned wrong pointer
    IR_FRMT_ERROR("Failed to reopen input in: %s", __FUNCTION__);

    throw io_error("failed to reopen input");
  }

  stream->seek(header().docs_index);

  // FIXME add score
  // FIXME special handling for fixed length columns
  // FIXME special handling for dense fixed length columns

  if (ColumnProperty::MASK == (header().props & ColumnProperty::MASK)) {
    // mask column
    return memory::make_managed<sparse_bitmap_iterator>(
      std::move(stream), header().docs_count);
  }

  return memory::make_managed<bitmap_column_iterator<sparse_value_reader<column_block>>>(
    sparse_value_reader<column_block>{stream->dup(), blocks.data()},
    std::move(stream),
    header().docs_count);
}

}

namespace iresearch {
namespace columns {

// -----------------------------------------------------------------------------
// --SECTION--                                             column implementation
// -----------------------------------------------------------------------------

void column::flush_block() {
  assert(ctx_.data_out);

  auto& data_out = *ctx_.data_out;
  auto& block = blocks_.emplace_back();

  // write block index
  // FIXME optimize cases with equal lengths

  // adjust number of elements to pack to the nearest value
  // that is multiple of the block size
  const uint32_t size = block_.size();
  docs_count_ += size;
  const uint64_t block_size = math::ceil64(size, packed::BLOCK_SIZE_64);
  auto* begin = block_.begin();
  auto* end = begin + block_size;

  block.addr = data_out.file_pointer();
  std::tie(block.data, block.avg) = encode::avg::encode(begin, block_.current());

  if (simd::all_equal<false>(begin, end)) {
    block.bits = bitpack::ALL_EQUAL;
    fixed_length_ &= (0 == *begin);
  } else {
    block.bits = packed::maxbits64(begin, end);
    const size_t buf_size = packed::bytes_required_64(block_size, block.bits);
    std::memset(ctx_.u64buf, 0, buf_size);
    packed::pack(begin, end, ctx_.u64buf, block.bits);

    data_out.write_bytes(ctx_.u8buf, buf_size);
    fixed_length_ = false;
  }

  block_.reset();
  data_.stream.flush();

  // write block data
  // FIXME we don't need to track data_offs/index_offs per block after merge
  block.data += data_out.file_pointer();
  block.size = data_.file.length();
  data_.file >> data_out; // FIXME write directly???
  if (data_.stream.file_pointer()) { // FIXME
    data_.stream.seek(0);
  }
}

void column::finish(index_output& index_out, doc_id_t docs_count) {
  docs_writer_.finish();
  if (!block_.empty()) {
    flush_block();
  }

  column_header hdr;
  hdr.docs_count = docs_count_;

  hdr.docs_index = 0;
  if (docs_count_ && docs_count_ != docs_count) {
    // we don't need to store bitmap index in case
    // if every document in a column has a value
    hdr.docs_index = ctx_.data_out->file_pointer();
    docs_.stream.flush();
    docs_.file >> *ctx_.data_out;
  }

  hdr.props = ColumnProperty::NORMAL;
  if (ctx_.consolidation) hdr.props |= ColumnProperty::DENSE;
  if (ctx_.cipher)        hdr.props |= ColumnProperty::ENCRYPT;
  if (fixed_length_)      hdr.props |= ColumnProperty::FIXED;
  if (data_.file.empty()) hdr.props |= ColumnProperty::MASK;

  irs::write_string(index_out, comp_type_.name());
  write(index_out, hdr);

//  if (!fixed_length_) {
    // we don't need to store block index in case
    // if every value in a column has the same length

    // FIXME optimize
    for (auto& block : blocks_) {
      index_out.write_long(block.addr);
      index_out.write_long(block.avg);
      index_out.write_byte(static_cast<byte_type>(block.bits));
      index_out.write_long(block.data);
      index_out.write_long(block.size);
    }
//  }
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

bool writer::commit(const flush_state& state) {
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
    column.finish(*index_out, state.doc_count);
  }

  format_utils::write_footer(*index_out);
  format_utils::write_footer(*data_out_);

  rollback();

  return true;
}

void writer::rollback() noexcept {
  data_filename_.clear();
  dir_ = nullptr;
  data_out_.reset(); // close output
  columns_.clear();
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

reader::column_ptr reader::read_column(
    index_input& in,
    compression::decompressor::ptr&& inflater) {
  column_header hdr;
  read(in, hdr);

  if (0 == hdr.docs_count) {
    return memory::make_unique<empty_column>();
  }

  auto column = memory::make_unique<sparse_column>(hdr, data_in_.get(), std::move(inflater));

  column->blocks.resize(math::div_ceil32(hdr.docs_count, column::BLOCK_SIZE));

//  if (ColumnProperty::FIXED == (hdr.props & ColumnProperty::FIXED)) {
    for (auto& block : column->blocks) {
      block.addr = in.read_long();
      block.avg = in.read_long();
      block.bits = in.read_byte();
      block.data = in.read_long();
      block.size = in.read_long();
    }
//  }

  return column;
}

// FIXME return result???
void reader::prepare_index(
    const directory& dir,
    const std::string& filename) {
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

  std::vector<column_ptr> columns;
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

    auto column = read_column(*index_in, std::move(inflater));
    assert(column);

    columns.emplace_back(std::move(column));
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

}
}
