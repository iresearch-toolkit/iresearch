////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "formats_10.hpp"

extern "C" {
#include <simdbitpacking.h>
#include <avxbitpacking.h>
}

#include "shared.hpp"
#include "skip_list.hpp"

#include "formats_10_attributes.hpp"
#include "formats_burst_trie.hpp"
#include "columnstore.hpp"
#include "columnstore2.hpp"
#include "format_utils.hpp"

#include "analysis/token_attributes.hpp"

#include "index/field_meta.hpp"
#include "index/file_names.hpp"
#include "index/index_meta.hpp"
#include "index/index_features.hpp"
#include "index/index_reader.hpp"

#include "search/cost.hpp"
#include "search/score.hpp"

#include "store/memory_directory.hpp"
#include "store/store_utils.hpp"

#include "utils/bitpack.hpp"
#include "utils/encryption.hpp"
#include "utils/frozen_attributes.hpp"
#include "utils/directory_utils.hpp"
#include "utils/log.hpp"
#include "utils/memory.hpp"
#include "utils/memory_pool.hpp"
#include "utils/noncopyable.hpp"
#include "utils/timer_utils.hpp"
#include "utils/type_limits.hpp"
#include "utils/std.hpp"

#if defined(_MSC_VER)
#pragma warning(disable : 4351)
#endif

namespace {

using namespace irs;

// name of the module holding different formats
constexpr string_ref MODULE_NAME = "10";

template<bool Wand, uint32_t PosMin>
struct format_traits {
  using align_type = uint32_t;

  static constexpr uint32_t block_size() noexcept { return 128; }
  // initial base value for writing positions offsets
  static constexpr uint32_t pos_min() noexcept { return PosMin; }
  static constexpr bool wand() noexcept { return Wand; }

  FORCE_INLINE static void pack_block32(
      const uint32_t* RESTRICT decoded,
      uint32_t* RESTRICT encoded,
      const uint32_t bits) noexcept {
    packed::pack_block(decoded, encoded, bits);
    packed::pack_block(decoded + packed::BLOCK_SIZE_32, encoded + bits, bits);
    packed::pack_block(decoded + 2*packed::BLOCK_SIZE_32, encoded + 2*bits, bits);
    packed::pack_block(decoded + 3*packed::BLOCK_SIZE_32, encoded + 3*bits, bits);
  }

  FORCE_INLINE static void unpack_block32(
      uint32_t* RESTRICT decoded,
      const uint32_t* RESTRICT encoded,
      const uint32_t bits) noexcept {
    packed::unpack_block(encoded, decoded, bits);
    packed::unpack_block(encoded + bits, decoded + packed::BLOCK_SIZE_32, bits);
    packed::unpack_block(encoded + 2*bits, decoded + 2*packed::BLOCK_SIZE_32, bits);
    packed::unpack_block(encoded + 3*bits, decoded + 3*packed::BLOCK_SIZE_32, bits);
  }

  FORCE_INLINE static void pack32(
      const uint32_t* RESTRICT decoded,
      uint32_t* RESTRICT encoded,
      size_t size,
      const uint32_t bits) noexcept {
    assert(encoded);
    assert(decoded);
    assert(size);
    packed::pack(decoded, decoded + size, encoded, bits);
  }

  FORCE_INLINE static void pack64(
      const uint64_t* RESTRICT decoded,
      uint64_t* RESTRICT encoded,
      size_t size,
      const uint32_t bits) noexcept {
    assert(encoded);
    assert(decoded);
    assert(size);
    packed::pack(decoded, decoded + size, encoded, bits);
  }

  FORCE_INLINE static void write_block(
      index_output& out, const uint32_t* in, uint32_t* buf) {
    bitpack::write_block32<block_size()>(&pack_block32, out, in, buf);
  }

  FORCE_INLINE static void read_block(
      index_input& in, uint32_t* buf,  uint32_t* out) {
    bitpack::read_block32<block_size()>(&unpack_block32, in, buf, out);
  }

  FORCE_INLINE static void skip_block(index_input& in) {
    bitpack::skip_block32(in, block_size());
  }
}; // format_traits

// ----------------------------------------------------------------------------
// --SECTION--                                             forward declarations
// ----------------------------------------------------------------------------

template<typename T, typename M>
std::string file_name(const M& meta);

// ----------------------------------------------------------------------------
// --SECTION--                                                 helper functions
// ----------------------------------------------------------------------------

void prepare_output(
    std::string& str,
    index_output::ptr& out,
    const flush_state& state,
    string_ref ext,
    string_ref format,
    const int32_t version) {
  assert(!out);
  file_name(str, state.name, ext);
  out = state.dir->create(str);

  if (!out) {
    throw io_error(string_utils::to_string(
      "failed to create file, path: %s",
      str.c_str()));
  }

  format_utils::write_header(*out, format, version);
}

void prepare_input(
    std::string& str,
    index_input::ptr& in,
    IOAdvice advice,
    const reader_state& state,
    string_ref ext,
    string_ref format,
    const int32_t min_ver,
    const int32_t max_ver ) {
  assert(!in);
  file_name(str, state.meta->name, ext);
  in = state.dir->open(str, advice);

  if (!in) {
    throw io_error(string_utils::to_string(
      "failed to open file, path: %s",
      str.c_str()));
  }

  format_utils::check_header(*in, format, min_ver, max_ver);
}

// ----------------------------------------------------------------------------
// --SECTION--                                                   helper buffers
// ----------------------------------------------------------------------------

//////////////////////////////////////////////////////////////////////////////
/// @struct skip_buffer
/// @brief buffer for storing skip data
//////////////////////////////////////////////////////////////////////////////
struct skip_buffer {
  explicit skip_buffer(uint64_t* skip_ptr) noexcept
    : skip_ptr{skip_ptr} {
  }

  void reset() noexcept {
    start = end = 0;
  }

  uint64_t* skip_ptr; // skip data
  uint64_t start{};   // start position of block
  uint64_t end{};     // end position of block
}; // skip_buffer

// Buffer for stroring doc data
struct doc_buffer : skip_buffer {
  doc_buffer(std::span<doc_id_t>& docs,
             std::span<uint32_t>& freqs,
             doc_id_t* skip_doc,
             uint64_t* skip_ptr) noexcept
    : skip_buffer{skip_ptr},
      docs{docs},
      freqs{freqs},
      skip_doc{skip_doc} {
  }

  bool full() const noexcept {
    return doc == std::end(docs);
  }

  bool empty() const noexcept {
    return doc == std::begin(docs);
  }

  void push(doc_id_t doc, uint32_t freq) noexcept {
    *this->doc = doc;
    ++this->doc;
    *this->freq = freq;
    ++this->freq;
    last = doc;
  }

  void reset() noexcept {
    skip_buffer::reset();
    doc = docs.begin();
    freq = freqs.begin();
    last = doc_limits::invalid();
    block_last = doc_limits::min();
  }

  std::span<doc_id_t> docs;
  std::span<uint32_t> freqs;
  uint32_t* skip_doc;
  std::span<doc_id_t>::iterator doc{ docs.begin() };
  std::span<uint32_t>::iterator freq{ freqs.begin() };
  doc_id_t last{ doc_limits::invalid() }; // last buffered document id
  doc_id_t block_last{ doc_limits::min() }; // last document id in a block
};

//////////////////////////////////////////////////////////////////////////////
/// @struct pos_buffer
/// @brief buffer for storing positions
//////////////////////////////////////////////////////////////////////////////
struct pos_buffer : skip_buffer {
  explicit pos_buffer(
      std::span<uint32_t> buf,
      uint64_t* skip_ptr) noexcept
    : skip_buffer{skip_ptr},
      buf{buf} {
  }

  bool full() const noexcept {
    return buf.size() == size;
  }

  void next(uint32_t pos) noexcept {
    last = pos;
    ++size;
  }

  void pos(uint32_t pos) noexcept {
    buf[size] = pos;
  }

  void reset() noexcept {
    skip_buffer::reset();
    last = 0;
    block_last = 0;
    size = 0;
  }

  std::span<uint32_t> buf;   // buffer to store position deltas
  uint32_t last{};       // last buffered position
  uint32_t block_last{}; // last position in a block
  uint32_t size{};       // number of buffered elements
}; // pos_buffer

//////////////////////////////////////////////////////////////////////////////
/// @struct pay_buffer
/// @brief buffer for storing payload data
//////////////////////////////////////////////////////////////////////////////
struct pay_buffer : skip_buffer {
  pay_buffer(uint32_t* pay_sizes,
             uint32_t* offs_start_buf,
             uint32_t* offs_len_buf,
             uint64_t* skip_ptr) noexcept
    : skip_buffer{skip_ptr},
      pay_sizes{pay_sizes},
      offs_start_buf{offs_start_buf},
      offs_len_buf{offs_len_buf} {
   }

    void push_payload(uint32_t i, bytes_ref pay) {
    if (!pay.empty()) {
      pay_buf_.append(pay.c_str(), pay.size());
    }
    pay_sizes[i] = static_cast<uint32_t>(pay.size());
  }

  void push_offset(uint32_t i, uint32_t start, uint32_t end) noexcept {
    assert(start >= last && start <= end);

    offs_start_buf[i] = start - last;
    offs_len_buf[i] = end - start;
    last = start;
  }

  void reset() noexcept {
    skip_buffer::reset();
    pay_buf_.clear();
    block_last = 0;
    last = 0;
  }

  uint32_t* pay_sizes;       // buffer to store payloads sizes
  uint32_t* offs_start_buf;  // buffer to store start offsets
  uint32_t* offs_len_buf;    // buffer to store offset lengths
  bstring pay_buf_;          // buffer for payload
  size_t block_last{};       // last payload buffer length in a block
  uint32_t last{};           // last start offset
}; // pay_buffer

//////////////////////////////////////////////////////////////////////////////
/// @struct score_buffer
/// @brief buffer carrying competitive block scores
//////////////////////////////////////////////////////////////////////////////
class score_buffer {
 public:
  using value_type = score_threshold::value_type;

  static void skip(index_input& in) {
    in.read_vint();
  }

  void add(value_type value) noexcept {
    freq_ = std::max(value, freq_);
  }

  void add(const score_buffer& rhs) noexcept {
    add(rhs.freq_);
  }

  void reset() noexcept {
    freq_ = 0;
  }

  void write(memory_index_output& out) const {
    out.write_vint(freq_);
  }

  void read(index_input& in) {
    freq_ = in.read_vint();
  }

  value_type value() const noexcept {
    return freq_;
  }

 private:
  value_type freq_{};
}; // score_buffer

//////////////////////////////////////////////////////////////////////////////
/// @enum TermsFormat
//////////////////////////////////////////////////////////////////////////////
enum class TermsFormat : int32_t {
  MIN = 0,
  MAX = MIN
};

//////////////////////////////////////////////////////////////////////////////
/// @enum PostingsFormat
//////////////////////////////////////////////////////////////////////////////
enum class PostingsFormat : int32_t {
  MIN = 0,

  // positions are stored one based (if first osition is 1 first offset is 0)
  // This forces reader to adjust first read position of every document additionally to
  // stored increment. Or incorrect positions will be read - 1 2 3 will be stored
  // (offsets 0 1 1) but 0 1 2 will be read. At least this will lead to incorrect results in
  // by_same_positions filter if searching for position 1
  POSITIONS_ONEBASED = MIN,

  // positions are stored one based, sse used
  POSITIONS_ONEBASED_SSE,

  // positions are stored zero based
  // if first position is 1 first offset is also 1
  // so no need to adjust position while reading first
  // position for document, always just increment from previous pos
  POSITIONS_ZEROBASED,

  // positions are stored zero based, sse used
  POSITIONS_ZEROBASED_SSE,

  // store competitive scores in blocks
  WAND,

  // store block max scores, sse used
  WAND_SSE,

  MAX = WAND_SSE
};

// ----------------------------------------------------------------------------
// --SECTION--                                                  postings_writer
// ----------------------------------------------------------------------------
//
// Assume that doc_count = 28, skip_n = skip_0 = 12
//
//  |       block#0       | |      block#1        | |vInts|
//  d d d d d d d d d d d d d d d d d d d d d d d d d d d d (posting list)
//                          ^                       ^       (level 0 skip point)
//
// ----------------------------------------------------------------------------

//////////////////////////////////////////////////////////////////////////////
/// @class postings_writer_base
//////////////////////////////////////////////////////////////////////////////
class postings_writer_base : public irs::postings_writer {
 public:
  static constexpr uint32_t MAX_SKIP_LEVELS = 10;
  static constexpr uint32_t SKIP_N = 8;

  static constexpr string_ref DOC_FORMAT_NAME = "iresearch_10_postings_documents";
  static constexpr string_ref DOC_EXT = "doc";
  static constexpr string_ref POS_FORMAT_NAME = "iresearch_10_postings_positions";
  static constexpr string_ref POS_EXT = "pos";
  static constexpr string_ref PAY_FORMAT_NAME = "iresearch_10_postings_payloads";
  static constexpr string_ref PAY_EXT = "pay";
  static constexpr string_ref TERMS_FORMAT_NAME = "iresearch_10_postings_terms";

 protected:
  postings_writer_base(
      doc_id_t block_size,
      std::span<doc_id_t> docs,
      std::span<uint32_t> freqs,
      doc_id_t* skip_doc,
      uint64_t* doc_skip_ptr,
      std::span<uint32_t> prox_buf,
      uint64_t* prox_skip_ptr,
      uint32_t* pay_sizes,
      uint32_t* offs_start_buf,
      uint32_t* offs_len_buf,
      uint64_t* pay_skip_ptr,
      uint32_t* enc_buf,
      PostingsFormat postings_format_version,
      TermsFormat terms_format_version)
    : skip_{block_size, SKIP_N},
      doc_{docs, freqs, skip_doc, doc_skip_ptr},
      pos_{prox_buf, prox_skip_ptr},
      pay_{pay_sizes, offs_start_buf, offs_len_buf, pay_skip_ptr},
      buf_{enc_buf},
      postings_format_version_{postings_format_version},
      terms_format_version_{terms_format_version} {
    assert(postings_format_version >= PostingsFormat::MIN &&
           postings_format_version <= PostingsFormat::MAX);
    assert(terms_format_version >= TermsFormat::MIN &&
           terms_format_version <= TermsFormat::MAX);
  }

 public:
  virtual irs::attribute* get_mutable(irs::type_info::type_id type) noexcept final {
    return irs::type<version10::documents>::id() == type ? &docs_ : nullptr;
  }

  virtual void begin_field(IndexFeatures features) final {
    features_ = features;
    docs_.value.clear();
    last_state_.clear();
  }

  virtual void begin_block() final {
    // clear state in order to write
    // absolute address of the first
    // entry in the block
    last_state_.clear();
  }

  virtual void prepare(index_output& out, const irs::flush_state& state) final;
  virtual void encode(data_output& out, const irs::term_meta& attrs) final;
  virtual void end() final;

 protected:
  struct attributes {
    const frequency* freq_{};
    irs::position* pos_{};
    const offset* offs_{};
    const payload* pay_{};

    void reset(attribute_provider& attrs) noexcept {
      pos_ = irs::position::empty();
      offs_ = nullptr;
      pay_ = nullptr;

      freq_ = irs::get<frequency>(attrs);
      if (freq_) {
        auto* pos = irs::get_mutable<irs::position>(&attrs);
        if (pos) {
          pos_ = pos;
          offs_ = irs::get<irs::offset>(*pos_);
          pay_ = irs::get<irs::payload>(*pos_);
        }
      }
    }
  };

  virtual void release(irs::term_meta *meta) noexcept final {
    auto* state = static_cast<version10::term_meta*>(meta);
    assert(state);

    alloc_.destroy(state);
    alloc_.deallocate(state);
  }

  void write_skip(size_t level, memory_index_output& out) const;
  void begin_term();
  void end_term(version10::term_meta& meta);
  void end_doc();

  memory::memory_pool<> meta_pool_;
  memory::memory_pool_allocator<version10::term_meta, decltype(meta_pool_)> alloc_{ meta_pool_ };
  SkipWriter skip_;
  version10::term_meta last_state_;               // last final term state
  version10::documents docs_;                     // bit set of all processed documents
  index_output::ptr doc_out_;                     // postings (doc + freq)
  index_output::ptr pos_out_;                     // positions
  index_output::ptr pay_out_;                     // payload (payl + offs)
  doc_buffer doc_;                                // document stream
  pos_buffer pos_;                                // proximity stream
  pay_buffer pay_;                                // payloads and offsets stream
  uint32_t* buf_;                                 // buffer for encoding
  attributes attrs_;                              // set of attributes
  IndexFeatures features_;                        // features supported by current field
  const PostingsFormat postings_format_version_;
  const TermsFormat terms_format_version_;
}; // postings_writer_base

void postings_writer_base::write_skip(
    size_t level, memory_index_output &out) const {
  const doc_id_t doc_delta = doc_.block_last; //- doc_.skip_doc[level];
  const uint64_t doc_ptr = doc_out_->file_pointer();

  out.write_vint(doc_delta);
  out.write_vlong(doc_ptr - doc_.skip_ptr[level]);

  doc_.skip_doc[level] = doc_.block_last;
  doc_.skip_ptr[level] = doc_ptr;

  if (IndexFeatures::NONE != (features_ & IndexFeatures::POS)) {
    const uint64_t pos_ptr = pos_out_->file_pointer();

    out.write_vint(pos_.block_last);
    out.write_vlong(pos_ptr - pos_.skip_ptr[level]);

    pos_.skip_ptr[level] = pos_ptr;

    if (IndexFeatures::NONE != (features_ & (IndexFeatures::OFFS | IndexFeatures::PAY))) {
      assert(pay_out_);

      if (IndexFeatures::NONE != (features_ & IndexFeatures::PAY)) {
        out.write_vint(static_cast<uint32_t>(pay_.block_last));
      }

      const uint64_t pay_ptr = pay_out_->file_pointer();

      out.write_vlong(pay_ptr - pay_.skip_ptr[level]);
      pay_.skip_ptr[level] = pay_ptr;
    }
  }
}

void postings_writer_base::prepare(index_output& out, const irs::flush_state& state) {
  assert(state.dir);
  assert(!state.name.null());

  std::string name;

  // prepare document stream
  prepare_output(name, doc_out_, state,
                 DOC_EXT, DOC_FORMAT_NAME,
                 static_cast<int32_t>(postings_format_version_));

  if (IndexFeatures::NONE != (state.index_features & IndexFeatures::POS)) {
    // prepare proximity stream
    pos_.reset();
    prepare_output(name, pos_out_, state,
                   POS_EXT, POS_FORMAT_NAME,
                   static_cast<int32_t>(postings_format_version_));

    if (IndexFeatures::NONE != (state.index_features & (IndexFeatures::PAY | IndexFeatures::OFFS))) {
      // prepare payload stream
      pay_.reset();
      prepare_output(name, pay_out_, state,
                     PAY_EXT, PAY_FORMAT_NAME,
                     static_cast<int32_t>(postings_format_version_));
    }
  }

  skip_.Prepare(
    MAX_SKIP_LEVELS,
    state.doc_count,
    state.dir->attributes().allocator());

  format_utils::write_header(
    out, TERMS_FORMAT_NAME,
    static_cast<int32_t>(terms_format_version_));
  out.write_vint(skip_.Skip0()); // write postings block size

  // prepare documents bitset
  docs_.value.reset(doc_limits::min() + state.doc_count);
}

void postings_writer_base::encode(
    data_output& out,
    const irs::term_meta& state) {
  const auto& meta = static_cast<const version10::term_meta&>(state);

  out.write_vint(meta.docs_count);
  if (meta.freq != std::numeric_limits<uint32_t>::max()) {
    assert(meta.freq >= meta.docs_count);
    out.write_vint(meta.freq - meta.docs_count);
  }

  out.write_vlong(meta.doc_start - last_state_.doc_start);
  if (IndexFeatures::NONE != (features_ & IndexFeatures::POS)) {
    out.write_vlong(meta.pos_start - last_state_.pos_start);
    if (type_limits<type_t::address_t>::valid(meta.pos_end)) {
      out.write_vlong(meta.pos_end);
    }
    if (IndexFeatures::NONE != (features_ & (IndexFeatures::OFFS | IndexFeatures::PAY))) {
      out.write_vlong(meta.pay_start - last_state_.pay_start);
    }
  }

  if (1U == meta.docs_count || meta.docs_count > skip_.Skip0()) {
    out.write_vlong(meta.e_skip_start);
  }

  last_state_ = meta;
}

void postings_writer_base::end() {
  format_utils::write_footer(*doc_out_);
  doc_out_.reset(); // ensure stream is closed

  if (pos_out_) {
    format_utils::write_footer(*pos_out_);
    pos_out_.reset(); // ensure stream is closed
  }

  if (pay_out_) {
    format_utils::write_footer(*pay_out_);
    pay_out_.reset(); // ensure stream is closed
  }
}

void postings_writer_base::begin_term() {
  doc_.start = doc_out_->file_pointer();
  std::fill_n(doc_.skip_ptr, MAX_SKIP_LEVELS, doc_.start);
  if (IndexFeatures::NONE != (features_ & IndexFeatures::POS)) {
    assert(pos_out_);
    pos_.start = pos_out_->file_pointer();
    std::fill_n(pos_.skip_ptr, MAX_SKIP_LEVELS, pos_.start);
    if (IndexFeatures::NONE != (features_ & (IndexFeatures::OFFS | IndexFeatures::PAY))) {
      assert(pay_out_);
      pay_.start = pay_out_->file_pointer();
      std::fill_n(pay_.skip_ptr, MAX_SKIP_LEVELS, pay_.start);
    }
  }

  doc_.last = doc_limits::invalid();
  doc_.block_last = doc_limits::min();
  skip_.Reset();
}

void postings_writer_base::end_doc() {
  if (doc_.full()) {
    doc_.block_last = doc_.last;
    doc_.end = doc_out_->file_pointer();
    if (IndexFeatures::NONE != (features_ & IndexFeatures::POS)) {
      assert(pos_out_);
      pos_.end = pos_out_->file_pointer();
      // documents stream is full, but positions stream is not
      // save number of positions to skip before the next block
      pos_.block_last = pos_.size;
      if (IndexFeatures::NONE != (features_ & (IndexFeatures::OFFS | IndexFeatures::PAY))) {
        assert(pay_out_);
        pay_.end = pay_out_->file_pointer();
        pay_.block_last = pay_.pay_buf_.size();
      }
    }

    doc_.doc = doc_.docs.begin();
    doc_.freq = doc_.freqs.begin();
  }
}

void postings_writer_base::end_term(version10::term_meta& meta) {
  if (meta.docs_count == 0) {
    return; // no documents to write
  }

  if (1 == meta.docs_count) {
    meta.e_single_doc = doc_.docs[0] - doc_limits::min();
  } else {
    // write remaining documents using
    // variable length encoding
    auto& out = *doc_out_;
    auto doc = doc_.docs.begin();
    auto prev = doc_.block_last;

    if (IndexFeatures::NONE != (features_ & IndexFeatures::FREQ)) {
      auto doc_freq = doc_.freqs.begin();
      for (; doc < doc_.doc; ++doc) {
        const uint32_t freq = *doc_freq;
        const doc_id_t delta = *doc - prev;

        if (1 == freq) {
          out.write_vint(shift_pack_32(delta, true));
        } else {
          out.write_vint(shift_pack_32(delta, false));
          out.write_vint(freq);
        }

        ++doc_freq;
        prev = *doc;
      }
    } else {
      for (; doc < doc_.doc; ++doc) {
        out.write_vint(*doc - prev);
        prev = *doc;
      }
    }
  }

  meta.pos_end = type_limits<type_t::address_t>::invalid();

  // write remaining position using
  // variable length encoding
  if (IndexFeatures::NONE != (features_ & IndexFeatures::POS)) {
    assert(pos_out_);

    if (meta.freq > skip_.Skip0()) {
      meta.pos_end = pos_out_->file_pointer() - pos_.start;
    }

    if (pos_.size > 0) {
      data_output& out = *pos_out_;
      uint32_t last_pay_size = std::numeric_limits<uint32_t>::max();
      uint32_t last_offs_len = std::numeric_limits<uint32_t>::max();
      uint32_t pay_buf_start = 0;
      for (uint32_t i = 0; i < pos_.size; ++i) {
        const uint32_t pos_delta = pos_.buf[i];
        if (IndexFeatures::NONE != (features_ & IndexFeatures::PAY)) {
          assert(pay_out_);

          const uint32_t size = pay_.pay_sizes[i];
          if (last_pay_size != size) {
            last_pay_size = size;
            out.write_vint(shift_pack_32(pos_delta, true));
            out.write_vint(size);
          } else {
            out.write_vint(shift_pack_32(pos_delta, false));
          }

          if (size != 0) {
            out.write_bytes(pay_.pay_buf_.c_str() + pay_buf_start, size);
            pay_buf_start += size;
          }
        } else {
          out.write_vint(pos_delta);
        }

        if (IndexFeatures::NONE != (features_ & IndexFeatures::OFFS)) {
          assert(pay_out_);

          const uint32_t pay_offs_delta = pay_.offs_start_buf[i];
          const uint32_t len = pay_.offs_len_buf[i];
          if (len == last_offs_len) {
            out.write_vint(shift_pack_32(pay_offs_delta, false));
          } else {
            out.write_vint(shift_pack_32(pay_offs_delta, true));
            out.write_vint(len);
            last_offs_len = len;
          }
        }
      }

      if (IndexFeatures::NONE != (features_ & IndexFeatures::PAY)) {
        assert(pay_out_);
        pay_.pay_buf_.clear();
      }
    }
  }

  if (!attrs_.freq_) {
    meta.freq = std::numeric_limits<uint32_t>::max();
  }

  // if we have flushed at least
  // one block there was buffered
  // skip data, so we need to flush it
  if (meta.docs_count > skip_.Skip0()) {
    meta.e_skip_start = doc_out_->file_pointer() - doc_.start;
    skip_.Flush(*doc_out_);
  }

  doc_.doc = doc_.docs.begin();
  doc_.freq = doc_.freqs.begin();
  doc_.last = doc_limits::invalid();
  meta.doc_start = doc_.start;

  if (pos_out_) {
    pos_.size = 0;
    meta.pos_start = pos_.start;
  }

  if (pay_out_) {
    pay_.pay_buf_.clear();
    pay_.last = 0;
    meta.pay_start = pay_.start;
  }
}

//////////////////////////////////////////////////////////////////////////////
/// @class postings_writer
//////////////////////////////////////////////////////////////////////////////
template<typename FormatTraits>
class postings_writer final: public postings_writer_base {
 public:
  explicit postings_writer(PostingsFormat version, bool volatile_attributes)
    : postings_writer_base{
        FormatTraits::block_size(),
        std::span{doc_buf_.docs},
        std::span{doc_buf_.freqs},
        doc_buf_.skip_doc,
        doc_buf_.skip_ptr,
        std::span{prox_buf_.buf},
        prox_buf_.skip_ptr,
        pay_buf_.pay_sizes,
        pay_buf_.offs_start_buf,
        pay_buf_.offs_len_buf,
        pay_buf_.skip_ptr,
        encbuf_.buf,
        version,
        TermsFormat::MAX },
      volatile_attributes_{volatile_attributes} {
    assert(
      (postings_format_version_ >= PostingsFormat::POSITIONS_ZEROBASED
         ? pos_limits::invalid()
         : pos_limits::min()) == FormatTraits::pos_min());
  }

  virtual irs::postings_writer::state write(irs::doc_iterator& docs) override;

 private:
  void add_position(uint32_t pos);
  void begin_doc(doc_id_t id, uint32_t freq);

  struct {
    doc_id_t docs[FormatTraits::block_size()]{};           // buffer to store document deltas
    uint32_t freqs[FormatTraits::block_size()]{};          // buffer to store frequencies
    doc_id_t skip_doc[MAX_SKIP_LEVELS]{};                  // buffer to store skip documents
    uint64_t skip_ptr[MAX_SKIP_LEVELS]{};                  // buffer to store skip pointers
  } doc_buf_;
  struct {
    uint32_t buf[FormatTraits::block_size()]{};             // buffer to store position deltas
    uint64_t skip_ptr[MAX_SKIP_LEVELS]{};                   // buffer to store skip pointers
  } prox_buf_;
  struct {
    uint32_t pay_sizes[FormatTraits::block_size()]{};       // buffer to store payloads sizes
    uint32_t offs_start_buf[FormatTraits::block_size()]{};  // buffer to store start offsets
    uint32_t offs_len_buf[FormatTraits::block_size()]{};    // buffer to store offset lengths
    uint64_t skip_ptr[MAX_SKIP_LEVELS]{};                   // buffer to store skip pointers
  } pay_buf_;
  struct {
    uint32_t buf[FormatTraits::block_size()];               // buffer for encoding (worst case)
  } encbuf_;
  score_buffer score_buf_;
  score_buffer score_levels_[MAX_SKIP_LEVELS];
  bool volatile_attributes_;
}; // postings_writer


template<typename FormatTraits>
void postings_writer<FormatTraits>::begin_doc(doc_id_t id, uint32_t freq) {
  if (IRS_LIKELY(doc_.last < id)) {
    doc_.push(id, freq);

    if (doc_.full()) {
      // FIXME do aligned?
      simd::delta_encode<FormatTraits::block_size(), false>(doc_.docs.data(), doc_.block_last);
      FormatTraits::write_block(*doc_out_, doc_.docs.data(), buf_);
      if (attrs_.freq_) {
        assert(freq);
        FormatTraits::write_block(*doc_out_, doc_.freqs.data(), buf_);
      }
    }

    docs_.value.set(id);

    // first position offsets now is format dependent
    pos_.last = FormatTraits::pos_min();
    pay_.last = 0;
  } else {
    throw index_error(string_utils::to_string(
      "while beginning doc_ in postings_writer, error: docs out of order '%d' < '%d'",
      id, doc_.last));
  }
}

template<typename FormatTraits>
void postings_writer<FormatTraits>::add_position(uint32_t pos) {
  // at least positions stream should be created
  assert(IndexFeatures::NONE != (features_ & IndexFeatures::POS) && pos_out_);
  assert(!attrs_.offs_ || attrs_.offs_->start <= attrs_.offs_->end);

  pos_.pos(pos - pos_.last);

  if (attrs_.pay_) {
    pay_.push_payload(pos_.size, attrs_.pay_->value);
  }

  if (attrs_.offs_) {
    pay_.push_offset(pos_.size, attrs_.offs_->start, attrs_.offs_->end);
  }

  pos_.next(pos);

  if (pos_.full()) {
    FormatTraits::write_block(*pos_out_, pos_.buf.data(), buf_);
    pos_.size = 0;

    if (attrs_.pay_) {
      assert(IndexFeatures::NONE != (features_ & IndexFeatures::PAY) && pay_out_);
      auto& pay_buf = pay_.pay_buf_;

      pay_out_->write_vint(static_cast<uint32_t>(pay_buf.size()));
      if (!pay_buf.empty()) {
        FormatTraits::write_block(*pay_out_, pay_.pay_sizes, buf_);
        pay_out_->write_bytes(pay_buf.c_str(), pay_buf.size());
        pay_buf.clear();
      }
    }

    if (attrs_.offs_) {
      assert(IndexFeatures::NONE != (features_ & IndexFeatures::OFFS) && pay_out_);
      FormatTraits::write_block(*pay_out_, pay_.offs_start_buf, buf_);
      FormatTraits::write_block(*pay_out_, pay_.offs_len_buf, buf_);
    }
  }
}

#if defined(_MSC_VER)
  #pragma warning(disable : 4706)
#elif defined(__GNUC__)
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wparentheses"
#endif

template<typename FormatTraits>
irs::postings_writer::state postings_writer<FormatTraits>::write(
    irs::doc_iterator& docs) {
  REGISTER_TIMER_DETAILED();

  auto* doc = irs::get<document>(docs);

  if (!doc) {
    assert(false);
    throw illegal_argument{"'document' attribute is missing"};
  }

  const frequency* freq{};

  auto refresh = [this, &freq, no_freq = frequency{}](auto& attrs) noexcept {
    attrs_.reset(attrs);
    freq = attrs_.freq_ ? attrs_.freq_ : &no_freq;
  };

  if (!volatile_attributes_) {
    refresh(docs);
  } else {
    auto* subscription = irs::get<attribute_provider_change>(docs);
    assert(subscription);

    subscription->subscribe([refresh](attribute_provider& attrs) {
      refresh(attrs);
    });
  }

  auto meta = memory::allocate_unique<version10::term_meta>(alloc_);

  begin_term();
  if constexpr (FormatTraits::wand()) {
    score_buf_.reset();
  }

  uint32_t docs_count = 0;
  uint32_t total_freq = 0;

  while (docs.next()) {
    const auto did = doc->value;
    assert(doc_limits::valid(did));
    assert(freq);
    const uint32_t freqv = freq->value;

    if (doc_limits::valid(doc_.last) && doc_.empty()) {
      score_levels_[0].add(score_buf_);

      skip_.Skip(
        docs_count,
        [this](size_t level, memory_index_output& out) {
          write_skip(level, out);

          if constexpr (FormatTraits::wand()) {
            if (IndexFeatures::NONE != (features_ & IndexFeatures::FREQ)) {
              auto& score = score_levels_[level];
              if (level) {
                score.add(score_levels_[level - 1]);
              }
              score.write(out);
            }
          }
      });

      if constexpr (FormatTraits::wand()) {
        score_buf_.reset();
      }
    }

    begin_doc(did, freqv);
    if constexpr (FormatTraits::wand()) {
      score_buf_.add(freqv);
    }

    assert(attrs_.pos_);
    while (attrs_.pos_->next()) {
      assert(pos_limits::valid(attrs_.pos_->value()));
      add_position(attrs_.pos_->value());
    }

    ++docs_count;
    total_freq += freqv;

    end_doc();
  }

  meta->docs_count = docs_count;
  meta->freq = total_freq;
  end_term(*meta);

  return make_state(*meta.release());
}

#if defined(_MSC_VER)
  #pragma warning(default : 4706)
#elif defined(__GNUC__)
  #pragma GCC diagnostic pop
#endif

struct skip_state {
  uint64_t pay_ptr{}; // pointer to the payloads of the first document in a document block
  uint64_t pos_ptr{}; // pointer to the positions of the first document in a document block
  size_t pend_pos{}; // positions to skip before new document block
  uint64_t doc_ptr{}; // pointer to the beginning of document block
  doc_id_t doc{ doc_limits::invalid() }; // last document in a previous block
  uint32_t pay_pos{}; // payload size to skip before in new document block
};

template<typename FieldTraits>
FORCE_INLINE void CopyState(skip_state& to, const skip_state& from) noexcept {
  if constexpr (FieldTraits::position() &&
                (FieldTraits::payload() || FieldTraits::offset())) {
    to = from;
  } else {
    if constexpr (FieldTraits::position()) {
      to.pos_ptr = from.pos_ptr;
      to.pend_pos = from.pend_pos;
    }
    to.doc_ptr = from.doc_ptr;
    to.doc = from.doc;
  }
}

template<typename FieldTraits>
FORCE_INLINE void ReadState(skip_state& state, index_input& in) {
  state.doc = in.read_vint();
  state.doc_ptr += in.read_vlong();

  if constexpr (FieldTraits::position()) {
    state.pend_pos = in.read_vint();
    state.pos_ptr += in.read_vlong();

    if constexpr (FieldTraits::payload() || FieldTraits::offset()) {
      if constexpr (FieldTraits::payload()) {
        state.pay_pos = in.read_vint();
      }

      state.pay_ptr += in.read_vlong();
    }
  }
}

struct doc_state {
  const index_input* pos_in;
  const index_input* pay_in;
  const version10::term_meta* term_state;
  const uint32_t* freq;
  uint32_t* enc_buf;
  uint64_t tail_start;
  size_t tail_length;
};

template<typename FieldTraits>
FORCE_INLINE void CopyState(skip_state& to, const version10::term_meta& from) noexcept {
  to.doc_ptr = from.doc_start;
  if constexpr (FieldTraits::position()) {
    to.pos_ptr = from.pos_start;
    if constexpr (FieldTraits::payload() || FieldTraits::offset()) {
      to.pay_ptr = from.pay_start;
    }
  }
}

///////////////////////////////////////////////////////////////////////////////
/// @class pos_iterator_base
///////////////////////////////////////////////////////////////////////////////
template<typename IteratorTraits,
         typename FieldTraits,
         bool Offset = IteratorTraits::offset(),
         bool Payload = IteratorTraits::payload()>
struct position_impl;

///////////////////////////////////////////////////////////////////////////////
/// @class pos_iterator_base (position + payload + offset)
///////////////////////////////////////////////////////////////////////////////
template<typename IteratorTraits, typename FieldTraits>
struct position_impl<IteratorTraits, FieldTraits, true, true>
    : public position_impl<IteratorTraits, FieldTraits, false, false> {
  typedef position_impl<IteratorTraits, FieldTraits, false, false> base;

  irs::attribute* attribute(irs::type_info::type_id type) noexcept {
    if (irs::type<payload>::id() == type) {
      return &pay_;
    }

    return irs::type<offset>::id() == type ? &offs_ : nullptr;
  }

  void prepare(const doc_state& state) {
    base::prepare(state);

    pay_in_ = state.pay_in->reopen(); // reopen thread-safe stream

    if (!pay_in_) {
      // implementation returned wrong pointer
      IR_FRMT_ERROR("Failed to reopen payload input in: %s", __FUNCTION__);

      throw io_error("failed to reopen payload input");
    }

    pay_in_->seek(state.term_state->pay_start);
  }

  void prepare(const skip_state& state)  {
    base::prepare(state);

    pay_in_->seek(state.pay_ptr);
    pay_data_pos_ = state.pay_pos;
  }

  void read_attributes() noexcept {
    offs_.start += offs_start_deltas_[this->buf_pos_];
    offs_.end = offs_.start + offs_lengts_[this->buf_pos_];

    pay_.value = bytes_ref(
      pay_data_.c_str() + pay_data_pos_,
      pay_lengths_[this->buf_pos_]);
    pay_data_pos_ += pay_lengths_[this->buf_pos_];
  }

  void clear_attributes() noexcept {
    offs_.clear();
    pay_.value = bytes_ref::NIL;
  }

  void read_block() {
    base::read_block();

    // read payload
    const uint32_t size = pay_in_->read_vint();
    if (size) {
      IteratorTraits::read_block(*pay_in_, this->enc_buf_, pay_lengths_);
      string_utils::oversize(pay_data_, size);

      #ifdef IRESEARCH_DEBUG
        const auto read = pay_in_->read_bytes(&(pay_data_[0]), size);
        assert(read == size);
        UNUSED(read);
      #else
        pay_in_->read_bytes(&(pay_data_[0]), size);
      #endif // IRESEARCH_DEBUG
    }

    // read offsets
    IteratorTraits::read_block(*pay_in_, this->enc_buf_, offs_start_deltas_);
    IteratorTraits::read_block(*pay_in_, this->enc_buf_, offs_lengts_);

    pay_data_pos_ = 0;
  }

  void read_tail_block() {
    size_t pos = 0;

    for (size_t i = 0; i < this->tail_length_; ++i) {
      // read payloads
      if (shift_unpack_32(this->pos_in_->read_vint(), base::pos_deltas_[i])) {
        pay_lengths_[i] = this->pos_in_->read_vint();
      } else {
        assert(i);
        pay_lengths_[i] = pay_lengths_[i-1];
      }

      if (pay_lengths_[i]) {
        const auto size = pay_lengths_[i]; // length of current payload

        string_utils::oversize(pay_data_, pos + size);

        #ifdef IRESEARCH_DEBUG
          const auto read = this->pos_in_->read_bytes(&(pay_data_[0]) + pos, size);
          assert(read == size);
          UNUSED(read);
        #else
          this->pos_in_->read_bytes(&(pay_data_[0]) + pos, size);
        #endif // IRESEARCH_DEBUG

        pos += size;
      }

      if (shift_unpack_32(this->pos_in_->read_vint(), offs_start_deltas_[i])) {
        offs_lengts_[i] = this->pos_in_->read_vint();
      } else {
        assert(i);
        offs_lengts_[i] = offs_lengts_[i - 1];
      }
    }

    pay_data_pos_ = 0;
  }

  void skip_block() {
    base::skip_block();
    base::skip_payload(*pay_in_);
    base::skip_offsets(*pay_in_);
  }

  void skip(size_t count) noexcept {
    // current payload start
    const auto begin = this->pay_lengths_ + this->buf_pos_;
    const auto end = begin + count;
    this->pay_data_pos_ = std::accumulate(begin, end, this->pay_data_pos_);

    base::skip(count);
  }

  uint32_t offs_start_deltas_[IteratorTraits::block_size()]{}; // buffer to store offset starts
  uint32_t offs_lengts_[IteratorTraits::block_size()]{}; // buffer to store offset lengths
  uint32_t pay_lengths_[IteratorTraits::block_size()]{}; // buffer to store payload lengths
  index_input::ptr pay_in_;
  offset offs_;
  payload pay_;
  size_t pay_data_pos_{}; // current position in a payload buffer
  bstring pay_data_; // buffer to store payload data
}; // position_impl

///////////////////////////////////////////////////////////////////////////////
/// @class pos_iterator_base (position + payload)
///////////////////////////////////////////////////////////////////////////////
template<typename IteratorTraits, typename FieldTraits>
struct position_impl<IteratorTraits, FieldTraits, false, true>
    : public position_impl<IteratorTraits, FieldTraits, false, false> {
  typedef position_impl<IteratorTraits, FieldTraits, false, false> base;

  irs::attribute* attribute(irs::type_info::type_id type) noexcept {
    return irs::type<payload>::id() == type ? &pay_ : nullptr;
  }

  void prepare(const doc_state& state) {
    base::prepare(state);

    pay_in_ = state.pay_in->reopen(); // reopen thread-safe stream

    if (!pay_in_) {
      // implementation returned wrong pointer
      IR_FRMT_ERROR("Failed to reopen payload input in: %s", __FUNCTION__);

      throw io_error("failed to reopen payload input");
    }

    pay_in_->seek(state.term_state->pay_start);
  }

  void prepare(const skip_state& state)  {
    base::prepare(state);

    pay_in_->seek(state.pay_ptr);
    pay_data_pos_ = state.pay_pos;
  }

  void read_attributes() noexcept {
    pay_.value = bytes_ref(
      pay_data_.c_str() + pay_data_pos_,
      pay_lengths_[this->buf_pos_]);
    pay_data_pos_ += pay_lengths_[this->buf_pos_];
  }

  void clear_attributes() noexcept {
    pay_.value = bytes_ref::NIL;
  }

  void read_block() {
    base::read_block();

    // read payload
    const uint32_t size = pay_in_->read_vint();
    if (size) {
      IteratorTraits::read_block(*pay_in_, this->enc_buf_, pay_lengths_);
      string_utils::oversize(pay_data_, size);

      #ifdef IRESEARCH_DEBUG
        const auto read = pay_in_->read_bytes(&(pay_data_[0]), size);
        assert(read == size);
        UNUSED(read);
      #else
        pay_in_->read_bytes(&(pay_data_[0]), size);
      #endif // IRESEARCH_DEBUG
    }

    if constexpr (FieldTraits::offset()) {
      base::skip_offsets(*pay_in_);
    }

    pay_data_pos_ = 0;
  }

  void read_tail_block() {
    size_t pos = 0;

    for (size_t i = 0; i < this->tail_length_; ++i) {
      // read payloads
      if (shift_unpack_32(this->pos_in_->read_vint(), this->pos_deltas_[i])) {
        pay_lengths_[i] = this->pos_in_->read_vint();
      } else {
        assert(i);
        pay_lengths_[i] = pay_lengths_[i-1];
      }

      if (pay_lengths_[i]) {
        const auto size = pay_lengths_[i]; // current payload length

        string_utils::oversize(pay_data_, pos + size);

        #ifdef IRESEARCH_DEBUG
          const auto read = this->pos_in_->read_bytes(&(pay_data_[0]) + pos, size);
          assert(read == size);
          UNUSED(read);
        #else
          this->pos_in_->read_bytes(&(pay_data_[0]) + pos, size);
        #endif // IRESEARCH_DEBUG

        pos += size;
      }

      // skip offsets
      if constexpr (FieldTraits::offset()) {
        uint32_t code;
        if (shift_unpack_32(this->pos_in_->read_vint(), code)) {
          this->pos_in_->read_vint();
        }
      }
    }

    pay_data_pos_ = 0;
  }

  void skip_block() {
    base::skip_block();
    base::skip_payload(*pay_in_);
    if constexpr (FieldTraits::offset()) {
      base::skip_offsets(*pay_in_);
    }
  }

  void skip(size_t count) noexcept {
    // current payload start
    const auto begin = this->pay_lengths_ + this->buf_pos_;
    const auto end = begin + count;
    this->pay_data_pos_ = std::accumulate(begin, end, this->pay_data_pos_);

    base::skip(count);
  }

  uint32_t pay_lengths_[IteratorTraits::block_size()]{}; // buffer to store payload lengths
  index_input::ptr pay_in_;
  payload pay_;
  size_t pay_data_pos_{}; // current position in a payload buffer
  bstring pay_data_; // buffer to store payload data
}; // position_impl

///////////////////////////////////////////////////////////////////////////////
/// @class pos_iterator_base (position + offset)
///////////////////////////////////////////////////////////////////////////////
template<typename IteratorTraits, typename FieldTraits>
struct position_impl<IteratorTraits, FieldTraits, true, false>
    : public position_impl<IteratorTraits, FieldTraits, false, false> {
  typedef position_impl<IteratorTraits, FieldTraits, false, false> base;

  irs::attribute* attribute(irs::type_info::type_id type) noexcept {
    return irs::type<offset>::id() == type ? &offs_ : nullptr;
  }

  void prepare(const doc_state& state) {
    base::prepare(state);

    pay_in_ = state.pay_in->reopen(); // reopen thread-safe stream

    if (!pay_in_) {
      // implementation returned wrong pointer
      IR_FRMT_ERROR("Failed to reopen payload input in: %s", __FUNCTION__);

      throw io_error("failed to reopen payload input");
    }

    pay_in_->seek(state.term_state->pay_start);
  }

  void prepare(const skip_state& state) {
    base::prepare(state);

    pay_in_->seek(state.pay_ptr);
  }

  void read_attributes() noexcept {
    offs_.start += offs_start_deltas_[this->buf_pos_];
    offs_.end = offs_.start + offs_lengts_[this->buf_pos_];
  }

  void clear_attributes() noexcept {
    offs_.clear();
  }

  void read_block() {
    base::read_block();

    if constexpr (FieldTraits::payload()) {
      base::skip_payload(*pay_in_);
    }

    // read offsets
    IteratorTraits::read_block(*pay_in_, this->enc_buf_, offs_start_deltas_);
    IteratorTraits::read_block(*pay_in_, this->enc_buf_, offs_lengts_);
  }

  void read_tail_block() {
    uint32_t pay_size = 0;
    for (size_t i = 0; i < this->tail_length_; ++i) {
      // skip payloads
      if constexpr (FieldTraits::payload()) {
        if (shift_unpack_32(this->pos_in_->read_vint(), this->pos_deltas_[i])) {
          pay_size = this->pos_in_->read_vint();
        }
        if (pay_size) {
          this->pos_in_->seek(this->pos_in_->file_pointer() + pay_size);
        }
      } else {
        this->pos_deltas_[i] = this->pos_in_->read_vint();
      }

      // read offsets
      if (shift_unpack_32(this->pos_in_->read_vint(), offs_start_deltas_[i])) {
        offs_lengts_[i] = this->pos_in_->read_vint();
      } else {
        assert(i);
        offs_lengts_[i] = offs_lengts_[i - 1];
      }
    }
  }

  void skip_block() {
    base::skip_block();
    if constexpr (FieldTraits::payload()) {
      base::skip_payload(*pay_in_);
    }
    base::skip_offsets(*pay_in_);
  }

  uint32_t offs_start_deltas_[IteratorTraits::block_size()]{}; // buffer to store offset starts
  uint32_t offs_lengts_[IteratorTraits::block_size()]{}; // buffer to store offset lengths
  index_input::ptr pay_in_;
  offset offs_;
}; // position_impl

///////////////////////////////////////////////////////////////////////////////
/// @class pos_iterator_base (position)
///////////////////////////////////////////////////////////////////////////////
template<typename IteratorTraits, typename FieldTraits>
struct position_impl<IteratorTraits, FieldTraits, false, false> {
  static void skip_payload(index_input& in) {
    const size_t size = in.read_vint();
    if (size) {
      IteratorTraits::skip_block(in);
      in.seek(in.file_pointer() + size);
    }
  }

  static void skip_offsets(index_input& in) {
    IteratorTraits::skip_block(in);
    IteratorTraits::skip_block(in);
  }

  irs::attribute* attribute(irs::type_info::type_id) noexcept {
    // implementation has no additional attributes
    return nullptr;
  }

  void prepare(const doc_state& state) {
    pos_in_ = state.pos_in->reopen(); // reopen thread-safe stream

    if (!pos_in_) {
      // implementation returned wrong pointer
      IR_FRMT_ERROR("Failed to reopen positions input in: %s", __FUNCTION__);

      throw io_error("failed to reopen positions input");
    }

    cookie_.file_pointer_ = state.term_state->pos_start;
    pos_in_->seek(state.term_state->pos_start);
    freq_ = state.freq;
    enc_buf_ = state.enc_buf;
    tail_start_ = state.tail_start;
    tail_length_ = state.tail_length;
  }

  void prepare(const skip_state& state) {
    pos_in_->seek(state.pos_ptr);
    pend_pos_ = state.pend_pos;
    buf_pos_ = IteratorTraits::block_size();
    cookie_.file_pointer_ = state.pos_ptr;
    cookie_.pend_pos_ = pend_pos_;
  }

  void reset() {
    if (std::numeric_limits<size_t>::max() != cookie_.file_pointer_) {
      buf_pos_ = IteratorTraits::block_size();
      pend_pos_ = cookie_.pend_pos_;
      pos_in_->seek(cookie_.file_pointer_);
    }
  }

  void read_attributes() { }

  void clear_attributes() { }

  void read_tail_block() {
    uint32_t pay_size = 0;
    for (size_t i = 0; i < tail_length_; ++i) {
      if constexpr (FieldTraits::payload()) {
        if (shift_unpack_32(pos_in_->read_vint(), pos_deltas_[i])) {
          pay_size = pos_in_->read_vint();
        }
        if (pay_size) {
          pos_in_->seek(pos_in_->file_pointer() + pay_size);
        }
      } else {
        pos_deltas_[i] = pos_in_->read_vint();
      }

      if constexpr (FieldTraits::offset()) {
        uint32_t delta;
        if (shift_unpack_32(pos_in_->read_vint(), delta)) {
          pos_in_->read_vint();
        }
      }
    }
  }

  void read_block() {
    IteratorTraits::read_block(*pos_in_, enc_buf_, pos_deltas_);
  }

  void skip_block() {
    IteratorTraits::skip_block(*pos_in_);
  }

  // skip within a block
  void skip(size_t count) noexcept {
    buf_pos_ += count;
  }

  struct cookie {
    size_t pend_pos_{};
    size_t file_pointer_ = std::numeric_limits<size_t>::max();
  };

  uint32_t pos_deltas_[IteratorTraits::block_size()]; // buffer to store position deltas
  const uint32_t* freq_; // lenght of the posting list for a document
  uint32_t* enc_buf_; // auxillary buffer to decode data
  size_t pend_pos_{}; // how many positions "behind" we are
  uint64_t tail_start_; // file pointer where the last (vInt encoded) pos delta block is
  size_t tail_length_; // number of positions in the last (vInt encoded) pos delta block
  uint32_t buf_pos_{ IteratorTraits::block_size() }; // current position in pos_deltas_ buffer
  cookie cookie_;
  index_input::ptr pos_in_;
}; // position_impl

template<typename IteratorTraits, typename FieldTraits, bool Position = IteratorTraits::position()>
class position final : public irs::position,
                       protected position_impl<IteratorTraits, FieldTraits> {
 public:
  using impl = position_impl<IteratorTraits, FieldTraits> ;

  virtual irs::attribute* get_mutable(irs::type_info::type_id type) override {
    return impl::attribute(type);
  }

  virtual value_t seek(value_t target) override {
    const uint32_t freq = *this->freq_;
    if (this->pend_pos_ > freq) {
      skip(this->pend_pos_ - freq);
      this->pend_pos_ = freq;
    }
    while (value_ < target && this->pend_pos_) {
      if (this->buf_pos_ == IteratorTraits::block_size()) {
        refill();
        this->buf_pos_ = 0;
      }
      if constexpr (IteratorTraits::one_based_position_storage()) {
        value_ += (uint32_t)(!pos_limits::valid(value_));
      }
      value_ += this->pos_deltas_[this->buf_pos_];
      assert(irs::pos_limits::valid(value_));
      this->read_attributes();

      ++this->buf_pos_;
      --this->pend_pos_;
    }
    if (0 == this->pend_pos_ && value_ < target) {
      value_ = pos_limits::eof();
    }
    return value_;
  }

  virtual bool next() override {
    if (0 == this->pend_pos_) {
      value_ = pos_limits::eof();

      return false;
    }

    const uint32_t freq = *this->freq_;

    if (this->pend_pos_ > freq) {
      skip(this->pend_pos_ - freq);
      this->pend_pos_ = freq;
    }

    if (this->buf_pos_ == IteratorTraits::block_size()) {
      refill();
      this->buf_pos_ = 0;
    }
    if constexpr (IteratorTraits::one_based_position_storage()) {
      value_ += (uint32_t)(!pos_limits::valid(value_));
    }
    value_ += this->pos_deltas_[this->buf_pos_];
    assert(irs::pos_limits::valid(value_));
    this->read_attributes();

    ++this->buf_pos_;
    --this->pend_pos_;
    return true;
  }

  virtual void reset() override {
    value_ = pos_limits::invalid();
    impl::reset();
  }

  // prepares iterator to work
  // or notifies iterator that doc iterator has skipped to a new block
  using impl::prepare;

  // notify iterator that corresponding doc_iterator has moved forward
  void notify(uint32_t n) {
    this->pend_pos_ += n;
    this->cookie_.pend_pos_ += n;
  }

  void clear() noexcept {
    value_ = pos_limits::invalid();
    impl::clear_attributes();
  }

 private:
  void refill() {
    if (this->pos_in_->file_pointer() == this->tail_start_) {
      this->read_tail_block();
    } else {
      this->read_block();
    }
  }

  void skip(uint32_t count) {
    auto left = IteratorTraits::block_size() - this->buf_pos_;
    if (count >= left) {
      count -= left;
      while (count >= IteratorTraits::block_size()) {
        this->skip_block();
        count -= IteratorTraits::block_size();
      }
      refill();
      this->buf_pos_ = 0;
      left = IteratorTraits::block_size();
    }

    if (count < left) {
      impl::skip(count);
    }
    clear();
  }
}; // position

///////////////////////////////////////////////////////////////////////////////
/// @class pos_iterator (empty)
///////////////////////////////////////////////////////////////////////////////
template<typename IteratorTraits, typename FieldTraits>
struct position<IteratorTraits, FieldTraits, false> : attribute {
  static constexpr string_ref type_name() noexcept {
    return irs::position::type_name();
  }

  void prepare(doc_state&) { }
  void prepare(skip_state&) { }
  void notify(uint32_t) { }
  void clear() { }
}; // position


// Buffer type containing only document buffer
template<typename IteratorTraits>
struct data_buffer {
  doc_id_t docs[IteratorTraits::block_size()]{};
};

// Buffer type containing both document and fequency buffers
template<typename IteratorTraits>
struct freq_buffer : data_buffer<IteratorTraits> {
  uint32_t freqs[IteratorTraits::block_size()];
};

template<typename IteratorTraits>
using buffer_type = std::conditional_t<
  IteratorTraits::frequency(),
  freq_buffer<IteratorTraits>,
  data_buffer<IteratorTraits>>;

///////////////////////////////////////////////////////////////////////////////
/// @class doc_iterator
/// @tparam IteratorTraits requested features
/// @tparam FieldTraits actual field features
///////////////////////////////////////////////////////////////////////////////
template<typename IteratorTraits, typename FieldTraits>
class doc_iterator final : public irs::doc_iterator {
 private:
  static_assert(IteratorTraits::block_size() <= std::numeric_limits<doc_id_t>::max());

  using attributes = std::conditional_t<
    IteratorTraits::frequency() && IteratorTraits::position(),
      std::tuple<document, frequency, cost, score, position<IteratorTraits, FieldTraits>>,
      std::conditional_t<IteratorTraits::frequency(),
        std::tuple<document, frequency, cost, score>,
        std::tuple<document, cost, score>
      >>;

 public:
  // hide 'ptr' defined in irs::doc_iterator
  using ptr = memory::managed_ptr<doc_iterator>;

  doc_iterator() noexcept
    : skip_levels_(1),
      skip_{IteratorTraits::block_size(), postings_writer_base::SKIP_N, ReadSkip{*this}} {
    assert(
      std::all_of(std::begin(buf_.docs), std::end(buf_.docs),
                  [](doc_id_t doc) { return !doc_limits::valid(doc); }));
    skip_levels_.front().doc = doc_limits::eof(); // prevent using skip-list by default
  }

  void prepare(
      const term_meta& meta,
      const index_input* doc_in,
      [[maybe_unused]] const index_input* pos_in,
      [[maybe_unused]] const index_input* pay_in);

  virtual attribute* get_mutable(irs::type_info::type_id type) noexcept override {
    return irs::get_mutable(attrs_, type);
  }

  virtual doc_id_t seek(doc_id_t target) override;

  virtual doc_id_t value() const noexcept final {
    return std::get<document>(attrs_).value;
  }

#if defined(_MSC_VER)
  #pragma warning(disable : 4706)
#elif defined(__GNUC__)
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wparentheses"
#endif

  virtual bool next() override {
    auto& doc = std::get<document>(attrs_);

    if (begin_ == end_) {
      cur_pos_ += relative_pos();

      if (cur_pos_ == term_state_.docs_count) {
        doc.value = doc_limits::eof();
        begin_ = end_ = buf_.docs; // seal the iterator
        return false;
      }

      refill();
    }

    doc.value += *begin_++; // update document attribute

    if constexpr (IteratorTraits::frequency()) {
      auto& freq = std::get<frequency>(attrs_);
      freq.value = *freq_++; // update frequency attribute

      if constexpr (IteratorTraits::position()) {
        auto& pos = std::get<position<IteratorTraits, FieldTraits>>(attrs_);
        pos.notify(freq.value);
        pos.clear();
      }
    }

    return true;
  }

#if defined(_MSC_VER)
  #pragma warning(default : 4706)
#elif defined(__GNUC__)
  #pragma GCC diagnostic pop
#endif

 private:
  class ReadSkip {
   public:
    explicit ReadSkip(doc_iterator& self) noexcept
      : self_{&self} {
    }

    void MoveDown(size_t level) const;
    doc_id_t Read(size_t level, size_t end, index_input& in) const;

   private:
    doc_iterator* self_;
  };

  void seek_to_block(doc_id_t target);

  // returns current position in the document block 'docs_'
  doc_id_t relative_pos() noexcept {
    assert(begin_ >= buf_.docs);
    return static_cast<doc_id_t>(begin_ - buf_.docs);
  }

  void read_end_block(size_t size);

  void refill() {
    // should never call refill for singleton documents
    assert(1 != term_state_.docs_count);
    const auto left = term_state_.docs_count - cur_pos_;

    if (left >= IteratorTraits::block_size()) {
      // read doc deltas
      IteratorTraits::read_block(*doc_in_, enc_buf_, buf_.docs);

      if constexpr (IteratorTraits::frequency()) {
        IteratorTraits::read_block(*doc_in_, enc_buf_, buf_.freqs);
      } else if constexpr (FieldTraits::frequency()) {
        IteratorTraits::skip_block(*doc_in_);
      }

      static_assert(IRESEARCH_COUNTOF(decltype(buf_.docs){}) == IteratorTraits::block_size());
      end_ = std::end(buf_.docs);
    } else {
      read_end_block(left);
    }

    // if this is the initial doc_id then set it to min() for proper delta value
    if (auto& doc = std::get<irs::document>(attrs_);
        !doc_limits::valid(doc.value)) {
      doc.value = (doc_limits::min)();
    }

    begin_ = buf_.docs;
    if constexpr (IteratorTraits::frequency()) {
      freq_ = buf_.freqs;
    }
  }

  buffer_type<IteratorTraits> buf_;
  uint32_t enc_buf_[IteratorTraits::block_size()]; // buffer for encoding
  std::vector<skip_state> skip_levels_;
  SkipReader<ReadSkip> skip_;
  skip_state* skip_ctx_; // pointer to skip context used by skip reader
  uint32_t cur_pos_{};
  const doc_id_t* begin_{buf_.docs};
  doc_id_t* end_{buf_.docs};
  uint32_t* freq_{}; // pointer into docs_ to the frequency attribute value for the current doc
  index_input::ptr doc_in_;
  version10::term_meta term_state_;
  attributes attrs_;
}; // doc_iterator

template<typename IteratorTraits, typename FieldTraits>
void doc_iterator<IteratorTraits, FieldTraits>::ReadSkip::MoveDown(
    size_t level) const {
  auto& last = *self_->skip_ctx_;
  auto& next = self_->skip_levels_[level];

  // move to the more granular level
  CopyState<FieldTraits>(next, last);
}

template<typename IteratorTraits, typename FieldTraits>
doc_id_t doc_iterator<IteratorTraits, FieldTraits>::ReadSkip::Read(
    size_t level, size_t skipped, index_input& in) const {
  auto& last = *self_->skip_ctx_;
  auto& next = self_->skip_levels_[level];

  // store previous step on the same level
  CopyState<FieldTraits>(last, next);

  if (skipped >= self_->term_state_.docs_count) {
    // stream exhausted
    return (next.doc = doc_limits::eof());
  }

  ReadState<FieldTraits>(next, in);

  if constexpr (FieldTraits::wand() && FieldTraits::frequency()) {
    score_buffer::skip(in);
  }

  return next.doc;
}

template<typename IteratorTraits, typename FieldTraits>
void doc_iterator<IteratorTraits, FieldTraits>::prepare(
    const term_meta& meta,
    const index_input* doc_in,
    [[maybe_unused]] const index_input* pos_in,
    [[maybe_unused]] const index_input* pay_in) {
  assert(!IteratorTraits::frequency() || IteratorTraits::frequency() == FieldTraits::frequency());
  assert(!IteratorTraits::position() || IteratorTraits::position() == FieldTraits::position());
  assert(!IteratorTraits::offset() || IteratorTraits::offset() == FieldTraits::offset());
  assert(!IteratorTraits::payload() || IteratorTraits::payload() == FieldTraits::payload());

  begin_ = end_ = buf_.docs;

  term_state_ = static_cast<const version10::term_meta&>(meta);

  // init document stream
  if (term_state_.docs_count > 1) {
    if (!doc_in_) {
      doc_in_ = doc_in->reopen(); // reopen thread-safe stream

      if (!doc_in_) {
        // implementation returned wrong pointer
        IR_FRMT_ERROR("Failed to reopen document input in: %s", __FUNCTION__);

        throw io_error("failed to reopen document input");
      }
    }

    doc_in_->seek(term_state_.doc_start);
    assert(!doc_in_->eof());
  }

  std::get<cost>(attrs_).reset(term_state_.docs_count); // estimate iterator

  if constexpr (IteratorTraits::frequency()) {
    assert(meta.freq);

    if constexpr (IteratorTraits::position()) {
      doc_state state;
      state.pos_in = pos_in;
      state.pay_in = pay_in;
      state.term_state = &term_state_;
      state.freq = &std::get<frequency>(attrs_).value;
      state.enc_buf = enc_buf_;

      const auto term_freq = meta.freq;

      if (term_freq < IteratorTraits::block_size()) {
        state.tail_start = term_state_.pos_start;
      } else if (term_freq == IteratorTraits::block_size()) {
        state.tail_start = type_limits<type_t::address_t>::invalid();
      } else {
        state.tail_start = term_state_.pos_start + term_state_.pos_end;
      }

      state.tail_length = term_freq % IteratorTraits::block_size();
      std::get<position<IteratorTraits, FieldTraits>>(attrs_).prepare(state);
    }
  }

  if (1 == term_state_.docs_count) {
    *buf_.docs = (doc_limits::min)() + term_state_.e_single_doc;
    if constexpr (IteratorTraits::frequency()) {
      *buf_.freqs = meta.freq;
      freq_ = buf_.freqs;
    }
    ++end_;
  } else if (term_state_.docs_count > IteratorTraits::block_size()) {
    // allow using skip-list for long enough postings
    skip_levels_.front().doc = doc_limits::invalid();
  }
}

template<typename IteratorTraits, typename FieldTraits>
void doc_iterator<IteratorTraits, FieldTraits>::read_end_block(size_t size) {
  end_ = buf_.docs + size;
  auto* doc = std::begin(buf_.docs);

  [[maybe_unused]] uint32_t* doc_freq;
  if constexpr (IteratorTraits::frequency()) {
    doc_freq = std::begin(buf_.freqs);
  }

  while (doc < end_) {
    if constexpr (FieldTraits::frequency()) {
      if constexpr (IteratorTraits::frequency()) {
        if (shift_unpack_32(doc_in_->read_vint(), *doc++)) {
          *doc_freq++ = 1;
        } else {
          *doc_freq++ = doc_in_->read_vint();
        }
      } else {
        if (!shift_unpack_32(doc_in_->read_vint(), *doc++)) {
          doc_in_->read_vint();
        }
      }
    } else {
      *doc++ = doc_in_->read_vint();
    }
  }
}

template<typename IteratorTraits, typename FieldTraits>
doc_id_t doc_iterator<IteratorTraits, FieldTraits>::seek(doc_id_t target) {
  auto& doc = std::get<document>(attrs_);

  if (target <= doc.value) {
    return doc.value;
  }

  seek_to_block(target);

  if (begin_ == end_) {
    cur_pos_ += relative_pos();

    if (cur_pos_ == term_state_.docs_count) {
      doc.value = doc_limits::eof();
      begin_ = end_ = buf_.docs; // seal the iterator
      return doc_limits::eof();
    }

    refill();
  }

  [[maybe_unused]] uint32_t notify{0};
  while (begin_ < end_) {
    doc.value += *begin_++;

    if constexpr (!IteratorTraits::position()) {
      if (doc.value >= target) {
        if constexpr (IteratorTraits::frequency()) {
          freq_ = buf_.freqs + relative_pos();
          assert((freq_ - 1) >= buf_.freqs && (freq_ - 1) < std::end(buf_.freqs));
          std::get<frequency>(attrs_).value = freq_[-1];
        }
        return doc.value;
      }
    } else {
      assert(IteratorTraits::frequency());
      auto& freq = std::get<frequency>(attrs_);
      auto& pos = std::get<position<IteratorTraits, FieldTraits>>(attrs_);
      freq.value = *freq_++;
      notify += freq.value;

      if (doc.value >= target) {
        pos.notify(notify);
        pos.clear();
        return doc.value;
      }
    }
  }

  if constexpr (IteratorTraits::position()) {
    std::get<position<IteratorTraits, FieldTraits>>(attrs_).notify(notify);
  }
  while (doc.value < target) {
    next();
  }

  return doc.value;
}

template<typename IteratorTraits, typename FieldTraits>
void doc_iterator<IteratorTraits, FieldTraits>::seek_to_block(doc_id_t target) {
  assert(!skip_levels_.empty()); // ensured by ctor
  assert(1 != term_state_.docs_count); // ensured by prepare(...)

  // check whether it make sense to use skip-list
  if (skip_levels_.front().doc < target) {
    // ensured by prepare(...)
    assert(term_state_.docs_count > IteratorTraits::block_size());

    skip_state last; // where block starts
    skip_ctx_ = &last;

    // init skip writer in lazy fashion
    if (skip_.NumLevels() != 0) {
seek_after_initialization:
      const doc_id_t skipped{skip_.Seek(target)};
      if (skipped > (cur_pos_ + relative_pos())) {
        doc_in_->seek(last.doc_ptr);
        std::get<document>(attrs_).value = last.doc;
        cur_pos_ = skipped;
        begin_ = end_ = buf_.docs; // will trigger refill in "next"
        if constexpr (IteratorTraits::position()) {
          std::get<position<IteratorTraits, FieldTraits>>(attrs_).prepare(last); // notify positions
        }
      }

      return;
    }

    auto skip_in = doc_in_->dup();

    if (!skip_in) {
      IR_FRMT_ERROR("Failed to duplicate input in: %s", __FUNCTION__);

      throw io_error("Failed to duplicate document input");
    }

    skip_in->seek(term_state_.doc_start + term_state_.e_skip_start);
    skip_.Prepare(std::move(skip_in));

    // initialize skip levels
    if (const auto num_levels = skip_.NumLevels(); IRS_LIKELY(num_levels)) {
      assert(!doc_limits::valid(skip_levels_.front().doc));
      skip_levels_.resize(num_levels);

      // since we store pointer deltas, add postings offset
      auto& top = skip_levels_.back();
      CopyState<FieldTraits>(top, term_state_);

      goto seek_after_initialization;
    } else {
      if (IRS_LIKELY(term_state_.docs_count <= IteratorTraits::block_size())) {
        // prevent using skip-list
        skip_levels_.front().doc = doc_limits::eof();
      } else {
        assert(false);
        throw index_error("Zero number of skip levels.");
      }
    }
  }
}

///////////////////////////////////////////////////////////////////////////////
/// @class wanderator
/// @tparam IteratorTraits requested features
/// @tparam FieldTraits actual field features
///////////////////////////////////////////////////////////////////////////////
template<typename IteratorTraits, typename FieldTraits>
class wanderator final : public irs::doc_iterator {
 private:
  static_assert(FieldTraits::wand());
  static_assert(IteratorTraits::block_size() <= std::numeric_limits<doc_id_t>::max());

  using attributes = std::conditional_t<
    IteratorTraits::frequency() && IteratorTraits::position(),
      std::tuple<document, frequency, cost, score_threshold, score, position<IteratorTraits, FieldTraits>>,
      std::conditional_t<IteratorTraits::frequency(),
        std::tuple<document, frequency, cost, score_threshold, score>,
        std::tuple<document, cost, score_threshold, score>
      >>;

 public:
  // hide 'ptr' defined in irs::doc_iterator
  using ptr = memory::managed_ptr<doc_iterator>;

  wanderator() noexcept
    : skip_levels_(1),
      skip_scores_(1),
      skip_{IteratorTraits::block_size(), postings_writer_base::SKIP_N, ReadSkip{*this}} {
    assert(
      std::all_of(std::begin(buf_.docs), std::end(buf_.docs),
                  [](doc_id_t doc) { return doc == doc_limits::invalid(); }));
    skip_levels_.front().doc = doc_limits::eof(); // prevent using skip-list by default
  }

  void prepare(const term_meta& meta,
               const index_input* doc_in,
               [[maybe_unused]] const index_input* pos_in,
               [[maybe_unused]] const index_input* pay_in);

  virtual attribute* get_mutable(irs::type_info::type_id type) noexcept override {
    return irs::get_mutable(attrs_, type);
  }

  virtual doc_id_t seek(doc_id_t target) override {
    return seek_to_target(target);
  }

  virtual doc_id_t value() const noexcept final {
    return std::get<document>(attrs_).value;
  }

  virtual bool next() override {
    return !doc_limits::eof(seek(value() + 1));
  }

 private:
  class ReadSkip {
   public:
    explicit ReadSkip(wanderator& self) noexcept
      : self_{&self} {
    }

    void MoveDown(size_t level) const;
    doc_id_t Read(size_t level, doc_id_t skipped, index_input& in) const;

   private:
    wanderator* self_;
  };

  void seek_to_block(doc_id_t target) {
    // FIXME(gnusi) don't use wanderator for short posting lists?

    // check whether it make sense to use skip-list
    if (skip_levels_.front().doc < target) {
      // ensured by prepare(...)
      assert(term_state_.docs_count > IteratorTraits::block_size());
      assert(skip_.NumLevels());
      assert(0 == prev_skip_.doc_ptr);

      const doc_id_t skipped{skip_.Seek(target)};
      if (skipped > (cur_pos_ + relative_pos())) {
        std::get<document>(attrs_).value = prev_skip_.doc;
        cur_pos_ = skipped;
        begin_ = end_ = buf_.docs; // will trigger refill in "next"
      }
    }
  }

  doc_id_t seek_to_target(doc_id_t target);

  // returns current position in the document block 'docs_'
  doc_id_t relative_pos() noexcept {
    assert(begin_ >= buf_.docs);
    return static_cast<doc_id_t>(begin_ - buf_.docs);
  }

  void read_end_block(size_t size);

  void refill();

  buffer_type<IteratorTraits> buf_;
  uint32_t enc_buf_[IteratorTraits::block_size()]; // buffer for encoding
  std::vector<skip_state> skip_levels_;
  std::vector<score_buffer> skip_scores_;
  SkipReader<ReadSkip> skip_;
  skip_state prev_skip_; // skip context used by skip reader
  uint32_t cur_pos_{};
  const doc_id_t* begin_{buf_.docs};
  doc_id_t* end_{buf_.docs};
  uint32_t* freq_{}; // pointer into docs_ to the frequency attribute value for the current doc
  index_input::ptr doc_in_;
  version10::term_meta term_state_;
  attributes attrs_;
}; // wanderator

template<typename IteratorTraits, typename FieldTraits>
void wanderator<IteratorTraits, FieldTraits>::ReadSkip::MoveDown(
    size_t level) const {
  auto& last = self_->prev_skip_;
  auto& next = self_->skip_levels_[level];

  // move to the more granular level
  CopyState<FieldTraits>(next, last);
}

template<typename IteratorTraits, typename FieldTraits>
doc_id_t wanderator<IteratorTraits, FieldTraits>::ReadSkip::Read(
    size_t level, doc_id_t skipped, index_input& in) const {
  auto& last = self_->prev_skip_;
  auto& next = self_->skip_levels_[level];

  // store previous step on the same level
  CopyState<FieldTraits>(last, next);

  if (skipped >= self_->term_state_.docs_count) {
    // stream exhausted
    return (next.doc = doc_limits::eof());
  }

  ReadState<FieldTraits>(next, in);

  if constexpr (FieldTraits::frequency()) {
    auto& skip_buffer = self_->skip_scores_[level];
    skip_buffer.read(in);

    auto& threshold = std::get<score_threshold>(self_->attrs_);

    if (skip_buffer.value() < threshold.get()) {
      return doc_limits::invalid();
    }
  }

  return next.doc;
}

template<typename IteratorTraits, typename FieldTraits>
void wanderator<IteratorTraits, FieldTraits>::prepare(
    const term_meta& meta,
    const index_input* doc_in,
    [[maybe_unused]] const index_input* pos_in,
    [[maybe_unused]] const index_input* pay_in) {
  assert(!IteratorTraits::frequency() || IteratorTraits::frequency() == FieldTraits::frequency());
  assert(!IteratorTraits::position() || IteratorTraits::position() == FieldTraits::position());
  assert(!IteratorTraits::offset() || IteratorTraits::offset() == FieldTraits::offset());
  assert(!IteratorTraits::payload() || IteratorTraits::payload() == FieldTraits::payload());
  assert(!skip_levels_.empty()); // ensured by ctor
  assert(doc_limits::eof(skip_levels_.front().doc)); // ensured by ctor

  // don't use wanderator for short posting lists
  assert(meta.docs_count > IteratorTraits::block_size());

  begin_ = end_ = buf_.docs;

  term_state_ = static_cast<const version10::term_meta&>(meta);

  // init document stream
  if (term_state_.docs_count > 1) {
    if (!doc_in_) {
      doc_in_ = doc_in->reopen(); // reopen thread-safe stream

      if (!doc_in_) {
        // implementation returned wrong pointer
        IR_FRMT_ERROR("Failed to reopen document input in: %s", __FUNCTION__);

        throw io_error("failed to reopen document input");
      }
    }

    doc_in_->seek(term_state_.doc_start);
    assert(!doc_in_->eof());
  }

  std::get<cost>(attrs_).reset(term_state_.docs_count); // estimate iterator

  if constexpr (IteratorTraits::frequency()) {
    assert(meta.freq);

    if constexpr (IteratorTraits::position()) {
      doc_state state;
      state.pos_in = pos_in;
      state.pay_in = pay_in;
      state.term_state = &term_state_;
      state.freq = &std::get<frequency>(attrs_).value;
      state.enc_buf = enc_buf_;

      const auto term_freq = meta.freq;

      if (term_freq < IteratorTraits::block_size()) {
        state.tail_start = term_state_.pos_start;
      } else if (term_freq == IteratorTraits::block_size()) {
        state.tail_start = type_limits<type_t::address_t>::invalid();
      } else {
        state.tail_start = term_state_.pos_start + term_state_.pos_end;
      }

      state.tail_length = term_freq % IteratorTraits::block_size();
      std::get<position<IteratorTraits, FieldTraits>>(attrs_).prepare(state);
    }
  }

  if (term_state_.docs_count > IteratorTraits::block_size()) {
    auto skip_in = doc_in_->dup();

    if (!skip_in) {
      IR_FRMT_ERROR("Failed to duplicate input in: %s", __FUNCTION__);

      throw io_error("Failed to duplicate document input");
    }

    skip_in->seek(term_state_.doc_start + term_state_.e_skip_start);

    skip_.Prepare(std::move(skip_in));

    // initialize skip levels
    if (const auto num_levels = skip_.NumLevels(); IRS_LIKELY(num_levels)) {
      skip_levels_.resize(num_levels);
      skip_scores_.resize(num_levels);

      // allow using skip-list for long enough postings
      skip_levels_.front().doc = doc_limits::invalid();

      // since we store pointer deltas, add postings offset
      auto& top = skip_levels_.back();
      CopyState<FieldTraits>(top, term_state_);
    }
  } else if (1 == term_state_.docs_count) {
    *buf_.docs = (doc_limits::min)() + term_state_.e_single_doc;
    if constexpr (IteratorTraits::frequency()) {
      *buf_.freqs = meta.freq;
      freq_ = buf_.freqs;
    }
    ++end_;
  } else if (term_state_.docs_count > IteratorTraits::block_size()) {
    assert(false);
    throw index_error("Zero number of skip levels.");
  }
}

template<typename IteratorTraits, typename FieldTraits>
doc_id_t wanderator<IteratorTraits, FieldTraits>::seek_to_target(doc_id_t target) {
  auto& doc = std::get<document>(attrs_);

  if (target <= doc.value) {
    return doc.value;
  }

  seek_to_block(target);

  if (begin_ == end_) {
    cur_pos_ += relative_pos();

    if (cur_pos_ == term_state_.docs_count) {
      doc.value = doc_limits::eof();
      begin_ = end_ = buf_.docs; // seal the iterator
      return doc_limits::eof();
    }

    if (prev_skip_.doc_ptr) {
      doc_in_->seek(prev_skip_.doc_ptr);
      if constexpr (IteratorTraits::position()) {
        std::get<position<IteratorTraits, FieldTraits>>(attrs_).prepare(prev_skip_); // notify positions
      }
      prev_skip_.doc_ptr = 0;
    }

    refill();
  }

  [[maybe_unused]] uint32_t notify{0};
  while (begin_ < end_) {
    doc.value += *begin_++;

    if constexpr (!IteratorTraits::position()) {
      if (doc.value >= target) {
        if constexpr (IteratorTraits::frequency()) {
          freq_ = buf_.freqs + relative_pos();
          assert((freq_ - 1) >= buf_.freqs && (freq_ - 1) < std::end(buf_.freqs));
          std::get<frequency>(attrs_).value = freq_[-1];
        }
        return doc.value;
      }
    } else {
      assert(IteratorTraits::frequency());
      auto& freq = std::get<frequency>(attrs_);
      auto& pos = std::get<position<IteratorTraits, FieldTraits>>(attrs_);
      freq.value = *freq_++;
      notify += freq.value;

      if (doc.value >= target) {
        pos.notify(notify);
        pos.clear();
        return doc.value;
      }
    }
  }

  if constexpr (IteratorTraits::position()) {
    std::get<position<IteratorTraits, FieldTraits>>(attrs_).notify(notify);
  }
  while (doc.value < target) {
    next();
  }

  return doc.value;
}


template<typename IteratorTraits, typename FieldTraits>
void wanderator<IteratorTraits, FieldTraits>::read_end_block(size_t size) {
  end_ = buf_.docs + size;
  auto* doc = std::begin(buf_.docs);

  [[maybe_unused]] uint32_t* doc_freq;
  if constexpr (IteratorTraits::frequency()) {
    doc_freq = std::begin(buf_.freqs);
  }

  while (doc < end_) {
    if constexpr (FieldTraits::frequency()) {
      if constexpr (IteratorTraits::frequency()) {
        if (shift_unpack_32(doc_in_->read_vint(), *doc++)) {
          *doc_freq++ = 1;
        } else {
          *doc_freq++ = doc_in_->read_vint();
        }
      } else {
        if (!shift_unpack_32(doc_in_->read_vint(), *doc++)) {
          doc_in_->read_vint();
        }
      }
    } else {
      *doc++ = doc_in_->read_vint();
    }
  }
}

template<typename IteratorTraits, typename FieldTraits>
void wanderator<IteratorTraits, FieldTraits>::refill() {
  // should never call refill for singleton documents
  assert(1 != term_state_.docs_count);
  const auto left = term_state_.docs_count - cur_pos_;

  if (left >= IteratorTraits::block_size()) {
    // read doc deltas
    IteratorTraits::read_block(*doc_in_, enc_buf_, buf_.docs);

    if constexpr (IteratorTraits::frequency()) {
      IteratorTraits::read_block(*doc_in_, enc_buf_, buf_.freqs);
    } else if constexpr (FieldTraits::frequency()) {
      IteratorTraits::skip_block(*doc_in_);
    }

    static_assert(IRESEARCH_COUNTOF(decltype(buf_.docs){}) == IteratorTraits::block_size());
    end_ = std::end(buf_.docs);
  } else {
    read_end_block(left);
  }

  // if this is the initial doc_id then set it to min() for proper delta value
  if (auto& doc = std::get<irs::document>(attrs_);
      !doc_limits::valid(doc.value)) {
    doc.value = (doc_limits::min)();
  }

  begin_ = buf_.docs;
  if constexpr (IteratorTraits::frequency()) {
    freq_ = buf_.freqs;
  }
}

// ----------------------------------------------------------------------------
// --SECTION--                                                index_meta_writer
// ----------------------------------------------------------------------------

struct index_meta_writer final: public irs::index_meta_writer {
  static constexpr string_ref FORMAT_NAME = "iresearch_10_index_meta";
  static constexpr string_ref FORMAT_PREFIX = "segments_";
  static constexpr string_ref FORMAT_PREFIX_TMP = "pending_segments_";

  static constexpr int32_t FORMAT_MIN = 0;
  static constexpr int32_t FORMAT_MAX = 1;

  enum { HAS_PAYLOAD = 1 };

  explicit index_meta_writer(int32_t version) noexcept
    : version_(version) {
    assert(version_ >= FORMAT_MIN && version <= FORMAT_MAX);
  }

  virtual std::string filename(const index_meta& meta) const override;
  using irs::index_meta_writer::prepare;
  virtual bool prepare(directory& dir, index_meta& meta) override;
  virtual bool commit() override;
  virtual void rollback() noexcept override;

 private:
  directory* dir_ = nullptr;
  index_meta* meta_ = nullptr;
  int32_t version_;
}; // index_meta_writer

template<>
std::string file_name<irs::index_meta_writer, index_meta>(const index_meta& meta) {
  return file_name(index_meta_writer::FORMAT_PREFIX_TMP, meta.generation());
}

struct index_meta_reader final: public irs::index_meta_reader {
  virtual bool last_segments_file(
    const directory& dir, std::string& name) const override;

  virtual void read(
    const directory& dir,
    index_meta& meta,
    string_ref filename = string_ref::NIL) override; // null == use meta
}; // index_meta_reader

template<>
std::string file_name<irs::index_meta_reader, index_meta>(const index_meta& meta) {
  return file_name(index_meta_writer::FORMAT_PREFIX, meta.generation());
}

std::string index_meta_writer::filename(const index_meta& meta) const {
  return file_name<irs::index_meta_reader>(meta);
}

bool index_meta_writer::prepare(directory& dir, index_meta& meta) {
  if (meta_) {
    // prepare() was already called with no corresponding call to commit()
    return false;
  }

  prepare(meta); // prepare meta before generating filename

  const auto seg_file = file_name<irs::index_meta_writer>(meta);

  auto out = dir.create(seg_file);

  if (!out) {
    throw io_error(string_utils::to_string(
      "Failed to create file, path: %s",
      seg_file.c_str()));
  }

  {
    format_utils::write_header(*out, FORMAT_NAME, version_);
    out->write_vlong(meta.generation());
    out->write_long(meta.counter());
    assert(meta.size() <= std::numeric_limits<uint32_t>::max());
    out->write_vint(uint32_t(meta.size()));

    for (auto& segment : meta) {
      write_string(*out, segment.filename);
      write_string(*out, segment.meta.codec->type().name());
    }

    if (version_ > FORMAT_MIN) {
      const byte_type flags = meta.payload().null() ? 0 : HAS_PAYLOAD;
      out->write_byte(flags);

      if (flags == HAS_PAYLOAD) {
        irs::write_string(*out, meta.payload());
      }
    }

    format_utils::write_footer(*out);
    // important to close output here
  }

  if (!dir.sync(seg_file)) {
    throw io_error(string_utils::to_string(
      "failed to sync file, path: %s",
      seg_file.c_str()));
  }

  // only noexcept operations below
  dir_ = &dir;
  meta_ = &meta;

  return true;
}

bool index_meta_writer::commit() {
  if (!meta_) {
    return false;
  }

  const auto src = file_name<irs::index_meta_writer>(*meta_);
  const auto dst = file_name<irs::index_meta_reader>(*meta_);

  if (!dir_->rename(src, dst)) {
    rollback();

    throw io_error(string_utils::to_string(
      "failed to rename file, src path: '%s' dst path: '%s'",
      src.c_str(),
      dst.c_str()));
  }

  // only noexcept operations below
  complete(*meta_);

  // clear pending state
  meta_ = nullptr;
  dir_ = nullptr;

  return true;
}

void index_meta_writer::rollback() noexcept {
  if (!meta_) {
    return;
  }

  std::string seg_file;

  try {
    seg_file = file_name<irs::index_meta_writer>(*meta_);
  } catch (const std::exception& e) {
    IR_FRMT_ERROR("Caught error while generating file name for index meta, reason: %s", e.what());
    return;
  } catch (...) {
    IR_FRMT_ERROR("Caught error while generating file name for index meta");
    return;
  }

  if (!dir_->remove(seg_file)) { // suppress all errors
    IR_FRMT_ERROR("Failed to remove file, path: %s", seg_file.c_str());
  }

  // clear pending state
  dir_ = nullptr;
  meta_ = nullptr;
}

// ----------------------------------------------------------------------------
// --SECTION--                                                index_meta_reader
// ----------------------------------------------------------------------------

uint64_t parse_generation(std::string_view segments_file) {
  assert(segments_file.starts_with(index_meta_writer::FORMAT_PREFIX));

  const char* gen_str = segments_file.data() + index_meta_writer::FORMAT_PREFIX.size();
  char* suffix;
  auto gen = std::strtoull(gen_str, &suffix, 10); // 10 for base-10

  return suffix[0] ? type_limits<type_t::index_gen_t>::invalid() : gen;
}

bool index_meta_reader::last_segments_file(const directory& dir, std::string& out) const {
  uint64_t max_gen = 0;
  directory::visitor_f visitor = [&out, &max_gen] (std::string_view name) {
    if (name.starts_with(index_meta_writer::FORMAT_PREFIX)) {
      const uint64_t gen = parse_generation(name);

      if (type_limits<type_t::index_gen_t>::valid(gen) && gen > max_gen) {
        out = std::move(name);
        max_gen = gen;
      }
    }
    return true; // continue iteration
  };

  dir.visit(visitor);
  return max_gen > 0;
}

void index_meta_reader::read(
    const directory& dir,
    index_meta& meta,
    string_ref filename /*= string_ref::NIL*/) {

  const std::string meta_file = filename.null()
    ? file_name<irs::index_meta_reader>(meta)
    : static_cast<std::string>(filename);

  auto in = dir.open(
    meta_file, irs::IOAdvice::SEQUENTIAL | irs::IOAdvice::READONCE);

  if (!in) {
    throw io_error(string_utils::to_string(
      "failed to open file, path: %s",
      meta_file.c_str()));
  }

  const auto checksum = format_utils::checksum(*in);

  // check header
  const int32_t version = format_utils::check_header(
    *in,
    index_meta_writer::FORMAT_NAME,
    index_meta_writer::FORMAT_MIN,
    index_meta_writer::FORMAT_MAX);

  // read data from segments file
  auto gen = in->read_vlong();
  auto cnt = in->read_long();
  auto seg_count = in->read_vint();
  index_meta::index_segments_t segments(seg_count);

  for (size_t i = 0, count = segments.size(); i < count; ++i) {
    auto& segment = segments[i];

    segment.filename = read_string<std::string>(*in);
    segment.meta.codec = formats::get(read_string<std::string>(*in));

    auto reader = segment.meta.codec->get_segment_meta_reader();

    reader->read(dir, segment.meta, segment.filename);
  }

  bool has_payload = false;
  bstring payload;
  if (version > index_meta_writer::FORMAT_MIN) {
    has_payload = (in->read_byte() & index_meta_writer::HAS_PAYLOAD);

    if (has_payload) {
      payload = irs::read_string<bstring>(*in);
    }
  }

  format_utils::check_footer(*in, checksum);

  complete(
    meta, gen, cnt,
    std::move(segments),
    has_payload ? &payload : nullptr);
}

// ----------------------------------------------------------------------------
// --SECTION--                                              segment_meta_writer
// ----------------------------------------------------------------------------

struct segment_meta_writer final : public irs::segment_meta_writer{
  static constexpr string_ref FORMAT_EXT = "sm";
  static constexpr string_ref FORMAT_NAME = "iresearch_10_segment_meta";

  static constexpr int32_t FORMAT_MIN = 0;
  static constexpr int32_t FORMAT_MAX = 1;

  enum {
    HAS_COLUMN_STORE = 1,
    SORTED = 2
  };

  explicit segment_meta_writer(int32_t version) noexcept
    : version_(version) {
    assert(version_ >= FORMAT_MIN && version <= FORMAT_MAX);
  }

  virtual void write(
    directory& dir,
    std::string& filename,
    const segment_meta& meta) override;

 private:
  int32_t version_;
}; // segment_meta_writer

template<>
std::string file_name<irs::segment_meta_writer, segment_meta>(const segment_meta& meta) {
  return irs::file_name(meta.name, meta.version, segment_meta_writer::FORMAT_EXT);
}

void segment_meta_writer::write(directory& dir, std::string& meta_file, const segment_meta& meta) {
  if (meta.docs_count < meta.live_docs_count) {
    throw index_error(string_utils::to_string(
      "invalid segment meta '%s' detected : docs_count=" IR_SIZE_T_SPECIFIER ", live_docs_count=" IR_SIZE_T_SPECIFIER "",
      meta.name.c_str(), meta.docs_count, meta.live_docs_count));
  }

  meta_file = file_name<irs::segment_meta_writer>(meta);
  auto out = dir.create(meta_file);

  if (!out) {
    throw io_error(string_utils::to_string(
      "failed to create file, path: %s",
      meta_file.c_str()));
  }

  byte_type flags = meta.column_store ? HAS_COLUMN_STORE : 0;

  format_utils::write_header(*out, FORMAT_NAME, version_);
  write_string(*out, meta.name);
  out->write_vlong(meta.version);
  out->write_vlong(meta.live_docs_count);
  out->write_vlong(meta.docs_count - meta.live_docs_count); // docs_count >= live_docs_count
  out->write_vlong(meta.size);
  if (version_ > FORMAT_MIN) {
    // sorted indices are not supported in version 1.0
    if (field_limits::valid(meta.sort)) {
      flags |= SORTED;
    }

    out->write_byte(flags);
    out->write_vlong(1+meta.sort); // max->0
  } else {
    out->write_byte(flags);
  }
  write_strings(*out, meta.files);
  format_utils::write_footer(*out);
}

// ----------------------------------------------------------------------------
// --SECTION--                                              segment_meta_reader
// ----------------------------------------------------------------------------

struct segment_meta_reader final : public irs::segment_meta_reader {
  virtual void read(
    const directory& dir,
    segment_meta& meta,
    string_ref filename = string_ref::NIL) override; // null == use meta
}; // segment_meta_reader

void segment_meta_reader::read(
    const directory& dir,
    segment_meta& meta,
    string_ref filename /*= string_ref::NIL*/) {

  const std::string meta_file = filename.null()
    ? file_name<irs::segment_meta_writer>(meta)
    : static_cast<std::string>(filename);

  auto in = dir.open(
    meta_file, irs::IOAdvice::SEQUENTIAL | irs::IOAdvice::READONCE);

  if (!in) {
    throw io_error(string_utils::to_string(
      "failed to open file, path: %s",
      meta_file.c_str()));
  }

  const auto checksum = format_utils::checksum(*in);

  const int32_t version = format_utils::check_header(
    *in,
    segment_meta_writer::FORMAT_NAME,
    segment_meta_writer::FORMAT_MIN,
    segment_meta_writer::FORMAT_MAX);

  auto name = read_string<std::string>(*in);
  const auto segment_version = in->read_vlong();
  const auto live_docs_count = in->read_vlong();
  const auto docs_count = in->read_vlong() + live_docs_count;

  if (docs_count < live_docs_count) {
    throw index_error(std::string("while reader segment meta '") + name
      + "', error: docs_count(" + std::to_string(docs_count)
      + ") < live_docs_count(" + std::to_string(live_docs_count) + ")");
  }

  const auto size = in->read_vlong();
  const auto flags = in->read_byte();
  field_id sort = field_limits::invalid();
  if (version > segment_meta_writer::FORMAT_MIN) {
    sort = in->read_vlong() - 1;
  }
  auto files = read_strings<segment_meta::file_set>(*in);

  if (flags & ~(segment_meta_writer::HAS_COLUMN_STORE | segment_meta_writer::SORTED)) {
    throw index_error(string_utils::to_string(
      "while reading segment meta '%s', error: use of unsupported flags '%u'",
      name.c_str(), flags));
  }

  const auto sorted = bool(flags & segment_meta_writer::SORTED);

  if ((!field_limits::valid(sort)) && sorted) {
    throw index_error(string_utils::to_string(
      "while reading segment meta '%s', error: incorrectly marked as sorted",
      name.c_str()));
  }

  if ((field_limits::valid(sort)) && !sorted) {
    throw index_error(string_utils::to_string(
      "while reading segment meta '%s', error: incorrectly marked as unsorted",
      name.c_str()));
  }

  format_utils::check_footer(*in, checksum);

  // ...........................................................................
  // all operations below are noexcept
  // ...........................................................................

  meta.name = std::move(name);
  meta.version = segment_version;
  meta.column_store = flags & segment_meta_writer::HAS_COLUMN_STORE;
  meta.docs_count = docs_count;
  meta.live_docs_count = live_docs_count;
  meta.sort = sort;
  meta.size = size;
  meta.files = std::move(files);
}

// ----------------------------------------------------------------------------
// --SECTION--                                             document_mask_writer
// ----------------------------------------------------------------------------

class document_mask_writer final: public irs::document_mask_writer {
 public:
  static constexpr string_ref FORMAT_NAME = "iresearch_10_doc_mask";
  static constexpr string_ref FORMAT_EXT = "doc_mask";

  static constexpr int32_t FORMAT_MIN = 0;
  static constexpr int32_t FORMAT_MAX = FORMAT_MIN;

  virtual ~document_mask_writer() = default;

  virtual std::string filename(const segment_meta& meta) const override;

  virtual void write(directory& dir,
                     const segment_meta& meta,
                     const document_mask& docs_mask) override;
}; // document_mask_writer

template<>
std::string file_name<irs::document_mask_writer, segment_meta>(const segment_meta& meta) {
  return file_name(meta.name, meta.version, document_mask_writer::FORMAT_EXT);
}

std::string document_mask_writer::filename(const segment_meta& meta) const {
  return file_name<irs::document_mask_writer>(meta);
}

void document_mask_writer::write(
    directory& dir,
    const segment_meta& meta,
    const document_mask& docs_mask) {
  const auto filename = file_name<irs::document_mask_writer>(meta);
  auto out = dir.create(filename);

  if (!out) {
    throw io_error(string_utils::to_string(
      "Failed to create file, path: %s",
      filename.c_str()));
  }

  // segment can't have more than std::numeric_limits<uint32_t>::max() documents
  assert(docs_mask.size() <= std::numeric_limits<uint32_t>::max());
  const auto count = static_cast<uint32_t>(docs_mask.size());

  format_utils::write_header(*out, FORMAT_NAME, FORMAT_MAX);
  out->write_vint(count);

  for (auto mask : docs_mask) {
    out->write_vint(mask);
  }

  format_utils::write_footer(*out);
}

// ----------------------------------------------------------------------------
// --SECTION--                                             document_mask_reader
// ----------------------------------------------------------------------------

class document_mask_reader final: public irs::document_mask_reader {
 public:
  virtual ~document_mask_reader() = default;

  virtual bool read(
    const directory& dir,
    const segment_meta& meta,
    document_mask& docs_mask) override;
}; // document_mask_reader

bool document_mask_reader::read(
    const directory& dir,
    const segment_meta& meta,
    document_mask& docs_mask
) {
  const auto in_name = file_name<irs::document_mask_writer>(meta);

  bool exists;

  if (!dir.exists(exists, in_name)) {
    throw io_error(string_utils::to_string(
      "failed to check existence of file, path: %s",
      in_name.c_str()));
  }

  if (!exists) {
    // possible that the file does not exist since document_mask is optional
    return false;
  }

  auto in = dir.open(
    in_name, irs::IOAdvice::SEQUENTIAL | irs::IOAdvice::READONCE);

  if (!in) {
    throw io_error(string_utils::to_string(
      "failed to open file, path: %s",
      in_name.c_str()));
  }

  const auto checksum = format_utils::checksum(*in);

  format_utils::check_header(
    *in,
    document_mask_writer::FORMAT_NAME,
    document_mask_writer::FORMAT_MIN,
    document_mask_writer::FORMAT_MAX);

  size_t count = in->read_vint();
  docs_mask.reserve(count);

  while (count--) {
    static_assert(
      sizeof(doc_id_t) == sizeof(decltype(in->read_vint())),
      "sizeof(doc_id) != sizeof(decltype(id))");

    docs_mask.insert(in->read_vint());
  }

  format_utils::check_footer(*in, checksum);

  return true;
}

// ----------------------------------------------------------------------------
// --SECTION--                                                  postings_reader
// ----------------------------------------------------------------------------

class postings_reader_base : public irs::postings_reader {
 public:
  virtual void prepare(
    index_input& in,
    const reader_state& state,
    IndexFeatures features) final;

  virtual size_t decode(
    const byte_type* in,
    IndexFeatures field_features,
    irs::term_meta& state) final;

 protected:
  explicit postings_reader_base(size_t block_size) noexcept
    : block_size_{block_size} {
  }

  size_t block_size_;
  index_input::ptr doc_in_;
  index_input::ptr pos_in_;
  index_input::ptr pay_in_;
}; // postings_reader

void postings_reader_base::prepare(
    index_input& in,
    const reader_state& state,
    IndexFeatures features) {
  std::string buf;

  // prepare document input
  prepare_input(
    buf, doc_in_, irs::IOAdvice::RANDOM, state,
    postings_writer_base::DOC_EXT,
    postings_writer_base::DOC_FORMAT_NAME,
    static_cast<int32_t>(PostingsFormat::MIN),
    static_cast<int32_t>(PostingsFormat::MAX));

  // Since terms doc postings too large
  //  it is too costly to verify checksum of
  //  the entire file. Here we perform cheap
  //  error detection which could recognize
  //  some forms of corruption.
  format_utils::read_checksum(*doc_in_);

  if (IndexFeatures::NONE != (features & IndexFeatures::POS)) {
    /* prepare positions input */
    prepare_input(
      buf, pos_in_, irs::IOAdvice::RANDOM, state,
      postings_writer_base::POS_EXT,
      postings_writer_base::POS_FORMAT_NAME,
      static_cast<int32_t>(PostingsFormat::MIN),
      static_cast<int32_t>(PostingsFormat::MAX));

    // Since terms pos postings too large
    // it is too costly to verify checksum of
    // the entire file. Here we perform cheap
    // error detection which could recognize
    // some forms of corruption.
    format_utils::read_checksum(*pos_in_);

    if (IndexFeatures::NONE != (features & (IndexFeatures::PAY | IndexFeatures::OFFS))) {
      // prepare positions input
      prepare_input(
        buf, pay_in_, irs::IOAdvice::RANDOM, state,
        postings_writer_base::PAY_EXT,
        postings_writer_base::PAY_FORMAT_NAME,
        static_cast<int32_t>(PostingsFormat::MIN),
        static_cast<int32_t>(PostingsFormat::MAX));

      // Since terms pos postings too large
      // it is too costly to verify checksum of
      // the entire file. Here we perform cheap
      // error detection which could recognize
      // some forms of corruption.
      format_utils::read_checksum(*pay_in_);
    }
  }

  // check postings format
  format_utils::check_header(in,
    postings_writer_base::TERMS_FORMAT_NAME,
    static_cast<int32_t>(TermsFormat::MIN),
    static_cast<int32_t>(TermsFormat::MAX));

  const uint64_t block_size = in.read_vint();

  if (block_size != block_size_) {
    throw index_error(string_utils::to_string(
      "while preparing postings_reader, error: "
      "invalid block size '" IR_UINT64_T_SPECIFIER "', "
      "expected '" IR_UINT64_T_SPECIFIER "'",
      block_size, block_size_));
  }
}

size_t postings_reader_base::decode(
    const byte_type* in,
    IndexFeatures features,
    irs::term_meta& state) {
  auto& term_meta = static_cast<version10::term_meta&>(state);

  const bool has_freq = IndexFeatures::NONE != (features & IndexFeatures::FREQ);
  const auto* p = in;

  term_meta.docs_count = vread<uint32_t>(p);
  if (has_freq) {
    term_meta.freq = term_meta.docs_count + vread<uint32_t>(p);
  }

  term_meta.doc_start += vread<uint64_t>(p);
  if (has_freq && term_meta.freq && IndexFeatures::NONE != (features & IndexFeatures::POS)) {
    term_meta.pos_start += vread<uint64_t>(p);

    term_meta.pos_end = term_meta.freq > block_size_
        ? vread<uint64_t>(p)
        : type_limits<type_t::address_t>::invalid();

    if (IndexFeatures::NONE != (features & (IndexFeatures::PAY | IndexFeatures::OFFS))) {
      term_meta.pay_start += vread<uint64_t>(p);
    }
  }

  if (1U == term_meta.docs_count || term_meta.docs_count > block_size_) {
    term_meta.e_skip_start = vread<uint64_t>(p);
  }

  assert(p >= in);
  return size_t(std::distance(in, p));
}

template<typename FormatTraits>
class postings_reader final: public postings_reader_base {
 public:
  template<bool Freq, bool Pos, bool Offset, bool Payload>
  struct iterator_traits : FormatTraits {
    static constexpr bool frequency() noexcept { return Freq; }
    static constexpr bool offset() noexcept { return position() && Offset; }
    static constexpr bool payload() noexcept { return position() && Payload; }
    static constexpr bool position() noexcept { return Freq && Pos; }
    static constexpr bool one_based_position_storage() noexcept {
      return FormatTraits::pos_min() == pos_limits::min();
    }
  };

  postings_reader() noexcept
    : postings_reader_base{FormatTraits::block_size()} {
  }

  virtual irs::doc_iterator::ptr iterator(
      IndexFeatures field_features,
      IndexFeatures required_features,
      const term_meta& meta) override {
    return iterator_impl<false>(field_features, required_features, meta);
  }

  virtual irs::doc_iterator::ptr wanderator(
      IndexFeatures field_features,
      IndexFeatures required_features,
      const term_meta& meta) override {
    if (meta.docs_count <= FormatTraits::block_size() ||
        IndexFeatures::NONE == (field_features & IndexFeatures::FREQ)) {
      // No need to use wanderator
      //  * for short lists
      //  * if term frequency isn't tracked
      return iterator(field_features, required_features, meta);
    }

    return iterator_impl<FormatTraits::wand()>(field_features, required_features, meta);
  }

  virtual size_t bit_union(
    IndexFeatures field,
    const term_provider_f& provider,
    size_t* set) override;

 private:
  template<typename IteratorTraits, typename FieldTraits, bool Wand>
  using doc_iterator_t = std::conditional_t<
    Wand, ::wanderator<IteratorTraits, FieldTraits>,
          doc_iterator<IteratorTraits, FieldTraits>>;

  template<typename IteratorTraits, typename FieldTraits, bool Wand>
  static irs::doc_iterator::ptr iterator_impl(
      const postings_reader& ctx,
      const term_meta& meta) {
    auto it = memory::make_managed<
      doc_iterator_t<IteratorTraits, FieldTraits, Wand>>();

    it->prepare(
      meta,
      ctx.doc_in_.get(),
      ctx.pos_in_.get(),
      ctx.pay_in_.get());

    return it;
  }

  template<typename FieldTraits, bool Wand, typename... Args>
  irs::doc_iterator::ptr iterator_impl(
    IndexFeatures enabled, Args&&... args);

  template<bool Wand>
  irs::doc_iterator::ptr iterator_impl(
    IndexFeatures field_features,
    IndexFeatures required_features,
    const term_meta& meta);
}; // postings_reader

#if defined(_MSC_VER)
#elif defined(__GNUC__)
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wswitch"
#endif

template<typename FormatTraits>
template<typename FieldTraits, bool Wand, typename... Args>
irs::doc_iterator::ptr postings_reader<FormatTraits>::iterator_impl(
    IndexFeatures enabled, Args&&... args) {
  switch (enabled) {
    case IndexFeatures::ALL: {
      using iterator_traits_t = iterator_traits<true, true, true, true>;
      return iterator_impl<iterator_traits_t, FieldTraits, Wand>(
        *this, std::forward<Args>(args)...);
    }
    case IndexFeatures::FREQ | IndexFeatures::POS | IndexFeatures::OFFS: {
      using iterator_traits_t = iterator_traits<true, true, true, false>;
      return iterator_impl<iterator_traits_t, FieldTraits, Wand>(
        *this, std::forward<Args>(args)...);
    }
    case IndexFeatures::FREQ | IndexFeatures::POS | IndexFeatures::PAY: {
      using iterator_traits_t = iterator_traits<true, true, false, true>;
      return iterator_impl<iterator_traits_t, FieldTraits, Wand>(
        *this, std::forward<Args>(args)...);
    }
    case IndexFeatures::FREQ | IndexFeatures::POS: {
      using iterator_traits_t = iterator_traits<true, true, false, false>;
      return iterator_impl<iterator_traits_t, FieldTraits, Wand>(
        *this, std::forward<Args>(args)...);
    }
    case IndexFeatures::FREQ: {
      using iterator_traits_t = iterator_traits<true, false, false, false>;
      return iterator_impl<iterator_traits_t, FieldTraits, Wand>(
        *this, std::forward<Args>(args)...);
    }
    default: {
      using iterator_traits_t = iterator_traits<false, false, false, false>;
      return iterator_impl<iterator_traits_t, FieldTraits, Wand>(
        *this, std::forward<Args>(args)...);
    }
  }
}

template<typename FormatTraits>
template<bool Wand>
irs::doc_iterator::ptr postings_reader<FormatTraits>::iterator_impl(
    IndexFeatures field_features,
    IndexFeatures required_features,
    const term_meta& meta) {
  // get enabled features as the intersection
  // between requested and available features
  const auto enabled = field_features & required_features;

  switch (field_features) {
    case IndexFeatures::ALL : {
      using field_traits_t = iterator_traits<true, true, true, true>;
      return iterator_impl<field_traits_t, Wand>(enabled, meta);
    }
    case IndexFeatures::FREQ | IndexFeatures::POS | IndexFeatures::OFFS: {
      using field_traits_t = iterator_traits<true, true, true, false>;
      return iterator_impl<field_traits_t, Wand>(enabled, meta);
    }
    case IndexFeatures::FREQ | IndexFeatures::POS | IndexFeatures::PAY: {
      using field_traits_t = iterator_traits<true, true, false, true>;
      return iterator_impl<field_traits_t, Wand>(enabled, meta);
    }
    case IndexFeatures::FREQ | IndexFeatures::POS: {
      using field_traits_t = iterator_traits<true, true, false, false>;
      return iterator_impl<field_traits_t, Wand>(enabled, meta);
    }
    case IndexFeatures::FREQ: {
      using field_traits_t = iterator_traits<true, false, false, false>;
      return iterator_impl<field_traits_t, Wand>(enabled, meta);
    }
    default: {
      using field_traits_t = iterator_traits<false, false, false, false>;
      return iterator_impl<field_traits_t, Wand>(enabled, meta);
    }
  }
}

#if defined(_MSC_VER)
#elif defined(__GNUC__)
  #pragma GCC diagnostic pop
#endif

template<typename FieldTraits, size_t N>
void bit_union(
    index_input& doc_in, doc_id_t docs_count,
    uint32_t (&docs)[N], uint32_t (&enc_buf)[N],
    size_t* set) {
  constexpr auto BITS{bits_required<std::remove_pointer_t<decltype(set)>>()};
  size_t num_blocks = docs_count / FieldTraits::block_size();

  doc_id_t doc = doc_limits::min();
  while (num_blocks--) {
    FieldTraits::read_block(doc_in, enc_buf, docs);
    if constexpr (FieldTraits::frequency()) {
      FieldTraits::skip_block(doc_in);
    }

    // FIXME optimize
    for (const auto delta : docs) {
      doc += delta;
      irs::set_bit(set[doc / BITS], doc % BITS);
    }
  }

  doc_id_t docs_left = docs_count % FieldTraits::block_size();

  while (docs_left--) {
    doc_id_t delta;
    if constexpr (FieldTraits::frequency()) {
      if (!shift_unpack_32(doc_in.read_vint(), delta)) {
        doc_in.read_vint();
      }
    } else {
      delta = doc_in.read_vint();
    }

    doc += delta;
    irs::set_bit(set[doc / BITS], doc % BITS);
  }
}

template<typename FormatTraits>
size_t postings_reader<FormatTraits>::bit_union(
    const IndexFeatures field_features,
    const term_provider_f& provider,
    size_t* set) {
  constexpr auto BITS{bits_required<std::remove_pointer_t<decltype(set)>>()};
  uint32_t enc_buf[FormatTraits::block_size()];
  uint32_t docs[FormatTraits::block_size()];
  const bool has_freq = IndexFeatures::NONE != (field_features & IndexFeatures::FREQ);

  assert(doc_in_);
  auto doc_in = doc_in_->reopen(); // reopen thread-safe stream

  if (!doc_in) {
    // implementation returned wrong pointer
    IR_FRMT_ERROR("Failed to reopen document input in: %s", __FUNCTION__);

    throw io_error("failed to reopen document input");
  }

  size_t count = 0;
  while (const irs::term_meta* meta = provider()) {
    auto& term_state = static_cast<const version10::term_meta&>(*meta);

    if (term_state.docs_count > 1) {
      doc_in->seek(term_state.doc_start);
      assert(!doc_in->eof());

      if (has_freq) {
        using field_traits_t = iterator_traits<true, false, false, false>;
        ::bit_union<field_traits_t>(*doc_in, term_state.docs_count, docs, enc_buf, set);
      } else {
        using field_traits_t = iterator_traits<false, false, false, false>;
        ::bit_union<field_traits_t>(*doc_in, term_state.docs_count, docs, enc_buf, set);
      }

      count += term_state.docs_count;
    } else {
      const doc_id_t doc = doc_limits::min() + term_state.e_single_doc;
      irs::set_bit(set[doc / BITS], doc % BITS);

      ++count;
    }
  }

  return count;
}

// ----------------------------------------------------------------------------
// --SECTION--                                                         format10
// ----------------------------------------------------------------------------

class format10 : public irs::version10::format {
 public:
  using format_traits = ::format_traits<false, pos_limits::min()>;

  static constexpr string_ref type_name() noexcept {
    return "1_0";
  }

  static ptr make();

  format10() noexcept : format10(irs::type<format10>::get()) { }

  virtual index_meta_writer::ptr get_index_meta_writer() const override;
  virtual index_meta_reader::ptr get_index_meta_reader() const override final;

  virtual segment_meta_writer::ptr get_segment_meta_writer() const override;
  virtual segment_meta_reader::ptr get_segment_meta_reader() const override final;

  virtual document_mask_writer::ptr get_document_mask_writer() const override final;
  virtual document_mask_reader::ptr get_document_mask_reader() const override final;

  virtual field_writer::ptr get_field_writer(bool consolidation) const override;
  virtual field_reader::ptr get_field_reader() const override final;

  virtual columnstore_writer::ptr get_columnstore_writer(bool consolidation) const override;
  virtual columnstore_reader::ptr get_columnstore_reader() const override;

  virtual irs::postings_writer::ptr get_postings_writer(bool consolidation) const override;
  virtual irs::postings_reader::ptr get_postings_reader() const override;

 protected:
  explicit format10(const irs::type_info& type) noexcept
    : version10::format(type) {
  }
}; // format10

const ::format10 FORMAT10_INSTANCE;

index_meta_writer::ptr format10::get_index_meta_writer() const {
  return memory::make_unique<::index_meta_writer>(
    int32_t(::index_meta_writer::FORMAT_MIN));
}

index_meta_reader::ptr format10::get_index_meta_reader() const {
  // can reuse stateless reader
  static ::index_meta_reader INSTANCE;

  return memory::to_managed<irs::index_meta_reader, false>(&INSTANCE);
}

segment_meta_writer::ptr format10::get_segment_meta_writer() const {
  // can reuse stateless writer
  static ::segment_meta_writer INSTANCE(::segment_meta_writer::FORMAT_MIN);

  return memory::to_managed<irs::segment_meta_writer, false>(&INSTANCE);
}

segment_meta_reader::ptr format10::get_segment_meta_reader() const {
  // can reuse stateless writer
  static ::segment_meta_reader INSTANCE;

  return memory::to_managed<irs::segment_meta_reader, false>(&INSTANCE);
}

document_mask_writer::ptr format10::get_document_mask_writer() const {
  // can reuse stateless writer
  static ::document_mask_writer INSTANCE;

  return memory::to_managed<irs::document_mask_writer, false>(&INSTANCE);
}

document_mask_reader::ptr format10::get_document_mask_reader() const {
  // can reuse stateless writer
  static ::document_mask_reader INSTANCE;

  return memory::to_managed<irs::document_mask_reader, false>(&INSTANCE);
}

field_writer::ptr format10::get_field_writer(bool consolidation) const {
  return burst_trie::make_writer(
    burst_trie::Version::MIN,
    get_postings_writer(consolidation),
    consolidation);
}

field_reader::ptr format10::get_field_reader() const  {
  return burst_trie::make_reader(get_postings_reader());
}

columnstore_writer::ptr format10::get_columnstore_writer(bool /*consolidation*/) const {
  return columnstore::make_writer(columnstore::Version::MIN,
                                  columnstore::ColumnMetaVersion::MIN);
}

columnstore_reader::ptr format10::get_columnstore_reader() const {
  return columnstore::make_reader();
}

irs::postings_writer::ptr format10::get_postings_writer(bool consolidation) const {
  return memory::make_unique<::postings_writer<format_traits>>(
    PostingsFormat::POSITIONS_ONEBASED, consolidation);
}

irs::postings_reader::ptr format10::get_postings_reader() const {
  return memory::make_unique<::postings_reader<format_traits>>();
}

/*static*/ irs::format::ptr format10::make() {
  return irs::format::ptr(irs::format::ptr(), &FORMAT10_INSTANCE);
}

REGISTER_FORMAT_MODULE(::format10, MODULE_NAME);

// ----------------------------------------------------------------------------
// --SECTION--                                                         format11
// ----------------------------------------------------------------------------

class format11 : public format10 {
 public:
  static constexpr string_ref type_name() noexcept {
    return "1_1";
  }

  static ptr make();

  format11() noexcept : format10(irs::type<format11>::get()) { }

  virtual index_meta_writer::ptr get_index_meta_writer() const override final;

  virtual field_writer::ptr get_field_writer(bool consolidation) const override;

  virtual segment_meta_writer::ptr get_segment_meta_writer() const override final;

  virtual columnstore_writer::ptr get_columnstore_writer(bool /*consolidation*/) const override;

 protected:
  explicit format11(const irs::type_info& type) noexcept
    : format10(type) {
  }
}; // format11

const ::format11 FORMAT11_INSTANCE;

index_meta_writer::ptr format11::get_index_meta_writer() const {
  return memory::make_unique<::index_meta_writer>(
    int32_t(::index_meta_writer::FORMAT_MAX));
}

field_writer::ptr format11::get_field_writer(bool consolidation) const {
  return burst_trie::make_writer(
    burst_trie::Version::ENCRYPTION_MIN,
    get_postings_writer(consolidation),
    consolidation);
}

segment_meta_writer::ptr format11::get_segment_meta_writer() const {
  // can reuse stateless writer
  static ::segment_meta_writer INSTANCE(::segment_meta_writer::FORMAT_MAX);

  return memory::to_managed<irs::segment_meta_writer, false>(&INSTANCE);
}

columnstore_writer::ptr format11::get_columnstore_writer(bool /*consolidation*/) const {
  return columnstore::make_writer(columnstore::Version::MIN,
                                  columnstore::ColumnMetaVersion::MAX);
}

/*static*/ irs::format::ptr format11::make() {
  return irs::format::ptr(irs::format::ptr(), &FORMAT11_INSTANCE);
}

REGISTER_FORMAT_MODULE(::format11, MODULE_NAME);

// ----------------------------------------------------------------------------
// --SECTION--                                                         format12
// ----------------------------------------------------------------------------

class format12 : public format11 {
 public:
  static constexpr string_ref type_name() noexcept {
    return "1_2";
  }

  static ptr make();

  format12() noexcept : format11(irs::type<format12>::get()) { }

  virtual columnstore_writer::ptr get_columnstore_writer(
      bool /*consolidation*/) const override;

 protected:
  explicit format12(const irs::type_info& type) noexcept
    : format11(type) {
  }
}; // format12

const ::format12 FORMAT12_INSTANCE;

columnstore_writer::ptr format12::get_columnstore_writer(
    bool /*consolidation*/) const {
  return columnstore::make_writer(columnstore::Version::MAX,
                                  columnstore::ColumnMetaVersion::MAX);
}

/*static*/ irs::format::ptr format12::make() {
  return irs::format::ptr(irs::format::ptr(), &FORMAT12_INSTANCE);
}

REGISTER_FORMAT_MODULE(::format12, MODULE_NAME);

// ----------------------------------------------------------------------------
// --SECTION--                                                         format13
// ----------------------------------------------------------------------------

class format13 : public format12 {
 public:
  using format_traits = ::format_traits<false, pos_limits::invalid()>;

  static constexpr string_ref type_name() noexcept {
    return "1_3";
  }

  static ptr make();

  format13() noexcept : format12(irs::type<format13>::get()) { }

  virtual irs::postings_writer::ptr get_postings_writer(bool consolidation) const override;
  virtual irs::postings_reader::ptr get_postings_reader() const override;

 protected:
  explicit format13(const irs::type_info& type) noexcept
    : format12(type) {
  }
};

const ::format13 FORMAT13_INSTANCE;

irs::postings_writer::ptr format13::get_postings_writer(bool consolidation) const {
  return memory::make_unique<::postings_writer<format_traits>>(
    PostingsFormat::POSITIONS_ZEROBASED, consolidation);
}

irs::postings_reader::ptr format13::get_postings_reader() const {
  return memory::make_unique<::postings_reader<format_traits>>();
}

/*static*/ irs::format::ptr format13::make() {
  static const ::format13 INSTANCE;

  return irs::format::ptr(irs::format::ptr(), &FORMAT13_INSTANCE);
}

REGISTER_FORMAT_MODULE(::format13, MODULE_NAME);

// ----------------------------------------------------------------------------
// --SECTION--                                                         format14
// ----------------------------------------------------------------------------

class format14 : public format13 {
 public:
  static constexpr string_ref type_name() noexcept {
    return "1_4";
  }

  static ptr make();

  format14() noexcept : format13(irs::type<format14>::get()) { }

  virtual irs::field_writer::ptr get_field_writer(bool consolidation) const override;

  virtual irs::columnstore_writer::ptr get_columnstore_writer(bool consolidation) const override;
  virtual irs::columnstore_reader::ptr get_columnstore_reader() const override;

 protected:
  explicit format14(const irs::type_info& type) noexcept
    : format13(type) {
  }
};

const ::format14 FORMAT14_INSTANCE;

irs::field_writer::ptr format14::get_field_writer(bool consolidation) const {
  return burst_trie::make_writer(
    burst_trie::Version::MAX,
    get_postings_writer(consolidation),
    consolidation);
}

columnstore_writer::ptr format14::get_columnstore_writer(
    bool consolidation) const {
  return columnstore2::make_writer(columnstore2::Version::kMin, consolidation);
}

columnstore_reader::ptr format14::get_columnstore_reader() const {
  return columnstore2::make_reader();
}

/*static*/ irs::format::ptr format14::make() {
  return irs::format::ptr(irs::format::ptr(), &FORMAT14_INSTANCE);
}

REGISTER_FORMAT_MODULE(::format14, MODULE_NAME);

// ----------------------------------------------------------------------------
// --SECTION--                                                         format15
// ----------------------------------------------------------------------------

class format15 : public format14 {
 public:
  using format_traits = ::format_traits<true, pos_limits::invalid()>;

  static constexpr string_ref type_name() noexcept {
    return "1_5";
  }

  static ptr make();

  format15() noexcept : format14(irs::type<format15>::get()) { }

  virtual irs::postings_writer::ptr get_postings_writer(bool consolidation) const override;
  virtual irs::postings_reader::ptr get_postings_reader() const override;

 protected:
  explicit format15(const irs::type_info& type) noexcept
    : format14(type) {
  }
};

const ::format15 FORMAT15_INSTANCE;

irs::postings_writer::ptr format15::get_postings_writer(bool consolidation) const {
  return memory::make_unique<::postings_writer<format_traits>>(
    PostingsFormat::WAND, consolidation);
}

irs::postings_reader::ptr format15::get_postings_reader() const {
  return memory::make_unique<::postings_reader<format_traits>>();
}

/*static*/ irs::format::ptr format15::make() {
  return irs::format::ptr(irs::format::ptr(), &FORMAT15_INSTANCE);
}

REGISTER_FORMAT_MODULE(::format15, MODULE_NAME);

// ----------------------------------------------------------------------------
// --SECTION--                                                     format12simd
// ----------------------------------------------------------------------------

#ifdef IRESEARCH_SSE2

template<bool Wand, uint32_t PosMin>
struct format_traits_sse4 {
  using align_type = __m128i;

  static constexpr bool wand() noexcept { return Wand; };
  static constexpr uint32_t pos_min() noexcept { return PosMin; }
  static constexpr uint32_t block_size() noexcept { return SIMDBlockSize; }

  FORCE_INLINE static void pack_block(
      const uint32_t* RESTRICT decoded,
      uint32_t* RESTRICT encoded,
      const uint32_t bits) noexcept {
    ::simdpackwithoutmask(decoded, reinterpret_cast<align_type*>(encoded), bits);
  }

  FORCE_INLINE static void unpack_block(
      uint32_t* decoded, const uint32_t* encoded, const uint32_t bits) noexcept {
    ::simdunpack(reinterpret_cast<const align_type*>(encoded), decoded, bits);
  }

  FORCE_INLINE static void write_block(
      index_output& out, const uint32_t* in, uint32_t* buf) {
    bitpack::write_block32<block_size()>(&pack_block, out, in, buf);
  }

  FORCE_INLINE static void read_block(
      index_input& in, uint32_t* buf, uint32_t* out) {
    bitpack::read_block32<block_size()>(&unpack_block, in, buf, out);
  }

  FORCE_INLINE static void skip_block(index_input& in) {
    bitpack::skip_block32(in, block_size());
  }
}; // format_traits_sse

class format12simd final : public format12 {
 public:
  using format_traits = format_traits_sse4<false, pos_limits::min()>;

  static constexpr string_ref type_name() noexcept {
    return "1_2simd";
  }

  static ptr make();

  format12simd() noexcept : format12(irs::type<format12simd>::get()) { }

  virtual irs::postings_writer::ptr get_postings_writer(bool consolidation) const override;
  virtual irs::postings_reader::ptr get_postings_reader() const override;
}; // format12simd

const ::format12simd FORMAT12SIMD_INSTANCE;

irs::postings_writer::ptr format12simd::get_postings_writer(bool consolidation) const {
  return memory::make_unique<::postings_writer<format_traits>>(
    PostingsFormat::POSITIONS_ONEBASED_SSE, consolidation);
}

irs::postings_reader::ptr format12simd::get_postings_reader() const {
  return memory::make_unique<::postings_reader<format_traits>>();
}

/*static*/ irs::format::ptr format12simd::make() {
  return irs::format::ptr(irs::format::ptr(), &FORMAT12SIMD_INSTANCE);
}

REGISTER_FORMAT_MODULE(::format12simd, MODULE_NAME);


// ----------------------------------------------------------------------------
// --SECTION--                                                     format13simd
// ----------------------------------------------------------------------------

class format13simd : public format13 {
 public:
  using format_traits = format_traits_sse4<false, pos_limits::invalid()>;

  static constexpr string_ref type_name() noexcept {
    return "1_3simd";
  }

  static ptr make();

  format13simd() noexcept : format13(irs::type<format13simd>::get()) { }

  virtual irs::postings_writer::ptr get_postings_writer(bool consolidation) const override;
  virtual irs::postings_reader::ptr get_postings_reader() const override;

 protected:
  explicit format13simd(const irs::type_info& type) noexcept
    : format13(type) {
  }
}; // format13simd

const ::format13simd FORMAT13SIMD_INSTANCE;

irs::postings_writer::ptr format13simd::get_postings_writer(bool consolidation) const {
  return memory::make_unique<::postings_writer<format_traits>>(
    PostingsFormat::POSITIONS_ZEROBASED_SSE, consolidation);
}

irs::postings_reader::ptr format13simd::get_postings_reader() const {
  return memory::make_unique<::postings_reader<format_traits>>();
}

/*static*/ irs::format::ptr format13simd::make() {
  return irs::format::ptr(irs::format::ptr(), &FORMAT13SIMD_INSTANCE);
}

REGISTER_FORMAT_MODULE(::format13simd, MODULE_NAME);

// ----------------------------------------------------------------------------
// --SECTION--                                                     format14simd
// ----------------------------------------------------------------------------

class format14simd : public format13simd {
 public:
  using format_traits = format_traits_sse4<true, pos_limits::invalid()>;

  static constexpr string_ref type_name() noexcept {
    return "1_4simd";
  }

  static ptr make();

  format14simd() noexcept : format13simd(irs::type<format14simd>::get()) { }

  virtual columnstore_writer::ptr get_columnstore_writer(bool consolidation) const override;
  virtual columnstore_reader::ptr get_columnstore_reader() const override;

  virtual irs::field_writer::ptr get_field_writer(bool consolidation) const override;

 protected:
  explicit format14simd(const irs::type_info& type) noexcept
    : format13simd(type) {
  }
};

const ::format14simd FORMAT14SIMD_INSTANCE;

irs::field_writer::ptr format14simd::get_field_writer(bool consolidation) const {
  return burst_trie::make_writer(
    burst_trie::Version::MAX,
    get_postings_writer(consolidation),
    consolidation);
}

columnstore_writer::ptr format14simd::get_columnstore_writer(
    bool consolidation) const {
  return columnstore2::make_writer(columnstore2::Version::kMin, consolidation);
}

columnstore_reader::ptr format14simd::get_columnstore_reader() const {
  return columnstore2::make_reader();
}

/*static*/ irs::format::ptr format14simd::make() {
  return irs::format::ptr(irs::format::ptr(), &FORMAT14SIMD_INSTANCE);
}

REGISTER_FORMAT_MODULE(::format14simd, MODULE_NAME);

// ----------------------------------------------------------------------------
// --SECTION--                                                     format15simd
// ----------------------------------------------------------------------------

class format15simd : public format14simd {
 public:
  using format_traits = format_traits_sse4<true, pos_limits::invalid()>;

  static constexpr string_ref type_name() noexcept {
    return "1_5simd";
  }

  static ptr make();

  format15simd() noexcept : format14simd(irs::type<format15simd>::get()) { }

  virtual irs::postings_writer::ptr get_postings_writer(bool consolidation) const override;
  virtual irs::postings_reader::ptr get_postings_reader() const override;

 protected:
  explicit format15simd(const irs::type_info& type) noexcept
    : format14simd(type) {
  }
};

const ::format15simd FORMAT15SIMD_INSTANCE;

irs::postings_writer::ptr format15simd::get_postings_writer(bool consolidation) const {
  return memory::make_unique<::postings_writer<format_traits>>(
    PostingsFormat::WAND_SSE, consolidation);
}

irs::postings_reader::ptr format15simd::get_postings_reader() const {
  return memory::make_unique<::postings_reader<format_traits>>();
}

/*static*/ irs::format::ptr format15simd::make() {
  return irs::format::ptr(irs::format::ptr(), &FORMAT15SIMD_INSTANCE);
}

REGISTER_FORMAT_MODULE(::format15simd, MODULE_NAME);

#endif // IRESEARCH_SSE2

}

namespace iresearch {
namespace version10 {

void init() {
#ifndef IRESEARCH_DLL
  REGISTER_FORMAT(::format10);
  REGISTER_FORMAT(::format11);
  REGISTER_FORMAT(::format12);
  REGISTER_FORMAT(::format13);
  REGISTER_FORMAT(::format14);
  REGISTER_FORMAT(::format15);
#ifdef IRESEARCH_SSE2
  REGISTER_FORMAT(::format12simd);
  REGISTER_FORMAT(::format13simd);
  REGISTER_FORMAT(::format14simd);
  REGISTER_FORMAT(::format15simd);
#endif // IRESEARCH_SSE2
#endif // IRESEARCH_DLL
}

// ----------------------------------------------------------------------------
// --SECTION--                                                           format
// ----------------------------------------------------------------------------

format::format(const irs::type_info& type) noexcept
  : irs::format(type) {
}

} // version10

// use base irs::position type for ancestors
template<typename IteratorTraits, typename FieldTraits, bool Position>
struct type<::position<IteratorTraits, FieldTraits, Position>> : type<irs::position> { };

} // root
