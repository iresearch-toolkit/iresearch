////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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

#include "shared.hpp"

#include "skip_list.hpp"

#include "formats_10_optimized.hpp"
#include "formats_10_attributes.hpp"
#include "formats_burst_trie.hpp"
#include "format_utils.hpp"

#include "index/file_names.hpp"

#include "store/store_utils_optimized.hpp"

#include "utils/bit_utils.hpp"
#include "utils/bitset.hpp"
#include "utils/directory_utils.hpp"
#include "utils/log.hpp"
#include "utils/memory_pool.hpp"
#include "utils/noncopyable.hpp"
#include "utils/object_pool.hpp"
#include "utils/timer_utils.hpp"
#include "utils/type_limits.hpp"
#include "utils/std.hpp"

#if defined(_MSC_VER)
  #pragma warning(disable : 4351)
#endif

#ifndef IRESEARCH_SSE2
  #error "Optimized format requires SSE2 support"
#endif

NS_LOCAL

// ----------------------------------------------------------------------------
// --SECTION--                                                  SIMD bitpacking
// ----------------------------------------------------------------------------

using namespace iresearch;

// ----------------------------------------------------------------------------
// --SECTION--                                                         features
// ----------------------------------------------------------------------------

// compiled features supported by current format
class features {
 public:
  enum Mask : uint32_t {
    POS = 3, POS_OFFS = 7, POS_PAY = 11, POS_OFFS_PAY = 15
  };

  features() = default;

  explicit features(const irs::flags& in) NOEXCEPT {
    irs::set_bit<0>(in.check<irs::frequency>(), mask_);
    irs::set_bit<1>(in.check<irs::position>(), mask_);
    irs::set_bit<2>(in.check<irs::offset>(), mask_);
    irs::set_bit<3>(in.check<irs::payload>(), mask_);
  }

  features operator&(const irs::flags& in) const NOEXCEPT {
    return features(*this) &= in;
  }

  features& operator&=(const irs::flags& in) NOEXCEPT {
    irs::unset_bit<0>(!in.check<irs::frequency>(), mask_);
    irs::unset_bit<1>(!in.check<irs::position>(), mask_);
    irs::unset_bit<2>(!in.check<irs::offset>(), mask_);
    irs::unset_bit<3>(!in.check<irs::payload>(), mask_);
    return *this;
  }

  bool freq() const NOEXCEPT { return irs::check_bit<0>(mask_); }
  bool position() const NOEXCEPT { return irs::check_bit<1>(mask_); }
  bool offset() const NOEXCEPT { return irs::check_bit<2>(mask_); }
  bool payload() const NOEXCEPT { return irs::check_bit<3>(mask_); }
  operator Mask() const NOEXCEPT { return static_cast<Mask>(mask_); }

 private:
  irs::byte_type mask_{};
}; // features

// ----------------------------------------------------------------------------
// --SECTION--                                                 helper functions
// ----------------------------------------------------------------------------

//FIXME move to a common helper

inline void prepare_output(
    std::string& str,
    index_output::ptr& out,
    const flush_state& state,
    const string_ref& ext,
    const string_ref& format,
    const int32_t version) {
  assert( !out );
  file_name(str, state.name, ext);
  out = state.dir->create(str);

  if (!out) {
    std::stringstream ss;

    ss << "Failed to create file, path: " << str;

    throw detailed_io_error(ss.str());
  }

  format_utils::write_header(*out, format, version);
}

inline void prepare_input(
    std::string& str,
    index_input::ptr& in,
    IOAdvice advice,
    const reader_state& state,
    const string_ref& ext,
    const string_ref& format,
    const int32_t min_ver,
    const int32_t max_ver ) {
  assert( !in );
  file_name(str, state.meta->name, ext);
  in = state.dir->open(str, advice);

  if (!in) {
    std::stringstream ss;

    ss << "Failed to open file, path: " << str;

    throw detailed_io_error(ss.str());
  }

  format_utils::check_header(*in, format, min_ver, max_ver);
}

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
class postings_writer final: public irs::postings_writer {
 public:
  static const string_ref TERMS_FORMAT_NAME;
  static const int32_t TERMS_FORMAT_MIN = 0;
  static const int32_t TERMS_FORMAT_MAX = TERMS_FORMAT_MIN;

  static const string_ref DOC_FORMAT_NAME;
  static const string_ref DOC_EXT;
  static const string_ref POS_FORMAT_NAME;
  static const string_ref POS_EXT;
  static const string_ref PAY_FORMAT_NAME;
  static const string_ref PAY_EXT;

  static const int32_t FORMAT_MIN = 0;
  static const int32_t FORMAT_MAX = FORMAT_MIN;

  static const uint32_t MAX_SKIP_LEVELS = 10;
  static const uint32_t BLOCK_SIZE = 128;
  static const uint32_t SKIP_N = 8;

  explicit postings_writer(bool volatile_attributes);

  // ------------------------------------------
  // const_attributes_provider
  // ------------------------------------------

  virtual const irs::attribute_view& attributes() const NOEXCEPT override final {
    return attrs_;
  }

  // ------------------------------------------
  // postings_writer
  // ------------------------------------------

  virtual void prepare(index_output& out, const iresearch::flush_state& state) override;
  virtual void begin_field(const iresearch::flags& meta) override;
  virtual irs::postings_writer::state write(irs::doc_iterator& docs) override;
  virtual void begin_block() override;
  virtual void encode(data_output& out, const irs::term_meta& attrs) override;
  virtual void end() override;

 protected:
  virtual void release(irs::term_meta *meta) NOEXCEPT override;

 private:
  struct stream {
    void reset() {
      start = end = 0;
    }

    uint64_t skip_ptr[MAX_SKIP_LEVELS]{}; // skip data
    index_output::ptr out;                // output stream
    uint64_t start{};                     // start position of block
    uint64_t end{};                       // end position of block
  }; // stream

  struct doc_stream : stream {
    void doc(doc_id_t delta) { deltas[size] = uint32_t(delta); }
    void flush(uint64_t* buf, bool freq);
    bool full() const { return BLOCK_SIZE == size; }
    void next(doc_id_t id) { last = id, ++size; }
    void freq(uint64_t frq) { freqs[size] = frq; }

    void reset() {
      stream::reset();
      last = type_limits<type_t::doc_id_t>::invalid();
      block_last = 0;
      size = 0;
    }

    uint32_t deltas[BLOCK_SIZE]{}; // document deltas
    doc_id_t skip_doc[MAX_SKIP_LEVELS]{};
    std::unique_ptr<uint32_t[]> freqs; /* document frequencies */
    doc_id_t last{ type_limits<type_t::doc_id_t>::invalid() }; // last buffered document id
    doc_id_t block_last{}; // last document id in a block
    uint32_t size{};            /* number of buffered elements */
  }; // doc_stream

  struct pos_stream : stream {
    DECLARE_UNIQUE_PTR(pos_stream);

    void flush(uint32_t* buf);

    bool full() const { return BLOCK_SIZE == size; }
    void next(uint32_t pos) { last = pos, ++size; }
    void pos(uint32_t pos) { buf[size] = pos; }

    void reset() {
      stream::reset();
      last = 0;
      block_last = 0;
      size = 0;
    }

    uint32_t buf[BLOCK_SIZE]{}; // buffer to store position deltas
    uint32_t last{};            // last buffered position
    uint32_t block_last{};      // last position in a block
    uint32_t size{};            // number of buffered elements
  }; // pos_stream

  struct pay_stream : stream {
    DECLARE_UNIQUE_PTR(pay_stream);

    void flush_payload(uint32_t* buf);
    void flush_offsets(uint32_t* buf);

    void payload(uint32_t i, const bytes_ref& pay);
    void offsets(uint32_t i, uint32_t start, uint32_t end);

    void reset() {
      stream::reset();
      pay_buf_.clear();
      block_last = 0;
      last = 0;
    }

    bstring pay_buf_;                       // buffer for payload
    uint32_t pay_sizes[BLOCK_SIZE]{};       // buffer to store payloads sizes
    uint32_t offs_start_buf[BLOCK_SIZE]{};  // buffer to store start offsets
    uint32_t offs_len_buf[BLOCK_SIZE]{};    // buffer to store offset lengths
    size_t block_last{};                    // last payload buffer length in a block
    uint32_t last{};                        // last start offset
  }; // pay_stream

  void write_skip(size_t level, index_output& out);
  void begin_term();
  void begin_doc(doc_id_t id, const frequency* freq);
  void add_position( uint32_t pos, const offset* offs, const payload* pay );
  void end_doc();
  void end_term(version10::term_meta& state, const uint64_t* tfreq);

  memory::memory_pool<> meta_pool_;
  memory::memory_pool_allocator<version10::term_meta, decltype(meta_pool_)> alloc_{ meta_pool_ };
  skip_writer skip_;
  irs::attribute_view attrs_;
  uint64_t buf[BLOCK_SIZE];        // buffer for encoding (worst case)
  version10::term_meta last_state; // last final term state
  doc_stream doc;                  // document stream
  pos_stream::ptr pos_;            // proximity stream
  pay_stream::ptr pay_;            // payloads and offsets stream
  uint64_t docs_count{};           // count of processed documents
  version10::documents docs_;      // bit set of all processed documents
  features features_;              // features supported by current field
  bool volatile_attributes_;       // attribute value memory locations may change after next()
}; // postings_writer

MSVC2015_ONLY(__pragma(warning(push)))
MSVC2015_ONLY(__pragma(warning(disable: 4592))) // symbol will be dynamically initialized (implementation limitation) false positive bug in VS2015.1

const string_ref postings_writer::TERMS_FORMAT_NAME = "iresearch_10_postings_terms";

const string_ref postings_writer::DOC_FORMAT_NAME = "iresearch_10_postings_documents_sse";
const string_ref postings_writer::DOC_EXT = "doc";

const string_ref postings_writer::POS_FORMAT_NAME = "iresearch_10_postings_positions_sse";
const string_ref postings_writer::POS_EXT = "pos";

const string_ref postings_writer::PAY_FORMAT_NAME = "iresearch_10_postings_payloads_sse";
const string_ref postings_writer::PAY_EXT = "pay";

MSVC2015_ONLY(__pragma(warning(pop)))

void postings_writer::doc_stream::flush(uint64_t* buf, bool freq) {
  auto* buf32 = reinterpret_cast<uint32_t*>(buf);
  encode::bitpack::write_block32_optimized(*out, deltas, BLOCK_SIZE, buf32);

  if (freq) {
    encode::bitpack::write_block32_optimized(*out, freqs.get(), BLOCK_SIZE, buf32);
  }
}

void postings_writer::pos_stream::flush(uint32_t* comp_buf) {
  encode::bitpack::write_block32_optimized(*out, this->buf, BLOCK_SIZE, comp_buf);
  size = 0;
}

/* postings_writer::pay_stream */

void postings_writer::pay_stream::payload(uint32_t i, const bytes_ref& pay) {
  if (!pay.empty()) {
    pay_buf_.append(pay.c_str(), pay.size());
  }

  pay_sizes[i] = static_cast<uint32_t>(pay.size());
}

void postings_writer::pay_stream::offsets(
    uint32_t i, uint32_t start_offset, uint32_t end_offset) {
  assert(start_offset >= last && start_offset <= end_offset);

  offs_start_buf[i] = start_offset - last;
  offs_len_buf[i] = end_offset - start_offset;
  last = start_offset;
}

void postings_writer::pay_stream::flush_payload(uint32_t* buf) {
  out->write_vint(static_cast<uint32_t>(pay_buf_.size()));
  if (pay_buf_.empty()) {
    return;
  }
  encode::bitpack::write_block32_optimized(*out, pay_sizes, BLOCK_SIZE, buf);
  out->write_bytes(pay_buf_.c_str(), pay_buf_.size());
  pay_buf_.clear();
}

void postings_writer::pay_stream::flush_offsets(uint32_t* buf) {
  encode::bitpack::write_block32_optimized(*out, offs_start_buf, BLOCK_SIZE, buf);
  encode::bitpack::write_block32_optimized(*out, offs_len_buf, BLOCK_SIZE, buf);
}

postings_writer::postings_writer(bool volatile_attributes)
  : skip_(BLOCK_SIZE, SKIP_N),
    volatile_attributes_(volatile_attributes) {
  attrs_.emplace(docs_);
}

void postings_writer::prepare(index_output& out, const iresearch::flush_state& state) {
  assert(state.dir);
  assert(!state.name.null());

  // reset writer state
  docs_count = 0;

  std::string name;

  // prepare document stream
  prepare_output(name, doc.out, state, DOC_EXT, DOC_FORMAT_NAME, FORMAT_MAX);

  auto& features = *state.features;
  if (features.check<frequency>() && !doc.freqs) {
    // prepare frequency stream
    doc.freqs = memory::make_unique<uint32_t[]>(BLOCK_SIZE);
    std::memset(doc.freqs.get(), 0, sizeof(uint32_t) * BLOCK_SIZE);
  }

  if (features.check< position >()) {
    // prepare proximity stream
    if (!pos_) {
      pos_ = memory::make_unique< pos_stream >();
    }

    pos_->reset();
    prepare_output(name, pos_->out, state, POS_EXT, POS_FORMAT_NAME, FORMAT_MAX);

    if (features.check< payload >() || features.check< offset >()) {
      // prepare payload stream
      if (!pay_) {
        pay_ = memory::make_unique<pay_stream>();
      }

      pay_->reset();
      prepare_output(name, pay_->out, state, PAY_EXT, PAY_FORMAT_NAME, FORMAT_MAX);
    }
  }

  skip_.prepare(
    MAX_SKIP_LEVELS,
    state.doc_count,
    [this] (size_t i, index_output& out) { write_skip(i, out); },
    directory_utils::get_allocator(*state.dir)
  );

  // write postings format name
  format_utils::write_header(out, TERMS_FORMAT_NAME, TERMS_FORMAT_MAX);
  // write postings block size
  out.write_vint(BLOCK_SIZE);

  // prepare documents bitset
  docs_.value.reset(state.doc_count);
}

void postings_writer::begin_field(const iresearch::flags& field) {
  features_ = ::features(field);
  docs_.value.clear();
  last_state.clear();
}

void postings_writer::begin_block() {
  /* clear state in order to write
   * absolute address of the first
   * entry in the block */
  last_state.clear();
}

#if defined(_MSC_VER)
  #pragma warning( disable : 4706 )
#elif defined (__GNUC__)
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wparentheses"
#endif

irs::postings_writer::state postings_writer::write(irs::doc_iterator& docs) {
  REGISTER_TIMER_DETAILED();
  auto& freq = docs.attributes().get<frequency>();

  auto& pos = freq
    ? docs.attributes().get<position>()
    : irs::attribute_view::ref<position>::NIL;

  const offset* offs = nullptr;
  const payload* pay = nullptr;

  uint64_t* tfreq = nullptr;

  auto meta = memory::allocate_unique<version10::term_meta>(alloc_);

  if (freq) {
    if (pos && !volatile_attributes_) {
      auto& attrs = pos->attributes();
      offs = attrs.get<offset>().get();
      pay = attrs.get<payload>().get();
    }

    tfreq = &meta->freq;
  }

  begin_term();

  while (docs.next()) {
    const auto did = docs.value();

    assert(type_limits<type_t::doc_id_t>::valid(did));
    begin_doc(did, freq.get());
    docs_.value.set(did - type_limits<type_t::doc_id_t>::min());

    if (pos) {
      if (volatile_attributes_) {
        auto& attrs = pos->attributes();
        offs = attrs.get<offset>().get();
        pay = attrs.get<payload>().get();
      }

      while (pos->next()) {
        add_position(pos->value(), offs, pay);
      }
    }

    ++meta->docs_count;
    if (tfreq) {
      (*tfreq) += freq->value;
    }

    end_doc();
  }

  end_term(*meta, tfreq);

  return make_state(*meta.release());
}

void postings_writer::release(irs::term_meta *meta) NOEXCEPT {
#ifdef IRESEARCH_DEBUG
  auto* state = dynamic_cast<version10::term_meta*>(meta);
#else
  auto* state = static_cast<version10::term_meta*>(meta);
#endif // IRESEARCH_DEBUG
  assert(state);

  alloc_.destroy(state);
  alloc_.deallocate(state);
}

#if defined(_MSC_VER)
  #pragma warning( default : 4706 )
#elif defined (__GNUC__)
  #pragma GCC diagnostic pop
#endif

void postings_writer::begin_term() {
  doc.start = doc.out->file_pointer();
  std::fill_n(doc.skip_ptr, MAX_SKIP_LEVELS, doc.start);
  if (features_.position()) {
    assert(pos_);
    pos_->start = pos_->out->file_pointer();
    std::fill_n(pos_->skip_ptr, MAX_SKIP_LEVELS, pos_->start);
    if (features_.payload() || features_.offset()) {
      assert(pay_);
      pay_->start = pay_->out->file_pointer();
      std::fill_n(pay_->skip_ptr, MAX_SKIP_LEVELS, pay_->start);
    }
  }

  doc.last = type_limits<type_t::doc_id_t>::min(); // for proper delta of 1st id
  doc.block_last = type_limits<type_t::doc_id_t>::invalid();
  skip_.reset();
}

void postings_writer::begin_doc(doc_id_t id, const frequency* freq) {
  if (type_limits<type_t::doc_id_t>::valid(doc.block_last) && 0 == doc.size) {
    skip_.skip(docs_count);
  }

  if (id < doc.last) {
    // docs out of order
    throw index_error();
  }

  doc.doc(id - doc.last);
  if (freq) {
    doc.freq(freq->value);
  }

  doc.next(id);
  if (doc.full()) {
    doc.flush(buf, freq != nullptr);
  }

  if (pos_) pos_->last = 0;
  if (pay_) pay_->last = 0;

  ++docs_count;
}

void postings_writer::add_position(uint32_t pos, const offset* offs, const payload* pay) {
  assert(!offs || offs->start <= offs->end);
  assert(pos_); /* at least positions stream should be created */

  pos_->pos(pos - pos_->last);
  if (pay) pay_->payload(pos_->size, pay->value);
  if (offs) pay_->offsets(pos_->size, offs->start, offs->end);

  pos_->next(pos);

  if (pos_->full()) {
    auto* buf32 = reinterpret_cast<uint32_t*>(buf);

    pos_->flush(buf32);

    if (pay) {
      pay_->flush_payload(buf32);
    }

    if (offs) {
      pay_->flush_offsets(buf32);
    }
  }
}

void postings_writer::end_doc() {
  if ( doc.full() ) {
    doc.block_last = doc.last;
    doc.end = doc.out->file_pointer();
    if ( pos_ ) {
      assert( pos_ );
      pos_->end = pos_->out->file_pointer();
      // documents stream is full, but positions stream is not
      // save number of positions to skip before the next block
      pos_->block_last = pos_->size;
      if ( pay_ ) {
        pay_->end = pay_->out->file_pointer();
        pay_->block_last = pay_->pay_buf_.size();
      }
    }

    doc.size = 0;
  }
}

void postings_writer::end_term(version10::term_meta& meta, const uint64_t* tfreq) {
  if (docs_count == 0) {
    return; // no documents to write
  }

  if (1 == meta.docs_count) {
    meta.e_single_doc = doc.deltas[0];
  } else {
    // write remaining documents using
    // variable length encoding
    data_output& out = *doc.out;

    for (uint32_t i = 0; i < doc.size; ++i) {
      const uint32_t doc_delta = doc.deltas[i];

      if (!features_.freq()) {
        out.write_vint(doc_delta);
      } else {
        assert(doc.freqs);
        const uint32_t freq = doc.freqs[i];

        if (1 == freq) {
          out.write_vint(shift_pack_32(doc_delta, true));
        } else {
          out.write_vint(shift_pack_32(doc_delta, false));
          out.write_vint(freq);
        }
      }
    }
  }

  meta.pos_end = type_limits<type_t::address_t>::invalid();

  /* write remaining position using
   * variable length encoding */
  if (features_.position()) {
    if (meta.freq > BLOCK_SIZE) {
      meta.pos_end = pos_->out->file_pointer() - pos_->start;
    }

    if (pos_->size > 0) {
      data_output& out = *pos_->out;
      uint32_t last_pay_size = integer_traits<uint32_t>::const_max;
      uint32_t last_offs_len = integer_traits<uint32_t>::const_max;
      uint32_t pay_buf_start = 0;
      for (uint32_t i = 0; i < pos_->size; ++i) {
        const uint32_t pos_delta = pos_->buf[i];
        if (features_.payload()) {
          const uint32_t size = pay_->pay_sizes[i];
          if (last_pay_size != size) {
            last_pay_size = size;
            out.write_vint(shift_pack_32(pos_delta, true));
            out.write_vint(size);
          } else {
            out.write_vint(shift_pack_32(pos_delta, false));
          }

          if (size != 0) {
            out.write_bytes(pay_->pay_buf_.c_str() + pay_buf_start, size);
            pay_buf_start += size;
          }
        } else {
          out.write_vint(pos_delta);
        }

        if (features_.offset()) {
          const uint32_t pay_offs_delta = pay_->offs_start_buf[i];
          const uint32_t len = pay_->offs_len_buf[i];
          if (len == last_offs_len) {
            out.write_vint(shift_pack_32(pay_offs_delta, false));
          } else {
            out.write_vint(shift_pack_32(pay_offs_delta, true));
            out.write_vint(len);
            last_offs_len = len;
          }
        }
      }

      if (features_.payload()) {
        pay_->pay_buf_.clear();
      }
    }
  }

  if (!tfreq) {
    meta.freq = integer_traits<uint64_t>::const_max;
  }

  /* if we have flushed at least
   * one block there was buffered
   * skip data, so we need to flush it*/
  if (docs_count > BLOCK_SIZE) {
    //const uint64_t start = doc.out->file_pointer();
    meta.e_skip_start = doc.out->file_pointer() - doc.start;
    skip_.flush(*doc.out);
  }

  docs_count = 0;
  doc.size = 0;
  doc.last = 0;
  meta.doc_start = doc.start;

  if (pos_) {
    pos_->size = 0;
    meta.pos_start = pos_->start;
  }

  if (pay_) {
    //pay_->buf_size = 0;
    pay_->pay_buf_.clear();
    pay_->last = 0;
    meta.pay_start = pay_->start;
  }
}

void postings_writer::write_skip(size_t level, index_output& out) {
  const uint64_t doc_delta = doc.block_last; //- doc.skip_doc[level];
  const uint64_t doc_ptr = doc.out->file_pointer();

  out.write_vlong(doc_delta);
  out.write_vlong(doc_ptr - doc.skip_ptr[level]);

  doc.skip_doc[level] = doc.block_last;
  doc.skip_ptr[level] = doc_ptr;

  if (features_.position()) {
    assert(pos_);

    const uint64_t pos_ptr = pos_->out->file_pointer();

    out.write_vint(pos_->block_last);
    out.write_vlong(pos_ptr - pos_->skip_ptr[level]);

    pos_->skip_ptr[level] = pos_ptr;

    if (features_.payload() || features_.offset()) {
      assert(pay_);

      if (features_.payload()) {
        out.write_vint(static_cast<uint32_t>(pay_->block_last));
      }

      const uint64_t pay_ptr = pay_->out->file_pointer();

      out.write_vlong(pay_ptr - pay_->skip_ptr[level]);
      pay_->skip_ptr[level] = pay_ptr;
    }
  }
}

void postings_writer::encode(
    data_output& out,
    const irs::term_meta& state) {
#ifdef IRESEARCH_DEBUG
  const auto& meta = dynamic_cast<const version10::term_meta&>(state);
#else
  const auto& meta = static_cast<const version10::term_meta&>(state);
#endif // IRESEARCH_DEBUG

  out.write_vlong(meta.docs_count);
  if (meta.freq != integer_traits<uint64_t>::const_max) {
    assert(meta.freq >= meta.docs_count);
    out.write_vlong(meta.freq - meta.docs_count);
  }

  out.write_vlong(meta.doc_start - last_state.doc_start);
  if (features_.position()) {
    out.write_vlong(meta.pos_start - last_state.pos_start);
    if (type_limits<type_t::address_t>::valid(meta.pos_end)) {
      out.write_vlong(meta.pos_end);
    }
    if (features_.payload() || features_.offset()) {
      out.write_vlong(meta.pay_start - last_state.pay_start);
    }
  }

  if (1U == meta.docs_count || meta.docs_count > postings_writer::BLOCK_SIZE) {
    out.write_vlong(meta.e_skip_start);
  }

  last_state = meta;
}

void postings_writer::end() {
  format_utils::write_footer(*doc.out);
  doc.out.reset(); // ensure stream is closed

  if (pos_) {
    format_utils::write_footer(*pos_->out);
    pos_->out.reset(); // ensure stream is closed
  }

  if (pay_) {
    format_utils::write_footer(*pay_->out);
    pay_->out.reset(); // ensure stream is closed
  }
}

struct skip_state {
  uint64_t doc_ptr{}; // pointer to the beginning of document block
  uint64_t pos_ptr{}; // pointer to the positions of the first document in a document block
  uint64_t pay_ptr{}; // pointer to the payloads of the first document in a document block
  size_t pend_pos{}; // positions to skip before new document block
  doc_id_t doc{ type_limits<type_t::doc_id_t>::invalid() }; // last document in a previous block
  uint32_t pay_pos{}; // payload size to skip before in new document block
}; // skip_state

struct skip_context : skip_state {
  size_t level{}; // skip level
}; // skip_context

struct doc_state {
  const index_input* pos_in;
  const index_input* pay_in;
  version10::term_meta* term_state;
  uint64_t* freq;
  uint64_t* enc_buf;
  uint64_t tail_start;
  size_t tail_length;
  ::features features;
}; // doc_state

// ----------------------------------------------------------------------------
// --SECTION--                                                 helper functions
// ----------------------------------------------------------------------------

FORCE_INLINE void skip_positions(index_input& in) {
  encode::bitpack::skip_block32(in, postings_writer::BLOCK_SIZE);
}

FORCE_INLINE void skip_payload(index_input& in) {
  const size_t size = in.read_vint();
  if (size) {
    encode::bitpack::skip_block32(in, postings_writer::BLOCK_SIZE);
    in.seek(in.file_pointer() + size);
  }
}

FORCE_INLINE void skip_offsets(index_input& in) {
  encode::bitpack::skip_block32(in, postings_writer::BLOCK_SIZE);
  encode::bitpack::skip_block32(in, postings_writer::BLOCK_SIZE);
}

///////////////////////////////////////////////////////////////////////////////
/// @class doc_iterator
///////////////////////////////////////////////////////////////////////////////
class doc_iterator : public iresearch::doc_iterator {
 public:
  DECLARE_UNIQUE_PTR(doc_iterator);

  DEFINE_FACTORY_INLINE(doc_iterator);

  doc_iterator() NOEXCEPT
    : skip_levels_(1),
      skip_(postings_writer::BLOCK_SIZE, postings_writer::SKIP_N) {
    std::fill(docs_, docs_ + postings_writer::BLOCK_SIZE, type_limits<type_t::doc_id_t>::invalid());
  }

  void prepare(
      const features& field,
      const features& enabled,
      const irs::attribute_view& attrs,
      const index_input* doc_in,
      const index_input* pos_in,
      const index_input* pay_in) {
    features_ = field; // set field features
    enabled_ = enabled; // set enabled features

    // add mandatory attributes
    attrs_.emplace(doc_);
    begin_ = end_ = docs_;

    // get state attribute
    assert(attrs.contains<version10::term_meta>());
    term_state_ = *attrs.get<version10::term_meta>();

    // init document stream
    if (term_state_.docs_count > 1) {
      if (!doc_in_) {
        doc_in_ = doc_in->reopen();

        if (!doc_in_) {
          IR_FRMT_FATAL("Failed to reopen document input in: %s", __FUNCTION__);

          throw detailed_io_error("Failed to reopen document input");
        }
      }

      doc_in_->seek(term_state_.doc_start);
      assert(!doc_in_->eof());
    }

    prepare_attributes(enabled, attrs, pos_in, pay_in);
  }

  virtual doc_id_t seek(doc_id_t target) override {
    if (target <= doc_.value) {
      return doc_.value;
    }

    seek_to_block(target);
    iresearch::seek(*this, target);
    return value();
  }

  virtual doc_id_t value() const override {
    return doc_.value;
  }

  virtual const irs::attribute_view& attributes() const NOEXCEPT override {
    return attrs_;
  }

#if defined(_MSC_VER)
  #pragma warning( disable : 4706 )
#elif defined (__GNUC__)
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wparentheses"
#endif

  virtual bool next() override {
    if (begin_ == end_) {
      cur_pos_ += relative_pos();

      if (cur_pos_ == term_state_.docs_count) {
        doc_.value = type_limits<type_t::doc_id_t>::eof();
        begin_ = end_ = docs_; // seal the iterator
        return false;
      }

      refill();
    }

    doc_.value = *begin_++;
    freq_.value = *doc_freq_++;

    return true;
  }

#if defined(_MSC_VER)
  #pragma warning( default : 4706 )
#elif defined (__GNUC__)
  #pragma GCC diagnostic pop
#endif

 protected:
  virtual void prepare_attributes(
      const features& enabled,
      const irs::attribute_view& attrs,
      const index_input* pos_in,
      const index_input* pay_in) {
    UNUSED(pos_in);
    UNUSED(pay_in);

    // term frequency attributes
    if (enabled.freq()) {
      assert(attrs.contains<frequency>());
      attrs_.emplace(freq_);
      term_freq_ = attrs.get<frequency>()->value;
    }
  }

  virtual void seek_notify(const skip_context& /*ctx*/) {
  }

  void seek_to_block(doc_id_t target);

  // returns current position in the document block 'docs_'
  size_t relative_pos() NOEXCEPT {
    assert(begin_ >= docs_);
    return begin_ - docs_;
  }

  doc_id_t read_skip(skip_state& state, index_input& in) {
    state.doc = in.read_vlong();
    state.doc_ptr += in.read_vlong();

    if (features_.position()) {
      state.pend_pos = in.read_vint();
      state.pos_ptr += in.read_vlong();

      const bool has_pay = features_.payload();

      if (has_pay || features_.offset()) {
        if (has_pay) {
          state.pay_pos = in.read_vint();
        }

        state.pay_ptr += in.read_vlong();
      }
    }

    return state.doc;
  }

  void read_end_block(uint64_t size) {
    if (features_.freq()) {
      for (uint64_t i = 0; i < size; ++i) {
        if (shift_unpack_32(doc_in_->read_vint(), docs_[i])) {
          doc_freqs_[i] = 1;
        } else {
          doc_freqs_[i] = doc_in_->read_vint();
        }
      }
    } else {
      for (uint64_t i = 0; i < size; ++i) {
        docs_[i] = doc_in_->read_vint();
      }
    }
  }

  void refill() {
    const auto left = term_state_.docs_count - cur_pos_;

    if (left >= postings_writer::BLOCK_SIZE) {
      // read doc deltas
      encode::bitpack::read_block32_optimized(
        *doc_in_,
         postings_writer::BLOCK_SIZE,
         reinterpret_cast<uint32_t*>(enc_buf_),
         docs_
      );

      if (features_.freq()) {
        // read frequency it is required by
        // the iterator or just skip it otherwise
        if (enabled_.freq()) {
          encode::bitpack::read_block32_optimized(
            *doc_in_,
            postings_writer::BLOCK_SIZE,
            reinterpret_cast<uint32_t*>(enc_buf_),
            doc_freqs_
          );
        } else {
          encode::bitpack::skip_block32(
            *doc_in_,
            postings_writer::BLOCK_SIZE
          );
        }
      }
      end_ = docs_ + postings_writer::BLOCK_SIZE;
    } else if (1 == term_state_.docs_count) {
      docs_[0] = term_state_.e_single_doc;
      if (term_freq_) {
        doc_freqs_[0] = term_freq_;
      }
      end_ = docs_ + 1;
    } else {
      read_end_block(left);
      end_ = docs_ + left;
    }

    // if this is the initial doc_id then set it to min() for proper delta value
    // add last doc_id before decoding
    *docs_ += type_limits<type_t::doc_id_t>::valid(doc_.value)
      ? doc_.value
      : (type_limits<type_t::doc_id_t>::min)();

    // decode delta encoded documents block
    encode::delta::decode(std::begin(docs_), end_);

    begin_ = docs_;
    doc_freq_ = docs_ + postings_writer::BLOCK_SIZE;
  }

  std::vector<skip_state> skip_levels_;
  skip_reader skip_;
  skip_context* skip_ctx_; // pointer to used skip context, will be used by skip reader
  irs::attribute_view attrs_;
  uint64_t enc_buf_[postings_writer::BLOCK_SIZE]; // buffer for encoding
  uint32_t docs_[postings_writer::BLOCK_SIZE]; // doc values
  uint32_t doc_freqs_[postings_writer::BLOCK_SIZE]; // document frequencies
  uint64_t cur_pos_{};
  const uint32_t* begin_{docs_};
  uint32_t* end_{docs_};
  uint32_t* doc_freq_{}; // pointer into docs_ to the frequency attribute value for the current doc
  uint64_t term_freq_{}; // total term frequency
  document doc_;
  frequency freq_;
  index_input::ptr doc_in_;
  version10::term_meta term_state_;
  features features_; // field features
  features enabled_; // enabled iterator features
}; // doc_iterator

void doc_iterator::seek_to_block(doc_id_t target) {
  // check whether it make sense to use skip-list
  if (skip_levels_.front().doc < target && term_state_.docs_count > postings_writer::BLOCK_SIZE) {
    skip_context last; // where block starts
    skip_ctx_ = &last;

    // init skip writer in lazy fashion
    if (!skip_) {
      index_input::ptr skip_in = doc_in_->dup();
      skip_in->seek(term_state_.doc_start + term_state_.e_skip_start);

      skip_.prepare(
        std::move(skip_in),
        [this](size_t level, index_input& in) {
          skip_state& last = *skip_ctx_;
          auto& last_level = skip_ctx_->level;
          auto& next = skip_levels_[level];

          if (last_level > level) {
            // move to the more granular level
            next = last;
          } else {
            // store previous step on the same level
            last = next;
          }

          last_level = level;

          if (in.eof()) {
            // stream exhausted
            return (next.doc = type_limits<type_t::doc_id_t>::eof());
          }

          return read_skip(next, in);
      });

      // initialize skip levels
      const auto num_levels = skip_.num_levels();
      if (num_levels) {
        skip_levels_.resize(num_levels);

        // since we store pointer deltas, add postings offset
        auto& top = skip_levels_.back();
        top.doc_ptr = term_state_.doc_start;
        top.pos_ptr = term_state_.pos_start;
        top.pay_ptr = term_state_.pay_start;
      }
    }

    const size_t skipped = skip_.seek(target);
    if (skipped > (cur_pos_ + relative_pos())) {
      doc_in_->seek(last.doc_ptr);
      doc_.value = last.doc;
      cur_pos_ = skipped;
      begin_ = end_ = docs_; // will trigger refill in "next"
      seek_notify(last); // notifies derivatives
    }
  }
}

///////////////////////////////////////////////////////////////////////////////
/// @class mask_doc_iterator
///////////////////////////////////////////////////////////////////////////////
template<typename DocIterator>
class mask_doc_iterator final: public DocIterator {
 public:
  typedef DocIterator doc_iterator_t;

  static_assert(
    std::is_base_of<irs::doc_iterator, doc_iterator_t>::value,
    "DocIterator must be derived from iresearch::doc_iterator"
   );

  explicit mask_doc_iterator(const document_mask& mask)
    : mask_(mask) {
  }

  virtual bool next() override {
    while (doc_iterator_t::next()) {
      if (mask_.find(this->value()) == mask_.end()) {
        return true;
      }
    }

    return false;
  }

  virtual doc_id_t seek(doc_id_t target) override {
    const auto doc = doc_iterator_t::seek(target);

    if (mask_.find(doc) == mask_.end()) {
      return doc;
    }

    this->next();

    return this->value();
  }

 private:
  const document_mask& mask_; /* excluded document ids */
}; // mask_doc_iterator

///////////////////////////////////////////////////////////////////////////////
/// @class pos_iterator
///////////////////////////////////////////////////////////////////////////////
class pos_iterator: public position {
 public:
  DECLARE_UNIQUE_PTR(pos_iterator);

  pos_iterator(size_t reserve_attrs = 0): position(reserve_attrs) {}

  virtual void clear() override {
    value_ = irs::type_limits<irs::type_t::pos_t>::invalid();
  }

  virtual bool next() override {
    if (0 == pend_pos_) {
      value_ = irs::type_limits<irs::type_t::pos_t>::eof();

      return false;
    }

    const uint64_t freq = *freq_;

    if (pend_pos_ > freq) {
      skip(pend_pos_ - freq);
      pend_pos_ = freq;
    }

    if (buf_pos_ == postings_writer::BLOCK_SIZE) {
      refill();
      buf_pos_ = 0;
    }

    // FIXME TODO: make INVALID = 0, remove this
    if (!irs::type_limits<irs::type_t::pos_t>::valid(value_)) {
      value_ = 0;
    }

    value_ += pos_deltas_[buf_pos_];
    read_attributes();
    ++buf_pos_;
    --pend_pos_;
    return true;
  }

  // prepares iterator to work
  virtual void prepare(const doc_state& state) {
    pos_in_ = state.pos_in->reopen();

    if (!pos_in_) {
      IR_FRMT_FATAL("Failed to reopen positions input in: %s", __FUNCTION__);

      throw detailed_io_error("Failed to reopen positions input");
    }

    pos_in_->seek(state.term_state->pos_start);
    freq_ = state.freq;
    features_ = state.features;
    enc_buf_ = reinterpret_cast<uint32_t*>(state.enc_buf);
    tail_start_ = state.tail_start;
    tail_length_ = state.tail_length;
  }

  // notifies iterator that doc iterator has skipped to a new block
  virtual void prepare(const skip_state& state) {
    pos_in_->seek(state.pos_ptr);
    pend_pos_ = state.pend_pos;
    buf_pos_ = postings_writer::BLOCK_SIZE;
  }

  virtual uint32_t value() const override { return value_; }

 protected:
  virtual void read_attributes() { }

  virtual void refill() {
    if (pos_in_->file_pointer() == tail_start_) {
      uint32_t pay_size = 0;
      for (size_t i = 0; i < tail_length_; ++i) {
        if (features_.payload()) {
          if (shift_unpack_32(pos_in_->read_vint(), pos_deltas_[i])) {
            pay_size = pos_in_->read_vint();
          }
          if (pay_size) {
            pos_in_->seek(pos_in_->file_pointer() + pay_size);
          }
        } else {
          pos_deltas_[i] = pos_in_->read_vint();
        }

        if (features_.offset()) {
          uint32_t delta;
          if (shift_unpack_32(pos_in_->read_vint(), delta)) {
            pos_in_->read_vint();
          }
        }
      }
    } else {
      encode::bitpack::read_block32_optimized(
        *pos_in_, postings_writer::BLOCK_SIZE, enc_buf_, pos_deltas_
      );
    }
  }

  virtual void skip(uint64_t count) {
    uint64_t left = postings_writer::BLOCK_SIZE - buf_pos_;
    if (count >= left) {
      count -= left;
      while (count >= postings_writer::BLOCK_SIZE) {
        // skip positions
        skip_positions(*pos_in_);
        count -= postings_writer::BLOCK_SIZE;
      }
      refill();
      buf_pos_ = 0;
      left = postings_writer::BLOCK_SIZE;
    }

    if (count < left) {
      buf_pos_ += uint32_t(count);
    }
    clear();
    value_ = 0;
  }

  uint32_t pos_deltas_[postings_writer::BLOCK_SIZE]; /* buffer to store position deltas */
  const uint64_t* freq_; /* lenght of the posting list for a document */
  uint32_t* enc_buf_; /* auxillary buffer to decode data */
  uint64_t pend_pos_{}; /* how many positions "behind" we are */
  uint64_t tail_start_; /* file pointer where the last (vInt encoded) pos delta block is */
  size_t tail_length_; /* number of positions in the last (vInt encoded) pos delta block */
  uint32_t value_{ irs::type_limits<irs::type_t::pos_t>::invalid() }; // current position
  uint32_t buf_pos_{ postings_writer::BLOCK_SIZE } ; /* current position in pos_deltas_ buffer */
  index_input::ptr pos_in_;
  features features_; /* field features */

 private:
  template<typename T>
  friend class pos_doc_iterator;
}; // pos_iterator

///////////////////////////////////////////////////////////////////////////////
/// @class offs_pay_iterator
///////////////////////////////////////////////////////////////////////////////
class offs_pay_iterator final: public pos_iterator {
 public:
  DECLARE_UNIQUE_PTR(offs_pay_iterator);

  offs_pay_iterator()
    : pos_iterator(2) { // offset + payload
    attrs_.emplace(offs_);
    attrs_.emplace(pay_);
  }

  virtual void clear() override {
    pos_iterator::clear();
    offs_.clear();
    pay_.clear();
  }

  virtual void prepare(const doc_state& state) override {
    pos_iterator::prepare(state);
    pay_in_ = state.pay_in->reopen();

    if (!pay_in_) {
      IR_FRMT_FATAL("Failed to reopen payload input in: %s", __FUNCTION__);

      throw detailed_io_error("Failed to reopen payload input");
    }

    pay_in_->seek(state.term_state->pay_start);
  }

  virtual void prepare(const skip_state& state) override {
    pos_iterator::prepare(state);
    pay_in_->seek(state.pay_ptr);
    pay_data_pos_ = state.pay_pos;
  }

 protected:
  virtual void read_attributes() override {
    offs_.start += offs_start_deltas_[buf_pos_];
    offs_.end = offs_.start + offs_lengts_[buf_pos_];

    pay_.value = bytes_ref(
      pay_data_.c_str() + pay_data_pos_,
      pay_lengths_[buf_pos_]);
    pay_data_pos_ += pay_lengths_[buf_pos_];
  }

  virtual void skip(uint64_t count) override {
    uint64_t left = postings_writer::BLOCK_SIZE - buf_pos_;
    if (count >= left) {
      count -= left;
      // skip block by block
      while (count >= postings_writer::BLOCK_SIZE) {
        skip_positions(*pos_in_);
        skip_payload(*pay_in_);
        skip_offsets(*pay_in_);
        count -= postings_writer::BLOCK_SIZE;
      }
      refill();
      buf_pos_ = 0;
      left = postings_writer::BLOCK_SIZE;
    }

    if (count < left) {
      // current payload start
      const auto begin = pay_lengths_ + buf_pos_;
      const auto end = begin + count;
      pay_data_pos_ = std::accumulate(begin, end, pay_data_pos_);
      buf_pos_ += uint32_t(count);
    }
    clear();
    value_ = 0;
  }

  virtual void refill() override {
    if (pos_in_->file_pointer() == tail_start_) {
      size_t pos = 0;

      for (size_t i = 0; i < tail_length_; ++i) {
        // read payloads
        if (shift_unpack_32(pos_in_->read_vint(), pos_deltas_[i])) {
          pay_lengths_[i] = pos_in_->read_vint();
        } else {
          assert(i);
          pay_lengths_[i] = pay_lengths_[i-1];
        }

        if (pay_lengths_[i]) {
          const auto size = pay_lengths_[i]; // length of current payload

          oversize(pay_data_, pos + size);

          #ifdef IRESEARCH_DEBUG
            const auto read = pos_in_->read_bytes(&(pay_data_[0]) + pos, size);
            assert(read == size);
          #else
            pos_in_->read_bytes(&(pay_data_[0]) + pos, size);
          #endif // IRESEARCH_DEBUG

          pos += size;
        }

        if (shift_unpack_32(pos_in_->read_vint(), offs_start_deltas_[i])) {
          offs_lengts_[i] = pos_in_->read_vint();
        } else {
          assert(i);
          offs_lengts_[i] = offs_lengts_[i - 1];
        }
      }
    } else {
      encode::bitpack::read_block32_optimized(*pos_in_, postings_writer::BLOCK_SIZE, enc_buf_, pos_deltas_);

      // read payloads
      const uint32_t size = pay_in_->read_vint();
      if (size) {
        encode::bitpack::read_block32_optimized(*pay_in_, postings_writer::BLOCK_SIZE, enc_buf_, pay_lengths_);
        oversize(pay_data_, size);

        #ifdef IRESEARCH_DEBUG
          const auto read = pay_in_->read_bytes(&(pay_data_[0]), size);
          assert(read == size);
        #else
          pay_in_->read_bytes(&(pay_data_[0]), size);
        #endif // IRESEARCH_DEBUG
      }

      // read offsets
      encode::bitpack::read_block32_optimized(*pay_in_, postings_writer::BLOCK_SIZE, enc_buf_, offs_start_deltas_);
      encode::bitpack::read_block32_optimized(*pay_in_, postings_writer::BLOCK_SIZE, enc_buf_, offs_lengts_);
    }
    pay_data_pos_ = 0;
  }

  index_input::ptr pay_in_;
  offset offs_;
  payload pay_;
  uint32_t offs_start_deltas_[postings_writer::BLOCK_SIZE]{}; /* buffer to store offset starts */
  uint32_t offs_lengts_[postings_writer::BLOCK_SIZE]{}; /* buffer to store offset lengths */
  uint32_t pay_lengths_[postings_writer::BLOCK_SIZE]{}; /* buffer to store payload lengths */
  size_t pay_data_pos_{}; /* current position in a payload buffer */
  bstring pay_data_; // buffer to store payload data
}; // pay_offs_iterator

///////////////////////////////////////////////////////////////////////////////
/// @class offs_iterator
///////////////////////////////////////////////////////////////////////////////
class offs_iterator final : public pos_iterator {
 public:
  DECLARE_UNIQUE_PTR(offs_iterator);

  offs_iterator()
    : pos_iterator(1) { // offset
    attrs_.emplace(offs_);
  }

  virtual void clear() override {
    pos_iterator::clear();
    offs_.clear();
  }

  virtual void prepare(const doc_state& state) override {
    pos_iterator::prepare(state);
    pay_in_ = state.pay_in->reopen();

    if (!pay_in_) {
      IR_FRMT_FATAL("Failed to reopen payload input in: %s", __FUNCTION__);

      throw detailed_io_error("Failed to reopen payload input");
    }

    pay_in_->seek(state.term_state->pay_start);
  }

  virtual void prepare(const skip_state& state) override {
    pos_iterator::prepare(state);
    pay_in_->seek(state.pay_ptr);
  }

 protected:
  virtual void read_attributes() override {
    offs_.start += offs_start_deltas_[buf_pos_];
    offs_.end = offs_.start + offs_lengts_[buf_pos_];
  }

  virtual void refill() override {
    if (pos_in_->file_pointer() == tail_start_) {
      uint32_t pay_size = 0;
      for (uint64_t i = 0; i < tail_length_; ++i) {
        /* skip payloads */
        if (features_.payload()) {
          if (shift_unpack_32(pos_in_->read_vint(), pos_deltas_[i])) {
            pay_size = pos_in_->read_vint();
          }
          if (pay_size) {
            pos_in_->seek(pos_in_->file_pointer() + pay_size);
          }
        } else {
          pos_deltas_[i] = pos_in_->read_vint();
        }

        /* read offsets */
        if (shift_unpack_32(pos_in_->read_vint(), offs_start_deltas_[i])) {
          offs_lengts_[i] = pos_in_->read_vint();
        } else {
          assert(i);
          offs_lengts_[i] = offs_lengts_[i - 1];
        }
      }
    } else {
      encode::bitpack::read_block32_optimized(*pos_in_, postings_writer::BLOCK_SIZE, enc_buf_, pos_deltas_);

      // skip payload
      if (features_.payload()) {
        skip_payload(*pay_in_);
      }

      // read offsets
      encode::bitpack::read_block32_optimized(*pay_in_, postings_writer::BLOCK_SIZE, enc_buf_, offs_start_deltas_);
      encode::bitpack::read_block32_optimized(*pay_in_, postings_writer::BLOCK_SIZE, enc_buf_, offs_lengts_);
    }
  }

  virtual void skip(uint64_t count) override {
    uint64_t left = postings_writer::BLOCK_SIZE - buf_pos_;
    if (count >= left) {
      count -= left;
      // skip block by block
      while (count >= postings_writer::BLOCK_SIZE) {
        skip_positions(*pos_in_);
        if (features_.payload()) {
          skip_payload(*pay_in_);
        }
        skip_offsets(*pay_in_);
        count -= postings_writer::BLOCK_SIZE;
      }
      refill();
      buf_pos_ = 0;
      left = postings_writer::BLOCK_SIZE;
    }

    if (count < left) {
      buf_pos_ += uint32_t(count);
    }
    clear();
    value_ = 0;
  }

  index_input::ptr pay_in_;
  offset offs_;
  uint32_t offs_start_deltas_[postings_writer::BLOCK_SIZE]; /* buffer to store offset starts */
  uint32_t offs_lengts_[postings_writer::BLOCK_SIZE]; /* buffer to store offset lengths */
}; // offs_iterator

///////////////////////////////////////////////////////////////////////////////
/// @class pay_iterator
///////////////////////////////////////////////////////////////////////////////
class pay_iterator final : public pos_iterator {
 public:
  DECLARE_UNIQUE_PTR(pay_iterator);

  pay_iterator()
    : pos_iterator(1) { // payload
    attrs_.emplace(pay_);
  }

  virtual void clear() override {
    pos_iterator::clear();
    pay_.clear();
  }

  virtual void prepare(const doc_state& state) override {
    pos_iterator::prepare(state);
    pay_in_ = state.pay_in->reopen();

    if (!pay_in_) {
      IR_FRMT_FATAL("Failed to reopen payload input in: %s", __FUNCTION__);

      throw detailed_io_error("Failed to reopen payload input");
    }

    pay_in_->seek(state.term_state->pay_start);
  }

  virtual void prepare(const skip_state& state) override {
    pos_iterator::prepare(state);
    pay_in_->seek(state.pay_ptr);
    pay_data_pos_ = state.pay_pos;
  }

 protected:
  virtual void read_attributes() override {
    pay_.value = bytes_ref(
      pay_data_.data() + pay_data_pos_,
      pay_lengths_[buf_pos_]
    );
    pay_data_pos_ += pay_lengths_[buf_pos_];
  }

  virtual void skip(uint64_t count) override {
    uint64_t left = postings_writer::BLOCK_SIZE - buf_pos_;
    if (count >= left) {
      count -= left;
      // skip block by block
      while (count >= postings_writer::BLOCK_SIZE) {
        skip_positions(*pos_in_);
        skip_payload(*pay_in_);
        if (features_.offset()) {
          skip_offsets(*pay_in_);
        }
        count -= postings_writer::BLOCK_SIZE;
      }
      refill();
      buf_pos_ = 0;
      left = postings_writer::BLOCK_SIZE;
    }

    if (count < left) {
      // current payload start
      const auto begin = pay_lengths_ + buf_pos_;
      const auto end = begin + count;
      pay_data_pos_ = std::accumulate(begin, end, pay_data_pos_);
      buf_pos_ += uint32_t(count);
    }
    clear();
    value_ = 0;
  }

  virtual void refill() override {
    if (pos_in_->file_pointer() == tail_start_) {
      size_t pos = 0;

      for (uint64_t i = 0; i < tail_length_; ++i) {
        // read payloads
        if (shift_unpack_32(pos_in_->read_vint(), pos_deltas_[i])) {
          pay_lengths_[i] = pos_in_->read_vint();
        } else {
          assert(i);
          pay_lengths_[i] = pay_lengths_[i-1];
        }

        if (pay_lengths_[i]) {
          const auto size = pay_lengths_[i]; // current payload length

          oversize(pay_data_, pos + size);

          #ifdef IRESEARCH_DEBUG
            const auto read = pos_in_->read_bytes(&(pay_data_[0]) + pos, size);
            assert(read == size);
          #else
            pos_in_->read_bytes(&(pay_data_[0]) + pos, size);
          #endif // IRESEARCH_DEBUG

          pos += size;
        }

        // skip offsets
        if (features_.offset()) {
          uint32_t code;
          if (shift_unpack_32(pos_in_->read_vint(), code)) {
            pos_in_->read_vint();
          }
        }
      }
    } else {
      encode::bitpack::read_block32_optimized(*pos_in_, postings_writer::BLOCK_SIZE, enc_buf_, pos_deltas_);

      /* read payloads */
      const uint32_t size = pay_in_->read_vint();
      if (size) {
        encode::bitpack::read_block32_optimized(*pay_in_, postings_writer::BLOCK_SIZE, enc_buf_, pay_lengths_);
        oversize(pay_data_, size);

        #ifdef IRESEARCH_DEBUG
          const auto read = pay_in_->read_bytes(&(pay_data_[0]), size);
          assert(read == size);
        #else
          pay_in_->read_bytes(&(pay_data_[0]), size);
        #endif // IRESEARCH_DEBUG
      }

      // skip offsets
      if (features_.offset()) {
        skip_offsets(*pay_in_);
      }
    }
    pay_data_pos_ = 0;
  }

  index_input::ptr pay_in_;
  payload pay_;
  uint32_t pay_lengths_[postings_writer::BLOCK_SIZE]{}; /* buffer to store payload lengths */
  uint64_t pay_data_pos_{}; /* current postition in payload buffer */
  bstring pay_data_; // buffer to store payload data
}; // pay_iterator

///////////////////////////////////////////////////////////////////////////////
/// @class pos_doc_iterator
///////////////////////////////////////////////////////////////////////////////
template<typename PosItrType>
class pos_doc_iterator final: public doc_iterator {
 public:
  virtual bool next() override {
    if (begin_ == end_) {
      cur_pos_ += relative_pos();

      if (cur_pos_ == term_state_.docs_count) {
        doc_.value = type_limits<type_t::doc_id_t>::eof();
        begin_ = end_ = docs_; // seal the iterator
        return false;
      }

      refill();
    }

    // update document attribute
    doc_.value = *begin_++;

    // update frequency attribute
    freq_.value = *doc_freq_++;

    // update position attribute
    pos_.pend_pos_ += freq_.value;
    pos_.clear();

    return true;
  }

 protected:
  virtual void prepare_attributes(
    const ::features& features,
    const irs::attribute_view& attrs,
    const index_input* pos_in,
    const index_input* pay_in
  ) final;

  virtual void seek_notify(const skip_context &ctx) final {
    pos_.prepare(ctx); // notify positions
  }

 private:
  PosItrType pos_;
}; // pos_doc_iterator

template<typename PosItrType>
void pos_doc_iterator<PosItrType>::prepare_attributes(
    const ::features& enabled,
    const irs::attribute_view& attrs,
    const index_input* pos_in,
    const index_input* pay_in) {
  assert(attrs.contains<frequency>());
  assert(enabled.position());
  attrs_.emplace(freq_);
  term_freq_ = attrs.get<frequency>()->value;

  // ...........................................................................
  // position attribute
  // ...........................................................................

  doc_state state;
  state.pos_in = pos_in;
  state.pay_in = pay_in;
  state.term_state = &term_state_;
  state.freq = &freq_.value;
  state.features = features_;
  state.enc_buf = enc_buf_;

  if (term_freq_ < postings_writer::BLOCK_SIZE) {
    state.tail_start = term_state_.pos_start;
  } else if (term_freq_ == postings_writer::BLOCK_SIZE) {
    state.tail_start = type_limits<type_t::address_t>::invalid();
  } else {
    state.tail_start = term_state_.pos_start + term_state_.pos_end;
  }

  state.tail_length = term_freq_ % postings_writer::BLOCK_SIZE;
  pos_.prepare(state);
  attrs_.emplace(pos_);
}

// ----------------------------------------------------------------------------
// --SECTION--                                                  postings_reader
// ----------------------------------------------------------------------------

class postings_reader final: public irs::postings_reader {
 public:
  virtual bool prepare(
    index_input& in,
    const reader_state& state,
    const flags& features
  ) override;

  virtual void decode(
    data_input& in,
    const flags& field,
    const attribute_view& attrs,
    irs::term_meta& state
  ) override;

  virtual irs::doc_iterator::ptr iterator(
    const flags& field,
    const attribute_view& attrs,
    const flags& features
  ) override;

 private:
  index_input::ptr doc_in_;
  index_input::ptr pos_in_;
  index_input::ptr pay_in_;
}; // postings_reader

bool postings_reader::prepare(
    index_input& in,
    const reader_state& state,
    const flags& features) {
  std::string buf;

  // prepare document input
  prepare_input(
    buf, doc_in_, irs::IOAdvice::RANDOM, state,
    postings_writer::DOC_EXT,
    postings_writer::DOC_FORMAT_NAME,
    postings_writer::FORMAT_MIN,
    postings_writer::FORMAT_MAX
  );

  // Since terms doc postings too large
  //  it is too costly to verify checksum of
  //  the entire file. Here we perform cheap
  //  error detection which could recognize
  //  some forms of corruption.
  format_utils::read_checksum(*doc_in_);

  if (features.check<position>()) {
    /* prepare positions input */
    prepare_input(
      buf, pos_in_, irs::IOAdvice::RANDOM, state,
      postings_writer::POS_EXT,
      postings_writer::POS_FORMAT_NAME,
      postings_writer::FORMAT_MIN,
      postings_writer::FORMAT_MAX
    );

    // Since terms pos postings too large
    // it is too costly to verify checksum of
    // the entire file. Here we perform cheap
    // error detection which could recognize
    // some forms of corruption.
    format_utils::read_checksum(*pos_in_);

    if (features.check<payload>() || features.check<offset>()) {
      // prepare positions input
      prepare_input(
        buf, pay_in_, irs::IOAdvice::RANDOM, state,
        postings_writer::PAY_EXT,
        postings_writer::PAY_FORMAT_NAME,
        postings_writer::FORMAT_MIN,
        postings_writer::FORMAT_MAX
      );

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
    postings_writer::TERMS_FORMAT_NAME,
    postings_writer::TERMS_FORMAT_MIN,
    postings_writer::TERMS_FORMAT_MAX
  );

  const uint64_t block_size = in.read_vlong();
  if (block_size != postings_writer::BLOCK_SIZE) {
    // invalid block size
    throw index_error();
  }

  return true;
}

void postings_reader::decode(
    data_input& in,
    const flags& meta,
    const attribute_view& attrs,
    irs::term_meta& state
) {
#ifdef IRESEARCH_DEBUG
  auto& term_meta = dynamic_cast<version10::term_meta&>(state);
#else
  auto& term_meta = static_cast<version10::term_meta&>(state);
#endif // IRESEARCH_DEBUG

  auto& term_freq = attrs.get<frequency>();

  term_meta.docs_count = in.read_vlong();
  if (term_freq) {
    term_freq->value = term_meta.docs_count + in.read_vlong();
  }

  term_meta.doc_start += in.read_vlong();
  if (term_freq && term_freq->value && meta.check<position>()) {
    term_meta.pos_start += in.read_vlong();

    term_meta.pos_end = term_freq->value > postings_writer::BLOCK_SIZE
        ? in.read_vlong()
        : type_limits<type_t::address_t>::invalid();

    if (meta.check<payload>() || meta.check<offset>()) {
      term_meta.pay_start += in.read_vlong();
    }
  }

  if (1U == term_meta.docs_count || term_meta.docs_count > postings_writer::BLOCK_SIZE) {
    term_meta.e_skip_start = in.read_vlong();
  }
}

irs::doc_iterator::ptr postings_reader::iterator(
    const flags& field,
    const attribute_view& attrs,
    const flags& req) {
  // compile field features
  const auto features = ::features(field);
  // get enabled features:
  // find intersection between requested and available features
  const auto enabled = features & req;
  doc_iterator::ptr it;

  switch(enabled) {
   case features::POS_OFFS_PAY:
    it = doc_iterator::make<pos_doc_iterator<offs_pay_iterator>>();
    break;
   case features::POS_OFFS:
    it = doc_iterator::make<pos_doc_iterator<offs_iterator>>();
    break;
   case features::POS_PAY:
    it = doc_iterator::make<pos_doc_iterator<pay_iterator>>();
    break;
   case features::POS:
    it = doc_iterator::make<pos_doc_iterator<pos_iterator>>();
    break;
   default:
    it = doc_iterator::make<doc_iterator>();
  }

  it->prepare(
    features, enabled, attrs,
    doc_in_.get(), pos_in_.get(), pay_in_.get()
  );

  return IMPLICIT_MOVE_WORKAROUND(it);
}

NS_END

NS_ROOT
NS_BEGIN(version10)

// -----------------------------------------------------------------------------
// --SECTION--                                                  format_optimized
// -----------------------------------------------------------------------------

format_optimized::format_optimized() NOEXCEPT
  : format(format_optimized::type()) {
}

irs::postings_writer::ptr format_optimized::get_postings_writer(bool volatile_state) const {
  return irs::postings_writer::make<::postings_writer>(volatile_state);
}

irs::postings_reader::ptr format_optimized::get_postings_reader() const {
  return irs::postings_reader::make<::postings_reader>();
}

/*static*/ irs::format::ptr format_optimized::make() {
  static format_optimized INSTANCE;

  // aliasing constructor
  return irs::format::ptr(irs::format::ptr(), &INSTANCE);
}

DEFINE_FORMAT_TYPE_NAMED(iresearch::version10::format_optimized, "1_0-optimized");
REGISTER_FORMAT(iresearch::version10::format_optimized);

NS_END // version10
NS_END // root

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------

