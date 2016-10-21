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
#include "field_data.hpp"
#include "field_meta.hpp"

#include "document/field.hpp"

#include "formats/formats.hpp"

#include "store/directory.hpp"
#include "store/store_utils.hpp"

#include "analysis/analyzer.hpp"
#include "analysis/token_attributes.hpp"
#include "analysis/token_streams.hpp"

#include "utils/bit_utils.hpp"
#include "utils/io_utils.hpp"
#include "utils/log.hpp"
#include "utils/memory.hpp"
#include "utils/object_pool.hpp"
#include "utils/timer_utils.hpp"
#include "utils/type_limits.hpp"
#include "utils/unicode_utils.hpp"

#include <set>
#include <algorithm>
#include <cassert>

NS_ROOT

NS_BEGIN( detail )

using iresearch::field_data;
using iresearch::bytes_ref;

/* -------------------------------------------------------------------
 * doc_iterator
 * ------------------------------------------------------------------*/

class payload : public iresearch::payload {
 public:
  DECLARE_FACTORY_DEFAULT();

  inline byte_type* data() {
    return &(value_[0]);
  }

  inline void resize( size_t size ) {
    value_.resize(size);
    value = value_;
  }

 private:
   bstring value_;
};
DEFINE_FACTORY_DEFAULT(payload);

class pos_iterator : public iresearch::position::impl {
 public:
  pos_iterator(
      const field_data& field, const frequency& freq,
      const byte_block_pool::sliced_reader& prox)
    : iresearch::position::impl(2), // offset + payload
      prox_in_(prox),
      freq_(freq),
      pos_{},
      pay_{},
      offs_{},
      field_(field),
      val_{} {
    auto& attrs = this->attributes();
    auto& features = field_.meta().features;
    if (features.check< offset >()) {
      offs_ = attrs.add< offset >();
    }
    if (features.check< payload >()) {
      pay_ = attrs.add< payload >();
    }
  }

  virtual void clear() override {
    pos_ = 0;
    val_ = 0;
    attributes().clear_state();
  }

  virtual uint32_t value() const override {
    return val_;
  }

  virtual bool next() {
    if ( pos_ == freq_.value ) {
      val_ = position::INVALID;
      return false;
    }

    uint32_t pos;
    if ( shift_unpack_32( bytes_io< uint32_t >::vread( prox_in_ ), pos ) ) {
      assert(pay_);
      const size_t size = bytes_io<size_t>::vread( prox_in_ );
      pay_->resize( size );
      prox_in_.read( pay_->data(), size );
    }
    val_ += pos;

    if ( offs_ ) {
      offs_->start += bytes_io< uint32_t >::vread( prox_in_ );
      offs_->end = offs_->start + bytes_io< uint32_t >::vread( prox_in_ );
    }
    ++pos_;
    return true;
  }

 private:
  byte_block_pool::sliced_reader prox_in_;
  const frequency& freq_; /* number of terms position in a document */
  uint64_t pos_; /* current position */
  payload* pay_;
  offset* offs_;
  const field_data& field_;
  uint32_t val_;
};

/* -------------------------------------------------------------------
 * doc_iterator
 * ------------------------------------------------------------------*/

class doc_iterator : public iresearch::doc_iterator {
 public:

  template<typename... Args>
  static ptr make(Args&&... args) {
    return ptr(new doc_iterator(std::forward<Args>(args)...));
  }

  doc_iterator(
    const field_data& field, const posting& posting,
    const byte_block_pool::sliced_reader& freq,
    const byte_block_pool::sliced_reader& prox
  ):
    attrs_(3), // document + frequency + position
    freq_in_(freq) {
    doc_ = attrs_.add< document >();
    init(field, posting, freq, prox);
  }

  virtual const iresearch::attributes& attributes() const override {
    return attrs_;
  }

  void init(
    const field_data& field, const posting& posting,
    const byte_block_pool::sliced_reader& freq,
    const byte_block_pool::sliced_reader& prox
  ) {
    freq_in_ = freq;
    freq_ = nullptr;
    pos_ = nullptr;
    posting_ = &posting;
    field_ = &field;
    doc_->value = 0;

    auto& features = field_->meta().features;
    if (features.check<frequency>()) {
      freq_ = attrs_.add<frequency>();
      freq_->value = 0;

      if (features.check<position>()) {
        attrs_.add<position>()->prepare(pos_ = new pos_iterator(field, *freq_, prox));
      }
    }
  }

  virtual doc_id_t seek(doc_id_t doc) override {
    return iresearch::seek(*this, doc);
  }

  virtual doc_id_t value() const override {
    return doc_->value;
  }

  virtual bool next() override {
    if ( freq_in_.eof() ) {
      if (!type_limits<type_t::doc_id_t>::valid(posting_->doc_code)) {
        return false;
      }

      doc_->value = posting_->doc;

      if (field_->meta().features.check<frequency>()) {
        freq_->value = posting_->freq; 
      }

      const_cast<posting*>(posting_)->doc_code = type_limits<type_t::doc_id_t>::invalid();
    } else {
      if ( freq_ ) {
        doc_id_t delta;

        if (shift_unpack_64( bytes_io<uint64_t>::vread(freq_in_), delta)) {
          freq_->value = 1U;
        } else {
          freq_->value = bytes_io< uint32_t >::vread( freq_in_ );
        }

        doc_->value += delta;
      } else {
        doc_->value += bytes_io<uint64_t>::vread(freq_in_);
      }

      assert(doc_->value != posting_->doc);
    }

    if ( pos_ ) pos_->clear();

    return true;
  }

 private:
  iresearch::attributes attrs_;
  byte_block_pool::sliced_reader freq_in_;
  document* doc_;
  frequency* freq_;
  position::impl* pos_;
  const posting* posting_;
  const field_data* field_;
};

/* -------------------------------------------------------------------
 * term_iterator
 * ------------------------------------------------------------------*/

class term_iterator : public iresearch::term_iterator {
 public:
  template<typename Iterator>
  term_iterator(const field_data& field, Iterator begin, Iterator end):
    field_(field),
    itr_increment_(false),
    postings_pool_(4) { // pool size 4
    postings_.insert(begin, end);
    itr_ = postings_.begin();

  }

  virtual const bytes_ref& value() const {
    return term_;
  }

  virtual const iresearch::attributes& attributes() const {
    return attrs_;
  }

  virtual void read() {
    // Does nothing now
  }

  virtual iresearch::doc_iterator::ptr postings(const flags&) const override {
    REGISTER_TIMER_DETAILED();
    assert(itr_ != postings_.end());
    auto& posting = itr_->second;

    // where the term's data starts
    int_block_pool::const_iterator ptr = field_.int_writer_->parent().seek(posting.int_start);
    const auto freq_end = *ptr; ++ptr;
    const auto prox_end = *ptr; ++ptr;
    const auto freq_begin = *ptr; ++ptr;
    const auto prox_begin = *ptr;

    auto& pool = field_.byte_writer_->parent();

    // term's frequencies
    byte_block_pool::sliced_reader freq = byte_block_pool::sliced_reader(pool.seek(freq_begin), freq_end);

    // TODO: create on demand!!!
    // term's proximity
    byte_block_pool::sliced_reader prox = byte_block_pool::sliced_reader(pool.seek(prox_begin), prox_end);
    auto doc_itr = postings_pool_.emplace(field_, posting, freq, prox);

    static_cast<doc_iterator&>(*doc_itr).init(field_, posting, freq, prox);

    return doc_itr;
  }

  virtual bool next() override {   
    if (itr_increment_) {
      ++itr_;
    }

    if (itr_ == postings_.end()) {
      itr_increment_ = false;
      term_ = iresearch::bytes_ref::nil;

      return false;
    }

    itr_increment_ = true;
    term_ = itr_->first;

    return true;
  }

 private:
  struct utf8_less_t {
    bool operator()(const bytes_ref& lhs, const bytes_ref& rhs) const {
      return utf8_less(lhs.c_str(), lhs.size(), rhs.c_str(), rhs.size());
    }
  };
  typedef std::map<bytes_ref, const posting&, utf8_less_t> map_t;

  iresearch::attributes attrs_;
  const field_data& field_;
  map_t::iterator itr_;
  bool itr_increment_;
  map_t postings_;
  iresearch::bytes_ref term_;
  mutable unbounded_object_pool<doc_iterator> postings_pool_;
};

NS_END

/* -------------------------------------------------------------------
 * field_data
 * ------------------------------------------------------------------*/

void field_data::write_offset( posting& p, int_block_pool::iterator& where, const offset* offs ) {
  assert(offs);
  const uint32_t start_offset = offs_ + offs->start;
  const uint32_t end_offset = offs_ + offs->end;

  assert( start_offset >= p.offs );

  byte_block_pool::sliced_inserter out( byte_writer_, *where );

  bytes_io<uint32_t>::vwrite( out, start_offset - p.offs );
  bytes_io<uint32_t>::vwrite( out, end_offset - start_offset );

  *where = out.pool_offset();
  p.offs = start_offset;
}

void field_data::write_prox( posting& p, int_block_pool::iterator& where,
                             uint32_t prox, const payload* pay ) {
  byte_block_pool::sliced_inserter out( byte_writer_, *where );

  if ( !pay || pay->value.null() ) {
    bytes_io<uint32_t>::vwrite( out, shift_pack_32( prox, false ) );
  } else {
    bytes_io<uint32_t>::vwrite( out, shift_pack_32( prox, true ) );
    bytes_io<size_t>::vwrite( out, pay->value.size() );
    out.write( pay->value.c_str(), pay->value.size() );

    // saw payloads
    meta_.features.add<payload>();
  }

  *where = out.pool_offset();
  p.pos = pos_;
}

field_data::field_data( 
    const string_ref& name,
    size_t id,
    byte_block_pool::inserter* byte_writer,
    int_block_pool::inserter* int_writer )
  : meta_(name, field_id(id), flags::empty_instance()),
    terms_(*byte_writer),
    byte_writer_(byte_writer),
    int_writer_(int_writer),
    last_doc_(type_limits<type_t::doc_id_t>::invalid()) {
  assert(byte_writer_);
  assert(int_writer_);
}

field_data::~field_data() { }

void field_data::init(const doc_id_t& doc_id) {
  assert(type_limits<type_t::doc_id_t>::valid(doc_id));

  if (doc_id == last_doc_) {
    return; // nothing to do
  }

  pos_ = integer_traits< uint32_t >::const_max;
  last_pos_ = 0;
  len_ = 0;
  num_overlap_ = 0;
  offs_ = 0;
  last_start_offs_ = 0;
  max_term_freq_ = 0;
  unq_term_cnt_ = 0;
  boost_ = 1.f;
  last_doc_ = doc_id;
}

void field_data::new_term(
  posting& p, doc_id_t did, const payload* pay, const offset* offs 
) {
  // where pointers to data starts
  p.int_start = int_writer_->pool_offset();

  const auto freq_start = byte_writer_->alloc_slice(); // pointer to freq stream
  const auto prox_start = byte_writer_->alloc_slice(); // pointer to prox stream
  *int_writer_ = freq_start; // freq stream end
  *int_writer_ = prox_start; // prox stream end
  *int_writer_ = freq_start; // freq stream start
  *int_writer_ = prox_start; // prox stream start

  auto& features = meta_.features;

  p.doc = did;
  if ( !features.check< frequency >() ) {
    p.doc_code = did;
  } else {
    p.doc_code = did << 1;
    p.freq = 1;

    if ( features.check< position >() ) {
      int_block_pool::iterator it = int_writer_->parent().seek(p.int_start + 1);
      write_prox( p, it, pos_, pay );
      if ( features.check< offset >() ) {
        write_offset( p, it, offs );
      }
    }
  }

  max_term_freq_ = std::max( 1U, max_term_freq_ );
  ++unq_term_cnt_;
}

void field_data::add_term(
  posting& p, doc_id_t did, const payload* pay, const offset* offs
) {
  int_block_pool::iterator it = int_writer_->parent().seek( p.int_start );

  auto& features = meta_.features;
  if ( !features.check< frequency >() ) {
    if ( p.doc != did ) {
      assert( did > p.doc );

      byte_block_pool::sliced_inserter out( byte_writer_, *it );
      bytes_io<uint64_t>::vwrite( out, p.doc_code );
      *it = out.pool_offset();

      p.doc_code = did - p.doc;
      p.doc = did;
      ++unq_term_cnt_;
    }
  } else if ( p.doc != did ) {
    assert( did > p.doc );

    byte_block_pool::sliced_inserter out( byte_writer_, *it );

    if ( 1U == p.freq ) {
      bytes_io<uint64_t>::vwrite(out, p.doc_code | 1);
    } else {
      bytes_io<uint64_t>::vwrite(out, p.doc_code);
      bytes_io<uint32_t>::vwrite( out, p.freq );
    }

    *it = out.pool_offset();

    p.doc_code = (did - p.doc) << 1;
    p.freq = 1;

    p.doc = did;
    max_term_freq_ = std::max( 1U, max_term_freq_ );
    ++unq_term_cnt_;

    if ( features.check< position >() ) {
      ++it;
      write_prox( p, it, pos_, pay );
      if ( features.check< offset >() ) {
        p.offs = 0;
        write_offset( p, it, offs );
      }
    }
  } else { // exists in current doc           
    max_term_freq_ = std::max( ++p.freq, max_term_freq_ );
    if ( features.check< position >() ) {
      ++it;
      write_prox( p, it, pos_ - p.pos, pay );
      if ( features.check< offset >() ) {
        write_offset( p, it, offs );
      }
    }

  }
}

term_iterator::ptr field_data::iterator() const {
  return term_iterator::ptr(new detail::term_iterator(*this, terms_.begin(), terms_.end()));
}

bool field_data::invert(
  token_stream* stream, const flags& features, float_t boost, doc_id_t id
) {
  REGISTER_TIMER_DETAILED();

  if (!stream) {
    return false;
  }

  // accumulate field features
  meta_.features |= features;

  // TODO: should check feature consistency 
  // among features & meta_.features()

  const attributes& attrs = stream->attributes();
  const term_attribute* term = attrs.get<term_attribute>();
  const increment* inc = attrs.get<increment>();
  const offset* offs = nullptr;
  const payload* pay = nullptr;
  if (meta_.features.check<offset>()) {
    offs = attrs.get<offset>();
    if (offs) {
      pay = attrs.get<payload>();
    }
  } 

  init(id); // initialize field_data for the supplied doc_id

  while (stream->next()) {
    pos_ += inc->value;
    if (pos_ < last_pos_) {
      IR_ERROR() << "invalid position " << pos_ << " < " << last_pos_;
      return false;
    }
    last_pos_ = pos_;

    if (0 == inc->value) {
      ++num_overlap_;
    }

    if (offs) {
      const uint32_t start_offset = offs_ + offs->start;
      const uint32_t end_offset = offs_ + offs->end;

      if (start_offset < last_start_offs_ || end_offset < start_offset) {
        IR_ERROR() << "invalid offset start=" << start_offset
          << " end=" << end_offset;
        return false;
      }

      last_start_offs_ = start_offset;
    }

    auto res = terms_.emplace(term->value());

    if (terms_.end() == res.first) {
      IR_ERROR() << "field \"" << meta_.name << "\" has invalid term \"" << ref_cast<char>(term->value()) << "\"";
      continue;
    }

    if (res.second) {
      new_term(res.first->second, id, pay, offs);
    } else {
      add_term(res.first->second, id, pay, offs);
    }

    if (0 == ++len_) {
      IR_ERROR() << "too many token in field, document \"" << id << "\"";
      return false;
    }
  }

  if (offs) {
    offs_ += offs->end;
  }

  boost_ *= boost;
  return true;
}

/* -------------------------------------------------------------------
 * fields_data
 * ------------------------------------------------------------------*/

fields_data::fields_data()
  : byte_pool_(BYTE_BLOCK_SIZE),
    byte_writer_(byte_pool_.begin()),
    int_pool_(INT_BLOCK_SIZE),
    int_writer_(int_pool_.begin()) {
}

field_data& fields_data::get(const string_ref& name) {
  const auto hashed_name = make_hashed_ref(name, string_ref_hash_t());

  auto res = fields_.emplace(
    std::piecewise_construct,
    std::forward_as_tuple(hashed_name),
    std::forward_as_tuple(
      name,
      fields_.size(),
      &byte_writer_,
      &int_writer_
    )
  );

  auto& slot = res.first->second;
  if (res.second) {
    // 'slot' gets its own copy of the 'name', 
    // here we safely replace original reference to 'name' provided by
    // user with the reference to the cached copy in 'slot'
    auto& key = const_cast<hashed_string_ref&>(res.first->first);
    key = hashed_string_ref(key.hash(), slot.meta().name);
  }

  return slot;
}

void fields_data::flush( flush_state& state ) {
  REGISTER_TIMER_DETAILED();
  /* set the segment meta */
  state.features = &features_;
  
  /* set total number of field in the segment */
  state.fields_count = fields_.size();

  {
    field_meta_writer::ptr fmw = state.codec->get_field_meta_writer();
    fmw->prepare(state);

    field_writer::ptr fw = state.codec->get_field_writer();
    fw->prepare(state);

    std::map<string_ref, fields_map::mapped_type*> fields;

    // ensure fields are sorted
    for (auto& entry: fields_) {
      fields.emplace(entry.first, &(entry.second));
    }

    for(auto& entry: fields) {
      auto& field = *(entry.second);
      auto& meta = field.meta();
      auto id = meta.id;
      auto& features = meta.features;

      // write field metadata
      fmw->write(id, meta.name, features);

      // write field invert data
      auto terms = field.iterator();

      if (terms) {
        fw->write(id, features, *terms);
      }
    }

    fw->end();
    fmw->end();
  }
}

void fields_data::reset() {
  byte_writer_ = byte_pool_.begin(); // reset position pointer to start of pool
  features_.clear();
  fields_.clear();
  int_writer_ = int_pool_.begin(); // reset position pointer to start of pool
}

NS_END