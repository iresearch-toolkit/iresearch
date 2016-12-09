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

#include "tests_shared.hpp"
#include "assert_format.hpp"

#include "analysis/token_attributes.hpp"
#include "analysis/token_stream.hpp"

#include "index/field_meta.hpp"
#include "index/index_reader.hpp"

#include "search/collector.hpp"
#include "search/term_filter.hpp"
#include "search/boolean_filter.hpp"
#include "search/tfidf.hpp"

#include "misc.hpp"

#include "utils/bit_utils.hpp"

#include "store/data_output.hpp"

#include <unordered_set>
#include <algorithm>
#include <iostream>
#include <cassert>

namespace tests {

/* -------------------------------------------------------------------
* FREQUENCY BASED DATA MODEL
* ------------------------------------------------------------------*/

/* -------------------------------------------------------------------
* position
* ------------------------------------------------------------------*/

position::position(
    uint32_t pos, uint32_t start,
    uint32_t end,
    const iresearch::bytes_ref& pay)
  : pos( pos ), start( start ),
    end( end ), payload( pay ) {
}

/* -------------------------------------------------------------------
* posting
* ------------------------------------------------------------------*/

posting::posting(iresearch::doc_id_t id): id_(id) {}

void posting::add(uint32_t pos, uint32_t offs_start, const iresearch::attributes& attrs) {
  const iresearch::offset* offs = attrs.get<iresearch::offset>();
  const iresearch::payload* pay = attrs.get<iresearch::payload>();

  uint32_t start = iresearch::offset::INVALID_OFFSET;
  uint32_t end = iresearch::offset::INVALID_OFFSET;
  if ( offs ) {
    start = offs_start + offs->start;
    end = offs_start + offs->end;
  }

  positions_.emplace( pos, start, end,
                      pay ? pay->value : iresearch::bytes_ref::nil);
}

/* -------------------------------------------------------------------
* term
* ------------------------------------------------------------------*/

term::term(const iresearch::bytes_ref& data): value( data ) {}

posting& term::add(iresearch::doc_id_t id) {
  return const_cast< posting& >( *postings.emplace( id ).first );
}

bool term::operator<( const term& rhs ) const {
  return utf8_less(
    value.c_str(), value.size(),
    rhs.value.c_str(), rhs.value.size()
  );
}

/* -------------------------------------------------------------------
* field
* ------------------------------------------------------------------*/

field::field(
    const iresearch::string_ref& name,
    const iresearch::flags& features,
    iresearch::field_id id)
  : pos(0), offs(0) {
  this->id = id;
  this->name = name;
  this->features = features;
}

field::field( field&& rhs )
  :field_meta( std::move( rhs ) ),
  terms( std::move( rhs.terms ) ),
  docs(std::move(rhs.docs)),
  pos( rhs.pos ),
  offs( rhs.offs ) {}

term& field::add(const iresearch::bytes_ref& t) {
  auto res = terms.emplace( t );
  return const_cast< term& >( *res.first );
}

term* field::find(const iresearch::bytes_ref& t) {
  auto it = terms.find( term( t ) );
  return terms.end() == it ? nullptr : const_cast< term* >(&*it);
}

size_t field::remove(const iresearch::bytes_ref& t) {
  return terms.erase( term( t ) );
}

/* -------------------------------------------------------------------
* index_segment
* ------------------------------------------------------------------*/

index_segment::index_segment() : count_( 0 ) {}

index_segment::index_segment( index_segment&& rhs) 
  : fields_( std::move( rhs.fields_)),
    count_( rhs.count_) {
  rhs.count_ = 0;
}

index_segment& index_segment::operator=( index_segment&& rhs ) {
  if ( this != &rhs ) {
    fields_ = std::move( rhs.fields_ );
    count_ = rhs.count_;
    rhs.count_ = 0;
  }
  return *this;
}

void index_segment::add(const ifield& f) {
  iresearch::field_id field_id = static_cast<iresearch::field_id>(fields_.size());
  const iresearch::string_ref& field_name = f.name();
  field field(field_name, f.features(), field_id);
  auto res = fields_.emplace(field_name, std::move(field));
  if (res.second) {
    id_to_field_.emplace_back(&res.first->second);
  }
  const_cast<iresearch::string_ref&>(res.first->first) = res.first->second.name;
  auto& fld = res.first->second;

  auto stream = f.get_tokens();
  if (!stream) {
    return;
  }

  const iresearch::attributes& attrs = stream->attributes();
  const iresearch::term_attribute* term = attrs.get<iresearch::term_attribute>();
  const iresearch::increment* inc = attrs.get<iresearch::increment>();
  const iresearch::offset* offs = attrs.get<iresearch::offset>();
  const iresearch::payload* pay = attrs.get<iresearch::payload>();

  bool empty = true;
  auto doc_id = (ir::type_limits<ir::type_t::doc_id_t>::min)() + count_;

  while (stream->next()) {
    tests::term& trm = fld.add(term->value());
    tests::posting& pst = trm.add(doc_id);

    pst.add(fld.pos, fld.offs, stream->attributes());
    fld.pos += inc->value;
    empty = false;
  }

  if (!empty) { 
    fld.docs.emplace(doc_id);
  }

  if (offs) {
    fld.offs += offs->end;
  }
}

/* -------------------------------------------------------------------
* FORMAT DEFINITION 
* ------------------------------------------------------------------*/

/* -------------------------------------------------------------------
 * index_meta_writer
 * ------------------------------------------------------------------*/

std::string index_meta_writer::filename(
  const iresearch::index_meta& meta
) const {
  return std::string();
}

bool index_meta_writer::prepare(
  iresearch::directory& dir,
  iresearch::index_meta& meta
) {
  return true;
}

void index_meta_writer::commit() {
}

void index_meta_writer::rollback() NOEXCEPT {
}

/* -------------------------------------------------------------------
 * index_meta_reader
 * ------------------------------------------------------------------*/

bool index_meta_reader::last_segments_file(
    const iresearch::directory&, 
    std::string&) const {
  return false;
}

void index_meta_reader::read(
  const iresearch::directory& dir,
  iresearch::index_meta& meta,
  const iresearch::string_ref& filename /*= string_ref::nil*/
) {
}

/* -------------------------------------------------------------------
 * segment_meta_writer 
 * ------------------------------------------------------------------*/

std::string segment_meta_writer::filename(
  const iresearch::segment_meta& meta
) const {
  return std::string();
}

void segment_meta_writer::write(
  iresearch::directory& dir,  
  const iresearch::segment_meta& meta ) { 
}

/* -------------------------------------------------------------------
 * segment_meta_reader
 * ------------------------------------------------------------------*/

void segment_meta_reader::read( 
  const iresearch::directory& dir,
  iresearch::segment_meta& meta,
  const iresearch::string_ref& filename /*= string_ref::nil*/
) {
}

/* -------------------------------------------------------------------
* document_mask_writer
* ------------------------------------------------------------------*/

document_mask_writer::document_mask_writer(const index_segment& data):
  data_(data) {
}

std::string document_mask_writer::filename(
  const iresearch::segment_meta& meta
) const {
  return std::string();
}

void document_mask_writer::prepare(
  iresearch::directory& dir,
  const iresearch::segment_meta& meta) {
}

void document_mask_writer::begin(uint32_t count) {
  EXPECT_EQ(data_.doc_mask().size(), count);
}

void document_mask_writer::write(const iresearch::doc_id_t& doc_id) {
  EXPECT_EQ(true, data_.doc_mask().find(doc_id) != data_.doc_mask().end());
}

void document_mask_writer::end() { }

/* -------------------------------------------------------------------
 * field_writer
 * ------------------------------------------------------------------*/

field_writer::field_writer(const index_segment& data, const iresearch::flags& features)
  : readers_( data ), features_( features) {
}

void field_writer::prepare(const iresearch::flush_state& state) {
  EXPECT_EQ( state.doc_count, readers_.data().doc_count() );
}

void field_writer::write(
    iresearch::field_id id,
    const iresearch::flags& expected_field, 
    iresearch::term_iterator& actual_term) {
  // features to check
  const tests::field* fld = readers_.data().find(id);
  ASSERT_NE(nullptr, fld);
  ASSERT_EQ(fld->features, expected_field);
  
  auto features = features_ & fld->features;

  const iresearch::term_reader* expected_term_reader = readers_.terms(fld->id);
  ASSERT_NE(nullptr, expected_term_reader);

  iresearch::bytes_ref actual_min;
  iresearch::bytes_ref actual_max;
  iresearch::bstring actual_min_buf;
  iresearch::bstring actual_max_buf;
  size_t actual_size = 0;

  for (auto expected_term = expected_term_reader->iterator(); 
       actual_term.next(); ++actual_size) {
    ASSERT_TRUE(expected_term->next());

    assert_term(*expected_term, actual_term, features);

    if (actual_min.null()) {
      actual_min_buf = actual_term.value();
      actual_min = actual_min_buf;
    }

    actual_max_buf = actual_term.value();
    actual_max = actual_max_buf;
  }

  // check term reader
  ASSERT_EQ(expected_term_reader->size(), actual_size);
  ASSERT_EQ((expected_term_reader->min)(), actual_min);
  ASSERT_EQ((expected_term_reader->max)(), actual_max);
}

void field_writer::end() { }

/* -------------------------------------------------------------------
 * field_reader
 * ------------------------------------------------------------------*/

NS_BEGIN( detail )

class pos_iterator;

class doc_iterator : public iresearch::doc_iterator {
 public:
   doc_iterator(const iresearch::flags& features,
                      const tests::term& data );

  iresearch::doc_id_t value() const override {
    return doc_->value;
  }

  const iresearch::attributes& attributes() const NOEXCEPT override {
    return attrs_;
  }

  virtual bool next() override {
    if ( next_ == data_.postings.end() ) {
      doc_->clear();
      return false;
    }

    prev_ = next_, ++next_;
    doc_->value = prev_->id();
    if ( freq_ ) freq_->value = prev_->positions().size();
    if ( pos_ ) pos_->clear();
    return true;
  }

  virtual iresearch::doc_id_t seek(iresearch::doc_id_t id) override {
    posting tmp( id );
    auto it = data_.postings.find( tmp );

    if ( it == data_.postings.end() ) {
      prev_ = next_ = it;
      return ir::type_limits<ir::type_t::doc_id_t>::eof();
    }

    prev_ = it;
    next_ = ++it;
    doc_->value = prev_->id();
    if ( pos_ ) {
      pos_->clear();
    }
    return doc_->value;
  }

 private:
  friend class pos_iterator;

  iresearch::attributes attrs_;
  iresearch::document* doc_;
  iresearch::frequency* freq_;
  const iresearch::flags& features_;
  iresearch::position::impl* pos_;
  const tests::term& data_;
  std::set<posting>::const_iterator prev_;
  std::set<posting>::const_iterator next_;
};

class pos_iterator : public iresearch::position::impl {
 public:
  pos_iterator(const doc_iterator& owner):
    value_(iresearch::type_limits<iresearch::type_t::pos_t>::invalid()),
    offs_(nullptr),
    pay_(nullptr),
    owner_( owner ) {

    if (owner_.features_.check<iresearch::offset>()) {
      offs_ = attributes().add<iresearch::offset>();
    }
    if (owner_.features_.check<iresearch::payload>()) {
      pay_ = attributes().add<iresearch::payload>();
    }
  }

  void clear() override {
    next_ = owner_.prev_->positions().begin();
    value_ = iresearch::type_limits<iresearch::type_t::pos_t>::invalid();
    if ( offs_ ) offs_->clear();
    if ( pay_ ) pay_->clear();
  }

  bool next() override {
    if ( next_ == owner_.prev_->positions().end() ) {
      return false;
    }

    value_ = next_->pos;

    if ( offs_ ) {
      offs_->start = next_->start;
      offs_->end = next_->end;
    }

    if ( pay_ ) {
      pay_->value = next_->payload;
    }

    ++next_;
    return true;
  }

  uint32_t value() const override {
    return value_;
  }
  
 private:
  std::set<position>::const_iterator next_;
  uint32_t value_;
  iresearch::offset* offs_;
  iresearch::payload* pay_;
  const doc_iterator& owner_;
};

doc_iterator::doc_iterator(const iresearch::flags& features, const tests::term& data)
  : freq_( nullptr ),
    pos_( nullptr ),
    features_( features ),
    data_( data ) {
  next_ = data_.postings.begin();

  doc_ = attrs_.add <iresearch::document>();
  if ( features.check<iresearch::frequency>() ) {
    freq_ = attrs_.add<iresearch::frequency>();
  }

  if ( features.check< iresearch::position >() ) {
    attrs_.add<iresearch::position>()->prepare(pos_ = new detail::pos_iterator(*this));
  }
}

class term_iterator : public iresearch::seek_term_iterator {
 public:
  term_iterator( const tests::field& data ) 
    : data_( data ) {
    next_ = data_.terms.begin();
  }

  const iresearch::attributes& attributes() const NOEXCEPT override {
    return attrs_;
  }

  const iresearch::bytes_ref& value() const override {
    return value_;
  }

  bool next() override {
    if ( next_ == data_.terms.end() ) {
      value_ = iresearch::bytes_ref::nil;
      return false;
    }

    prev_ = next_, ++next_;
    value_ = prev_->value;
    return true;
  }

  virtual void read() override  { }

  virtual bool seek(const iresearch::bytes_ref& value) override {
    auto it = data_.terms.find(value);

    if (it == data_.terms.end()) {
      prev_ = next_ = it;
      value_ = iresearch::bytes_ref::nil;
      return false;
    }

    prev_ = it;
    next_ = ++it;
    value_ = prev_->value;
    return true;
  }

  virtual iresearch::SeekResult seek_ge(const iresearch::bytes_ref& value) override {
    auto it = data_.terms.lower_bound(value);
    if (it == data_.terms.end()) {
      prev_ = next_ = it;
      value_ = iresearch::bytes_ref::nil;
      return iresearch::SeekResult::END;
    }

    if (it->value == value) {
      prev_ = it;
      next_ = ++it;
    value_ = prev_->value;
      return iresearch::SeekResult::FOUND;
    }

    prev_ = ++it;
    next_ = ++it;
    value_ = prev_->value;
    return iresearch::SeekResult::NOT_FOUND;
  }

  virtual doc_iterator::ptr postings(const iresearch::flags& features) const override {
    return doc_iterator::make< detail::doc_iterator >( features, *prev_ );
  }

  virtual bool seek(
      const iresearch::bytes_ref& term,
      const iresearch::attribute& cookie) {
    return false;
  }

  virtual iresearch::attribute::ptr cookie() const {
    return nullptr;
  }

 private:
  iresearch::attributes attrs_;
  const tests::field& data_;
  std::set< tests::term >::const_iterator prev_;
  std::set< tests::term >::const_iterator next_;
  iresearch::bytes_ref value_;
};

size_t term_reader::size() const {
  return data_.terms.size();
}

uint64_t term_reader::docs_count() const {
  return data_.docs.size();
}

const iresearch::bytes_ref& (term_reader::min)() const {
  return min_;
}

const iresearch::bytes_ref& (term_reader::max)() const {
  return max_;
}

iresearch::seek_term_iterator::ptr term_reader::iterator() const {
  return iresearch::seek_term_iterator::ptr(
    new detail::term_iterator( data_ )
  );
}

const iresearch::field_meta& term_reader::meta() const {
  return data_;
}

const iresearch::attributes& term_reader::attributes() const NOEXCEPT {
  return iresearch::attributes::empty_instance();
}

class field_iterator : public iresearch::iterator <const iresearch::string_ref&> {
 public:
  field_iterator( const index_segment& data ) : data_( data ) {
    next_ = data_.fields().begin();
  }

  const iresearch::string_ref& value() const override {
    return prev_->first;
  }

  bool next() override {
    if ( next_ == data_.fields().end() ) {
      return false;
    }

    prev_ = next_, ++next_;
    return true;
  }

 private:
  const index_segment& data_;
  index_segment::iterator next_;
  index_segment::iterator prev_;
};

NS_END

field_reader::field_reader(const index_segment& data)
  : data_(data) {

  readers_.resize(data.fields().size());

  for (const auto& pair : data_.fields()) {
    readers_[pair.second.id] = iresearch::term_reader::make<detail::term_reader>(pair.second);
  }
}

void field_reader::prepare(const iresearch::reader_state& state) {
}

const iresearch::term_reader* field_reader::terms(iresearch::field_id field) const {
  if (field >= readers_.size()) {
    return nullptr;
  }

  return readers_[field].get();
}
  
size_t field_reader::size() const {
  return data_.size();
}
/* -------------------------------------------------------------------
 * field_meta_writer 
 * ------------------------------------------------------------------*/

field_meta_writer::field_meta_writer( const index_segment& data )
  : data_( data ) { 
} 

void field_meta_writer::prepare(const iresearch::flush_state& state) {
  EXPECT_EQ(data_.size(), state.fields_count);
}

void field_meta_writer::write(
    iresearch::field_id id, 
    const std::string& name, 
    const iresearch::flags& features,
    iresearch::field_id) {
  const tests::field* fld = data_.find(name);
  EXPECT_NE(nullptr, fld);
  EXPECT_EQ(fld->name, name);
  EXPECT_EQ(fld->id, id);
  EXPECT_EQ(fld->features, features);
}

void field_meta_writer::end() { }

/* -------------------------------------------------------------------
 * stored_fields_writer 
 * ------------------------------------------------------------------*/

stored_fields_writer::stored_fields_writer(const index_segment& data)
  : data_(data) {
}

void stored_fields_writer::prepare(
  iresearch::directory& dir,
  const iresearch::string_ref& seg_name) {
}

bool stored_fields_writer::write(const iresearch::serializer&) {
  return false;
}

void stored_fields_writer::end(const iresearch::serializer*) { }

void stored_fields_writer::finish() {}

void stored_fields_writer::reset() {}

/* -------------------------------------------------------------------
 * format 
 * ------------------------------------------------------------------*/

/*static*/ decltype(format::DEFAULT_SEGMENT) format::DEFAULT_SEGMENT;

format::format(): format(DEFAULT_SEGMENT) {}

format::format(const index_segment& data):
  iresearch::format(format::type()), data_(data) {
}

iresearch::index_meta_writer::ptr format::get_index_meta_writer() const {
  return iresearch::index_meta_writer::make<index_meta_writer>();
}

iresearch::index_meta_reader::ptr format::get_index_meta_reader() const {
  static iresearch::index_meta_reader::ptr reader =
    iresearch::index_meta_reader::make<index_meta_reader>();

  // can reuse stateless reader
  return reader;
}

iresearch::segment_meta_writer::ptr format::get_segment_meta_writer() const {
  static iresearch::segment_meta_writer::ptr w = iresearch::segment_meta_writer::make< segment_meta_writer >();
  return w;
}

iresearch::segment_meta_reader::ptr format::get_segment_meta_reader() const {
  static iresearch::segment_meta_reader::ptr r = iresearch::segment_meta_reader::make< segment_meta_reader >();
  return r;
}

iresearch::document_mask_reader::ptr format::get_document_mask_reader() const {
  return iresearch::document_mask_reader::ptr();
}

iresearch::document_mask_writer::ptr format::get_document_mask_writer() const {
  return iresearch::document_mask_writer::make<tests::document_mask_writer>(data_);
}

iresearch::field_meta_reader::ptr format::get_field_meta_reader() const {
  return iresearch::field_meta_reader::ptr();
}

iresearch::field_meta_writer::ptr format::get_field_meta_writer() const {
  return iresearch::field_meta_writer::make<tests::field_meta_writer>(data_);
}

iresearch::field_writer::ptr format::get_field_writer(bool volatile_attributes /*=false*/) const {
  return iresearch::field_writer::make<tests::field_writer>(data_);
}

iresearch::field_reader::ptr format::get_field_reader() const {
  return iresearch::field_reader::make<tests::field_reader>(data_);
}

iresearch::stored_fields_writer::ptr format::get_stored_fields_writer() const {
  return iresearch::stored_fields_writer::make<tests::stored_fields_writer>(data_);
}

iresearch::stored_fields_reader::ptr format::get_stored_fields_reader() const {
  return nullptr;
}

iresearch::column_meta_writer::ptr format::get_column_meta_writer() const {
  return nullptr;
}

iresearch::column_meta_reader::ptr format::get_column_meta_reader() const {
  return nullptr;
}

iresearch::columnstore_writer::ptr format::get_columnstore_writer() const {
  return nullptr;
}

iresearch::columnstore_reader::ptr format::get_columnstore_reader() const {
  return nullptr;
}

DEFINE_FORMAT_TYPE_NAMED(tests::format, "iresearch_format_tests");
REGISTER_FORMAT(tests::format);
DEFINE_FACTORY_SINGLETON(format);

void assert_term(
    const iresearch::term_iterator& expected_term, 
    const iresearch::term_iterator& actual_term,
    const iresearch::flags& features) {
  ASSERT_EQ(expected_term.value(), actual_term.value());

  const iresearch::doc_iterator::ptr expected_docs = expected_term.postings(features);
  const iresearch::doc_iterator::ptr actual_docs = actual_term.postings(features);

  // check docs
  for (; expected_docs->next();) {
    ASSERT_TRUE(actual_docs->next());
    ASSERT_EQ(expected_docs->value(), actual_docs->value());

    // check document attributes
    {
      auto& expected_attrs = expected_docs->attributes();
      auto& actual_attrs = actual_docs->attributes();
      ASSERT_EQ(expected_attrs.features(), actual_attrs.features());

      const iresearch::frequency* expected_freq = expected_attrs.get<iresearch::frequency>();
      const iresearch::frequency* actual_freq = actual_attrs.get<iresearch::frequency>();
      if (expected_freq) {
        ASSERT_NE(nullptr, actual_freq);
        ASSERT_EQ(expected_freq->value, actual_freq->value);
      }

      const iresearch::position* expected_pos = expected_attrs.get< iresearch::position >();
      const iresearch::position* actual_pos = actual_attrs.get< iresearch::position >();
      if (expected_pos) {
        ASSERT_NE(nullptr, actual_pos);

        const iresearch::offset* expected_offs = expected_pos->get<iresearch::offset>();
        const iresearch::offset* actual_offs = actual_pos->get<iresearch::offset>();
        if (expected_offs) ASSERT_NE(nullptr, actual_offs);

        const iresearch::payload* expected_pay = expected_pos->get<iresearch::payload>();
        const iresearch::payload* actual_pay = actual_pos->get<iresearch::payload>();
        if (expected_pay) ASSERT_NE(nullptr, actual_pay);

        for (; expected_pos->next();) {
          ASSERT_TRUE(actual_pos->next());
          ASSERT_EQ(expected_pos->value(), actual_pos->value());

          if (expected_offs) {
            ASSERT_EQ(expected_offs->start, actual_offs->start);
            ASSERT_EQ(expected_offs->end, actual_offs->end);
          }

          if (expected_pay) {
            ASSERT_EQ(expected_pay->value, actual_pay->value);
          }
        }

        ASSERT_FALSE(actual_pos->next());
      }
    }
  }

  ASSERT_FALSE(actual_docs->next());
}

void assert_terms_next(
    const iresearch::term_reader& expected_term_reader,
    const iresearch::term_reader& actual_term_reader,
    const iresearch::flags& features) {
  // check term reader
  ASSERT_EQ((expected_term_reader.min)(), (actual_term_reader.min)());
  ASSERT_EQ((expected_term_reader.max)(), (actual_term_reader.max)());
  ASSERT_EQ(expected_term_reader.size(), actual_term_reader.size());
  ASSERT_EQ(expected_term_reader.docs_count(), actual_term_reader.docs_count());

  iresearch::bytes_ref actual_min;
  iresearch::bytes_ref actual_max;
  iresearch::bstring actual_min_buf;
  iresearch::bstring actual_max_buf;
  size_t actual_size = 0;

  auto expected_term = expected_term_reader.iterator();
  auto actual_term = actual_term_reader.iterator();
  for (; actual_term->next(); ++actual_size) {
    ASSERT_TRUE(expected_term->next());

    assert_term(*expected_term, *actual_term, features);

    if (actual_min.null()) {
      actual_min_buf = actual_term->value();
      actual_min = actual_min_buf;
    }

    actual_max_buf = actual_term->value();
    actual_max = actual_max_buf;
  }

  // check term reader
  ASSERT_EQ(expected_term_reader.size(), actual_size);
  ASSERT_EQ((expected_term_reader.min)(), actual_min);
  ASSERT_EQ((expected_term_reader.max)(), actual_max);
}

void assert_terms_seek(
    const iresearch::term_reader& expected_term_reader,
    const iresearch::term_reader& actual_term_reader,
    const iresearch::flags& features,
    size_t lookahead /* = 10 */) {
  // check term reader
  ASSERT_EQ((expected_term_reader.min)(), (actual_term_reader.min)());
  ASSERT_EQ((expected_term_reader.max)(), (actual_term_reader.max)());
  ASSERT_EQ(expected_term_reader.size(), actual_term_reader.size());
  ASSERT_EQ(expected_term_reader.docs_count(), actual_term_reader.docs_count());

  auto expected_term = expected_term_reader.iterator();
  auto actual_term_with_state = actual_term_reader.iterator();
  for (; expected_term->next();) {
    // seek with state
    {
      ASSERT_TRUE(actual_term_with_state->seek(expected_term->value()));
      assert_term(*expected_term, *actual_term_with_state, features);
    }

    // seek without state, iterate forward
    iresearch::attribute::ptr cookie; // cookie
    {
      auto actual_term = actual_term_reader.iterator();
      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      assert_term(*expected_term, *actual_term, features);
      actual_term->read();
      cookie = actual_term->cookie();

      // iterate forward
      {
        auto copy_expected_term = expected_term_reader.iterator();
        ASSERT_TRUE(copy_expected_term->seek(expected_term->value()));
        ASSERT_EQ(expected_term->value(), copy_expected_term->value());
        for(size_t i = 0; i < lookahead; ++i) {
          const bool copy_expected_next = copy_expected_term->next();
          const bool actual_next = actual_term->next();
          ASSERT_EQ(copy_expected_next, actual_next);
          if (!copy_expected_next) {
            break;
          }
          assert_term(*copy_expected_term, *actual_term, features);
        }
      }
      
      // seek back to initial term
      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      assert_term(*expected_term, *actual_term, features);
    }

    // seek greater or equal without state, iterate forward
    {
      auto actual_term = actual_term_reader.iterator();
      ASSERT_EQ(iresearch::SeekResult::FOUND, actual_term->seek_ge(expected_term->value()));
      assert_term(*expected_term, *actual_term, features);

      // iterate forward
      {
        auto copy_expected_term = expected_term_reader.iterator();
        ASSERT_TRUE(copy_expected_term->seek(expected_term->value()));
        ASSERT_EQ(expected_term->value(), copy_expected_term->value());
        for(size_t i = 0; i < lookahead; ++i) {
          const bool copy_expected_next = copy_expected_term->next();
          const bool actual_next = actual_term->next();
          ASSERT_EQ(copy_expected_next, actual_next);
          if (!copy_expected_next) {
            break;
          }
          assert_term(*copy_expected_term, *actual_term, features);
        }
      }
      
      // seek back to initial term
      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      assert_term(*expected_term, *actual_term, features);
    }

    // seek to cookie without state, iterate to the end
    {
      auto actual_term = actual_term_reader.iterator();
      ASSERT_TRUE(actual_term->seek(expected_term->value(), *cookie));
      ASSERT_EQ(expected_term->value(), actual_term->value());
      assert_term(*expected_term, *actual_term, features);

      // iterate forward
      {
        auto copy_expected_term = expected_term_reader.iterator();
        ASSERT_TRUE(copy_expected_term->seek(expected_term->value()));
        ASSERT_EQ(expected_term->value(), copy_expected_term->value());
        for(size_t i = 0; i < lookahead; ++i) {
          const bool copy_expected_next = copy_expected_term->next();
          const bool actual_next = actual_term->next();
          ASSERT_EQ(copy_expected_next, actual_next);
          if (!copy_expected_next) {
            break;
          }
          assert_term(*copy_expected_term, *actual_term, features);
        }
      }

      // seek to the same term
      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      assert_term(*expected_term, *actual_term, features);

      // seek to the same term
      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      assert_term(*expected_term, *actual_term, features);

      // seek greater equal to the same term
      ASSERT_EQ(iresearch::SeekResult::FOUND, actual_term->seek_ge(expected_term->value()));
      assert_term(*expected_term, *actual_term, features);
    }
  }
}

void assert_index(
    const iresearch::directory& dir,
    iresearch::format::ptr codec,
    const index_t& expected_index,
    const iresearch::flags& features,
    size_t skip /*= 0*/) {
  auto actual_index_reader = iresearch::directory_reader::open(dir, codec);

  /* check number of segments */
  ASSERT_EQ(expected_index.size(), actual_index_reader->size());
  size_t i = 0;
  for (auto& actual_sub_reader : *actual_index_reader) {
    // skip segment if validation not required
    if (skip) {
      ++i;
      --skip;
      continue;
    }

    /* setting up test reader/writer */
    const tests::index_segment& expected_segment = expected_index[i];
    tests::field_reader expected_reader(expected_segment);

    /* get field name iterators */
    auto& expected_fields = expected_segment.fields();
    auto expected_fields_begin = expected_fields.begin();
    auto expected_fields_end = expected_fields.end();

    auto& actual_fields = actual_sub_reader.fields();
    auto actual_fields_begin = actual_fields.begin();
    auto actual_fields_end = actual_fields.end();

    /* iterate over fields */
    for (;actual_fields_begin != actual_fields_end;++actual_fields_begin, ++expected_fields_begin) {
      /* check field name */
      ASSERT_EQ(expected_fields_begin->first, actual_fields_begin->name);

      /* check field terms */
      auto expected_term_reader = expected_reader.terms(expected_fields_begin->second.id);
      ASSERT_NE(nullptr, expected_term_reader);
      auto actual_term_reader = (*actual_index_reader)[i].terms(actual_fields_begin->name);
      ASSERT_NE(nullptr, actual_term_reader);

      const iresearch::field_meta* expected_field = expected_segment.find(expected_fields_begin->first);
      ASSERT_NE(nullptr, expected_field);
      auto features_to_check = features & expected_field->features;

      assert_terms_next(*expected_term_reader, *actual_term_reader, features_to_check);
      assert_terms_seek(*expected_term_reader, *actual_term_reader, features_to_check);
    }
    ++i;
  }
}

} // tests
