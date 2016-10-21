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

#include "doc_generator.hpp"
#include "index/field_data.hpp"
#include "utils/block_pool.hpp"
#include "analysis/token_streams.hpp"
#include "store/store_utils.hpp"
#include "unicode/utf8.h"

#include <sstream>
#include <iomanip>
#include <numeric>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/property_tree/json_parser.hpp>

using boost::property_tree::read_json;
using boost::property_tree::read_xml;
using boost::property_tree::write_xml;
using boost::property_tree::xml_writer_make_settings;
using boost::property_tree::ptree;

namespace utf8 {
namespace unchecked {

template<typename octet_iterator>
class break_iterator : public std::iterator<std::forward_iterator_tag, std::string> {
 public:
  typedef unchecked::iterator<octet_iterator> utf8iterator;

  break_iterator( utf8::uint32_t delim, const octet_iterator& begin, const octet_iterator& end )
    : delim_( delim ), wbegin_( begin ), wend_( begin ), end_( end ) {
    if ( !done() ) {
      next();
    }
  }

  explicit break_iterator( const octet_iterator& end )
    : wbegin_( end ), wend_( end ), end_( end ) {
  }

  const std::string& operator*() const { return res_; }

  const std::string* operator->( ) const { return &res_; }

  bool operator==( const break_iterator& rhs ) const {
    assert( end_ == rhs.end_ );
    return ( wbegin_ == rhs.wbegin_ && wend_ == rhs.wend_ );
  }

  bool operator!=( const break_iterator& rhs ) const {
    return !(operator==(rhs));
  }

  bool done() const { return wbegin_ == end_; }

  break_iterator& operator++() {
    next();
    return *this;
  }

  break_iterator operator++( int ) {
    break_iterator tmp( delim_, wbegin_, end_ );
    next();
    return tmp;
  }

 private:
  void next() {
    wbegin_ = wend_;
    wend_ = std::find( wbegin_, end_, delim_ );
    if ( wend_ != end_ ) {
      res_.assign( wbegin_.base(), wend_.base() );
      ++wend_;
    } else {
      res_.assign( wbegin_.base(), end_.base() );
    }
  }

  utf8::uint32_t delim_;
  std::string res_;
  utf8iterator wbegin_;
  utf8iterator wend_;
  utf8iterator end_;
};

} // unchecked
} // utf8

namespace tests {

// -----------------------------------------------------------------------------
// --SECTION--                                         field_base implementation
// -----------------------------------------------------------------------------

field_base::field_base(field_base&& rhs) 
  : name_(std::move(rhs.name_)),
    boost_(rhs.boost_),
    indexed_(rhs.indexed_),
    stored_(rhs.stored_) {
}

field_base& field_base::operator=(field_base&& rhs) {
  if (this != &rhs) {
    name_ = std::move(rhs.name_);
    boost_ = rhs.boost_;
    indexed_ = rhs.indexed_;
    stored_ = rhs.stored_;
  }
  return *this;
}

field_base::~field_base() { }

// -----------------------------------------------------------------------------
// --SECTION--                                         long_field implementation
// -----------------------------------------------------------------------------

ir::token_stream* long_field::get_tokens() const {
  if (!indexed()) {
    return nullptr;
  }

  stream_.reset(value_);
  return &stream_;
}

bool long_field::write(ir::data_output& out) const {
  ir::write_zvlong(out, value_);
  return true;
}

const ir::flags& long_field::features() const {
  return ir::flags::empty_instance();
}

// -----------------------------------------------------------------------------
// --SECTION--                                         int_field implementation
// -----------------------------------------------------------------------------

bool int_field::write(ir::data_output& out) const {
  ir::write_zvint(out, value_);
  return true;
}

ir::token_stream* int_field::get_tokens() const {
  if (!indexed()) {
    return nullptr;
  }

  stream_.reset(value_);
  return &stream_;
}

const ir::flags& int_field::features() const {
  return ir::flags::empty_instance();
}

// -----------------------------------------------------------------------------
// --SECTION--                                       double_field implementation
// -----------------------------------------------------------------------------

bool double_field::write(ir::data_output& out) const {
  ir::write_zvdouble(out, value_);
  return true;
}

ir::token_stream* double_field::get_tokens() const {
  if (!indexed()) {
    return nullptr;
  }

  stream_.reset(value_);
  return &stream_;
}

const ir::flags& double_field::features() const {
  return ir::flags::empty_instance();
}

// -----------------------------------------------------------------------------
// --SECTION--                                        float_field implementation
// -----------------------------------------------------------------------------

bool float_field::write(ir::data_output& out) const {
  ir::write_zvfloat(out, value_);
  return true;
}

ir::token_stream* float_field::get_tokens() const {
  if (!indexed()) {
    return nullptr;
  }

  stream_.reset(value_);
  return &stream_;
}

const ir::flags& float_field::features() const {
  return ir::flags::empty_instance();
}

// -----------------------------------------------------------------------------
// --SECTION--                                       binary_field implementation
// -----------------------------------------------------------------------------

bool binary_field::write(ir::data_output& out) const {
  ir::write_string(out, value_);
  return true;
}

ir::token_stream* binary_field::get_tokens() const {
  if (!indexed()) {
    return nullptr;
  }

  stream_.reset(value_);
  return &stream_;
}

const ir::flags& binary_field::features() const {
  return ir::flags::empty_instance();
}

// -----------------------------------------------------------------------------
// --SECTION--                                           document implementation
// -----------------------------------------------------------------------------
  
document::document( document&& rhs ) : fields_( std::move( rhs.fields_ ) ) { }

document& document::operator=( document&& rhs ) {
  if ( this != &rhs ) {
    fields_ = std::move( rhs.fields_ );
  }

  return *this;
}

document::~document() { }

void document::remove( const ir::string_ref& name ) {
  fields_.erase(
    std::remove_if( fields_.begin(), fields_.end(),
      [&name] ( const ifield::ptr& fld ) {
        return name == fld->name(); 
    })
  );
}

bool document::contains( const ir::string_ref& name ) const {
  return fields_.end() != std::find_if(
    fields_.begin(), fields_.end(),
    [&name] ( const ifield::ptr& fld ) {
      return name == fld->name();
  } );
}

std::vector<const ifield*> document::find(const ir::string_ref& name) const {
  std::vector<const ifield*> fields;
  std::for_each(
    fields_.begin(), fields_.end(),
    [&fields, &name] (const ifield::ptr& fld) {
      if (name == fld->name()) {
        fields.emplace_back(fld.get());
      }
  });

  return fields;
}

ifield* document::get( const ir::string_ref& name ) const {
  auto it = std::find_if(
    fields_.begin(), fields_.end(),
    [&name] ( const ifield::ptr& fld ) {
      return name == fld->name();
  });

  return fields_.end() == it ? nullptr : it->get();
}

// -----------------------------------------------------------------------------
// --SECTION--                                delim_doc_generator implementation
// -----------------------------------------------------------------------------

delim_doc_generator::delim_doc_generator(
    const fs::path& file, 
    doc_template& doc,
    uint32_t delim /* = 0x0009 */)
  : ifs_(file.native(), std::ifstream::in | std::ifstream::binary),
    doc_(&doc),
    delim_(delim) {
  doc_->init();
  doc_->reset();
}

const tests::document* delim_doc_generator::next() {
  if (!getline(ifs_, str_)) {
    return nullptr;
  } 

  {
    const std::string::const_iterator end = utf8::find_invalid(str_.begin(), str_.end());
    if (end != str_.end()) {
      /* invalid utf8 string */
      return nullptr;
    }
  }

  using word_iterator = utf8::unchecked::break_iterator<std::string::const_iterator>;

  const word_iterator end(str_.end());
  word_iterator begin(delim_, str_.begin(), str_.end());
  for (size_t i = 0; begin != end; ++begin, ++i) {
    doc_->value(i, *begin);
  }
  doc_->end();
  return doc_;
}
 
void delim_doc_generator::reset() {
  ifs_.seekg(0U);
  doc_->reset();
}

//////////////////////////////////////////////////////////////////////////////
/// @class json_doc_visitor
/// @brief visitor for JSON document-derived column value types
//////////////////////////////////////////////////////////////////////////////
class json_doc_visitor: iresearch::util::noncopyable {
 public:
  typedef std::function<void(
    tests::document&,
    const std::string&,
    const tests::json::json_value&)
  > factory_f;

  typedef std::vector<tests::document> documents_t;

  json_doc_visitor(
      const factory_f& factory, 
      documents_t& docs) 
    : factory_(factory),
      docs_(docs) {
  }

  /* called at the beginning of a document */
  void begin_doc() {}

  /* called at the beginning of an array */
  void begin_array(const json::parser_context& ctx) {}

  /* called at the beginning of an object */
  void begin_object(const json::parser_context& ctx) {
    if (1 == ctx.level) {
      docs_.emplace_back();
    }
  }

  /* called at value entry */
  void node(
      const json::parser_context& ctx, 
      const tests::json::json_tree::value_type& value) {
    const std::string& name = value.first.empty() 
      ? ctx.path.back() 
      : value.first;
    const auto& data = value.second.data();
    factory_(docs_.back(), name, data);
  }

  /* called at the end of an array  */
  void end_array(const json::parser_context& ctx) {}

  /* called at the end of an object */
  void end_object(const json::parser_context& ctx) {}

  /* called at the end of document */
  void end_doc() { }

 private:
  documents_t& docs_;
  const factory_f& factory_;
}; // json_doc_visitor

json_doc_generator::json_doc_generator(
    const fs::path& file, 
    const json_doc_generator::factory_f& factory) {
  tests::json::json_tree pt;
  json_doc_visitor visitor(factory, docs_);
  read_json(file.string(), pt);
  json::parse_json(pt, visitor);
  next_ = docs_.begin();
}

json_doc_generator::json_doc_generator(json_doc_generator&& rhs)
  : docs_(std::move(rhs.docs_)), 
    prev_(std::move(rhs.prev_)), 
    next_(std::move(rhs.next_)) {
}

const tests::document* json_doc_generator::next() {
  if (docs_.end() == next_) {
    return nullptr;
  }

  prev_ = next_, ++next_;
  return &*prev_;
}

void json_doc_generator::reset() {
  next_ = docs_.begin();
}

} // tests
