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

#ifndef IRESEARCH_JSON_PARSER_TESTS
#define IRESEARCH_JSON_PARSER_TESTS

#include <iostream>
#include <vector>
#include <boost/property_tree/ptree.hpp>

namespace tests {
namespace json {

using boost::property_tree::ptree;

////////////////////////////////////////////////////////////////////////////////
/// @brief a class to be used with boost::property_tree::basic_ptree as its
///        value_type
////////////////////////////////////////////////////////////////////////////////
struct json_value {
  bool quoted;
  std::string value;
  json_value() {}
  json_value(std::string& v_value, bool v_quoted):
    quoted(v_quoted), value(std::move(v_value)) {}
  json_value(const json_value& other):
    quoted(other.quoted), value(other.value) {}
  json_value(json_value&& other):
    quoted(other.quoted), value(std::move(other.value)) {}
  json_value& operator=(json_value&& other) {
    quoted = other.quoted;
    value = std::move(other.value);
    return *this;
  }
};

////////////////////////////////////////////////////////////////////////////////
/// @brief a class to be used with boost::property_tree::basic_ptree as key_type
///        Note: Boost JSON parser uses key_type both as a key holder and as a
///              factory method for boost::property_tree::basic_ptree value_type
///        Note2: Boost JSON parser assumes key_type can be used interchangeably
///               with std::string
////////////////////////////////////////////////////////////////////////////////
class json_key {
 public:
  typedef char value_type; // myst be a type initializable from char
  json_key(): quoted_(true) {} // constructor used by parser for string values
  template <class InputIterator>
  json_key(InputIterator first, InputIterator last):
    quoted_(false), value_(first, last) {
  } // constructor used by parser for literal values

  bool operator<(const json_key& other) const { return value_ < other.value_; }
  json_key& operator+=(const value_type& v) { value_ += v; return *this; }
  operator const std::string&() const { return value_; }
  operator json_value() {
    // operator used by Boost JSON parser as a factory method for value_type
    json_value value;

    value.quoted = quoted_;
    value.value = std::move(value_);

    return value;
  }

  const char* c_str() const { return value_.c_str(); }
  void clear() { value_.clear(); }
  bool empty() const { return value_.empty(); }
  void swap(json_key& v) { value_.swap(v.value_); }
 private:
   bool quoted_;
   std::string value_;
};

typedef boost::property_tree::basic_ptree<json_key, json_value> json_tree;

struct parser_context {
  size_t level;
  std::vector<std::string> path;
};

struct visitor {
  /* called at the beginning of a document */
  void begin_doc();

  /* called at the beginning of an array */
  void begin_array( const parser_context& ctx );
  
  /* called at the beginning of an object */
  void begin_object( const parser_context& ctx );
  
  /* called at value entry */
  void node( const parser_context& ctx,
             const ptree::value_type& name );

  /* called at the end of an array  */
  void end_array( const parser_context& ctx );
  
  /* called at the end of an object */
  void end_object( const parser_context& ctx );
    
  /* called at the end of document */
  void end_doc();
};

template<typename JsonVisitor, typename JsonTreeItr>
void parse_json(JsonTreeItr begin, JsonTreeItr end, JsonVisitor& v, parser_context& ctx) {
  for ( ; begin != end; ++begin ) {
    if ( begin->second.empty() ) {
      v.node( ctx, *begin );
      continue;
    }

    ++ctx.level;

    /* we are in array */
    const bool in_array = begin->first.empty();
    /* next element is array (arrays have empty name) */
    const bool next_is_array = begin->second.begin()->first.empty();

    if ( !in_array ) {
      ctx.path.push_back( begin->first ); 
    }    

    if ( next_is_array ) {
      v.begin_array( ctx ); 
    } else { 
      v.begin_object( ctx ); 
    }

    parse_json( begin->second.begin(), begin->second.end(), v, ctx );

    if ( next_is_array ) {
      v.end_array( ctx );
    } else { 
      v.end_object( ctx ); 
    }

    if ( !in_array ) {
      ctx.path.pop_back(); 
    }

    --ctx.level;
  }
}

template<typename JsonVisitor, typename JsonTree>
void parse_json(const JsonTree& json, JsonVisitor& v) {
  parser_context ctx{ 0 };
  v.begin_doc();
  parse_json( json.begin(), json.end(), v, ctx );
  v.end_doc();
}

} // json
} // tests

// required for 'json_tree' type to compile
namespace boost {
  namespace property_tree {
    template <>
    struct path_of<tests::json::json_key>
    {
      typedef std::string type;
    };
  }
}

#endif