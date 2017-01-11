//
// IResearch search engine 
// 
// Copyright (c) 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#include "tests_shared.hpp"
#include "field_data_tests.hpp"

#include "formats/formats_10.hpp"

#include "index/segment_writer.hpp"
#include "index/segment_reader.hpp"
#include "index/field_meta.hpp"
#include "index/index_meta.hpp"
#include "index/def_index_chain.hpp"

#include "store/memory_directory.hpp"

#include "formats_tests.hpp"

#include "document.hpp"
#include "analysis/token_attributes.hpp"

using namespace tests;
using namespace iresearch;

static void test_core( const std::vector< tests::document >& docs ) {

  tests::fields_data data;

  // directory
  directory::ptr dir = memory::make_unique< memory_directory >();

  // use testing format, that checks
  // correctness of the invertion operation
  //iresearch::format::ptr codec = memory::make_unique< tests::format >( &data );    
  iresearch::format::ptr codec = memory::make_unique< version10::format >();

  iresearch::field_ids field_numbers;

  // create writer
  segment_writer writer( dir.get(), codec.get(), &field_numbers, "_0" );

  // create test data
  for ( const auto& doc : docs ) {
    //data.add( doc );
  }

  // invert documents
  for ( const auto& doc : docs ) {
    writer.update( doc, nullptr, nullptr );
  }

  // perform flush
  segment_meta meta( writer.name() );
  writer.flush( meta );
}



TEST( segment_writer_test, string_fields_smoke ) {
  std::vector< tests::document > docs;

  add< std::string, iresearch::string_field >
    ( docs, {
      { "field2", "bla_bla_bla" },
      { "field3", "name" },
      { "field1", "name" },
      { "field0", "string with spaces" },
      { "field4", "123" }
    } );

  add< std::string, iresearch::string_field >
    ( docs, {
      { "field2", "bla bla bla" },
      { "field1", "name" },
    } );

  // duplication
  add< std::string, iresearch::string_field >
    ( docs, {
      { "field2", "bla_bla_bla" },
      { "field3", "name" },
      { "field1", "name" },
      { "field0", "string with spaces" },
      { "field4", "123" }
    } );

  add< std::string, iresearch::string_field >
    ( docs, {
      { "field with spaces", "quick brown fox jumps over the lazy dog" },
      { "another_strange_string_field", "value &*&1!!!!! qwery " },
    } );

  // multi-valued field
  add< std::string, iresearch::string_field >
    ( docs, {
      { "multi_value_field", "value 0" },
      { "multi_value_field", "value 1" },
      { "multi_value_field", "value 2" },
      { "multi_value_field", "value 3" },
      { "single_value_field", "value" },
      { "another_single_value_field", "value 1" }
    } );

  test_core( docs );
}
