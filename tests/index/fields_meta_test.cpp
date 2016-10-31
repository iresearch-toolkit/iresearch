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
#include "index/field_meta.hpp"
#include "analysis/token_attributes.hpp"

using namespace iresearch;

TEST(fields_meta_test, ctor) {
  const fields_meta fm;
  ASSERT_EQ(0, fm.size());
  ASSERT_TRUE(fm.empty());
  ASSERT_EQ(iresearch::flags::empty_instance(), fm.features());
}

TEST(fields_meta_test, move) {
  std::vector<iresearch::field_meta> fields;
  fields.emplace_back("field0", 0, iresearch::flags{ increment::type(), offset::type() });
  fields.emplace_back("field1", 1, iresearch::flags{ document::type() });
  fields.emplace_back("field2", 2, iresearch::flags{ term_meta::type(), offset::type() });
  fields.emplace_back("field3", 3, iresearch::flags{ offset::type(), position::type() });
    
  iresearch::flags expected{
    increment::type(), offset::type(),
    document::type(), term_meta::type(),
    position::type()
  };
  
  // move ctor
  {
    iresearch::fields_meta moved(std::vector<field_meta>(fields.begin(), fields.end()), iresearch::flags(expected));
    iresearch::fields_meta meta(std::move(moved));

    ASSERT_EQ(0, moved.size());
    ASSERT_TRUE(moved.empty());
    ASSERT_EQ(flags::empty_instance(), moved.features());

    ASSERT_EQ(4, meta.size());
    ASSERT_FALSE(meta.empty());

    ASSERT_EQ(expected, meta.features());
  
    // check 
    for (auto& field : fields) {    
      auto* by_name = meta.find(field.name);
      ASSERT_NE(nullptr, by_name);
      auto* by_id = meta.find(field.id);
      ASSERT_NE(nullptr, by_id);
      ASSERT_EQ(by_name, by_id);
      ASSERT_EQ(field, *by_id);
      ASSERT_EQ(*by_name, *by_id);
    }
  }

  // move assignment
  {
    iresearch::fields_meta moved(std::vector<field_meta>(fields.begin(), fields.end()), iresearch::flags(expected));
    iresearch::fields_meta meta;

    meta = std::move(moved);
    ASSERT_EQ(0, moved.size());
    ASSERT_TRUE(moved.empty());
    ASSERT_EQ(flags::empty_instance(), moved.features());

    ASSERT_EQ(4, meta.size());
    ASSERT_FALSE(meta.empty());
    
    iresearch::flags expected{
      increment::type(), offset::type(),
      document::type(), term_meta::type(),
      position::type() 
    };
    ASSERT_EQ(expected, meta.features());

    // check 
    for (auto& field : fields) {    
      auto* by_name = meta.find(field.name);
      ASSERT_NE(nullptr, by_name);
      auto* by_id = meta.find(field.id);
      ASSERT_NE(nullptr, by_id);
      ASSERT_EQ(by_name, by_id);
      ASSERT_EQ(field, *by_id);
      ASSERT_EQ(*by_name, *by_id);
    }
  }
}