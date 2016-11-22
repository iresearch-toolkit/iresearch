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

TEST(field_meta_test, ctor) {
  {
    const field_meta fm;
    ASSERT_EQ("", fm.name);
    ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::field_id_t>::valid(fm.id));
    ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::field_id_t>::valid(fm.norm));
    ASSERT_EQ(iresearch::flags::empty_instance(), fm.features);
  }

  {
    iresearch::flags features;
    features.add<iresearch::offset>();
    features.add<increment>();
    features.add<offset>();

    const std::string name("name");
    const field_meta fm(name, 0, features, 5);
    ASSERT_EQ(name, fm.name);
    ASSERT_EQ(0, fm.id);
    ASSERT_EQ(features, fm.features);
    ASSERT_EQ(5, fm.norm);
  }
}

TEST(field_meta_test, move) {
  iresearch::flags features;
  features.add<iresearch::offset>();
  features.add<increment>();
  features.add<offset>();

  const field_id id = 5;
  const field_id norm = 10;
  const std::string name("name");

  // ctor
  {
    field_meta moved;
    moved.id = id;
    moved.name = name;
    moved.features = features;
    moved.norm = norm;

    field_meta fm(std::move(moved));
    ASSERT_EQ(name, fm.name);
    ASSERT_EQ(id, fm.id);
    ASSERT_EQ(features, fm.features);
    ASSERT_EQ(norm, fm.norm);

    ASSERT_EQ("", moved.name);
    ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::field_id_t>::valid(moved.id));
    ASSERT_EQ(iresearch::flags::empty_instance(), moved.features);
    ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::field_id_t>::valid(moved.norm));
  }

  // assign operator
  {
    field_meta moved;
    moved.id = id;
    moved.name = name;
    moved.features = features;
    moved.norm = norm;

    field_meta fm;
    ASSERT_EQ("", fm.name);
    ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::field_id_t>::valid(fm.id));
    ASSERT_EQ(iresearch::flags::empty_instance(), fm.features);
    ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::field_id_t>::valid(fm.norm));

    fm = std::move(moved);
    ASSERT_EQ(name, fm.name);
    ASSERT_EQ(id, fm.id);
    ASSERT_EQ(features, fm.features);
    ASSERT_EQ(norm, fm.norm);

    ASSERT_EQ("", moved.name);
    ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::field_id_t>::valid(moved.id));
    ASSERT_EQ(iresearch::flags::empty_instance(), moved.features);
    ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::field_id_t>::valid(moved.norm));
  }
}

TEST(field_meta_test, compare) {
  iresearch::flags features;
  features.add<offset>();
  features.add<increment>();
  features.add<offset>();
  features.add<document>();

  const field_id id = 5;
  const std::string name("name");

  field_meta lhs;
  lhs.id = id;
  lhs.name = name;
  lhs.features = features;
  field_meta rhs = lhs;
  ASSERT_EQ(lhs, rhs);

  rhs.name = "test";
  ASSERT_NE(lhs, rhs);
  lhs.name = "test";
  ASSERT_EQ(lhs, rhs);

  rhs.id = iresearch::type_limits<iresearch::type_t::field_id_t>::invalid();
  ASSERT_EQ(lhs, rhs);
}
