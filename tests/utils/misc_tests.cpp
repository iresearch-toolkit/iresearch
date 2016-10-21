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

#include "utils/misc.hpp"

namespace ir = iresearch;

TEST(misc_tests, vencode_size_32) {
  ASSERT_EQ(1, ir::vencode_size_32(0));
  ASSERT_EQ(1, ir::vencode_size_32(1));
  ASSERT_EQ(1, ir::vencode_size_32(100));
  ASSERT_EQ(1, ir::vencode_size_32(0x7F));

  ASSERT_EQ(2, ir::vencode_size_32(0xFF));
  ASSERT_EQ(2, ir::vencode_size_32(0x7D00));
  ASSERT_EQ(2, ir::vencode_size_32(0x7FFF));
  
  ASSERT_EQ(3, ir::vencode_size_32(0xFFFF));
  ASSERT_EQ(3, ir::vencode_size_32(0x42FFFF));
  ASSERT_EQ(3, ir::vencode_size_32(0x7FFFFF));
  
  ASSERT_EQ(4, ir::vencode_size_32(0xFFFFFF));
  ASSERT_EQ(4, ir::vencode_size_32(0x48FFFFFF));
  ASSERT_EQ(4, ir::vencode_size_32(0x7FFFFFFF));
  
  ASSERT_EQ(5, ir::vencode_size_32(0xFFFFFFFF));
  ASSERT_EQ(5, ir::vencode_size_32(0x80000000));
}
