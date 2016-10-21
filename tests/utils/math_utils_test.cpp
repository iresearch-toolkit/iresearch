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
#include "utils/math_utils.hpp"

namespace math = iresearch::math;

TEST(math_utils_test, log) {
  ASSERT_EQ(0, math::log(1,2));
  ASSERT_EQ(1, math::log(235,235));
  ASSERT_EQ(2, math::log(49,7));
  ASSERT_EQ(3, math::log(8,2));
  ASSERT_EQ(4, math::log(12341, 7)); 
  ASSERT_EQ(6, math::log(34563456, 17)); 
  ASSERT_EQ(31, math::log(std::numeric_limits<uint32_t>::max(), 2));
}

TEST(math_utils_test, log2_32) {
  ASSERT_EQ(0, math::log2_32(1));
  ASSERT_EQ(1, math::log2_32(2));
  ASSERT_EQ(1, math::log2_32(3));
  ASSERT_EQ(2, math::log2_32(4));
  ASSERT_EQ(7, math::log2_32(128));
  ASSERT_EQ(10, math::log2_32(1025));
  ASSERT_EQ(31, math::log2_32(std::numeric_limits<uint32_t>::max()));
}

TEST(math_utils_test, log2_64) {
  ASSERT_EQ(0, math::log2_64(1));
  ASSERT_EQ(1, math::log2_64(2));
  ASSERT_EQ(1, math::log2_64(3));
  ASSERT_EQ(2, math::log2_64(4));
  ASSERT_EQ(7, math::log2_64(128));
  ASSERT_EQ(10, math::log2_64(1025));
  ASSERT_EQ(31, math::log2_64(std::numeric_limits<uint32_t>::max()));
  ASSERT_EQ(63, math::log2_64(std::numeric_limits<uint64_t>::max()));
}

TEST(math_utils, popcnt) {
  {
    const uint32_t value = 0;
    ASSERT_EQ(0, math::popcnt(&value, sizeof(uint32_t)));
  }
  
  {
    const uint32_t value = 1;
    ASSERT_EQ(1, math::popcnt(&value, sizeof(uint32_t)));
  }
  
  {
    const uint32_t value = 32768;
    ASSERT_EQ(0, math::popcnt(&value, 1)); // number of 1 in the 1st byte
    ASSERT_EQ(1, math::popcnt(&value, 2));
    ASSERT_EQ(1, math::popcnt(&value, sizeof(uint32_t)));
  }
 
  {
    const uint32_t value = 2863311530;
    ASSERT_EQ(16, math::popcnt(&value, sizeof(uint32_t)));
  }
 
  {
    const uint32_t value = std::numeric_limits<uint32_t>::max();
    ASSERT_EQ(8, math::popcnt(&value, 1)); // number of 1 in the 1st byte
    ASSERT_EQ(16, math::popcnt(&value, 2));
    ASSERT_EQ(24, math::popcnt(&value, 3));
    ASSERT_EQ(32, math::popcnt(&value, sizeof(uint32_t)));
  }
  
  {
    const uint64_t value = std::numeric_limits<uint64_t>::max();
    ASSERT_EQ(8, math::popcnt(&value, 1)); // number of 1 in the 1st byte
    ASSERT_EQ(16, math::popcnt(&value, 2));
    ASSERT_EQ(24, math::popcnt(&value, 3));
    ASSERT_EQ(32, math::popcnt(&value, 4));
    ASSERT_EQ(64, math::popcnt(&value, sizeof(uint64_t)));
  }
} 

TEST(math_utils, popcnt32) {
  ASSERT_EQ(0, math::popcnt32(0));
  ASSERT_EQ(1, math::popcnt32(1));
  ASSERT_EQ(16, math::popcnt32(2863311530));
  ASSERT_EQ(32, math::popcnt32(std::numeric_limits<uint32_t>::max()));
  
  ASSERT_EQ(0, math::pop32(0));
  ASSERT_EQ(1, math::pop32(1));
  ASSERT_EQ(16, math::pop32(2863311530));
  ASSERT_EQ(32, math::pop32(std::numeric_limits<uint32_t>::max()));
}

TEST(math_utils, popcnt64) {
  ASSERT_EQ(0, math::popcnt64(0));
  ASSERT_EQ(1, math::popcnt64(1));
  ASSERT_EQ(16, math::popcnt64(2863311530));
  ASSERT_EQ(32, math::popcnt64(std::numeric_limits<uint32_t>::max()));
  ASSERT_EQ(64, math::popcnt64(std::numeric_limits<uint64_t>::max()));
  
  ASSERT_EQ(0, math::pop64(0));
  ASSERT_EQ(1, math::pop64(1));
  ASSERT_EQ(16, math::pop64(2863311530));
  ASSERT_EQ(32, math::pop64(std::numeric_limits<uint32_t>::max()));
  ASSERT_EQ(64, math::pop64(std::numeric_limits<uint64_t>::max()));
}

TEST(math_utils, clz32) {
  ASSERT_EQ(32, math::clz32(0));
  ASSERT_EQ(31, math::clz32(1));
  ASSERT_EQ(9, math::clz32(4333568));
  ASSERT_EQ(0, math::clz32(std::numeric_limits<uint32_t>::max()));
}
