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
#include "utils/type_utils.hpp"

TEST(type_utils_tests, template_traits) {
  // test count
  {
    ASSERT_EQ(0, (irs::template_traits_t<>::count()));
    ASSERT_EQ(1, (irs::template_traits_t<int32_t>::count()));
    ASSERT_EQ(2, (irs::template_traits_t<int32_t, bool>::count()));
    ASSERT_EQ(3, (irs::template_traits_t<int32_t, bool, int64_t>::count()));
    ASSERT_EQ(4, (irs::template_traits_t<int32_t, bool, int64_t, short>::count()));
  }

  // test size
  {
    ASSERT_EQ(0, (irs::template_traits_t<>::size()));
    ASSERT_EQ(4, (irs::template_traits_t<int32_t>::size()));
    ASSERT_EQ(5, (irs::template_traits_t<int32_t, bool>::size()));
    ASSERT_EQ(13, (irs::template_traits_t<int32_t, bool, int64_t>::size()));
    ASSERT_EQ(15, (irs::template_traits_t<int32_t, bool, int64_t, short>::size()));
  }

  // test size_alligned
  {
    ASSERT_EQ(0, (irs::template_traits_t<>::size_aligned()));
    ASSERT_EQ(4, (irs::template_traits_t<int32_t>::size_aligned()));
    ASSERT_EQ(5, (irs::template_traits_t<int32_t, bool>::size_aligned()));
    ASSERT_EQ(16, (irs::template_traits_t<int32_t, bool, int64_t>::size_aligned()));
    ASSERT_EQ(18, (irs::template_traits_t<int32_t, bool, int64_t, short>::size_aligned()));
  }

  // test size_alligned with offset
  {
    ASSERT_EQ(3, (irs::template_traits_t<>::size_aligned(3)));
    ASSERT_EQ(8, (irs::template_traits_t<int32_t>::size_aligned(3)));
    ASSERT_EQ(9, (irs::template_traits_t<int32_t, bool>::size_aligned(3)));
    ASSERT_EQ(24, (irs::template_traits_t<int32_t, bool, int64_t>::size_aligned(3)));
    ASSERT_EQ(26, (irs::template_traits_t<int32_t, bool, int64_t, short>::size_aligned(3)));
  }
}
