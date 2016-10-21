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

#ifndef IRESEARCH_TEST_UTILS_H
#define IRESEARCH_TEST_UTILS_H

#include <algorithm>

namespace tests {

template<typename Cont>
void assert_equal( const Cont& expect, const Cont& actual) {
  ASSERT_EQ( expect.size(), actual.size());
  ASSERT_TRUE( std::equal( expect.begin(), expect.end(), actual.begin()));
}

} // tests

#endif
