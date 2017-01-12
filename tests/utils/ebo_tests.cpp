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
#include "utils/ebo.hpp"

TEST(compact_pair_tests, check_size) {
  struct empty { };
  struct non_empty { int i; };

  static_assert(
    sizeof(iresearch::compact_pair<empty, non_empty>) == sizeof(non_empty),
    "Invalid size"
  );
  
  static_assert(
    sizeof(iresearch::compact_pair<non_empty, empty>) == sizeof(non_empty),
    "Invalid size"
  );
  
  static_assert(
    sizeof(iresearch::compact_pair<non_empty, non_empty>) == 2*sizeof(non_empty),
    "Invalid size"
  );
}
