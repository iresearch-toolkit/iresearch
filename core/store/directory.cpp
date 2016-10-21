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

#include "shared.hpp"
#include "directory.hpp"
#include "data_input.hpp"
#include "data_output.hpp"
#include "utils/thread_utils.hpp"

NS_ROOT

index_lock::~index_lock() {}

directory::~directory() {}

bool index_lock::lock(index_lock& l, size_t wait_timeout /* = 1000 */) {
  bool locked = l.lock();

  const size_t max_sleep_count = wait_timeout / LOCK_POLL_INTERVAL;
  for (size_t sleep_count = 0; 
       !locked && (wait_timeout == LOCK_WAIT_FOREVER || sleep_count < max_sleep_count); 
       ++sleep_count) {
    sleep_ms(LOCK_POLL_INTERVAL);
    locked = l.lock();
  }

  return locked;
}


NS_END
