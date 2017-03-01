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
#include "utils/memory_pool.hpp"
#include "utils/memory.hpp"
#include "utils/timer_utils.hpp"

#include <list>
#include <set>
#include <map>

TEST(memory_pool_test, allocate_deallocate) {
  irs::memory::memory_pool<> pool(64);
  ASSERT_EQ(64, pool.slot_size());
  ASSERT_EQ(0, pool.capacity());
  ASSERT_TRUE(pool.empty());
  ASSERT_EQ(32, pool.next_size());

  // allocate 1 node
  ASSERT_NE(nullptr, pool.allocate());
  ASSERT_EQ(32, pool.capacity());
  ASSERT_FALSE(pool.empty());
  ASSERT_EQ(64, pool.next_size()); // log2_grow by default

  // allocate continious block of 31 nodes
  ASSERT_NE(nullptr, pool.allocate(31));
  ASSERT_EQ(32, pool.capacity());
  ASSERT_FALSE(pool.empty());
  ASSERT_EQ(64, pool.next_size()); // log2_grow by default

  // allocate 1 node (should cause pool growth)
  ASSERT_NE(nullptr, pool.allocate());
  ASSERT_EQ(32+64, pool.capacity());
  ASSERT_FALSE(pool.empty());
  ASSERT_EQ(128, pool.next_size()); // log2_grow by default

  // allocate 65 nodes (should cause pool growth)
  for (size_t i = 0; i < 65; ++i) {
    ASSERT_NE(nullptr, pool.allocate());
  }
  ASSERT_EQ(32+64+128, pool.capacity());
  ASSERT_FALSE(pool.empty());
  ASSERT_EQ(256, pool.next_size()); // log2_grow by default

  // allocate 126 nodes (should not cause pool growth)
  for (size_t i = 0; i < 126; ++i) {
    ASSERT_NE(nullptr, pool.allocate());
  }
  ASSERT_EQ(32+64+128, pool.capacity());
  ASSERT_FALSE(pool.empty());
  ASSERT_EQ(256, pool.next_size()); // log2_grow by default

  // allocate 1 node (should cause pool growth)
  ASSERT_NE(nullptr, pool.allocate());
  ASSERT_EQ(32+64+128+256, pool.capacity());
  ASSERT_FALSE(pool.empty());
  ASSERT_EQ(512, pool.next_size()); // log2_grow by default

  // allocate continious block of 256 slots (should cause pool growth)
  ASSERT_NE(nullptr, pool.allocate(256));
  ASSERT_EQ(32+64+128+256+512, pool.capacity());
  ASSERT_FALSE(pool.empty());
  ASSERT_EQ(1024, pool.next_size()); // log2_grow by default

  // allocate 255+256 nodes (should not cause pool growth)
  for (size_t i = 0; i < (255+256); ++i) {
    ASSERT_NE(nullptr, pool.allocate());
  }
  ASSERT_EQ(32+64+128+256+512, pool.capacity());
  ASSERT_FALSE(pool.empty());
  ASSERT_EQ(1024, pool.next_size()); // log2_grow by default
}

TEST(memory_pool_allocator_test, allocate_deallocate) {
  size_t ctor_calls = 0;
  size_t dtor_calls = 0;

  struct checker {
    checker(size_t& ctor_calls, size_t& dtor_calls)
      : dtor_calls_(&dtor_calls) {
      ++ctor_calls;
    }

    ~checker() {
      ++(*dtor_calls_);
    }

    size_t* dtor_calls_;
  }; // test

  irs::memory::memory_pool_allocator<
    checker
  > pool;

  auto* p = pool.allocate(1);
  ASSERT_EQ(0, ctor_calls);
  ASSERT_EQ(0, dtor_calls);
  pool.construct(p, ctor_calls, dtor_calls);
  ASSERT_EQ(1, ctor_calls);
  ASSERT_EQ(0, dtor_calls);
  pool.destroy(p);
  ASSERT_EQ(1, ctor_calls);
  ASSERT_EQ(1, dtor_calls);
  pool.deallocate(p, 1);
}

TEST(memory_pool_allocator_test, profile_std_map) {
  struct test_data {
    test_data(size_t i)
      : a(i), b(i), c(i), d(i) {
    }

    bool operator<(const test_data& rhs) const {
      return a < rhs.a;
    }

    size_t a;
    size_t b;
    size_t c;
    size_t d;
  };

  auto comparer = [](const test_data& lhs, const test_data& rhs) {
    return lhs.a == rhs.a;
  };

  const size_t size = 10000;
  iresearch::timer_utils::init_stats(true);

  // default allocator
  {
    std::map<size_t, test_data, std::less<test_data>> data;
    for (size_t i = 0; i < size; ++i) {
      REGISTER_TIMER__("std::allocator", __LINE__);
      data.emplace(i, i);
    }
  }

  // pool allocator
  {
    typedef irs::memory::memory_pool_allocator<
      test_data,
      irs::memory::identity_grow,
      irs::memory::malloc_free_allocator,
      irs::memory::single_allocator_tag
    > pool_t;

    std::map<size_t, test_data, std::less<test_data>, pool_t> data;
    for (size_t i = 0; i < size; ++i) {
      REGISTER_TIMER__("irs::allocator", __LINE__);
      data.emplace(i, i);
    }

    // check data
    for (size_t i = 0; i < size; ++i) {
      const auto it = data.find(i);
      ASSERT_NE(data.end(), it);
      ASSERT_EQ(i, it->second.a);
    }
  }

  flush_timers(std::cout);
}

TEST(memory_pool_allocator_test, profile_std_list) {
  struct test_data {
    test_data(size_t i)
      : a(i), b(i), c(i), d(i) {
    }

    size_t a;
    size_t b;
    size_t c;
    size_t d;
  };

  const size_t size = 10000;
  iresearch::timer_utils::init_stats(true);

  // default allocator
  {
    std::list<test_data> data;
    for (size_t i = 0; i < size; ++i) {
      REGISTER_TIMER__("std::allocator", __LINE__);
      data.emplace_back(i);
    }
  }

  // pool allocator
  {
    typedef irs::memory::memory_pool_allocator<
      test_data,
      irs::memory::identity_grow,
      irs::memory::malloc_free_allocator,
      irs::memory::single_allocator_tag
    > pool_t;

    std::list<test_data, pool_t> data;
    for (size_t i = 0; i < size; ++i) {
      REGISTER_TIMER__("irs::allocator", __LINE__);
      data.emplace_back(i);
    }

    // check data
    size_t i = 0;
    for (auto& item : data) {
      ASSERT_EQ(i, item.a);
      ++i;
    }
  }

  flush_timers(std::cout);
}

TEST(memory_pool_allocator_test, profile_std_set) {
  struct test_data {
    test_data(size_t i)
      : a(i), b(i), c(i), d(i) {
    }

    bool operator<(const test_data& rhs) const {
      return a < rhs.a;
    }

    size_t a;
    size_t b;
    size_t c;
    size_t d;
  };

  auto comparer = [](const test_data& lhs, const test_data& rhs) {
    return lhs.a == rhs.a;
  };

  const size_t size = 10000;
  iresearch::timer_utils::init_stats(true);

  // default allocator
  {
    std::set<test_data, std::less<test_data>> data;
    for (size_t i = 0; i < size; ++i) {
      REGISTER_TIMER__("std::allocator", __LINE__);
      data.emplace(i);
    }
  }

  // pool allocator
  {
    typedef irs::memory::memory_pool_allocator<
      test_data,
      irs::memory::identity_grow,
      irs::memory::malloc_free_allocator,
      irs::memory::single_allocator_tag
    > pool_t;

    std::set<test_data, std::less<test_data>, pool_t> data;
    for (size_t i = 0; i < size; ++i) {
      REGISTER_TIMER__("irs::allocator", __LINE__);
      data.emplace(i);
    }

    // check data
    for (size_t i = 0; i < size; ++i) {
      const auto it = data.find(test_data(i));
      ASSERT_NE(data.end(), it);
      ASSERT_EQ(i, it->a);
    }
  }

  flush_timers(std::cout);
}

TEST(memory_pool_allocator_test, allocate_unique) {
  size_t ctor_calls = 0;
  size_t dtor_calls = 0;

  struct checker {
    checker(size_t& ctor_calls, size_t& dtor_calls)
      : dtor_calls_(&dtor_calls) {
      ++ctor_calls;
    }

    ~checker() {
      ++(*dtor_calls_);
    }

    size_t* dtor_calls_;
  }; // test

  irs::memory::memory_pool_allocator<
    checker,
    irs::memory::identity_grow,
    irs::memory::malloc_free_allocator,
    irs::memory::single_allocator_tag
  > pool;

  {
    auto ptr = irs::memory::allocate_unique<checker>(pool, ctor_calls, dtor_calls);
    ASSERT_EQ(1, ctor_calls);
    ASSERT_EQ(0, dtor_calls);
  }
  ASSERT_EQ(1, ctor_calls);
  ASSERT_EQ(1, dtor_calls);
}

TEST(memory_pool_allocator_test, allocate_shared) {
  size_t ctor_calls = 0;
  size_t dtor_calls = 0;

  struct checker {
    checker(size_t& ctor_calls, size_t& dtor_calls)
      : dtor_calls_(&dtor_calls) {
      ++ctor_calls;
    }

    ~checker() {
      ++(*dtor_calls_);
    }

    size_t* dtor_calls_;
  }; // test

  // can't use memory_pool_allocator here since
  // allocator should satisfy CopyConstructible concept
  irs::memory::memory_multi_size_pool<
    irs::memory::identity_grow,
    irs::memory::malloc_free_allocator
  > pool;

  irs::memory::memory_pool_multi_size_allocator<
    checker,
    irs::memory::identity_grow,
    irs::memory::malloc_free_allocator,
    irs::memory::single_allocator_tag
  > alloc(pool);

  {
    auto ptr = std::allocate_shared<checker>(alloc, ctor_calls, dtor_calls);
    ASSERT_EQ(1, ctor_calls);
    ASSERT_EQ(0, dtor_calls);
  }
  ASSERT_EQ(1, ctor_calls);
  ASSERT_EQ(1, dtor_calls);
}
