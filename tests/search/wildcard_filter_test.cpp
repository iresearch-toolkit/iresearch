////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "tests_shared.hpp"
#include "filter_test_case_base.hpp"
#include "search/wildcard_filter.hpp"

TEST(by_wildcard_test, ctor) {
  irs::by_wildcard q;
  ASSERT_EQ(irs::by_wildcard::type(), q.type());
  ASSERT_TRUE(q.term().empty());
  ASSERT_TRUE(q.field().empty());
  ASSERT_EQ(irs::no_boost(), q.boost());
}

TEST(by_wildcard_test, equal) { 
  irs::by_wildcard q;
  q.field("field").term("bar*");

  ASSERT_EQ(q, irs::by_wildcard().field("field").term("bar*"));
  ASSERT_NE(q, irs::by_term().field("field").term("bar*"));
  ASSERT_NE(q, irs::by_term().field("field1").term("bar*"));
}

TEST(by_wildcard_test, boost) {
  // no boost
  {
    irs::by_wildcard q;
    q.field("field").term("bar*");

    auto prepared = q.prepare(irs::sub_reader::empty());
    ASSERT_EQ(irs::no_boost(), prepared->boost());
  }

  // with boost
  {
    irs::boost_t boost = 1.5f;

    irs::by_wildcard q;
    q.field("field").term("bar*");
    q.boost(boost);

    auto prepared = q.prepare(irs::sub_reader::empty());
    ASSERT_EQ(boost, prepared->boost());
  }
}
