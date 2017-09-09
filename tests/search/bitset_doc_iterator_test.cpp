////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "tests_shared.hpp"
#include "utils/bitset.hpp"
#include "search/bitset_doc_iterator.hpp"

TEST(bitset_iterator_test, sequential) {
  // empty bitset
  {
    irs::bitset bs;
    irs::bitset_doc_iterator it(bs);
    ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(it.value()));

    auto& attrs = it.attributes();
    auto& cost = attrs.get<irs::cost>();
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(0, cost->estimate());

    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(it.value()));

    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(it.value()));
  }

  // non-empty bitset
  {
    irs::bitset bs(13);
    irs::bitset_doc_iterator it(bs);
    ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(it.value()));

    auto& attrs = it.attributes();
    auto& cost = attrs.get<irs::cost>();
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(0, cost->estimate());

    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(it.value()));

    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(it.value()));
  }

  // dense bitset
  {
    const size_t size = 73;
    irs::bitset bs(size);

    // set all bits
    irs::bitset::word_t data[] {
      ~irs::bitset::word_t(0),
      ~irs::bitset::word_t(0),
      ~irs::bitset::word_t(0)
    };

    bs.memset(data);

    irs::bitset_doc_iterator it(bs);
    ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(it.value()));

    auto& attrs = it.attributes();
    auto& cost = attrs.get<irs::cost>();
    ASSERT_TRUE(bool(cost));
    ASSERT_EQ(size, cost->estimate());

    for (auto i = 0; i < size; ++i) {
      ASSERT_TRUE(it.next());
      ASSERT_EQ(irs::type_limits<irs::type_t::doc_id_t>::min() + i, it.value());
    }
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(it.value()));

  }

//  // sparse bitset
//  {
//    const size_t size = 176;
//    irs::bitset bs(size);
//
//
//
//    // set all bits
//    for (auto begin = bs.data(), end = begin + bs.words(); begin < end; ++begin) {
//      *begin = ~irs::bitset::word_t(0);
//    }
//
//    irs::bitset_doc_iterator it(bs);
//    ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::invalid(it.value()));
//
//    auto& attrs = it.attributes();
//    auto& cost = attrs.get<irs::cost>();
//    ASSERT_TRUE(bool(cost));
//    ASSERT_EQ(size, cost->estimate());
//
//    for (auto i = 0; i < size; ++i) {
//      ASSERT_TRUE(it.next());
//      ASSERT_EQ(i, it.value());
//    }
//    ASSERT_FALSE(it.next());
//    ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(it.value()));
//
//  }
}
