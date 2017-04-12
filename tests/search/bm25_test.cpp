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
#include "index/index_tests.hpp"
#include "store/memory_directory.hpp"
#include "search/scorers.hpp"
#include "search/sort.hpp"
#include "search/bm25.hpp"
#include "search/term_filter.hpp"
#include "utils/utf8_path.hpp"

NS_BEGIN(tests)

class bm25_test: public index_test_base { 
 protected:
  virtual iresearch::directory* get_directory() {
    return new iresearch::memory_directory();
   }

  virtual iresearch::format::ptr get_codec() {
    return ir::formats::get("1_0");
  }
};

NS_END

using namespace tests;

// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------

TEST_F(bm25_test, test_load) {
  irs::order order;
  auto scorer = irs::scorers::get("bm25", irs::string_ref::nil);

  ASSERT_NE(nullptr, scorer);
  ASSERT_EQ(1, order.add(scorer).size());
}

#ifndef IRESEARCH_DLL

TEST_F(bm25_test, test_order) {
  {
    tests::json_doc_generator gen(
      resource("simple_sequential_order.json"),
      [](tests::document& doc, const std::string& name, const tests::json::json_value& data) {
        static const std::string s_seq = "seq";
        static const std::string s_field = "field";
        doc.insert(std::make_shared<templates::string_field>(name, data.value), name == s_field, name == s_seq);
    });
    add_segment(gen);
  }

  auto reader = iresearch::directory_reader::open(dir(), codec());
  auto& segment = *(reader.begin());

  iresearch::by_term query;
  query.field("field");

  iresearch::order ord;
  ord.add<iresearch::bm25_sort>(irs::string_ref::nil);
  auto prepared_order = ord.prepare();

  auto comparer = [&prepared_order] (const iresearch::bstring& lhs, const iresearch::bstring& rhs) {
    return prepared_order.less(lhs.c_str(), rhs.c_str());
  };

  uint64_t seq = 0;
  auto values = segment.values("seq");

  {
    query.term("7");

    std::multimap<iresearch::bstring, uint64_t, decltype(comparer)> sorted(comparer);
    std::vector<uint64_t> expected{ 0, 1, 5, 7 };

    irs::bytes_ref actual_value;
    irs::bytes_ref_input in;
    auto prepared = query.prepare(reader, prepared_order);
    auto docs = prepared->execute(segment, prepared_order);
    auto& score = docs->attributes().get<iresearch::score>();
    for (; docs->next();) {
      docs->score();
      ASSERT_TRUE(values(docs->value(), actual_value));
      in.reset(actual_value);

      auto str_seq = iresearch::read_string<std::string>(in);
      seq = strtoull(str_seq.c_str(), nullptr, 10);
      sorted.emplace(score->value(), seq);
    }

    ASSERT_EQ(expected.size(), sorted.size());
    const bool eq = std::equal(
      sorted.begin(), sorted.end(), expected.begin(),
      [](const std::pair<iresearch::bstring, uint64_t>& lhs, uint64_t rhs) {
        return lhs.second == rhs;
    });
    ASSERT_TRUE(eq);
  }
}

#endif