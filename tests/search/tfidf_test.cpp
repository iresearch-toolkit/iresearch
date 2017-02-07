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
#include "search/term_filter.hpp"
#include "search/tfidf.hpp"
#include "utils/utf8_path.hpp"

NS_BEGIN(tests)

class tfidf_test: public index_test_base { 
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
namespace ir = iresearch;

// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------

TEST_F(tfidf_test, test_load) {
  auto scorer = iresearch::scorers::get("tfidf");

  ASSERT_NE(nullptr, scorer);
}

#ifndef IRESEARCH_DLL

TEST_F(tfidf_test, test_order) {
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
  ord.add<iresearch::tfidf_sort>().reverse(true);
  auto prepared_order = ord.prepare();

  auto comparer = [&prepared_order] (const iresearch::bstring& lhs, const iresearch::bstring& rhs) {
    return prepared_order.less(lhs.c_str(), rhs.c_str());
  };
  
  uint64_t seq = 0;
  iresearch::columnstore_reader::value_reader_f visitor = [&seq](iresearch::data_input& in) {
    auto str_seq = iresearch::read_string<std::string>(in);
    seq = strtoull(str_seq.c_str(), nullptr, 10);
    return true;
  };
  auto values = segment.values("seq", visitor);

  {
    query.term("7");

    std::multimap<iresearch::bstring, uint64_t, decltype(comparer)> sorted(comparer);
    std::vector<uint64_t> expected{ 0, 1, 5, 7 };

    auto prepared = query.prepare(reader, prepared_order);
    auto docs = prepared->execute(segment, prepared_order);
    auto& score = docs->attributes().get<iresearch::score>();
    for (; docs->next();) {
      docs->score();
      ASSERT_TRUE(values(docs->value()));
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