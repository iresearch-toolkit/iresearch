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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "tests_shared.hpp"
#include "iql/query_builder.hpp"

#include "index_tests.hpp"

NS_LOCAL

class sorted_index_test_case : public tests::index_test_base {

};

TEST_P(sorted_index_test_case, check_document_order) {
  struct comparer : irs::comparer {
    virtual bool less(const irs::bytes_ref& lhs, const irs::bytes_ref& rhs) const {
      const auto lhs_value = irs::to_string<irs::bytes_ref>(lhs.c_str());
      const auto rhs_value = irs::to_string<irs::bytes_ref>(rhs.c_str());

      return lhs_value > rhs_value;
    }
  } less;

  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [] (tests::document& doc, const std::string& name, const tests::json_doc_generator::json_value& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::templates::string_field>(
          irs::string_ref(name),
          data.str,
          name == "name" ? irs::flags{irs::sorted::type()} : irs::flags{}
        ));
      }
  });

  auto* doc0 = gen.next(); // name == 'A'
  auto* doc1 = gen.next(); // name == 'B'

  // create index segment
  {
    irs::index_writer::init_options opts;
    opts.comparator = &less;

    auto writer = open_writer(irs::OM_CREATE, opts);
    ASSERT_NE(nullptr, writer);
    ASSERT_NE(nullptr, writer->comparator());

    ASSERT_TRUE(insert(*writer,
      doc0->indexed.begin(), doc0->indexed.end(),
      doc0->stored.begin(), doc0->stored.end()
    ));
    ASSERT_TRUE(insert(*writer,
      doc1->indexed.begin(), doc1->indexed.end(),
      doc1->stored.begin(), doc1->stored.end()
    ));

    writer->commit();
  }

  // read documents
  {
    auto reader = irs::directory_reader::open(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    irs::bytes_ref actual_value;

    auto& segment = reader[0];
    const auto* column = segment.sort();
    ASSERT_NE(nullptr, column);
    auto values = column->values();
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    ASSERT_TRUE(values(docsItr->value(), actual_value));
    ASSERT_EQ("B", irs::to_string<irs::string_ref>(actual_value.c_str()));
    ASSERT_TRUE(docsItr->next());
    ASSERT_TRUE(values(docsItr->value(), actual_value));
    ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
    ASSERT_FALSE(docsItr->next());
  }
}

INSTANTIATE_TEST_CASE_P(
  sorted_index_test,
  sorted_index_test_case,
  ::testing::Combine(
    ::testing::Values(
      &tests::memory_directory,
      &tests::fs_directory,
      &tests::mmap_directory
    ),
    ::testing::Values("1_0", "1_1")
  ),
  tests::to_string
);

NS_END
