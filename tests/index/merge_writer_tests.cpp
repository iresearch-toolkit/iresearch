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

#include "tests_shared.hpp"
#include "index_tests.hpp"
#include "formats/formats_10.hpp"
#include "iql/query_builder.hpp"
#include "store/memory_directory.hpp"
#include "utils/type_limits.hpp"
#include "index/merge_writer.hpp"

namespace tests {
  class merge_writer_tests: public ::testing::Test {

    virtual void SetUp() {
      // Code here will be called immediately after the constructor (right before each test).
    }

    virtual void TearDown() {
      // Code here will be called immediately after each test (right before the destructor).
    }
  };

  template<typename T>
  void validate_terms(
    const iresearch::term_reader& terms,
    uint64_t doc_count,
    const iresearch::bytes_ref& min,
    const iresearch::bytes_ref& max,
    size_t term_size,
    const iresearch::flags& term_features,
    std::unordered_map<T, std::unordered_set<iresearch::doc_id_t>>& expected_terms,
    size_t* frequency = nullptr,
    std::vector<uint32_t>* position = nullptr
  ) {
    ASSERT_EQ(doc_count, terms.docs_count());
    ASSERT_EQ(max, terms.max());
    ASSERT_EQ(min, terms.min());
    ASSERT_EQ(term_size, terms.size());
    ASSERT_EQ(term_features, terms.features());

    for (auto term_itr = terms.iterator(); term_itr->next();) {
      auto itr = expected_terms.find(term_itr->value());

      ASSERT_NE(expected_terms.end(), itr);

      for (auto docs_itr = term_itr->postings(term_features); docs_itr->next();) {
        auto& attrs = docs_itr->attributes();

        ASSERT_EQ(1, itr->second.erase(docs_itr->value()));
        ASSERT_EQ(1 + (frequency ? 1 : 0) + (position ? 1 : 0), attrs.size());
        ASSERT_TRUE(attrs.contains(iresearch::document::type()));

        if (frequency) {
          ASSERT_TRUE(attrs.contains(iresearch::frequency::type()));
          ASSERT_EQ(*frequency, attrs.get<iresearch::frequency>()->value);
        }

        if (position) {
          ASSERT_TRUE(attrs.contains(iresearch::position::type()));

          for (auto pos: *position) {
            ASSERT_TRUE(attrs.get<iresearch::position>()->next());
            ASSERT_EQ(pos, attrs.get<iresearch::position>()->value());
          }

          ASSERT_FALSE(attrs.get<iresearch::position>()->next());
        }
      }

      ASSERT_TRUE(itr->second.empty());
      expected_terms.erase(itr);
    }

    ASSERT_TRUE(expected_terms.empty());
  }
}

using namespace tests;

// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------

TEST_F(merge_writer_tests, test_merge_writer) {
  iresearch::version10::format codec;
  iresearch::format::ptr codec_ptr(&codec, [](iresearch::format*)->void{});
  iresearch::memory_directory dir;

  iresearch::bstring bytes1;
  iresearch::bstring bytes2;
  iresearch::bstring bytes3;

  bytes1.append(iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("bytes1_data")));
  bytes2.append(iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("bytes2_data")));
  bytes3.append(iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("bytes3_data")));

  iresearch::flags STRING_FIELD_FEATURES{ iresearch::frequency::type(), iresearch::position::type() };
  iresearch::flags TEXT_FIELD_FEATURES{ iresearch::frequency::type(), iresearch::position::type(), iresearch::offset::type(), iresearch::payload::type() };

  std::string string1;
  std::string string2;
  std::string string3;
  std::string string4;

  string1.append("string1_data");
  string2.append("string2_data");
  string3.append("string3_data");
  string4.append("string4_data");

  std::string text1;
  std::string text2;
  std::string text3;

  text1.append("text1_data");
  text2.append("text2_data");
  text3.append("text3_data");

  tests::document doc1;
  tests::document doc2;
  tests::document doc3;
  tests::document doc4;

  doc1.add(new tests::binary_field()); {
    auto& field = doc1.back<tests::binary_field>();
    field.name(iresearch::string_ref("doc_bytes"));
    field.value(bytes1);
  }
  doc2.add(new tests::binary_field()); {
    auto& field = doc2.back<tests::binary_field>();
    field.name(iresearch::string_ref("doc_bytes"));
    field.value(bytes2);
  }
  doc3.add(new tests::binary_field()); {
    auto& field = doc3.back<tests::binary_field>();
    field.name(iresearch::string_ref("doc_bytes"));
    field.value(bytes3);
  }
  doc1.add(new tests::double_field()); {
    auto& field = doc1.back<tests::double_field>();
    field.name(iresearch::string_ref("doc_double"));
    field.value(2.718281828 * 1);
  }
  doc2.add(new tests::double_field()); {
    auto& field = doc2.back<tests::double_field>();
    field.name(iresearch::string_ref("doc_double"));
    field.value(2.718281828 * 2);
  }
  doc3.add(new tests::double_field()); {
    auto& field = doc3.back<tests::double_field>();
    field.name(iresearch::string_ref("doc_double"));
    field.value(2.718281828 * 3);
  }
  doc1.add(new tests::float_field()); {
    auto& field = doc1.back<tests::float_field>();
    field.name(iresearch::string_ref("doc_float"));
    field.value(3.1415926535f * 1);
  }
  doc2.add(new tests::float_field()); {
    auto& field = doc2.back<tests::float_field>();
    field.name(iresearch::string_ref("doc_float"));
    field.value(3.1415926535f * 2);
  }
  doc3.add(new tests::float_field()); {
    auto& field = doc3.back<tests::float_field>();
    field.name(iresearch::string_ref("doc_float"));
    field.value(3.1415926535f * 3);
  }
  doc1.add(new tests::int_field()); {
    auto& field = doc1.back<tests::int_field>();
    field.name(iresearch::string_ref("doc_int"));
    field.value(42 * 1);
  }
  doc2.add(new tests::int_field()); {
    auto& field = doc2.back<tests::int_field>();
    field.name(iresearch::string_ref("doc_int"));
    field.value(42 * 2);
  }
  doc3.add(new tests::int_field()); {
    auto& field = doc3.back<tests::int_field>();
    field.name(iresearch::string_ref("doc_int"));
    field.value(42 * 3);
  }
  doc1.add(new tests::long_field()); {
    auto& field = doc1.back<tests::long_field>();
    field.name(iresearch::string_ref("doc_long"));
    field.value(12345 * 1);
  }
  doc2.add(new tests::long_field()); {
    auto& field = doc2.back<tests::long_field>();
    field.name(iresearch::string_ref("doc_long"));
    field.value(12345 * 2);
  }
  doc3.add(new tests::long_field()); {
    auto& field = doc3.back<tests::long_field>();
    field.name(iresearch::string_ref("doc_long"));
    field.value(12345 * 3);
  }
  doc1.add(new tests::templates::string_field("doc_string", string1, true, true));
  doc2.add(new tests::templates::string_field("doc_string", string2, true, true));
  doc3.add(new tests::templates::string_field("doc_string", string3, true, true));
  doc4.add(new tests::templates::string_field("doc_string", string4, true, true));
  doc1.add(new tests::templates::text_field<iresearch::string_ref>("doc_text", text1, true));
  doc2.add(new tests::templates::text_field<iresearch::string_ref>("doc_text", text2, true));
  doc3.add(new tests::templates::text_field<iresearch::string_ref>("doc_text", text3, true));

  // populate directory
  {
    auto query_doc4 = iresearch::iql::query_builder().build("doc_string==string4_data", std::locale::classic());
    auto writer = iresearch::index_writer::make(dir, codec_ptr, iresearch::OM_CREATE);

    writer->add(doc1.begin(), doc1.end());
    writer->add(doc2.begin(), doc2.end());
    writer->commit();
    writer->add(doc3.begin(), doc3.end());
    writer->add(doc4.begin(), doc4.end());
    writer->commit();
    writer->remove(std::move(query_doc4.filter));
    writer->commit();
    writer->close();
  }

  auto reader = iresearch::directory_reader::open(dir, codec_ptr);
  iresearch::merge_writer writer(dir, codec_ptr, "merged");

  ASSERT_EQ(2, reader->size());
  ASSERT_EQ(2, (*reader)[0].docs_count());
  ASSERT_EQ(2, (*reader)[1].docs_count());

  // validate initial data (segment 0)
  {
    auto& segment = (*reader)[0];
    ASSERT_EQ(2, segment.docs_count());

    auto& fields = segment.fields();

    ASSERT_EQ(7, fields.size());

    // validate bytes field
    {
      auto field = fields.find("doc_bytes");
      auto terms = segment.terms("doc_bytes");
      auto features = tests::binary_field().features();
      std::unordered_map<iresearch::bytes_ref, std::unordered_set<iresearch::doc_id_t>> expected_terms;

      expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("bytes1_data"))].emplace(1);
      expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("bytes2_data"))].emplace(2);

      ASSERT_EQ(2, segment.docs_count("doc_bytes"));
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(features, field->features);
      ASSERT_NE(nullptr, terms);
      validate_terms(
        *terms,
        2,
        bytes1,
        bytes2,
        2,
        features,
        expected_terms
      );
    }

    // validate double field
    {
      auto field = fields.find("doc_double");
      auto terms = segment.terms("doc_double");
      auto features = tests::double_field().features();
      iresearch::numeric_token_stream max;
      max.reset((double_t) (2.718281828 * 2));
      iresearch::numeric_token_stream min;
      min.reset((double_t) (2.718281828 * 1));
      std::unordered_map<iresearch::bstring, std::unordered_set<iresearch::doc_id_t>> expected_terms;

      {
        iresearch::numeric_token_stream itr;
        itr.reset((double_t) (2.718281828 * 1));
        for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(1));
      }

      {
        iresearch::numeric_token_stream itr;
        itr.reset((double_t) (2.718281828 * 2));
        for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(2));
      }

      ASSERT_EQ(2, segment.docs_count("doc_double"));
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(features, field->features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next() && max.next() && max.next()); // skip to last value
      ASSERT_TRUE(min.next()); // skip to first value
      validate_terms(
        *terms,
        2,
        min.attributes().get<iresearch::term_attribute>()->value(),
        max.attributes().get<iresearch::term_attribute>()->value(),
        8,
        features,
        expected_terms
      );
    }

    // validate float field
    {
      auto field = fields.find("doc_float");
      auto terms = segment.terms("doc_float");
      auto features = tests::float_field().features();
      iresearch::numeric_token_stream max;
      max.reset((float_t) (3.1415926535 * 2));
      iresearch::numeric_token_stream min;
      min.reset((float_t) (3.1415926535 * 1));
      std::unordered_map<iresearch::bstring, std::unordered_set<iresearch::doc_id_t>> expected_terms;

      {
        iresearch::numeric_token_stream itr;
        itr.reset((float_t) (3.1415926535 * 1));
        for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(1));
      }

      {
        iresearch::numeric_token_stream itr;
        itr.reset((float_t) (3.1415926535 * 2));
        for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(2));
      }

      ASSERT_EQ(2, segment.docs_count("doc_float"));
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(features, field->features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next()); // skip to last value
      ASSERT_TRUE(min.next()); // skip to first value
      validate_terms(
        *terms,
        2,
        min.attributes().get<iresearch::term_attribute>()->value(),
        max.attributes().get<iresearch::term_attribute>()->value(),
        4,
        features,
        expected_terms
      );
    }

    // validate int field
    {
      auto field = fields.find("doc_int");
      auto terms = segment.terms("doc_int");
      auto features = tests::int_field().features();
      iresearch::numeric_token_stream max;
      max.reset(42 * 2);
      iresearch::numeric_token_stream min;
      min.reset(42 * 1);
      std::unordered_map<iresearch::bstring, std::unordered_set<iresearch::doc_id_t>> expected_terms;

      {
        iresearch::numeric_token_stream itr;
        itr.reset(42 * 1);
        for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(1));
      }

      {
        iresearch::numeric_token_stream itr;
        itr.reset(42 * 2);
        for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(2));
      }

      ASSERT_EQ(2, segment.docs_count("doc_int"));
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(features, field->features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next()); // skip to last value
      ASSERT_TRUE(min.next()); // skip to first value
      validate_terms(
        *terms,
        2,
        min.attributes().get<iresearch::term_attribute>()->value(),
        max.attributes().get<iresearch::term_attribute>()->value(),
        3,
        features,
        expected_terms
      );
    }

    // validate long field
    {
      auto field = fields.find("doc_long");
      auto terms = segment.terms("doc_long");
      auto features = tests::long_field().features();
      iresearch::numeric_token_stream max;
      max.reset((int64_t) 12345 * 2);
      iresearch::numeric_token_stream min;
      min.reset((int64_t) 12345 * 1);
      std::unordered_map<iresearch::bstring, std::unordered_set<iresearch::doc_id_t>> expected_terms;

      {
        iresearch::numeric_token_stream itr;
        itr.reset((int64_t) 12345 * 1);
        for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(1));
      }

      {
        iresearch::numeric_token_stream itr;
        itr.reset((int64_t) 12345 * 2);
        for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(2));
      }

      ASSERT_EQ(2, segment.docs_count("doc_long"));
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(features, field->features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next() && max.next() && max.next()); // skip to last value
      ASSERT_TRUE(min.next()); // skip to first value
      validate_terms(
        *terms,
        2,
        min.attributes().get<iresearch::term_attribute>()->value(),
        max.attributes().get<iresearch::term_attribute>()->value(),
        5,
        features,
        expected_terms
      );
    }

    // validate string field
    {
      auto field = fields.find("doc_string");
      auto terms = segment.terms("doc_string");
      auto& features = STRING_FIELD_FEATURES;
      size_t frequency = 1;
      std::vector<uint32_t> position = { 0 };
      std::unordered_map<iresearch::bytes_ref, std::unordered_set<iresearch::doc_id_t>> expected_terms;

      expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("string1_data"))].emplace(1);
      expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("string2_data"))].emplace(2);

      ASSERT_EQ(2, segment.docs_count("doc_string"));
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(features, field->features);
      ASSERT_NE(nullptr, terms);
      validate_terms(
        *terms,
        2,
        iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref(string1)),
        iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref(string2)),
        2,
        features,
        expected_terms,
        &frequency,
        &position
      );
    }

    // validate text field
    {
      auto field = fields.find("doc_text");
      auto terms = segment.terms("doc_text");
      auto& features = TEXT_FIELD_FEATURES;
      size_t frequency = 1;
      std::vector<uint32_t> position = { 0 };
      std::unordered_map<iresearch::bytes_ref, std::unordered_set<iresearch::doc_id_t>> expected_terms;

      expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("text1_data"))].emplace(1);
      expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("text2_data"))].emplace(2);

      ASSERT_EQ(2, segment.docs_count("doc_text"));
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(features, field->features);
      ASSERT_NE(nullptr, terms);
      validate_terms(
        *terms,
        2,
        iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref(text1)),
        iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref(text2)),
        2,
        features,
        expected_terms,
        &frequency,
        &position
      );
    }

    // ...........................................................................
    // validate documents
    // ...........................................................................
    std::unordered_set<iresearch::bytes_ref> expected_bytes;
    std::unordered_set<double> expected_double;
    std::unordered_set<float> expected_float;
    std::unordered_set<int> expected_int;
    std::unordered_set<int64_t> expected_long;
    std::unordered_set<std::string> expected_string;
    iresearch::index_reader::document_visitor_f visitor = 
      [&expected_bytes, &expected_double, &expected_float, &expected_int, &expected_long, &expected_string] 
      (const iresearch::field_meta& field, data_input& in) {

      if (field.name == "doc_bytes") { 
        auto value = iresearch::read_string<iresearch::bstring>(in);
        return size_t(1) == expected_bytes.erase(value);
      }
      
      if (field.name == "doc_double") { 
        auto value = iresearch::read_zvdouble(in);
        return size_t(1) == expected_double.erase(value);
      }
      
      if (field.name == "doc_float") { 
        auto value = iresearch::read_zvfloat(in);
        return size_t(1) == expected_float.erase(value);
      }
      
      if (field.name == "doc_int") { 
        auto value = iresearch::read_zvint(in);
        return size_t(1) == expected_int.erase(value);
      }
      
      if (field.name == "doc_long") { 
        auto value = iresearch::read_zvlong(in);
        return size_t(1) == expected_long.erase(value);
      }
      
      if (field.name == "doc_string") { 
        auto value = iresearch::read_string<std::string>(in);
        return size_t(1) == expected_string.erase(value);
      }

      return false;
    };

    expected_bytes = { iresearch::bytes_ref(bytes1), iresearch::bytes_ref(bytes2) };
    expected_double = { 2.718281828 * 1, 2.718281828 * 2 };
    expected_float = { (float)(3.1415926535 * 1), (float)(3.1415926535 * 2) };
    expected_int = { 42 * 1, 42 * 2 };
    expected_long = { 12345 * 1, 12345 * 2 };
    expected_string = { string1, string2 };

    // can't have more docs then highest doc_id
    for (size_t i = 0, count = segment.docs_count(); i < count; ++i) {
      ASSERT_TRUE(segment.document(iresearch::doc_id_t(iresearch::type_limits<iresearch::type_t::doc_id_t>::min() + i), visitor));
    }

    ASSERT_TRUE(expected_bytes.empty());
    ASSERT_TRUE(expected_double.empty());
    ASSERT_TRUE(expected_float.empty());
    ASSERT_TRUE(expected_int.empty());
    ASSERT_TRUE(expected_long.empty());
    ASSERT_TRUE(expected_string.empty());
  }

  // validate initial data (segment 1)
  {
    auto& segment = (*reader)[1];
    ASSERT_EQ(2, segment.docs_count());

    auto& fields = segment.fields();

    ASSERT_EQ(7, fields.size());

    // validate bytes field
    {
      auto field = fields.find("doc_bytes");
      auto terms = segment.terms("doc_bytes");
      auto features = tests::binary_field().features();
      std::unordered_map<iresearch::bytes_ref, std::unordered_set<iresearch::doc_id_t>> expected_terms;

      expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("bytes3_data"))].emplace(1);

      ASSERT_EQ(1, segment.docs_count("doc_bytes"));
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(features, field->features);
      ASSERT_NE(nullptr, terms);
      validate_terms(
        *terms,
        1,
        bytes3,
        bytes3,
        1,
        features,
        expected_terms
      );
    }

    // validate double field
    {
      auto field = fields.find("doc_double");
      auto terms = segment.terms("doc_double");
      auto features = tests::double_field().features();
      iresearch::numeric_token_stream max;
      max.reset((double_t) (2.718281828 * 3));
      iresearch::numeric_token_stream min;
      min.reset((double_t) (2.718281828 * 3));
      std::unordered_map<iresearch::bstring, std::unordered_set<iresearch::doc_id_t>> expected_terms;

      {
        iresearch::numeric_token_stream itr;
        itr.reset((double_t) (2.718281828 * 3));
        for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(1));
      }

      ASSERT_EQ(1, segment.docs_count("doc_double"));
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(features, field->features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next() && max.next() && max.next()); // skip to last value
      ASSERT_TRUE(min.next()); // skip to first value
      validate_terms(
        *terms,
        1,
        min.attributes().get<iresearch::term_attribute>()->value(),
        max.attributes().get<iresearch::term_attribute>()->value(),
        4,
        features,
        expected_terms
      );
    }

    // validate float field
    {
      auto field = fields.find("doc_float");
      auto terms = segment.terms("doc_float");
      auto features = tests::float_field().features();
      iresearch::numeric_token_stream max;
      max.reset((float_t) (3.1415926535 * 3));
      iresearch::numeric_token_stream min;
      min.reset((float_t) (3.1415926535 * 3));
      std::unordered_map<iresearch::bstring, std::unordered_set<iresearch::doc_id_t>> expected_terms;

      {
        iresearch::numeric_token_stream itr;
        itr.reset((float_t) (3.1415926535 * 3));
        for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(1));
      }

      ASSERT_EQ(1, segment.docs_count("doc_float"));
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(features, field->features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next()); // skip to last value
      ASSERT_TRUE(min.next()); // skip to first value
      validate_terms(
        *terms,
        1,
        min.attributes().get<iresearch::term_attribute>()->value(),
        max.attributes().get<iresearch::term_attribute>()->value(),
        2,
        features,
        expected_terms
      );
    }

    // validate int field
    {
      auto field = fields.find("doc_int");
      auto terms = segment.terms("doc_int");
      auto features = tests::int_field().features();
      iresearch::numeric_token_stream max;
      max.reset(42 * 3);
      iresearch::numeric_token_stream min;
      min.reset(42 * 3);
      std::unordered_map<iresearch::bstring, std::unordered_set<iresearch::doc_id_t>> expected_terms;

      {
        iresearch::numeric_token_stream itr;
        itr.reset(42 * 3);
        for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(1));
      }

      ASSERT_EQ(1, segment.docs_count("doc_int"));
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(features, field->features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next()); // skip to last value
      ASSERT_TRUE(min.next()); // skip to first value
      validate_terms(
        *terms,
        1,
        min.attributes().get<iresearch::term_attribute>()->value(),
        max.attributes().get<iresearch::term_attribute>()->value(),
        2,
        features,
        expected_terms
      );
    }

    // validate long field
    {
      auto field = fields.find("doc_long");
      auto terms = segment.terms("doc_long");
      auto features = tests::long_field().features();
      iresearch::numeric_token_stream max;
      max.reset((int64_t) 12345 * 3);
      iresearch::numeric_token_stream min;
      min.reset((int64_t) 12345 * 3);
      std::unordered_map<iresearch::bstring, std::unordered_set<iresearch::doc_id_t>> expected_terms;

      {
        iresearch::numeric_token_stream itr;
        itr.reset((int64_t) 12345 * 3);
        for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(1));
      }

      ASSERT_EQ(1, segment.docs_count("doc_long"));
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(features, field->features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next() && max.next() && max.next()); // skip to last value
      ASSERT_TRUE(min.next()); // skip to first value
      validate_terms(
        *terms,
        1,
        min.attributes().get<iresearch::term_attribute>()->value(),
        max.attributes().get<iresearch::term_attribute>()->value(),
        4,
        features,
        expected_terms
      );
    }

    // validate string field
    {
      auto field = fields.find("doc_string");
      auto terms = segment.terms("doc_string");
      auto& features = STRING_FIELD_FEATURES;
      size_t frequency = 1;
      std::vector<uint32_t> position = { 0 };
      std::unordered_map<iresearch::bytes_ref, std::unordered_set<iresearch::doc_id_t>> expected_terms;

      expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("string3_data"))].emplace(1);
      expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("string4_data"))];

      ASSERT_EQ(2, segment.docs_count("doc_string"));
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(features, field->features);
      ASSERT_NE(nullptr, terms);
      validate_terms(
        *terms,
        2,
        iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref(string3)),
        iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref(string4)),
        2,
        features,
        expected_terms,
        &frequency,
        &position
      );
    }

    // validate text field
    {
      auto field = fields.find("doc_text");
      auto terms = segment.terms("doc_text");
      auto& features = TEXT_FIELD_FEATURES;
      size_t frequency = 1;
      std::vector<uint32_t> position = { 0 };
      std::unordered_map<iresearch::bytes_ref, std::unordered_set<iresearch::doc_id_t>> expected_terms;

      expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("text3_data"))].emplace(1);

      ASSERT_EQ(1, segment.docs_count("doc_text"));
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(features, field->features);
      ASSERT_NE(nullptr, terms);
      validate_terms(
        *terms,
        1,
        iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref(text3)),
        iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref(text3)),
        1,
        features,
        expected_terms,
        &frequency,
        &position
      );
    }

    // ...........................................................................
    // validate documents
    // ...........................................................................
    std::unordered_set<iresearch::bytes_ref> expected_bytes;
    std::unordered_set<double> expected_double;
    std::unordered_set<float> expected_float;
    std::unordered_set<int> expected_int;
    std::unordered_set<int64_t> expected_long;
    std::unordered_set<std::string> expected_string;
    iresearch::index_reader::document_visitor_f visitor = 
      [&expected_bytes, &expected_double, &expected_float, &expected_int, &expected_long, &expected_string] 
      (const iresearch::field_meta& field, data_input& in) {

      if (field.name == "doc_bytes") { 
        auto value = iresearch::read_string<iresearch::bstring>(in);
        return size_t(1) == expected_bytes.erase(value);
      }
      
      if (field.name == "doc_double") { 
        auto value = iresearch::read_zvdouble(in);
        return size_t(1) == expected_double.erase(value);
      }
      
      if (field.name == "doc_float") { 
        auto value = iresearch::read_zvfloat(in);
        return size_t(1) == expected_float.erase(value);
      }
      
      if (field.name == "doc_int") { 
        auto value = iresearch::read_zvint(in);
        return size_t(1) == expected_int.erase(value);
      }
      
      if (field.name == "doc_long") { 
        auto value = iresearch::read_zvlong(in);
        return size_t(1) == expected_long.erase(value);
      }
      
      if (field.name == "doc_string") { 
        auto value = iresearch::read_string<std::string>(in);
        return 1 == expected_string.erase(value);
      }

      return false;
    };

    expected_bytes = { iresearch::bytes_ref(bytes3) };
    expected_double = { 2.718281828 * 3 };
    expected_float = { (float)(3.1415926535 * 3) };
    expected_int = { 42 * 3 };
    expected_long = { 12345 * 3 };
    expected_string = { string3, string4 };

    // can't have more docs then highest doc_id
    for (size_t i = 0, count = segment.docs_count(); i < count; ++i) {
      ASSERT_TRUE(segment.document(iresearch::doc_id_t(iresearch::type_limits<iresearch::type_t::doc_id_t>::min() + i), visitor));
    }

    ASSERT_TRUE(expected_bytes.empty());
    ASSERT_TRUE(expected_double.empty());
    ASSERT_TRUE(expected_float.empty());
    ASSERT_TRUE(expected_int.empty());
    ASSERT_TRUE(expected_long.empty());
    ASSERT_TRUE(expected_string.empty());
  }

  writer.add((*reader)[0]);
  writer.add((*reader)[1]);

  std::string filename;
  iresearch::segment_meta meta;

  writer.flush(filename, meta);

  auto segment = iresearch::segment_reader::open(dir, meta);

  ASSERT_EQ(3, segment->docs_count()); //doc4 removed during merge

  auto& fields = segment->fields();

  ASSERT_EQ(7, fields.size());

  // validate bytes field
  {
    auto field = fields.find("doc_bytes");
    auto terms = segment->terms("doc_bytes");
    auto features = tests::binary_field().features();
    std::unordered_map<iresearch::bytes_ref, std::unordered_set<iresearch::doc_id_t>> expected_terms;

    expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("bytes1_data"))].emplace(1);
    expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("bytes2_data"))].emplace(2);
    expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("bytes3_data"))].emplace(3);

    ASSERT_EQ(3, segment->docs_count("doc_bytes"));
    ASSERT_NE(nullptr, field);
    ASSERT_EQ(features, field->features);
    ASSERT_NE(nullptr, terms);
    validate_terms(
      *terms,
      3,
      bytes1,
      bytes3,
      3,
      features,
      expected_terms
    );
  }

  // validate double field
  {
    auto field = fields.find("doc_double");
    auto terms = segment->terms("doc_double");
    auto features = tests::double_field().features();
    iresearch::numeric_token_stream max;
    max.reset((double_t) (2.718281828 * 3));
    iresearch::numeric_token_stream min;
    min.reset((double_t) (2.718281828 * 1));
    std::unordered_map<iresearch::bstring, std::unordered_set<iresearch::doc_id_t>> expected_terms;

    {
      iresearch::numeric_token_stream itr;
      itr.reset((double_t) (2.718281828 * 1));
      for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(1));
    }

    {
      iresearch::numeric_token_stream itr;
      itr.reset((double_t) (2.718281828 * 2));
      for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(2));
    }

    {
      iresearch::numeric_token_stream itr;
      itr.reset((double_t) (2.718281828 * 3));
      for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(3));
    }

    ASSERT_EQ(3, segment->docs_count("doc_double"));
    ASSERT_NE(nullptr, field);
    ASSERT_EQ(features, field->features);
    ASSERT_NE(nullptr, terms);
    ASSERT_TRUE(max.next() && max.next() && max.next() && max.next()); // skip to last value
    ASSERT_TRUE(min.next()); // skip to first value
    validate_terms(
      *terms,
      3,
      min.attributes().get<iresearch::term_attribute>()->value(),
      max.attributes().get<iresearch::term_attribute>()->value(),
      12,
      features,
      expected_terms
    );
  }

  // validate float field
  {
    auto field = fields.find("doc_float");
    auto terms = segment->terms("doc_float");
    auto features = tests::float_field().features();
    iresearch::numeric_token_stream max;
    max.reset((float_t) (3.1415926535 * 3));
    iresearch::numeric_token_stream min;
    min.reset((float_t) (3.1415926535 * 1));
    std::unordered_map<iresearch::bstring, std::unordered_set<iresearch::doc_id_t>> expected_terms;

    {
      iresearch::numeric_token_stream itr;
      itr.reset((float_t) (3.1415926535 * 1));
      for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(1));
    }

    {
      iresearch::numeric_token_stream itr;
      itr.reset((float_t) (3.1415926535 * 2));
      for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(2));
    }

    {
      iresearch::numeric_token_stream itr;
      itr.reset((float_t) (3.1415926535 * 3));
      for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(3));
    }

    ASSERT_EQ(3, segment->docs_count("doc_float"));
    ASSERT_NE(nullptr, field);
    ASSERT_EQ(features, field->features);
    ASSERT_NE(nullptr, terms);
    ASSERT_TRUE(max.next() && max.next()); // skip to last value
    ASSERT_TRUE(min.next()); // skip to first value
    validate_terms(
      *terms,
      3,
      min.attributes().get<iresearch::term_attribute>()->value(),
      max.attributes().get<iresearch::term_attribute>()->value(),
      6,
      features,
      expected_terms
    );
  }

  // validate int field
  {
    auto field = fields.find("doc_int");
    auto terms = segment->terms("doc_int");
    auto features = tests::int_field().features();
    iresearch::numeric_token_stream max;
    max.reset(42 * 3);
    iresearch::numeric_token_stream min;
    min.reset(42 * 1);
    std::unordered_map<iresearch::bstring, std::unordered_set<iresearch::doc_id_t>> expected_terms;

    {
      iresearch::numeric_token_stream itr;
      itr.reset(42 * 1);
      for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(1));
    }

    {
      iresearch::numeric_token_stream itr;
      itr.reset(42 * 2);
      for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(2));
    }

    {
      iresearch::numeric_token_stream itr;
      itr.reset(42 * 3);
      for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(3));
    }

    ASSERT_EQ(3, segment->docs_count("doc_int"));
    ASSERT_NE(nullptr, field);
    ASSERT_EQ(features, field->features);
    ASSERT_NE(nullptr, terms);
    ASSERT_TRUE(max.next() && max.next()); // skip to last value
    ASSERT_TRUE(min.next()); // skip to first value
    validate_terms(
      *terms,
      3,
      min.attributes().get<iresearch::term_attribute>()->value(),
      max.attributes().get<iresearch::term_attribute>()->value(),
      4,
      features,
      expected_terms
    );
  }

  // validate long field
  {
    auto field = fields.find("doc_long");
    auto terms = segment->terms("doc_long");
    auto features = tests::long_field().features();
    iresearch::numeric_token_stream max;
    max.reset((int64_t) 12345 * 3);
    iresearch::numeric_token_stream min;
    min.reset((int64_t) 12345 * 1);
    std::unordered_map<iresearch::bstring, std::unordered_set<iresearch::doc_id_t>> expected_terms;

    {
      iresearch::numeric_token_stream itr;
      itr.reset((int64_t) 12345 * 1);
      for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(1));
    }

    {
      iresearch::numeric_token_stream itr;
      itr.reset((int64_t) 12345 * 2);
      for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(2));
    }

    {
      iresearch::numeric_token_stream itr;
      itr.reset((int64_t) 12345 * 3);
      for (; itr.next(); expected_terms[iresearch::bstring(itr.attributes().get<iresearch::term_attribute>()->value())].emplace(3));
    }

    ASSERT_EQ(3, segment->docs_count("doc_long"));
    ASSERT_NE(nullptr, field);
    ASSERT_EQ(features, field->features);
    ASSERT_NE(nullptr, terms);
    ASSERT_TRUE(max.next() && max.next() && max.next() && max.next()); // skip to last value
    ASSERT_TRUE(min.next()); // skip to first value
    validate_terms(
      *terms,
      3,
      min.attributes().get<iresearch::term_attribute>()->value(),
      max.attributes().get<iresearch::term_attribute>()->value(),
      6,
      features,
      expected_terms
    );
  }

  // validate string field
  {
    auto field = fields.find("doc_string");
    auto terms = segment->terms("doc_string");
    auto& features = STRING_FIELD_FEATURES;
    size_t frequency = 1;
    std::vector<uint32_t> position = { 0 };
    std::unordered_map<iresearch::bytes_ref, std::unordered_set<iresearch::doc_id_t>> expected_terms;

    expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("string1_data"))].emplace(1);
    expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("string2_data"))].emplace(2);
    expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("string3_data"))].emplace(3);

    ASSERT_EQ(3, segment->docs_count("doc_string"));
    ASSERT_NE(nullptr, field);
    ASSERT_EQ(features, field->features);
    ASSERT_NE(nullptr, terms);
    validate_terms(
      *terms,
      3,
      iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref(string1)),
      iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref(string3)),
      3,
      features,
      expected_terms,
      &frequency,
      &position
    );
  }

  // validate text field
  {
    auto field = fields.find("doc_text");
    auto terms = segment->terms("doc_text");
    auto& features = TEXT_FIELD_FEATURES;
    size_t frequency = 1;
    std::vector<uint32_t> position = { 0 };
    std::unordered_map<iresearch::bytes_ref, std::unordered_set<iresearch::doc_id_t>> expected_terms;

    expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("text1_data"))].emplace(1);
    expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("text2_data"))].emplace(2);
    expected_terms[iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref("text3_data"))].emplace(3);

    ASSERT_EQ(3, segment->docs_count("doc_text"));
    ASSERT_NE(nullptr, field);
    ASSERT_EQ(features, field->features);
    ASSERT_NE(nullptr, terms);
    validate_terms(
      *terms,
      3,
      iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref(text1)),
      iresearch::ref_cast<iresearch::byte_type>(iresearch::string_ref(text3)),
      3,
      features,
      expected_terms,
      &frequency,
      &position
    );
  }

  // ...........................................................................
  // validate documents
  // ...........................................................................
  std::unordered_set<iresearch::bytes_ref> expected_bytes;
  std::unordered_set<double> expected_double;
  std::unordered_set<float> expected_float;
  std::unordered_set<int> expected_int;
  std::unordered_set<int64_t> expected_long;
  std::unordered_set<std::string> expected_string;
  iresearch::index_reader::document_visitor_f visitor =
    [&expected_bytes, &expected_double, &expected_float, &expected_int, &expected_long, &expected_string]
  (const iresearch::field_meta& field, data_input& in) {

    if (field.name == "doc_bytes") {
      auto value = iresearch::read_string<iresearch::bstring>(in);
      return size_t(1) == expected_bytes.erase(value);
    }

    if (field.name == "doc_double") {
      auto value = iresearch::read_zvdouble(in);
      return size_t(1) == expected_double.erase(value);
    }

    if (field.name == "doc_float") {
      auto value = iresearch::read_zvfloat(in);
      return size_t(1) == expected_float.erase(value);
    }

    if (field.name == "doc_int") {
      auto value = iresearch::read_zvint(in);
      return size_t(1) == expected_int.erase(value);
    }

    if (field.name == "doc_long") {
      auto value = iresearch::read_zvlong(in);
      return size_t(1) == expected_long.erase(value);
    }

    if (field.name == "doc_string") {
      auto value = iresearch::read_string<std::string>(in);
      return size_t(1) == expected_string.erase(value);
    }

    return false;
  };

  expected_bytes = { iresearch::bytes_ref(bytes1), iresearch::bytes_ref(bytes2), iresearch::bytes_ref(bytes3) };
  expected_double = { 2.718281828 * 1, 2.718281828 * 2, 2.718281828 * 3 };
  expected_float = { (float)(3.1415926535 * 1), (float)(3.1415926535 * 2), (float)(3.1415926535 * 3) };
  expected_int = { 42 * 1, 42 * 2, 42 * 3 };
  expected_long = { 12345 * 1, 12345 * 2, 12345 * 3 };
  expected_string = { string1, string2, string3 };

  // can't have more docs then highest doc_id
  for (size_t i = 0, count = segment->docs_count(); i < count; ++i) {
    ASSERT_TRUE(segment->document(iresearch::doc_id_t(iresearch::type_limits<iresearch::type_t::doc_id_t>::min() + i), visitor));
  }

  ASSERT_TRUE(expected_bytes.empty());
  ASSERT_TRUE(expected_double.empty());
  ASSERT_TRUE(expected_float.empty());
  ASSERT_TRUE(expected_int.empty());
  ASSERT_TRUE(expected_long.empty());
  ASSERT_TRUE(expected_string.empty());
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------