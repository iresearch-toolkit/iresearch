////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "tests_shared.hpp"
#include "filter_test_case_base.hpp"
#include "analysis/token_attributes.hpp"
#include "search/phrase_filter.hpp"
#ifndef IRESEARCH_DLL
#include "search/multiterm_query.hpp"
#include "search/term_query.hpp"
#endif

NS_BEGIN(tests)

void analyzed_json_field_factory(
    tests::document& doc,
    const std::string& name,
    const tests::json_doc_generator::json_value& data) {
  typedef templates::text_field<std::string> text_field;
 
  class string_field : public templates::string_field {
   public:
    string_field(const irs::string_ref& name, const irs::string_ref& value)
      : templates::string_field(name, value) {
    }

    const irs::flags& features() const {
      static irs::flags features{ irs::frequency::type() };
      return features;
    }
  }; // string_field

  if (data.is_string()) {
    // analyzed field
    doc.indexed.push_back(std::make_shared<text_field>(
      std::string(name.c_str()) + "_anl",
      data.str
    ));

    // not analyzed field
    doc.insert(std::make_shared<string_field>(
      irs::string_ref(name),
      data.str
    ));
  }
}

NS_END

class phrase_filter_test_case : public tests::filter_test_case_base {
 protected:
  void sequential() {
    // add segment
    {
      tests::json_doc_generator gen(
        resource("phrase_sequential.json"),
        &tests::analyzed_json_field_factory);
      add_segment(gen);
    }

    // read segment
    auto rdr = open_reader();

    // empty field 
    {
      irs::by_phrase q;

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
    }

    // empty phrase
    {
      irs::by_phrase q;
      q.field("phrase_anl");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
    }

    // equals to term_filter "fox"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "fox");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
    }

    // equals to prefix_filter "fo*"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::prefix_term{}, "fo");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("D", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("D", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("H", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("H", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("W", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("W", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("X", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("X", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("Y", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("Y", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
    }

    // equals to wildcard_filter "fo%"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::wildcard_term{}, "fo%");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("D", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("D", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("H", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("H", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("W", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("W", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("X", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("X", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("Y", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("Y", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
    }

    // search "fox" on field without positions
    // which is ok for single word phrases
    {
      irs::by_phrase q;
      q.field("phrase").push_back(irs::by_phrase::info_t::simple_term{}, "fox");

      auto prepared = q.prepare(rdr);
#ifndef IRESEARCH_DLL
      // check single word phrase optimization
      ASSERT_NE(nullptr, dynamic_cast<const irs::term_query*>(prepared.get()));
#endif
      irs::bytes_ref actual_value;
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
    }

    // search "fo*" on field without positions
    // which is ok for the first word in phrase
    {
      irs::by_phrase q;
      q.field("phrase").push_back(irs::by_phrase::info_t::prefix_term{}, "fo");

      auto prepared = q.prepare(rdr);
#ifndef IRESEARCH_DLL
      // check single word phrase optimization
      ASSERT_NE(nullptr, dynamic_cast<const irs::multiterm_query*>(prepared.get()));
#endif
      irs::bytes_ref actual_value;
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
    }

    // search "fo%" on field without positions
    // which is ok for first word in phrase
    {
      irs::by_phrase q;
      q.field("phrase").push_back(irs::by_phrase::info_t::wildcard_term{}, "fo%");

      auto prepared = q.prepare(rdr);
#ifndef IRESEARCH_DLL
      // check single word phrase optimization
      ASSERT_NE(nullptr, dynamic_cast<const irs::multiterm_query*>(prepared.get()));
#endif
      irs::bytes_ref actual_value;
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
    }

    // equals to term_filter "fox" with phrase offset
    // which does not matter
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "fox", irs::integer_traits<size_t>::const_max);

      auto prepared = q.prepare(rdr);
#ifndef IRESEARCH_DLL
      // check single word phrase optimization
      ASSERT_NE(nullptr, dynamic_cast<const irs::term_query*>(prepared.get()));
#endif
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
    }

    // equals to prefix_filter "fo*" with phrase offset
    // which does not matter
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::prefix_term{}, "fo", irs::integer_traits<size_t>::const_max);

      auto prepared = q.prepare(rdr);
#ifndef IRESEARCH_DLL
      // check single word phrase optimization
      ASSERT_NE(nullptr, dynamic_cast<const irs::multiterm_query*>(prepared.get()));
#endif
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("D", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("D", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("H", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("H", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("W", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("W", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("X", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("X", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("Y", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("Y", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
    }

    // equals to prefix_filter "fo%" with phrase offset
    // which does not matter
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::wildcard_term{}, "fo%", irs::integer_traits<size_t>::const_max);

      auto prepared = q.prepare(rdr);
#ifndef IRESEARCH_DLL
      // check single word phrase optimization
      ASSERT_NE(nullptr, dynamic_cast<const irs::multiterm_query*>(prepared.get()));
#endif
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("D", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("D", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("H", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("H", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("W", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("W", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("X", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("X", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("Y", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("Y", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
    }

    // "quick brown fox"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "quick")
       .push_back(irs::by_phrase::info_t::simple_term{}, "brown")
       .push_back(irs::by_phrase::info_t::simple_term{}, "fox");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "qui* brown fox"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::prefix_term{}, "qui")
       .push_back(irs::by_phrase::info_t::simple_term{}, "brown")
       .push_back(irs::by_phrase::info_t::simple_term{}, "fox");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "qui% brown fox"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::wildcard_term{}, "qui%")
       .push_back(irs::by_phrase::info_t::simple_term{}, "brown")
       .push_back(irs::by_phrase::info_t::simple_term{}, "fox");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "quick bro* fox"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "quick")
       .push_back(irs::by_phrase::info_t::prefix_term{}, "bro")
       .push_back(irs::by_phrase::info_t::simple_term{}, "fox");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "quick bro% fox"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "quick")
       .push_back(irs::by_phrase::info_t::wildcard_term{}, "bro%")
       .push_back(irs::by_phrase::info_t::simple_term{}, "fox");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "quick brown fo*"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "quick")
       .push_back(irs::by_phrase::info_t::simple_term{}, "brown")
       .push_back(irs::by_phrase::info_t::prefix_term{}, "fo");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "quick brown fo%"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "quick")
       .push_back(irs::by_phrase::info_t::simple_term{}, "brown")
       .push_back(irs::by_phrase::info_t::wildcard_term{}, "fo%");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "qui* bro* fox"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::prefix_term{}, "qui")
       .push_back(irs::by_phrase::info_t::prefix_term{}, "bro")
       .push_back(irs::by_phrase::info_t::simple_term{}, "fox");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "qui% bro% fox"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::wildcard_term{}, "qui%")
       .push_back(irs::by_phrase::info_t::wildcard_term{}, "bro%")
       .push_back(irs::by_phrase::info_t::simple_term{}, "fox");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "qui* brown fo*"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::prefix_term{}, "qui")
       .push_back(irs::by_phrase::info_t::simple_term{}, "brown")
       .push_back(irs::by_phrase::info_t::prefix_term{}, "fo");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("W", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("W", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "qui% brown fo%"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::wildcard_term{}, "qui%")
       .push_back(irs::by_phrase::info_t::simple_term{}, "brown")
       .push_back(irs::by_phrase::info_t::wildcard_term{}, "fo%");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("W", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("W", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "quick bro* fo*"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "quick")
       .push_back(irs::by_phrase::info_t::prefix_term{}, "bro")
       .push_back(irs::by_phrase::info_t::prefix_term{}, "fo");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("X", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("X", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "quick bro% fo%"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "quick")
       .push_back(irs::by_phrase::info_t::wildcard_term{}, "bro%")
       .push_back(irs::by_phrase::info_t::wildcard_term{}, "fo%");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("X", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("X", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "qui* bro* fo*"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::prefix_term{}, "qui")
       .push_back(irs::by_phrase::info_t::prefix_term{}, "bro")
       .push_back(irs::by_phrase::info_t::prefix_term{}, "fo");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("W", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("W", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("X", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("X", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("Y", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("Y", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "qui% bro% fo%"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::wildcard_term{}, "qui%")
       .push_back(irs::by_phrase::info_t::wildcard_term{}, "bro%")
       .push_back(irs::by_phrase::info_t::wildcard_term{}, "fo%");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();

      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("W", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("W", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("X", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("X", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("Y", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("Y", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }




    // "quick brown fox" with order
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "quick")
       .push_back(irs::by_phrase::info_t::simple_term{}, "brown")
       .push_back(irs::by_phrase::info_t::simple_term{}, "fox");

      size_t collect_field_count = 0;
      size_t collect_term_count = 0;
      size_t finish_count = 0;
      irs::order ord;
      auto& sort = ord.add<tests::sort::custom_sort>(false);

      sort.collector_collect_field = [&collect_field_count](const irs::sub_reader&, const irs::term_reader&)->void{
        ++collect_field_count;
      };
      sort.collector_collect_term = [&collect_term_count](const irs::sub_reader&, const irs::term_reader&, const irs::attribute_view&)->void{
        ++collect_term_count;
      };
      sort.collectors_collect_ = [&finish_count](irs::byte_type*, const irs::index_reader&, const irs::sort::field_collector*, const irs::sort::term_collector*)->void {
        ++finish_count;
      };
      sort.prepare_field_collector_ = [&sort]()->irs::sort::field_collector::ptr {
        return irs::memory::make_unique<tests::sort::custom_sort::prepared::collector>(sort);
      };
      sort.prepare_term_collector_ = [&sort]()->irs::sort::term_collector::ptr {
        return irs::memory::make_unique<tests::sort::custom_sort::prepared::collector>(sort);
      };
      sort.scorer_add = [](irs::doc_id_t& dst, const irs::doc_id_t& src)->void {
        ASSERT_TRUE(
          irs::type_limits<irs::type_t::doc_id_t>::invalid() == dst
          || dst == src
        );
        dst = src;
      };

      auto pord = ord.prepare();
      auto prepared = q.prepare(rdr, pord);
      ASSERT_EQ(1, collect_field_count); // 1 field in 1 segment
      ASSERT_EQ(3, collect_term_count); // 3 different terms
      ASSERT_EQ(3, finish_count); // 3 sub-terms in phrase
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();
      auto docs = prepared->execute(*sub, pord);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));
      auto& score = docs->attributes().get<irs::score>();
      ASSERT_FALSE(!score);

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("G", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("I", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "fox ... quick"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "fox")
       .push_back(irs::by_phrase::info_t::simple_term{}, "quick", 1);

      auto prepared = q.prepare(rdr);

      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();
      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // =============================
    // "fox ... quick"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::prefix_term{}, "fo")
       .push_back(irs::by_phrase::info_t::prefix_term{}, "qui", 1);

      irs::order order;
      order.add(true, irs::scorers::get("bm25", irs::text_format::json, "{ \"b\" : 0 }"));
      auto prepared_order = order.prepare();

      auto prepared = q.prepare(rdr, prepared_order);

      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();
      auto docs = prepared->execute(*sub, prepared_order);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }
    // =============================

    // "fox ... quick" with phrase offset
    // which is does not matter
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "fox", irs::integer_traits<size_t>::const_max)
       .push_back(irs::by_phrase::info_t::simple_term{}, "quick", 1);

      auto prepared = q.prepare(rdr);

      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();
      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid( docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "fox ... ... ... ... ... ... ... ... ... ... quick"
    {
      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "fox")
       .push_back(irs::by_phrase::info_t::simple_term{}, "quick", 10);

      auto prepared = q.prepare(rdr);

      auto sub = rdr.begin();
      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
    }

    // "eye ... eye"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "eye")
       .push_back(irs::by_phrase::info_t::simple_term{}, "eye", 1);

      auto prepared = q.prepare(rdr);

      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();
      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("C", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("C", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "as in the past we are looking forward"
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "as")
       .push_back(irs::by_phrase::info_t::simple_term{}, "in")
       .push_back(irs::by_phrase::info_t::simple_term{}, "the")
       .push_back(irs::by_phrase::info_t::simple_term{}, "past")
       .push_back(irs::by_phrase::info_t::simple_term{}, "we")
       .push_back(irs::by_phrase::info_t::simple_term{}, "are")
       .push_back(irs::by_phrase::info_t::simple_term{}, "looking")
       .push_back(irs::by_phrase::info_t::simple_term{}, "forward");

      auto prepared = q.prepare(rdr);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();
      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("H", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("H", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // "as in the past we are looking forward" with order
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "as")
       .push_back(irs::by_phrase::info_t::simple_term{}, "in")
       .push_back(irs::by_phrase::info_t::simple_term{}, "the")
       .push_back(irs::by_phrase::info_t::simple_term{}, "past")
       .push_back(irs::by_phrase::info_t::simple_term{}, "we")
       .push_back(irs::by_phrase::info_t::simple_term{}, "are")
       .push_back(irs::by_phrase::info_t::simple_term{}, "looking")
       .push_back(irs::by_phrase::info_t::simple_term{}, "forward");

      irs::order ord;
      auto& sort = ord.add<tests::sort::custom_sort>(false);
      sort.scorer_add = [](irs::doc_id_t& dst, const irs::doc_id_t& src)->void {
        ASSERT_TRUE(
          irs::type_limits<irs::type_t::doc_id_t>::invalid() == dst
          || dst == src
        );
        dst = src;
      };

      auto pord = ord.prepare();
      auto prepared = q.prepare(rdr, pord);
      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();
      auto docs = prepared->execute(*sub, pord);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));
      auto& score = docs->attributes().get<irs::score>();
      ASSERT_FALSE(!score);

      ASSERT_TRUE(docs->next());
      score->evaluate();
      ASSERT_EQ(docs->value(),pord.get<irs::doc_id_t>(score->c_str(), 0));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("H", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("H", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // fox quick
    {
      irs::bytes_ref actual_value;

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "fox")
       .push_back(irs::by_phrase::info_t::simple_term{}, "quick");

      auto prepared = q.prepare(rdr);

      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();
      auto docs = prepared->execute(*sub);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));
      // Check repeatable seek to the same document given frequency of the phrase within the document = 2
      auto v = docs->value();
      ASSERT_EQ(v, docs->seek(docs->value()));
      ASSERT_EQ(v, docs->seek(docs->value()));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }

    // fox quick with order
    {
      irs::bytes_ref actual_value;

      irs::order ord;
      auto& sort = ord.add<tests::sort::custom_sort>(false);
      sort.scorer_add = [](irs::doc_id_t& dst, const irs::doc_id_t& src)->void {
        ASSERT_TRUE(
          irs::type_limits<irs::type_t::doc_id_t>::invalid() == dst
          || dst == src
        );
        dst = src;
      };
      auto pord = ord.prepare();

      irs::by_phrase q;
      q.field("phrase_anl")
       .push_back(irs::by_phrase::info_t::simple_term{}, "fox")
       .push_back(irs::by_phrase::info_t::simple_term{}, "quick");

      auto prepared = q.prepare(rdr, pord);

      auto sub = rdr.begin();
      auto column = sub->column_reader("name");
      ASSERT_NE(nullptr, column);
      auto values = column->values();
      auto docs = prepared->execute(*sub, pord);
      auto& doc = docs->attributes().get<irs::document>();
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs->value()));
      auto docs_seek = prepared->execute(*sub, pord);
      ASSERT_FALSE(irs::type_limits<irs::type_t::doc_id_t>::valid(docs_seek->value()));

      ASSERT_TRUE(docs->next());
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));
      ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
      ASSERT_TRUE(values(docs->value(), actual_value));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(actual_value.c_str()));

      ASSERT_FALSE(docs->next());
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs->value()));
      ASSERT_TRUE(irs::type_limits<irs::type_t::doc_id_t>::eof(docs_seek->seek(irs::type_limits<irs::type_t::doc_id_t>::eof())));
    }
  }
}; // phrase_filter_test_case

TEST_P(phrase_filter_test_case, by_phrase) {
  sequential();
}

TEST(by_phrase_test, ctor) {
  irs::by_phrase q;
  ASSERT_EQ(irs::by_phrase::type(), q.type());
  ASSERT_EQ("", q.field());
  ASSERT_TRUE(q.empty());
  ASSERT_EQ(0, q.size());
  ASSERT_EQ(q.begin(), q.end());
  ASSERT_EQ(irs::no_boost(), q.boost());

  auto& features = irs::by_phrase::required();
  ASSERT_EQ(2, features.size());
  ASSERT_TRUE(features.check<irs::frequency>());
  ASSERT_TRUE(features.check<irs::position>());
}

TEST(by_phrase_test, boost) {
  // no boost
  {
    // no terms
    {
      irs::by_phrase q;
      q.field("field");

      auto prepared = q.prepare(irs::sub_reader::empty());
      ASSERT_EQ(irs::no_boost(), prepared->boost());
    }

    // single term
    {
      irs::by_phrase q;
      q.field("field").push_back(irs::by_phrase::info_t::simple_term{}, "quick");

      auto prepared = q.prepare(irs::sub_reader::empty());
      ASSERT_EQ(irs::no_boost(), prepared->boost());
    }

    // multiple terms
    {
      irs::by_phrase q;
      q.field("field").push_back(irs::by_phrase::info_t::simple_term{}, "quick")
          .push_back(irs::by_phrase::info_t::simple_term{}, "brown");

      auto prepared = q.prepare(irs::sub_reader::empty());
      ASSERT_EQ(irs::no_boost(), prepared->boost());
    }
  }

  // with boost
  {
    irs::boost_t boost = 1.5f;
    
    // no terms, return empty query
    {
      irs::by_phrase q;
      q.field("field");
      q.boost(boost);

      auto prepared = q.prepare(irs::sub_reader::empty());
      ASSERT_EQ(irs::no_boost(), prepared->boost());
    }

    // single term
    {
      irs::by_phrase q;
      q.field("field").push_back(irs::by_phrase::info_t::simple_term{}, "quick");
      q.boost(boost);

      auto prepared = q.prepare(irs::sub_reader::empty());
      ASSERT_EQ(boost, prepared->boost());
    }
    
    // single multiple terms 
    {
      irs::by_phrase q;
      q.field("field").push_back(irs::by_phrase::info_t::simple_term{}, "quick")
          .push_back(irs::by_phrase::info_t::simple_term{}, "brown");
      q.boost(boost);

      auto prepared = q.prepare(irs::sub_reader::empty());
      ASSERT_EQ(boost, prepared->boost());
    }
  }
}

TEST(by_phrase_test, push_back_insert) {
  irs::by_phrase q;

  // push_back 
  {
    q.push_back(irs::by_phrase::info_t::simple_term{}, "quick");
    q.push_back(irs::by_phrase::info_t::simple_term{}, irs::ref_cast<irs::byte_type>(irs::string_ref("brown")), 1);
    q.push_back(irs::by_phrase::info_t::simple_term{}, irs::bstring(irs::ref_cast<irs::byte_type>(irs::string_ref("fox"))));
    ASSERT_FALSE(q.empty());
    ASSERT_EQ(3, q.size());

    // check elements via positions
    {
      ASSERT_EQ((irs::by_phrase::term_info_t{irs::by_phrase::info_t::simple_term{}, irs::ref_cast<irs::byte_type>(irs::string_ref("quick"))}), q[0]);
      ASSERT_EQ((irs::by_phrase::term_info_t{irs::by_phrase::info_t::simple_term{}, irs::ref_cast<irs::byte_type>(irs::string_ref("brown"))}), q[2]);
      ASSERT_EQ((irs::by_phrase::term_info_t{irs::by_phrase::info_t::simple_term{}, irs::ref_cast<irs::byte_type>(irs::string_ref("fox"))}), q[3]);
    }

    // check elements via iterators 
    {
      auto it = q.begin();
      ASSERT_NE(q.end(), it); 
      ASSERT_EQ(0, it->first);
      ASSERT_EQ((irs::by_phrase::term_info_t{irs::by_phrase::info_t::simple_term{}, irs::ref_cast<irs::byte_type>(irs::string_ref("quick"))}), it->second);

      ++it;
      ASSERT_NE(q.end(), it); 
      ASSERT_EQ(2, it->first);
      ASSERT_EQ((irs::by_phrase::term_info_t{irs::by_phrase::info_t::simple_term{}, irs::ref_cast<irs::byte_type>(irs::string_ref("brown"))}), it->second);

      ++it;
      ASSERT_NE(q.end(), it); 
      ASSERT_EQ(3, it->first);
      ASSERT_EQ((irs::by_phrase::term_info_t{irs::by_phrase::info_t::simple_term{}, irs::ref_cast<irs::byte_type>(irs::string_ref("fox"))}), it->second);

      ++it;
      ASSERT_EQ(q.end(), it); 
    }

    // push term 
    {
      q.push_back(irs::by_phrase::info_t::simple_term{}, "squirrel", 0);
      ASSERT_EQ((irs::by_phrase::term_info_t{irs::by_phrase::info_t::simple_term{}, irs::ref_cast<irs::byte_type>(irs::string_ref("squirrel"))}), q[4]);
    }
    ASSERT_EQ(4, q.size());
  }

  // insert
  {
    q[3] = irs::by_phrase::term_info_t{irs::by_phrase::info_t::simple_term{}, irs::ref_cast<irs::byte_type>(irs::string_ref("jumps"))};
    ASSERT_EQ((irs::by_phrase::term_info_t{irs::by_phrase::info_t::simple_term{}, irs::ref_cast<irs::byte_type>(irs::string_ref("jumps"))}), q[3]);
    ASSERT_EQ(4, q.size());

    q.insert(irs::by_phrase::info_t::simple_term{}, 5, "lazy");
    ASSERT_EQ((irs::by_phrase::term_info_t{irs::by_phrase::info_t::simple_term{}, irs::ref_cast<irs::byte_type>(irs::string_ref("lazy"))}), q[5]);
    ASSERT_EQ(5, q.size());

    q.insert(irs::by_phrase::info_t::simple_term{}, 28, irs::bstring(irs::ref_cast<irs::byte_type>(irs::string_ref("dog"))));
    ASSERT_EQ((irs::by_phrase::term_info_t{irs::by_phrase::info_t::simple_term{}, irs::ref_cast<irs::byte_type>(irs::string_ref("dog"))}), q[28]);
    ASSERT_EQ(6, q.size());
  }
}

TEST(by_phrase_test, equal) {
  ASSERT_EQ(irs::by_phrase(), irs::by_phrase());

  {
    irs::by_phrase q0;
    q0.field("name");
    q0.push_back(irs::by_phrase::info_t::simple_term{}, "quick");
    q0.push_back(irs::by_phrase::info_t::simple_term{}, "brown");

    irs::by_phrase q1;
    q1.field("name");
    q1.push_back(irs::by_phrase::info_t::simple_term{}, "quick");
    q1.push_back(irs::by_phrase::info_t::simple_term{}, "brown");
    ASSERT_EQ(q0, q1);
    ASSERT_EQ(q0.hash(), q1.hash());
  }

  {
    irs::by_phrase q0;
    q0.field("name");
    q0.push_back(irs::by_phrase::info_t::simple_term{}, "quick");
    q0.push_back(irs::by_phrase::info_t::simple_term{}, "squirrel");

    irs::by_phrase q1;
    q1.field("name");
    q1.push_back(irs::by_phrase::info_t::simple_term{}, "quick");
    q1.push_back(irs::by_phrase::info_t::simple_term{}, "brown");
    ASSERT_NE(q0, q1);
  }

  {
    irs::by_phrase q0;
    q0.field("name1");
    q0.push_back(irs::by_phrase::info_t::simple_term{}, "quick");
    q0.push_back(irs::by_phrase::info_t::simple_term{}, "brown");

    irs::by_phrase q1;
    q1.field("name");
    q1.push_back(irs::by_phrase::info_t::simple_term{}, "quick");
    q1.push_back(irs::by_phrase::info_t::simple_term{}, "brown");
    ASSERT_NE(q0, q1);
  }

  {
    irs::by_phrase q0;
    q0.field("name");
    q0.push_back(irs::by_phrase::info_t::simple_term{}, "quick");

    irs::by_phrase q1;
    q1.field("name");
    q1.push_back(irs::by_phrase::info_t::simple_term{}, "quick");
    q1.push_back(irs::by_phrase::info_t::simple_term{}, "brown");
    ASSERT_NE(q0, q1);
  }
}

INSTANTIATE_TEST_CASE_P(
  phrase_filter_test,
  phrase_filter_test_case,
  ::testing::Combine(
    ::testing::Values(
      &tests::memory_directory,
      &tests::fs_directory,
      &tests::mmap_directory
    ),
    ::testing::Values("1_0")
  ),
  tests::to_string
);

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
