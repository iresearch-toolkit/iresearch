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
#include "filter_test_case_base.hpp"
#include "index/doc_generator.hpp"
#include "search/column_existence_filter.hpp"
#include "search/sort.hpp"
#include "utils/lz4compression.hpp"


namespace {

irs::by_column_existence make_filter(const irs::string_ref& field,
                                     bool prefix_match) {
  irs::by_column_existence filter;
  *filter.mutable_field() = field;
  filter.mutable_options()->prefix_match = prefix_match;
  return filter;
}

class column_existence_filter_test_case : public tests::FilterTestCaseBase {
 protected:
  void simple_sequential_mask() {
    // add segment
    {
      class mask_field : public tests::ifield {
       public:
        explicit mask_field(const std::string& name) : name_(name) {}

        bool write(irs::data_output&) const { return true; }
        irs::string_ref name() const { return name_; }
        irs::IndexFeatures index_features() const noexcept {
          return irs::IndexFeatures::NONE;
        }
        irs::features_t features() const { return {}; }
        irs::token_stream& get_tokens() const {
          // nothing to index
          stream_.next();
          return stream_;
        }

       private:
        std::string name_;
        mutable irs::null_token_stream stream_;
      };

      tests::json_doc_generator gen(
        resource("simple_sequential.json"),
        [](tests::document& doc, const std::string& name,
           const tests::json_doc_generator::json_value& /*data*/) {
          doc.insert(std::make_shared<mask_field>(name));
        });
      add_segment(gen);
    }

    auto rdr = open_reader();

    // 'prefix' column
    {
      const std::string column_name = "prefix";

      irs::by_column_existence filter = make_filter(column_name, false);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto column = segment.column(column_name);
      ASSERT_NE(nullptr, column);
      auto column_it = column->iterator(irs::ColumnHint::kNormal);
      auto filter_it = prepared->execute(segment);

      auto* doc = irs::get<irs::document>(*filter_it);
      ASSERT_TRUE(bool(doc));

      ASSERT_EQ(column->size(), irs::cost::extract(*filter_it));

      while (filter_it->next()) {
        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(filter_it->value(), column_it->value());
        ASSERT_EQ(filter_it->value(), doc->value);
      }
      ASSERT_FALSE(column_it->next());
    }

    // 'name' column
    {
      const std::string column_name = "name";

      irs::by_column_existence filter = make_filter(column_name, false);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto column = segment.column(column_name);
      ASSERT_NE(nullptr, column);
      auto column_it = column->iterator(irs::ColumnHint::kNormal);
      auto filter_it = prepared->execute(segment);
      ASSERT_EQ(column->size(), irs::cost::extract(*filter_it));

      auto* doc = irs::get<irs::document>(*filter_it);
      ASSERT_TRUE(bool(doc));

      size_t docs_count = 0;
      while (filter_it->next()) {
        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(filter_it->value(), column_it->value());
        ASSERT_EQ(filter_it->value(), doc->value);
        ++docs_count;
      }
      ASSERT_FALSE(column_it->next());
      ASSERT_EQ(segment.docs_count(), docs_count);
      ASSERT_EQ(segment.live_docs_count(), docs_count);
    }

    // 'seq' column
    {
      const std::string column_name = "seq";

      irs::by_column_existence filter = make_filter(column_name, false);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto column = segment.column(column_name);
      ASSERT_NE(nullptr, column);
      auto column_it = column->iterator(irs::ColumnHint::kNormal);
      auto filter_it = prepared->execute(segment);
      ASSERT_EQ(column->size(), irs::cost::extract(*filter_it));

      size_t docs_count = 0;
      while (filter_it->next()) {
        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(filter_it->value(), column_it->value());
        ++docs_count;
      }
      ASSERT_FALSE(column_it->next());
      ASSERT_EQ(segment.docs_count(), docs_count);
      ASSERT_EQ(segment.live_docs_count(), docs_count);
    }

    // 'same' column
    {
      const std::string column_name = "same";

      irs::by_column_existence filter = make_filter(column_name, false);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto column = segment.column(column_name);
      ASSERT_NE(nullptr, column);
      auto column_it = column->iterator(irs::ColumnHint::kNormal);
      auto filter_it = prepared->execute(segment);
      ASSERT_EQ(column->size(), irs::cost::extract(*filter_it));

      auto* doc = irs::get<irs::document>(*filter_it);
      ASSERT_TRUE(bool(doc));

      size_t docs_count = 0;
      while (filter_it->next()) {
        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(filter_it->value(), column_it->value());
        ++docs_count;
      }
      ASSERT_FALSE(column_it->next());
      ASSERT_EQ(segment.docs_count(), docs_count);
      ASSERT_EQ(segment.live_docs_count(), docs_count);
    }

    // 'value' column
    {
      const std::string column_name = "value";

      irs::by_column_existence filter = make_filter(column_name, false);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto column = segment.column(column_name);
      ASSERT_NE(nullptr, column);
      auto column_it = column->iterator(irs::ColumnHint::kNormal);
      auto filter_it = prepared->execute(segment);
      ASSERT_EQ(column->size(), irs::cost::extract(*filter_it));

      auto* doc = irs::get<irs::document>(*filter_it);
      ASSERT_TRUE(bool(doc));

      while (filter_it->next()) {
        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(filter_it->value(), column_it->value());
      }
      ASSERT_FALSE(column_it->next());
    }

    // 'duplicated' column
    {
      const std::string column_name = "duplicated";

      irs::by_column_existence filter = make_filter(column_name, false);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto column = segment.column(column_name);
      ASSERT_NE(nullptr, column);
      auto column_it = column->iterator(irs::ColumnHint::kNormal);
      auto filter_it = prepared->execute(segment);
      ASSERT_EQ(column->size(), irs::cost::extract(*filter_it));

      auto* doc = irs::get<irs::document>(*filter_it);
      ASSERT_TRUE(bool(doc));

      while (filter_it->next()) {
        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(filter_it->value(), column_it->value());
      }
      ASSERT_FALSE(column_it->next());
    }

    // invalid column
    {
      const std::string column_name = "invalid_column";

      irs::by_column_existence filter = make_filter(column_name, false);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto filter_it = prepared->execute(segment);
      ASSERT_EQ(0, irs::cost::extract(*filter_it));

      auto* doc = irs::get<irs::document>(*filter_it);
      ASSERT_TRUE(bool(doc));

      ASSERT_EQ(irs::doc_limits::eof(), filter_it->value());
      ASSERT_FALSE(filter_it->next());
    }
  }

  void simple_sequential_exact_match() {
    // add segment
    {
      tests::json_doc_generator gen(resource("simple_sequential.json"),
                                    &tests::generic_json_field_factory);
      add_segment(gen);
    }

    auto rdr = open_reader();

    // 'prefix' column
    {
      const std::string column_name = "prefix";

      irs::by_column_existence filter = make_filter(column_name, false);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto column = segment.column(column_name);
      ASSERT_NE(nullptr, column);
      auto column_it = column->iterator(irs::ColumnHint::kNormal);
      auto filter_it = prepared->execute(segment);

      auto* doc = irs::get<irs::document>(*filter_it);
      ASSERT_TRUE(bool(doc));

      ASSERT_EQ(column->size(), irs::cost::extract(*filter_it));

      while (filter_it->next()) {
        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(filter_it->value(), column_it->value());
        ASSERT_EQ(filter_it->value(), doc->value);
      }
      ASSERT_FALSE(column_it->next());
    }

    // 'name' column
    {
      const std::string column_name = "name";

      irs::by_column_existence filter = make_filter(column_name, false);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto column = segment.column(column_name);
      ASSERT_NE(nullptr, column);
      auto column_it = column->iterator(irs::ColumnHint::kNormal);
      auto filter_it = prepared->execute(segment);
      ASSERT_EQ(column->size(), irs::cost::extract(*filter_it));

      auto* doc = irs::get<irs::document>(*filter_it);
      ASSERT_TRUE(bool(doc));

      size_t docs_count = 0;
      while (filter_it->next()) {
        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(filter_it->value(), column_it->value());
        ASSERT_EQ(filter_it->value(), doc->value);
        ++docs_count;
      }
      ASSERT_FALSE(column_it->next());
      ASSERT_EQ(segment.docs_count(), docs_count);
      ASSERT_EQ(segment.live_docs_count(), docs_count);
    }

    // 'seq' column
    {
      const std::string column_name = "seq";

      irs::by_column_existence filter = make_filter(column_name, false);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto column = segment.column(column_name);
      ASSERT_NE(nullptr, column);
      auto column_it = column->iterator(irs::ColumnHint::kNormal);
      auto filter_it = prepared->execute(segment);
      ASSERT_EQ(column->size(), irs::cost::extract(*filter_it));

      size_t docs_count = 0;
      while (filter_it->next()) {
        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(filter_it->value(), column_it->value());
        ++docs_count;
      }
      ASSERT_FALSE(column_it->next());
      ASSERT_EQ(segment.docs_count(), docs_count);
      ASSERT_EQ(segment.live_docs_count(), docs_count);
    }

    // 'same' column
    {
      const std::string column_name = "same";

      irs::by_column_existence filter = make_filter(column_name, false);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto column = segment.column(column_name);
      ASSERT_NE(nullptr, column);
      auto column_it = column->iterator(irs::ColumnHint::kNormal);
      auto filter_it = prepared->execute(segment);
      ASSERT_EQ(column->size(), irs::cost::extract(*filter_it));

      auto* doc = irs::get<irs::document>(*filter_it);
      ASSERT_TRUE(bool(doc));

      size_t docs_count = 0;
      while (filter_it->next()) {
        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(filter_it->value(), column_it->value());
        ++docs_count;
      }
      ASSERT_FALSE(column_it->next());
      ASSERT_EQ(segment.docs_count(), docs_count);
      ASSERT_EQ(segment.live_docs_count(), docs_count);
    }

    // 'value' column
    {
      const std::string column_name = "value";

      irs::by_column_existence filter = make_filter(column_name, false);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto column = segment.column(column_name);
      ASSERT_NE(nullptr, column);
      auto column_it = column->iterator(irs::ColumnHint::kNormal);
      auto filter_it = prepared->execute(segment);
      ASSERT_EQ(column->size(), irs::cost::extract(*filter_it));

      auto* doc = irs::get<irs::document>(*filter_it);
      ASSERT_TRUE(bool(doc));

      while (filter_it->next()) {
        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(filter_it->value(), column_it->value());
      }
      ASSERT_FALSE(column_it->next());
    }

    // 'duplicated' column
    {
      const std::string column_name = "duplicated";

      irs::by_column_existence filter = make_filter(column_name, false);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto column = segment.column(column_name);
      ASSERT_NE(nullptr, column);
      auto column_it = column->iterator(irs::ColumnHint::kNormal);
      auto filter_it = prepared->execute(segment);
      ASSERT_EQ(column->size(), irs::cost::extract(*filter_it));

      auto* doc = irs::get<irs::document>(*filter_it);
      ASSERT_TRUE(bool(doc));

      while (filter_it->next()) {
        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(filter_it->value(), column_it->value());
      }
      ASSERT_FALSE(column_it->next());
    }

    // invalid column
    {
      const std::string column_name = "invalid_column";

      irs::by_column_existence filter = make_filter(column_name, false);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto filter_it = prepared->execute(segment);
      ASSERT_EQ(0, irs::cost::extract(*filter_it));

      auto* doc = irs::get<irs::document>(*filter_it);
      ASSERT_TRUE(bool(doc));

      ASSERT_EQ(irs::doc_limits::eof(), filter_it->value());
      ASSERT_FALSE(filter_it->next());
    }
  }

  void simple_sequential_prefix_match() {
    // add segment
    {
      tests::json_doc_generator gen(
        resource("simple_sequential_common_prefix.json"),
        &tests::generic_json_field_factory);
      add_segment(gen);
    }

    auto rdr = open_reader();

    // looking for 'foo*' columns
    {
      const std::string column_prefix = "foo";

      irs::by_column_existence filter = make_filter(column_prefix, true);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];
      auto column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, value);

      auto it = prepared->execute(segment);

      auto* doc = irs::get<irs::document>(*it);
      ASSERT_TRUE(bool(doc));

      // #(foo) + #(foobar) + #(foobaz) + #(fookar)
      ASSERT_EQ(8 + 9 + 1 + 10, irs::cost::extract(*it));

      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("C", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("D", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("J", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("K", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("R", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("S", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("T", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("!", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("%", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_FALSE(it->next());
    }

    // looking for 'koob*' columns
    {
      const std::string column_prefix = "koob";

      irs::by_column_existence filter = make_filter(column_prefix, true);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];
      auto column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, value);

      auto it = prepared->execute(segment);

      auto* doc = irs::get<irs::document>(*it);
      ASSERT_TRUE(bool(doc));

      // #(koobar) + #(koobaz)
      ASSERT_EQ(4 + 2, irs::cost::extract(*it));

      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("B", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("U", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("V", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("X", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("Z", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_FALSE(it->next());
    }

    // looking for 'oob*' columns
    {
      const std::string column_prefix = "oob";

      irs::by_column_existence filter = make_filter(column_prefix, true);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];
      auto column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, value);

      auto it = prepared->execute(segment);

      auto* doc = irs::get<irs::document>(*it);
      ASSERT_TRUE(bool(doc));

      // #(oobar) + #(oobaz)
      ASSERT_EQ(5 + 3, irs::cost::extract(*it));

      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("Z", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("~", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("@", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("#", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("$", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_FALSE(it->next());
    }

    // looking for 'collection*' columns
    {
      const std::string column_prefix = "collection";

      irs::by_column_existence filter = make_filter(column_prefix, true);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];
      auto column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, value);

      auto it = prepared->execute(segment);

      auto* doc = irs::get<irs::document>(*it);
      ASSERT_TRUE(bool(doc));

      // #(collection)
      ASSERT_EQ(4, irs::cost::extract(*it));

      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("A", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("J", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("L", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_TRUE(it->next());
      ASSERT_EQ(it->value(), values->seek(it->value()));
      ASSERT_EQ("N", irs::to_string<irs::string_ref>(value->value.c_str()));
      ASSERT_FALSE(it->next());
    }

    // invalid prefix
    {
      const std::string column_prefix = "invalid_prefix";

      irs::by_column_existence filter = make_filter(column_prefix, true);

      auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto filter_it = prepared->execute(segment);
      ASSERT_EQ(0, irs::cost::extract(*filter_it));

      auto* doc = irs::get<irs::document>(*filter_it);
      ASSERT_TRUE(bool(doc));

      ASSERT_EQ(irs::doc_limits::eof(), filter_it->value());
      ASSERT_FALSE(filter_it->next());
    }
  }

  void simple_sequential_order() {
    // add segment
    {
      tests::json_doc_generator gen(resource("simple_sequential.json"),
                                    &tests::generic_json_field_factory);
      add_segment(gen);
    }

    auto rdr = open_reader();

    // 'seq' column
    {
      const std::string column_name = "seq";

      irs::by_column_existence filter = make_filter(column_name, false);

      size_t collector_collect_field_count = 0;
      size_t collector_collect_term_count = 0;
      size_t collector_finish_count = 0;
      size_t scorer_score_count = 0;

      tests::sort::custom_sort sort;

      sort.collector_collect_field = [&collector_collect_field_count](
                                       const irs::sub_reader&,
                                       const irs::term_reader&) -> void {
        ++collector_collect_field_count;
      };
      sort.collector_collect_term = [&collector_collect_term_count](
                                      const irs::sub_reader&,
                                      const irs::term_reader&,
                                      const irs::attribute_provider&) -> void {
        ++collector_collect_term_count;
      };
      sort.collectors_collect_ = [&collector_finish_count](
                                   irs::byte_type*, const irs::index_reader&,
                                   const irs::sort::field_collector*,
                                   const irs::sort::term_collector*) -> void {
        ++collector_finish_count;
      };
      sort.scorer_score = [&scorer_score_count](irs::doc_id_t doc,
                                                irs::score_t* score) -> void {
        ++scorer_score_count;
        *score = irs::score_t(doc & 0xAAAAAAAA);
      };

      auto prepared_order = irs::Order::Prepare(sort);
      auto prepared_filter = filter.prepare(*rdr, prepared_order);
      std::multimap<irs::score_t, iresearch::doc_id_t> scored_result;

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto column = segment.column(column_name);
      ASSERT_NE(nullptr, column);
      auto column_itr = column->iterator(irs::ColumnHint::kNormal);
      auto filter_itr = prepared_filter->execute(segment, prepared_order);
      ASSERT_EQ(column->size(), irs::cost::extract(*filter_itr));

      auto* doc = irs::get<irs::document>(*filter_itr);
      ASSERT_TRUE(bool(doc));

      size_t docs_count = 0;
      auto* score = irs::get<irs::score>(*filter_itr);
      ASSERT_TRUE(bool(score));

      while (filter_itr->next()) {
        ASSERT_FALSE(!score);
        irs::score_t score_value;
        (*score)(&score_value);
        scored_result.emplace(score_value, filter_itr->value());
        ASSERT_TRUE(column_itr->next());
        ASSERT_EQ(filter_itr->value(), column_itr->value());
        ASSERT_EQ(filter_itr->value(), doc->value);
        ++docs_count;
      }

      ASSERT_FALSE(column_itr->next());
      ASSERT_EQ(segment.docs_count(), docs_count);
      ASSERT_EQ(segment.live_docs_count(), docs_count);

      ASSERT_EQ(
        0, collector_collect_field_count);  // should not be executed (field
                                            // statistics not applicable to
                                            // columnstore) FIXME TODO discuss
      ASSERT_EQ(0, collector_collect_term_count);  // should not be executed
      ASSERT_EQ(1, collector_finish_count);
      ASSERT_EQ(32, scorer_score_count);

      std::vector<irs::doc_id_t> expected = {
        1, 4,  5,  16, 17, 20, 21, 2,  3,  6,  7,  18, 19, 22, 23, 8,
        9, 12, 13, 24, 25, 28, 29, 10, 11, 14, 15, 26, 27, 30, 31, 32};
      std::vector<irs::doc_id_t> actual;

      for (auto& entry : scored_result) {
        actual.emplace_back(entry.second);
      }

      ASSERT_EQ(expected, actual);
    }

    // 'seq*' column (prefix single)
    {
      const std::string column_name = "seq";

      irs::by_column_existence filter = make_filter(column_name, true);

      size_t collector_collect_field_count = 0;
      size_t collector_collect_term_count = 0;
      size_t collector_finish_count = 0;
      size_t scorer_score_count = 0;

      tests::sort::custom_sort sort;

      sort.collector_collect_field = [&collector_collect_field_count](
                                       const irs::sub_reader&,
                                       const irs::term_reader&) -> void {
        ++collector_collect_field_count;
      };
      sort.collector_collect_term = [&collector_collect_term_count](
                                      const irs::sub_reader&,
                                      const irs::term_reader&,
                                      const irs::attribute_provider&) -> void {
        ++collector_collect_term_count;
      };
      sort.collectors_collect_ = [&collector_finish_count](
                                   irs::byte_type*, const irs::index_reader&,
                                   const irs::sort::field_collector*,
                                   const irs::sort::term_collector*) -> void {
        ++collector_finish_count;
      };
      sort.scorer_score = [&scorer_score_count](irs::doc_id_t doc,
                                                irs::score_t* score) -> void {
        ++scorer_score_count;
        *score = irs::score_t(doc & 0xAAAAAAAA);
      };

      auto prepared_order = irs::Order::Prepare(sort);
      auto prepared_filter = filter.prepare(*rdr, prepared_order);
      std::multimap<irs::score_t, iresearch::doc_id_t> scored_result;

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto column = segment.column(column_name);
      ASSERT_NE(nullptr, column);
      auto column_itr = column->iterator(irs::ColumnHint::kNormal);
      auto filter_itr = prepared_filter->execute(segment, prepared_order);
      ASSERT_EQ(column->size(), irs::cost::extract(*filter_itr));

      size_t docs_count = 0;
      auto* score = irs::get<irs::score>(*filter_itr);
      ASSERT_TRUE(bool(score));

      while (filter_itr->next()) {
        ASSERT_FALSE(!score);

        irs::score_t score_value;
        (*score)(&score_value);

        scored_result.emplace(score_value, filter_itr->value());
        ASSERT_TRUE(column_itr->next());
        ASSERT_EQ(filter_itr->value(), column_itr->value());
        ++docs_count;
      }

      ASSERT_FALSE(column_itr->next());
      ASSERT_EQ(segment.docs_count(), docs_count);
      ASSERT_EQ(segment.live_docs_count(), docs_count);

      ASSERT_EQ(
        0, collector_collect_field_count);  // should not be executed (field
                                            // statistics not applicable to
                                            // columnstore) FIXME TODO discuss
      ASSERT_EQ(0, collector_collect_term_count);  // should not be executed
      ASSERT_EQ(1, collector_finish_count);
      ASSERT_EQ(32, scorer_score_count);

      std::vector<irs::doc_id_t> expected = {
        1, 4,  5,  16, 17, 20, 21, 2,  3,  6,  7,  18, 19, 22, 23, 8,
        9, 12, 13, 24, 25, 28, 29, 10, 11, 14, 15, 26, 27, 30, 31, 32};
      std::vector<irs::doc_id_t> actual;

      for (auto& entry : scored_result) {
        actual.emplace_back(entry.second);
      }

      ASSERT_EQ(expected, actual);
    }

    // 's*' column (prefix multiple)
    {
      const std::string column_name = "s";
      const std::string column_name_full = "seq";

      irs::by_column_existence filter = make_filter(column_name, true);

      size_t collector_collect_field_count = 0;
      size_t collector_collect_term_count = 0;
      size_t collector_finish_count = 0;
      size_t scorer_score_count = 0;

      tests::sort::custom_sort sort;

      sort.collector_collect_field = [&collector_collect_field_count](
                                       const irs::sub_reader&,
                                       const irs::term_reader&) -> void {
        ++collector_collect_field_count;
      };
      sort.collector_collect_term = [&collector_collect_term_count](
                                      const irs::sub_reader&,
                                      const irs::term_reader&,
                                      const irs::attribute_provider&) -> void {
        ++collector_collect_term_count;
      };
      sort.collectors_collect_ = [&collector_finish_count](
                                   irs::byte_type*, const irs::index_reader&,
                                   const irs::sort::field_collector*,
                                   const irs::sort::term_collector*) -> void {
        ++collector_finish_count;
      };
      sort.scorer_score = [&scorer_score_count](irs::doc_id_t doc,
                                                irs::score_t* score) -> void {
        ++scorer_score_count;
        *score = irs::score_t(doc & 0xAAAAAAAA);
      };

      auto prepared_order = irs::Order::Prepare(sort);
      auto prepared_filter = filter.prepare(*rdr, prepared_order);
      std::multimap<irs::score_t, irs::doc_id_t> scored_result;

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto column = segment.column(column_name_full);
      ASSERT_NE(nullptr, column);
      auto column_itr = column->iterator(irs::ColumnHint::kNormal);
      auto filter_itr = prepared_filter->execute(segment, prepared_order);
      ASSERT_EQ(column->size() * 2,
                irs::cost::extract(*filter_itr));  // 2 columns matched

      size_t docs_count = 0;
      auto* score = irs::get<irs::score>(*filter_itr);
      ASSERT_TRUE(bool(score));

      while (filter_itr->next()) {
        ASSERT_FALSE(!score);
        irs::score_t score_value;
        (*score)(&score_value);
        scored_result.emplace(score_value, filter_itr->value());
        ASSERT_TRUE(column_itr->next());
        ASSERT_EQ(filter_itr->value(), column_itr->value());
        ++docs_count;
      }

      ASSERT_FALSE(column_itr->next());
      ASSERT_EQ(segment.docs_count(), docs_count);
      ASSERT_EQ(segment.live_docs_count(), docs_count);

      ASSERT_EQ(
        0, collector_collect_field_count);  // should not be executed (field
                                            // statistics not applicable to
                                            // columnstore) FIXME TODO discuss
      ASSERT_EQ(0, collector_collect_term_count);  // should not be executed
      ASSERT_EQ(1, collector_finish_count);
      ASSERT_EQ(32 * 2, scorer_score_count);  // 2 columns matched

      std::vector<irs::doc_id_t> expected = {
        1, 4,  5,  16, 17, 20, 21, 2,  3,  6,  7,  18, 19, 22, 23, 8,
        9, 12, 13, 24, 25, 28, 29, 10, 11, 14, 15, 26, 27, 30, 31, 32};
      std::vector<irs::doc_id_t> actual;

      for (auto& entry : scored_result) {
        actual.emplace_back(entry.second);
      }

      ASSERT_EQ(expected, actual);
    }
  }
};  // column_existence_filter_test_case

TEST_P(column_existence_filter_test_case, mask_column) {
  simple_sequential_mask();
}

TEST_P(column_existence_filter_test_case, exact_prefix_match) {
  simple_sequential_exact_match();
  simple_sequential_prefix_match();
  simple_sequential_order();
}

TEST(by_column_existence, options) {
  irs::by_column_existence_options opts;
  ASSERT_FALSE(opts.prefix_match);
}

TEST(by_column_existence, ctor) {
  irs::by_column_existence filter;
  ASSERT_EQ(irs::type<irs::by_column_existence>::id(), filter.type());
  ASSERT_EQ(irs::by_column_existence_options{}, filter.options());
  ASSERT_TRUE(filter.field().empty());
  ASSERT_EQ(irs::kNoBoost, filter.boost());
}

TEST(by_column_existence, boost) {
  // FIXME
}

TEST(by_column_existence, equal) {
  ASSERT_EQ(irs::by_column_existence(), irs::by_column_existence());

  {
    irs::by_column_existence q0 = make_filter("name", false);
    irs::by_column_existence q1 = make_filter("name", false);
    ASSERT_EQ(q0, q1);
    ASSERT_EQ(q0.hash(), q1.hash());
  }

  {
    irs::by_column_existence q0 = make_filter("name", true);
    irs::by_column_existence q1 = make_filter("name", false);
    ASSERT_NE(q0, q1);
  }

  {
    irs::by_column_existence q0 = make_filter("name", true);
    irs::by_column_existence q1 = make_filter("name1", true);
    ASSERT_NE(q0, q1);
  }
}

class column_existence_filter_test_case2 : public tests::FilterTestCaseBase {};


TEST_P(column_existence_filter_test_case2, mixed_seeks) {
  irs::doc_id_t with_fields[] = {
    6, 8,     15,    22,    30,    32,    38,    40,    42,    46, 57,
    59,    64,    66,    71,    73,    84,    89,    92,    102,   109,   115,
    122,   125,   133,   135,   143,   147,   156,   167,   172,   175,   185,
    189,   191,   200,   208,   213,   224,   230,   232,   240,   245,   253,
    257,   264,   267,   275,   281,   283,   294,   300,   302,   307,   313,
    323,   326,   331,   333,   339,   346,   348,   352,   359,   364,   369,
    371,   374,   381,   385,   391,   398,   406,   408,   411,   420,   429,
    435,   437,   440,   446,   454,   464,   467,   475,   484,   491,   495,
    497,   502,   505,   516,   520,   522,   526,   536,   538,   547,   549,
    560,   562,   569,   575,   579,   586,   595,   605,   607,   611,   615,
    625,   627,   638,   644,   646,   649,   651,   662,   667,   677,   683,
    689,   693,   696,   700,   707,   716,   720,   728,   731,   733,   737,
    742,   747,   755,   760,   762,   764,   774,   778,   781,   784,   789,
    795,   797,   802,   810,   821,   828,   833,   842,   845,   856,   863,
    870,   874,   879,   882,   892,   895,   901,   912,   921,   928,   935,
    939,   946,   952,   954,   960,   965,   975,   983,   992,   994,   1005,
    1009,  1015,  1018,  1022,  1025,  1027,  1034,  1044,  1052,  1056,  1059,
    1069,  1078,  1088,  1090,  1100,  1104,  1112,  1118,  1127,  1136,  1143,
    1147,  1154,  1156,  1162,  1165,  1168,  1176,  1183,  1189,  1194,  1204,
    1210,  1212,  1215,  1226,  1233,  1241,  1247,  1249,  1259,  1268,  1276,
    1283,  1285,  1295,  1305,  1315,  1321,  1325,  1330,  1332,  1334,  1342,
    1344,  1354,  1365,  1369,  1372,  1379,  1382,  1390,  1394,  1396,  1399,
    1405,  1411,  1417,  1426,  1432,  1436,  1438,  1445,  1455,  1465,  1467,
    1474,  1485,  1490,  1495,  1501,  1503,  1513,  1518,  1527,  1529,  1531,
    1542,  1550,  1560,  1567,  1578,  1588,  1591,  1594,  1600,  1611,  1620,
    1628,  1632,  1635,  1642,  1651,  1659,  1666,  1671,  1676,  1685,  1695,
    1704,  1708,  1711,  1716,  1723,  1729,  1736,  1742,  1745,  1756,  1766,
    1768,  1776,  1786,  1797,  1799,  1804,  1815,  1819,  1821,  1831,  1836,
    1845,  1856,  1867,  1870,  1881,  1889,  1891,  1902,  1909,  1911,  1914,
    1918,  1923,  1928,  1934,  1938,  1942,  1950,  1956,  1967,  1973,  1977,
    1980,  1986,  1994,  1996,  1998,  2009,  2016,  2023,  2025,  2030,  2033,
    2044,  2047,  2053,  2059,  2061,  2065,  2072,  2074,  2081,  2092,  2100,
    2106,  2117,  2120,  2122,  2129,  2131,  2135,  2142,  2150,  2158,  2160,
    2165,  2172,  2174,  2178,  2182,  2191,  2201,  2210,  2221,  2223,  2225,
    2233,  2241,  2248,  2257,  2264,  2269,  2272,  2276,  2287,  2293,  2295,
    2306,  2308,  2310,  2316,  2325,  2327,  2329,  2337,  2346,  2355,  2364,
    2367,  2369,  2371,  2378,  2382,  2393,  2397,  2406,  2408,  2414,  2421,
    2428,  2438,  2442,  2452,  2462,  2468,  2476,  2480,  2485,  2487,  2496,
    2500,  2502,  2507,  2509,  2513,  2523,  2528,  2537,  2547,  2549,  2557,
    2560,  2568,  2577,  2587,  2592,  2603,  2606,  2608,  2611,  2620,  2623,
    2625,  2630,  2640,  2643,  2653,  2660,  2663,  2670,  2675,  2680,  2684,
    2689,  2691,  2693,  2700,  2710,  2712,  2721,  2732,  2741,  2747,  2754,
    2758,  2760,  2766,  2774,  2779,  2781,  2787,  2798,  2800,  2808,  2819,
    2829,  2834,  2839,  2849,  2855,  2861,  2871,  2874,  2882,  2888,  2898,
    2909,  2919,  2922,  2925,  2929,  2935,  2944,  2946,  2955,  2957,  2959,
    2966,  2968,  2970,  2979,  2986,  2994,  2996,  3006,  3010,  3014,  3018,
    3024,  3030,  3039,  3041,  3044,  3047,  3051,  3056,  3060,  3070,  3079,
    3089,  3098,  3101,  3105,  3115,  3121,  3123,  3128,  3134,  3138,  3141,
    3148,  3158,  3165,  3175,  3177,  3187,  3189,  3199,  3201,  3210,  3212,
    3217,  3219,  3226,  3229,  3231,  3236,  3244,  3252,  3261,  3270,  3278,
    3284,  3294,  3304,  3315,  3322,  3328,  3332,  3338,  3345,  3355,  3358,
    3365,  3376,  3386,  3391,  3393,  3396,  3407,  3411,  3414,  3420,  3422,
    3433,  3439,  3448,  3459,  3463,  3468,  3473,  3476,  3482,  3484,  3490,
    3495,  3501,  3504,  3510,  3516,  3522,  3525,  3529,  3535,  3546,  3557,
    3562,  3568,  3573,  3578,  3585,  3595,  3604,  3608,  3619,  3626,  3636,
    3638,  3648,  3658,  3660,  3662,  3664,  3668,  3679,  3682,  3687,  3697,
    3704,  3706,  3708,  3716,  3723,  3725,  3733,  3743,  3746,  3754,  3764,
    3766,  3771,  3773,  3775,  3786,  3794,  3798,  3804,  3812,  3822,  3825,
    3834,  3836,  3841,  3850,  3860,  3869,  3874,  3878,  3881,  3883,  3889,
    3891,  3895,  3905,  3916,  3923,  3926,  3930,  3933,  3939,  3943,  3947,
    3952,  3959,  3961,  3970,  3977,  3985,  3989,  3994,  3997,  3999,  4004,
    4014,  4022,  4024,  4032,  4034,  4036,  4047,  4054,  4064,  4069,  4071,
    4080,  4085,  4088,  4097,  4101,  4110,  4112,  4116,  4126,  4135,  4138,
    4141,  4144,  4154,  4156,  4163,  4172,  4181,  4191,  4196,  4198,  4200,
    4208,  4214,  4221,  4230,  4232,  4243,  4246,  4254,  4263,  4265,  4269,
    4280,  4287,  4289,  4300,  4308,  4311,  4318,  4324,  4332,  4339,  4342,
    4344,  4348,  4355,  4365,  4368,  4375,  4379,  4385,  4387,  4393,  4398,
    4402,  4412,  4414,  4422,  4426,  4432,  4443,  4454,  4456,  4467,  4472,
    4479,  4483,  4485,  4495,  4498,  4508,  4516,  4523,  4530,  4541,  4551,
    4562,  4567,  4573,  4583,  4589,  4592,  4599,  4601,  4603,  4612,  4620,
    4622,  4624,  4630,  4637,  4647,  4658,  4669,  4671,  4673,  4675,  4682,
    4690,  4697,  4704,  4714,  4717,  4724,  4726,  4734,  4744,  4752,  4754,
    4764,  4775,  4782,  4790,  4796,  4807,  4809,  4816,  4822,  4827,  4829,
    4833,  4840,  4848,  4858,  4861,  4863,  4873,  4883,  4894,  4897,  4905,
    4916,  4920,  4929,  4931,  4937,  4947,  4957,  4959,  4965,  4974,  4976,
    4987,  4994,  4997,  5003,  5005,  5014,  5020,  5028,  5035,  5037,  5044,
    5050,  5055,  5059,  5069,  5080,  5082,  5093,  5095,  5099,  5109,  5111,
    5113,  5124,  5130,  5132,  5139,  5143,  5148,  5155,  5157,  5160,  5168,
    5170,  5178,  5189,  5196,  5198,  5205,  5208,  5210,  5215,  5221,  5229,
    5233,  5240,  5249,  5253,  5257,  5265,  5267,  5271,  5276,  5286,  5297,
    5300,  5309,  5320,  5322,  5324,  5329,  5335,  5342,  5349,  5351,  5354,
    5359,  5366,  5371,  5374,  5376,  5387,  5389,  5393,  5399,  5405,  5413,
    5421,  5431,  5440,  5442,  5444,  5453,  5463,  5466,  5477,  5482,  5488,
    5493,  5497,  5499,  5501,  5510,  5512,  5518,  5521,  5524,  5527,  5529,
    5531,  5534,  5536,  5543,  5545,  5548,  5550,  5552,  5562,  5570,  5580,
    5590,  5598,  5608,  5610,  5612,  5614,  5616,  5627,  5633,  5636,  5639,
    5643,  5647,  5656,  5664,  5666,  5670,  5674,  5676,  5678,  5685,  5687,
    5697,  5699,  5707,  5710,  5716,  5727,  5733,  5735,  5746,  5753,  5762,
    5765,  5773,  5781,  5784,  5786,  5788,  5790,  5798,  5805,  5816,  5824,
    5832,  5834,  5836,  5842,  5850,  5859,  5867,  5877,  5880,  5887,  5889,
    5895,  5902,  5904,  5914,  5916,  5924,  5932,  5942,  5953,  5956,  5961,
    5963,  5974,  5978,  5980,  5983,  5989,  5999,  6007,  6012,  6017,  6027,
    6037,  6045,  6048,  6054,  6056,  6060,  6063,  6074,  6077,  6087,  6090,
    6092,  6095,  6098,  6102,  6105,  6111,  6115,  6123,  6126,  6130,  6133,
    6144,  6147,  6150,  6160,  6163,  6165,  6171,  6176,  6187,  6195,  6202,
    6209,  6220,  6231,  6235,  6240,  6249,  6258,  6262,  6268,  6270,  6277,
    6279,  6289,  6299,  6307,  6311,  6319,  6326,  6330,  6332,  6334,  6339,
    6348,  6355,  6363,  6374,  6376,  6382,  6389,  6395,  6401,  6406,  6417,
    6422,  6433,  6440,  6451,  6461,  6467,  6473,  6475,  6486,  6491,  6502,
    6504,  6513,  6521,  6523,  6525,  6530,  6540,  6547,  6555,  6566,  6568,
    6570,  6577,  6586,  6588,  6599,  6603,  6609,  6614,  6625,  6635,  6640,
    6651,  6653,  6659,  6670,  6675,  6682,  6686,  6695,  6697,  6706,  6717,
    6723,  6725,  6729,  6732,  6737,  6744,  6748,  6759,  6767,  6775,  6780,
    6782,  6786,  6788,  6791,  6799,  6808,  6814,  6822,  6830,  6840,  6847,
    6852,  6862,  6865,  6867,  6875,  6882,  6886,  6896,  6901,  6903,  6905,
    6912,  6923,  6934,  6936,  6946,  6956,  6962,  6971,  6977,  6981,  6985,
    6996,  7003,  7014,  7016,  7024,  7026,  7033,  7036,  7043,  7048,  7052,
    7058,  7060,  7065,  7072,  7077,  7084,  7086,  7088,  7090,  7093,  7103,
    7113,  7121,  7123,  7133,  7137,  7139,  7150,  7152,  7156,  7167,  7178,
    7184,  7189,  7195,  7200,  7208,  7218,  7223,  7227,  7233,  7239,  7250,
    7260,  7270,  7278,  7284,  7293,  7295,  7301,  7307,  7313,  7323,  7332,
    7337,  7348,  7351,  7356,  7361,  7365,  7370,  7374,  7376,  7382,  7385,
    7393,  7399,  7401,  7410,  7412,  7418,  7429,  7435,  7437,  7446,  7456,
    7467,  7472,  7480,  7488,  7491,  7493,  7495,  7501,  7512,  7517,  7524,
    7532,  7541,  7547,  7556,  7566,  7569,  7571,  7577,  7585,  7590,  7593,
    7597,  7603,  7613,  7618,  7627,  7636,  7640,  7651,  7658,  7665,  7673,
    7678,  7687,  7697,  7702,  7704,  7706,  7710,  7713,  7724,  7731,  7733,
    7742,  7752,  7761,  7764,  7766,  7769,  7775,  7778,  7781,  7784,  7786,
    7796,  7806,  7811,  7817,  7827,  7836,  7842,  7844,  7847,  7857,  7862,
    7864,  7866,  7868,  7877,  7879,  7887,  7889,  7895,  7901,  7910,  7912,
    7919,  7927,  7935,  7943,  7947,  7955,  7958,  7966,  7976,  7986,  7992,
    7995,  8000,  8003,  8011,  8013,  8022,  8031,  8033,  8044,  8051,  8054,
    8063,  8073,  8084,  8086,  8088,  8092,  8102,  8104,  8113,  8122,  8124,
    8129,  8138,  8143,  8146,  8150,  8157,  8167,  8170,  8179,  8181,  8189,
    8199,  8208,  8219,  8230,  8235,  8242,  8252,  8263,  8274,  8283,  8292,
    8302,  8313,  8316,  8318,  8329,  8331,  8333,  8336,  8344,  8353,  8355,
    8363,  8372,  8381,  8386,  8392,  8399,  8409,  8413,  8415,  8423,  8430,
    8432,  8440,  8442,  8451,  8458,  8463,  8467,  8469,  8472,  8475,  8483,
    8493,  8498,  8505,  8508,  8511,  8514,  8519,  8525,  8533,  8536,  8543,
    8549,  8553,  8555,  8561,  8570,  8573,  8575,  8583,  8585,  8596,  8605,
    8615,  8620,  8625,  8636,  8638,  8649,  8658,  8669,  8675,  8678,  8682,
    8691,  8697,  8708,  8710,  8717,  8728,  8738,  8741,  8746,  8748,  8750,
    8754,  8760,  8764,  8773,  8776,  8780,  8783,  8788,  8798,  8809,  8812,
    8816,  8818,  8827,  8830,  8841,  8845,  8852,  8863,  8869,  8879,  8890,
    8896,  8903,  8906,  8913,  8917,  8928,  8930,  8936,  8941,  8943,  8952,
    8962,  8964,  8972,  8980,  8984,  8994,  9000,  9008,  9017,  9020,  9029,
    9039,  9050,  9060,  9062,  9070,  9078,  9085,  9094,  9102,  9108,  9115,
    9126,  9137,  9140,  9142,  9150,  9154,  9159,  9168,  9175,  9183,  9193,
    9197,  9201,  9209,  9217,  9219,  9223,  9231,  9236,  9246,  9254,  9259,
    9267,  9272,  9275,  9284,  9290,  9294,  9297,  9306,  9313,  9324,  9332,
    9337,  9344,  9347,  9355,  9357,  9362,  9371,  9382,  9384,  9393,  9398,
    9404,  9412,  9422,  9424,  9430,  9432,  9437,  9447,  9452,  9460,  9468,
    9470,  9472,  9482,  9486,  9497,  9503,  9505,  9513,  9516,  9520,  9528,
    9534,  9542,  9552,  9561,  9563,  9572,  9583,  9588,  9596,  9602,  9608,
    9615,  9624,  9629,  9631,  9634,  9644,  9648,  9654,  9658,  9665,  9667,
    9676,  9684,  9692,  9703,  9707,  9716,  9719,  9721,  9728,  9739,  9742,
    9747,  9751,  9758,  9760,  9769,  9771,  9779,  9786,  9791,  9802,  9807,
    9813,  9822,  9831,  9837,  9844,  9849,  9851,  9853,  9855,  9857,  9868,
    9872,  9876,  9883,  9889,  9895,  9905,  9912,  9921,  9932,  9938,  9940,
    9951,  9958,  9968,  9971,  9975,  9979,  9981,  9983,  9986,  9992,  10003,
    10013, 10019, 10030, 10035, 10037, 10045, 10047, 10049, 10059, 10068, 10070,
    10072, 10083, 10093, 10104, 10111, 10119, 10124, 10133, 10139, 10147, 10156,
    10166, 10176, 10180, 10184, 10186, 10196, 10205, 10213, 10221, 10226, 10236,
    10241, 10245, 10247, 10251, 10255, 10259, 10266, 10271, 10279, 10289, 10291,
    10298, 10304, 10310, 10320, 10322, 10332, 10335, 10337, 10339, 10350, 10352,
    10363, 10373, 10380, 10391, 10402, 10407, 10412, 10414, 10416, 10426, 10435,
    10445, 10449, 10456, 10466, 10468, 10473, 10475, 10486, 10497, 10503, 10509,
    10520, 10522, 10524, 10528, 10531, 10536, 10546, 10553, 10561, 10570, 10577,
    10584, 10586, 10594, 10603, 10614, 10621, 10632, 10643, 10645, 10655, 10661,
    10665, 10676, 10687, 10692, 10703, 10710, 10720, 10727, 10732, 10734, 10738,
    10746, 10750, 10752, 10759, 10763, 10768, 10773, 10776, 10784, 10789, 10795,
    10801, 10807, 10814, 10818, 10825, 10835, 10841, 10851, 10855, 10857, 10868,
    10874, 10880, 10882, 10886, 10890, 10898, 10906, 10917, 10926, 10937, 10948,
    10957, 10960, 10966, 10971, 10978, 10980, 10983, 10993, 11000, 11002, 11005,
    11009, 11016, 11023, 11027, 11031, 11035, 11041, 11044, 11048, 11058, 11066,
    11072, 11082, 11086, 11091, 11097, 11101, 11104, 11111, 11114, 11118, 11124,
    11130, 11140, 11144, 11153, 11155, 11161, 11170, 11176, 11184, 11192, 11202,
    11209, 11219, 11223, 11227, 11229, 11231, 11236, 11238, 11249, 11258, 11268,
    11272, 11274, 11281, 11283, 11290, 11293, 11299, 11306, 11315, 11320, 11328,
    11338, 11340, 11344, 11350, 11357, 11363, 11373, 11384, 11388, 11397, 11406,
    11408, 11417, 11424, 11431, 11436, 11447, 11457, 11466, 11468, 11473, 11478,
    11481, 11490, 11495, 11501, 11506, 11511, 11518, 11528, 11537, 11539, 11541,
    11551, 11562, 11564, 11570, 11575, 11583, 11594, 11600, 11605, 11607, 11612,
    11617, 11622, 11633, 11643, 11654, 11656, 11661, 11667, 11672, 11678, 11686,
    11696, 11700, 11706, 11712, 11720, 11729, 11734, 11744, 11746, 11753, 11761,
    11769, 11773, 11783, 11788, 11798, 11800, 11803, 11811, 11815, 11822, 11824,
    11829, 11831, 11835, 11839, 11843, 11845, 11848, 11859, 11862, 11869, 11879,
    11885, 11890, 11895, 11900, 11902, 11904, 11915, 11917, 11927, 11929, 11932,
    11943, 11950, 11953, 11960, 11970, 11977, 11981, 11991, 11996, 12003, 12005,
    12009, 12014, 12017, 12020, 12028, 12036, 12046, 12055, 12062, 12071, 12073,
    12084, 12086, 12089, 12099, 12110, 12121, 12127, 12138, 12149, 12155, 12164,
    12173, 12182, 12186, 12192, 12201, 12211, 12214, 12224, 12230, 12237, 12242,
    12246, 12252, 12261, 12271, 12273, 12281, 12289, 12295, 12297, 12308, 12316,
    12320, 12322, 12324, 12329, 12337, 12344, 12349, 12352, 12359, 12368, 12377,
    12379, 12388, 12390, 12396, 12398, 12405, 12407, 12417, 12422, 12426, 12431,
    12433, 12442, 12446, 12448, 12453, 12459, 12462, 12469, 12474, 12480, 12482,
    12485, 12492, 12498, 12500, 12507, 12513, 12515, 12520, 12527, 12534, 12537,
    12544, 12552, 12559, 12563, 12571, 12577, 12581, 12585, 12592, 12595, 12602,
    12610, 12614, 12619, 12624, 12626, 12635, 12646, 12650, 12652, 12656, 12658,
    12666, 12672, 12676, 12686, 12696, 12706, 12712, 12721, 12724, 12730, 12739,
    12743, 12749, 12756, 12762, 12768, 12774, 12784, 12786, 12790, 12792, 12801,
    12810, 12821, 12826, 12828, 12836, 12840, 12845, 12847, 12855, 12858, 12861,
    12863, 12867, 12878, 12886, 12897, 12907, 12914, 12921, 12930, 12937, 12945,
    12948, 12955, 12963, 12965, 12967, 12974, 12980, 12986, 12990, 12998, 13005,
    13013, 13015, 13023, 13029, 13035, 13039, 13050, 13052, 13057, 13060, 13062,
    13065, 13070, 13072, 13081, 13087, 13098, 13100, 13106, 13111, 13118, 13125,
    13130, 13134, 13136, 13144, 13147, 13150, 13152, 13161, 13165, 13176, 13181,
    13188, 13195, 13200, 13208, 13213, 13219, 13228, 13238, 13242, 13251, 13256,
    13258, 13264, 13268, 13276, 13287, 13289, 13292, 13295, 13299, 13303, 13311,
    13313, 13323, 13330, 13332, 13335, 13337, 13341, 13344, 13346, 13354, 13357,
    13367, 13369, 13379, 13382, 13389, 13392, 13397, 13406, 13416, 13418, 13427,
    13436, 13444, 13447, 13457, 13466, 13468, 13473, 13482, 13484, 13495, 13502,
    13508, 13513, 13521, 13528, 13537, 13545, 13554, 13559, 13564, 13569, 13576,
    13578, 13588, 13598, 13606, 13610, 13612, 13622, 13630, 13635, 13642, 13644,
    13654, 13661, 13666, 13668, 13673, 13677, 13681, 13692, 13699, 13706, 13713,
    13715, 13722, 13728, 13738, 13747, 13758, 13762, 13769, 13775, 13781, 13789,
    13800, 13811, 13817, 13819, 13822, 13827, 13837, 13845, 13847, 13850, 13859,
    13869, 13877, 13885, 13896, 13901, 13912, 13915, 13923, 13931, 13939, 13947,
    13954, 13959, 13961, 13969, 13972, 13974, 13984, 13988, 13991, 13993, 14004,
    14006, 14010, 14019, 14030, 14033, 14035, 14045, 14053, 14061, 14070, 14074,
    14076, 14084, 14086, 14088, 14096, 14098, 14100, 14109, 14111, 14118, 14121,
    14125, 14136, 14140, 14144, 14154, 14165, 14175, 14179, 14181, 14183, 14189,
    14200, 14211, 14220, 14228, 14230, 14238, 14249, 14260, 14264, 14269, 14275,
    14285, 14293, 14303, 14310, 14321, 14324, 14332, 14335, 14338, 14344, 14353,
    14360, 14364, 14372, 14382, 14387, 14389, 14391, 14400, 14404, 14406, 14413,
    14418, 14428, 14437, 14443, 14448, 14456, 14461, 14466, 14476, 14484, 14490,
    14501, 14509, 14520, 14530, 14535, 14538, 14545, 14547, 14553, 14562, 14566,
    14568, 14571, 14577, 14583, 14593, 14595, 14598, 14609, 14620, 14622, 14627,
    14631, 14641, 14645, 14652, 14661, 14663, 14668, 14675, 14682, 14689, 14697,
    14706, 14708, 14711, 14714, 14722, 14724, 14727, 14732, 14742, 14752, 14757,
    14767, 14772, 14780, 14784, 14795, 14805, 14811, 14815, 14824, 14830, 14839,
    14850, 14861, 14867, 14877, 14886, 14891, 14894, 14901, 14908, 14913, 14917,
    14926, 14929, 14936, 14946, 14953, 14957, 14963, 14970, 14974, 14984, 14992,
    14994, 15002, 15012, 15016, 15022, 15030, 15036, 15043, 15045, 15053, 15062,
    15065, 15068, 15079, 15088, 15095, 15099, 15103, 15114, 15118, 15126, 15137,
    15147, 15157, 15159, 15162, 15173, 15175, 15183, 15190, 15196, 15203, 15211,
    15217, 15226, 15228, 15232, 15238, 15242, 15251, 15262, 15272, 15274, 15282,
    15284, 15294, 15303, 15309, 15314, 15325, 15329, 15333, 15340, 15350, 15355,
    15361, 15369, 15373, 15384, 15388, 15399, 15408, 15418, 15424, 15431, 15433,
    15438, 15441, 15448, 15455, 15457, 15463, 15472, 15481, 15486, 15492, 15500,
    15506, 15510, 15518, 15523, 15532, 15538, 15540, 15549, 15560, 15562, 15566,
    15568, 15573, 15575, 15578, 15583, 15594, 15601, 15607, 15618, 15627, 15638,
    15643, 15652, 15659, 15669, 15671, 15679, 15689, 15696, 15701, 15705, 15714,
    15719, 15730, 15738, 15743, 15746, 15748, 15755, 15757, 15761, 15772, 15776,
    15786, 15788, 15793, 15799, 15807, 15814, 15816, 15820, 15824, 15826, 15830,
    15832, 15837, 15847, 15857, 15859, 15866, 15869, 15871, 15880, 15891, 15898,
    15904, 15912, 15915, 15917, 15925, 15930, 15932, 15942, 15951, 15962, 15969,
    15977, 15983, 15991, 15994, 16003, 16008, 16012, 16021, 16032, 16034, 16044,
    16055, 16065, 16070, 16076, 16084, 16088, 16092, 16094, 16096, 16106, 16110,
    16120, 16124, 16135, 16146, 16148, 16156, 16160, 16162, 16171, 16181, 16183,
    16188, 16190, 16192, 16198, 16200, 16208, 16217, 16219, 16230, 16232, 16239,
    16250, 16253, 16261, 16271, 16279, 16290, 16300, 16306, 16311, 16319, 16321,
    16330, 16339, 16342, 16346, 16348, 16358, 16364, 16369, 16373, 16375, 16381,
    16388, 16396, 16402, 16411, 16422, 16433, 16444, 16446, 16456, 16458, 16460,
    16463, 16468, 16470, 16472, 16479, 16483, 16488, 16492, 16497, 16506, 16512,
    16519, 16530, 16541, 16543, 16549, 16554, 16559, 16562, 16564, 16573, 16575,
    16578, 16580, 16587, 16596, 16598, 16608, 16618, 16629, 16640, 16643, 16650,
    16652, 16660, 16662, 16664, 16666, 16668, 16674, 16676, 16684, 16687, 16694,
    16696, 16702, 16707, 16710, 16718, 16724, 16726, 16731, 16742, 16750, 16760,
    16762, 16765, 16771, 16775, 16777, 16785, 16787, 16790, 16799, 16807, 16809,
    16812, 16818, 16821, 16825, 16828, 16832, 16834, 16836, 16840, 16847, 16857,
    16863, 16866, 16875, 16877, 16885, 16887, 16892, 16903, 16909, 16911, 16916,
    16920, 16925, 16927, 16932, 16938, 16942, 16950, 16960, 16965, 16970, 16972,
    16977, 16987, 16995, 17003, 17007, 17012, 17021, 17023, 17030, 17034, 17043,
    17050, 17052, 17063, 17065, 17074, 17085, 17096, 17104, 17113, 17123, 17126,
    17137, 17139, 17145, 17151, 17153, 17158, 17162, 17164, 17173, 17184, 17193,
    17204, 17211, 17217, 17225, 17234, 17236, 17244, 17253, 17263, 17272, 17278,
    17280, 17289, 17291, 17302, 17312, 17322, 17329, 17333, 17335, 17346, 17356,
    17358, 17360, 17364, 17375, 17384, 17388, 17392, 17399, 17407, 17414, 17425,
    17430, 17433, 17439, 17447, 17449, 17460, 17471, 17476, 17481, 17483, 17490,
    17501, 17511, 17522, 17531, 17539, 17543, 17553, 17564, 17571, 17579, 17590,
    17598, 17605, 17607, 17618, 17626, 17633, 17642, 17645, 17647, 17657, 17668,
    17673, 17680, 17691, 17695, 17697, 17708, 17714, 17716, 17727, 17730, 17734,
    17744, 17746, 17748, 17757, 17761, 17772, 17774, 17776, 17785, 17788, 17799,
    17801, 17811, 17820, 17830, 17832, 17842, 17845, 17852, 17861, 17863, 17865,
    17867, 17872, 17882, 17886, 17890, 17893, 17895, 17904, 17906, 17912, 17919,
    17921, 17932, 17934, 17940, 17948, 17950, 17955, 17964, 17971, 17976, 17984,
    17989, 17994, 18004, 18015, 18026, 18033, 18036, 18044, 18053, 18062, 18066,
    18068, 18075, 18085, 18095, 18099, 18108, 18115, 18126, 18136, 18143, 18146,
    18156, 18159, 18168, 18170, 18177, 18188, 18195, 18206, 18214, 18221, 18228,
    18236, 18241, 18247, 18249, 18259, 18265, 18268, 18275, 18285, 18294, 18304,
    18309, 18315, 18326, 18330, 18335, 18344, 18346, 18350, 18359, 18361, 18363,
    18374, 18377, 18382, 18390, 18392, 18394, 18398, 18401, 18411, 18422, 18430,
    18432, 18440, 18447, 18451, 18457, 18459, 18469, 18472, 18479, 18486, 18496,
    18499, 18503, 18507, 18511, 18520, 18526, 18533, 18542, 18546, 18548, 18558,
    18569, 18580, 18584, 18586, 18590, 18592, 18597, 18603, 18605, 18610, 18617,
    18625, 18634, 18639, 18650, 18661, 18669, 18671, 18681, 18692, 18696, 18698,
    18703, 18709, 18718, 18723, 18730, 18734, 18739, 18746, 18750, 18754, 18764,
    18772, 18781, 18784, 18786, 18792, 18794, 18796, 18803, 18808, 18818, 18823,
    18828, 18830, 18836, 18845, 18849, 18860, 18867, 18878, 18887, 18894, 18896,
    18898, 18900, 18906, 18917, 18921, 18925, 18936, 18942, 18944, 18950, 18959,
    18965, 18971, 18973, 18979, 18986, 18991, 19000, 19007, 19009, 19011, 19019,
    19024, 19032, 19038, 19040, 19045, 19050, 19052, 19054, 19060, 19062, 19068,
    19073, 19078, 19081, 19086, 19088, 19095, 19106, 19111, 19117, 19126, 19133,
    19135, 19146, 19157, 19165, 19172, 19174, 19181, 19189, 19197, 19208, 19219,
    19230, 19237, 19247, 19249, 19251, 19261, 19266, 19276, 19281, 19284, 19294,
    19305, 19316, 19321, 19327, 19329, 19339, 19344, 19346, 19352, 19354, 19361,
    19371, 19373, 19375, 19381, 19384, 19389, 19397, 19405, 19408, 19416, 19427,
    19436, 19441, 19444, 19446, 19457, 19463, 19470, 19478, 19486, 19490, 19495,
    19499, 19507, 19512, 19520, 19526, 19532, 19540, 19546, 19550, 19553, 19562,
    19568, 19575, 19577, 19579, 19586, 19597, 19605, 19612, 19622, 19624, 19632,
    19634, 19637, 19641, 19643, 19649, 19653, 19661, 19670, 19672, 19675, 19682,
    19684, 19691, 19693, 19695, 19697, 19704, 19711, 19718, 19720, 19728, 19737,
    19745, 19747, 19749, 19753, 19763, 19774, 19779, 19786, 19797, 19808, 19811,
    19813, 19821, 19828, 19838, 19847, 19853, 19864, 19871, 19873, 19879, 19881,
    19889, 19891, 19894, 19900, 19907, 19912, 19920, 19926, 19931, 19935, 19945,
    19954, 19956, 19966, 19968, 19976, 19978, 19981, 19983, 19986, 19995, 19997,
    20008, 20012, 20014, 20016, 20020, 20022, 20025, 20035, 20039, 20043, 20050,
    20052, 20054, 20060, 20066, 20068, 20070, 20075, 20083, 20091, 20097, 20101,
    20105, 20109, 20113, 20123, 20128, 20135, 20144, 20147, 20158, 20166, 20168,
    20178, 20185, 20196, 20203, 20212, 20221, 20227, 20231, 20237, 20240, 20245,
    20248, 20259, 20269, 20279, 20287, 20295, 20304, 20312, 20321, 20327, 20330,
    20338, 20346, 20348, 20358, 20361, 20368, 20374, 20382, 20388, 20390, 20396,
    20398, 20409, 20411, 20414, 20416, 20421, 20426, 20429, 20433, 20442, 20446,
    20457, 20460, 20471, 20473, 20484, 20486, 20495, 20506, 20508, 20517, 20521,
    20529, 20538, 20544, 20551, 20558, 20568, 20570, 20579, 20590, 20599, 20609,
    20619, 20626, 20628, 20638, 20640, 20651, 20659, 20668, 20675, 20683, 20692,
    20701, 20703, 20708, 20712, 20721, 20732, 20742, 20747, 20757, 20766, 20773,
    20782, 20793, 20795, 20797, 20806, 20817, 20820, 20829, 20832, 20836, 20840,
    20845, 20854, 20863, 20870, 20872, 20877, 20881, 20887, 20895, 20900, 20903,
    20912, 20922, 20931, 20933, 20937, 20940, 20944, 20952, 20955, 20960, 20966,
    20968, 20971, 20982, 20985, 20990, 20995, 21004, 21006, 21010, 21015, 21021,
    21031, 21041, 21051, 21059, 21069, 21071, 21077, 21079, 21087, 21094, 21096,
    21098, 21106, 21112, 21119, 21129, 21136, 21138, 21141, 21151, 21155, 21166,
    21177, 21182, 21185, 21188, 21198, 21200, 21202, 21210, 21213, 21216, 21225,
    21234, 21236, 21241, 21252, 21254, 21262, 21267, 21275, 21277, 21288, 21296,
    21301, 21304, 21312, 21322, 21332, 21343, 21345, 21354, 21361, 21367, 21369,
    21371, 21376, 21380, 21390, 21392, 21401, 21412, 21414, 21418, 21424, 21433,
    21444, 21448, 21454, 21463, 21472, 21483, 21485, 21493, 21501, 21503, 21505,
    21516, 21518, 21520, 21528, 21534, 21536, 21538, 21545, 21548, 21553, 21559,
    21563, 21569, 21580, 21584, 21593, 21595, 21605, 21607, 21609, 21619, 21623,
    21632, 21637, 21643, 21652, 21654, 21663, 21668, 21674, 21682, 21686, 21690,
    21695, 21700, 21703, 21713, 21718, 21724, 21734, 21742, 21752, 21762, 21767,
    21778, 21784, 21791, 21799, 21801, 21812, 21816, 21818, 21825, 21832, 21843,
    21852, 21863, 21874, 21877, 21887, 21897, 21906, 21910, 21919, 21928, 21936,
    21940, 21950, 21955, 21960, 21964, 21973, 21976, 21983, 21988, 21994, 22005,
    22013, 22023, 22026, 22032, 22034, 22041, 22045, 22047, 22058, 22061, 22072,
    22076, 22084, 22087, 22097, 22099, 22103, 22105, 22111, 22119, 22126, 22133,
    22136, 22138, 22143, 22145, 22147, 22155, 22166, 22169, 22175, 22177, 22179,
    22182, 22192, 22200, 22204, 22212, 22222, 22224, 22231, 22236, 22238, 22241,
    22250, 22259, 22267, 22273, 22277, 22279, 22289, 22298, 22309, 22315, 22319,
    22321, 22327, 22329, 22334, 22338, 22340, 22342, 22353, 22355, 22366, 22377,
    22381, 22384, 22386, 22397, 22401, 22408, 22413, 22415, 22422, 22427, 22434,
    22444, 22449, 22460, 22467, 22474, 22476, 22484, 22493, 22496, 22500, 22508,
    22510, 22514, 22521, 22531, 22537, 22548, 22557, 22562, 22567, 22574, 22582,
    22584, 22586, 22597, 22601, 22612, 22614, 22619, 22621, 22627, 22630, 22636,
    22640, 22648, 22650, 22659, 22662, 22672, 22674, 22684, 22689, 22694, 22705,
    22711, 22719, 22727, 22729, 22734, 22737, 22747, 22754, 22763, 22771, 22778,
    22784, 22788, 22792, 22794, 22796, 22804, 22812, 22821, 22827, 22836, 22839,
    22844, 22851, 22854, 22860, 22867, 22873, 22875, 22885, 22887, 22898, 22904,
    22909, 22912, 22921, 22931, 22934, 22943, 22947, 22957, 22959, 22962, 22967,
    22977, 22988, 22999, 23005, 23013, 23022, 23032, 23037, 23044, 23052, 23059,
    23065, 23075, 23083, 23088, 23096, 23106, 23117, 23123, 23130, 23140, 23142,
    23153, 23155, 23160, 23169, 23178, 23182, 23188, 23194, 23196, 23205, 23214,
    23218, 23228, 23238, 23246, 23252, 23257, 23259, 23261, 23268, 23270, 23272,
    23274, 23276, 23283, 23287, 23296, 23300, 23309, 23320, 23328, 23333, 23336,
    23338, 23345, 23354, 23357, 23360, 23364, 23372, 23378, 23383, 23385, 23387,
    23397, 23399, 23407, 23409, 23418, 23420, 23423, 23434, 23436, 23443, 23447,
    23450, 23460, 23462, 23471, 23476, 23487, 23496, 23498, 23500, 23504, 23511,
    23514, 23517, 23519, 23523, 23533, 23542, 23548, 23551, 23557, 23560, 23566,
    23568, 23577, 23585, 23594, 23602, 23605, 23607, 23614, 23616, 23624, 23627,
    23630, 23636, 23638, 23646, 23649, 23651, 23653, 23663, 23674, 23677, 23682,
    23685, 23692, 23703, 23709, 23720, 23729, 23738, 23748, 23756, 23765, 23769,
    23774, 23776, 23787, 23791, 23797, 23807, 23809, 23816, 23821, 23830, 23832,
    23834, 23841, 23852, 23859, 23862, 23873, 23875, 23877, 23887, 23889, 23900,
    23911, 23914, 23918, 23920, 23931, 23941, 23950, 23952, 23956, 23966, 23977,
    23980, 23988, 23992, 23995, 24003, 24005, 24010, 24019, 24021, 24031, 24033,
    24041, 24051, 24054, 24056, 24062, 24068, 24073, 24078, 24085, 24095, 24099,
    24104, 24111, 24122, 24130, 24133, 24143, 24147, 24155, 24165, 24169, 24175,
    24179, 24187, 24190, 24198, 24204, 24214, 24224, 24233, 24235, 24237, 24239,
    24249, 24251, 24256, 24258, 24260, 24270, 24272, 24283, 24286, 24297, 24299,
    24302, 24304, 24315, 24323, 24327, 24338, 24340, 24350, 24353, 24357, 24367,
    24369, 24371, 24382, 24390, 24397, 24400, 24403, 24406, 24412, 24422, 24427,
    24429, 24437, 24447, 24454, 24459, 24461, 24467, 24473, 24475, 24477, 24487,
    24495, 24505, 24515, 24525, 24534, 24540, 24544, 24553, 24556, 24564, 24574,
    24578, 24589, 24591, 24593, 24595, 24606, 24612, 24617, 24625, 24627, 24632,
    24640, 24649, 24652, 24656, 24663, 24671, 24681, 24683, 24685, 24689, 24693,
    24702, 24712, 24720, 24724, 24726, 24737, 24741, 24752, 24755, 24762, 24772,
    24783, 24785, 24790, 24795, 24801, 24805, 24810, 24818, 24829, 24832, 24842,
    24847, 24849, 24854, 24863, 24872, 24874, 24885, 24890, 24901, 24905, 24916,
    24922, 24930, 24932, 24935, 24942, 24944, 24953, 24958, 24966, 24970, 24973,
    24984, 24995, 25000, 25002, 25006, 25011, 25015, 25023, 25027, 25032, 25040,
    25042, 25049, 25054, 25062, 25064, 25075, 25082, 25087, 25090, 25094, 25099,
    25108, 25116, 25118, 25126, 25134, 25143, 25147, 25153, 25160, 25171, 25182,
    25193, 25201, 25206, 25208, 25215, 25222, 25232, 25237, 25246, 25257, 25262,
    25273, 25279, 25281, 25289, 25296, 25302, 25304, 25307, 25313, 25316, 25324,
    25327, 25330, 25337, 25340, 25346, 25352, 25355, 25360, 25370, 25378, 25388,
    25392, 25399, 25401, 25407, 25418, 25426, 25431, 25440, 25451, 25460, 25468,
    25479, 25485, 25489, 25500, 25502, 25505, 25512, 25514, 25525, 25536, 25542,
    25546, 25557, 25563, 25572, 25574, 25582, 25584, 25586, 25595, 25605, 25608,
    25611, 25620, 25631, 25634, 25644, 25654, 25662, 25664, 25670, 25679, 25690,
    25692, 25701, 25707, 25716, 25718, 25726, 25729, 25731, 25735, 25739, 25747,
    25749, 25755, 25763, 25773, 25775, 25777, 25787, 25797, 25807, 25809, 25817,
    25828, 25833, 25837, 25839, 25842, 25853, 25863, 25866, 25874, 25876, 25880,
    25888, 25890, 25901, 25907, 25913, 25918, 25922, 25925, 25936, 25944, 25955,
    25960, 25966, 25969, 25979, 25988, 25994, 25998, 26008, 26016, 26027, 26033,
    26039, 26050, 26059, 26061, 26066, 26076, 26085, 26095, 26098, 26108, 26116,
    26120, 26122, 26133, 26138, 26147, 26156, 26164, 26166, 26173, 26182, 26188,
    26193, 26195, 26200, 26207, 26209, 26216, 26225, 26230, 26240, 26248, 26252,
    26254, 26264, 26274, 26280, 26288, 26290, 26293, 26296, 26298, 26300, 26302,
    26313, 26316, 26325, 26327, 26334, 26344, 26352, 26357, 26361, 26364, 26366,
    26376, 26387, 26395, 26404, 26413, 26422, 26431, 26439, 26450, 26457, 26467,
    26478, 26484, 26486, 26490, 26492, 26496, 26506, 26511, 26514, 26517, 26522,
    26531, 26537, 26540, 26548, 26553, 26557, 26560, 26570, 26577, 26586, 26589,
    26596, 26601, 26605, 26609, 26614, 26620, 26631, 26641, 26643, 26651, 26654,
    26663, 26665, 26673, 26677, 26679, 26687, 26690, 26699, 26709, 26713, 26724,
    26732, 26734, 26736, 26744, 26751, 26757, 26759, 26770, 26772, 26775, 26778,
    26787, 26795, 26803, 26809, 26812, 26818, 26828, 26838, 26840, 26844, 26848,
    26852, 26862, 26867, 26869, 26874, 26880, 26887, 26894, 26905, 26912, 26918,
    26922, 26930, 26940, 26942, 26945, 26953, 26961, 26966, 26973, 26982, 26990,
    26992, 27000, 27006, 27012, 27014, 27019, 27025, 27027, 27036, 27039, 27047,
    27052, 27060, 27064, 27067, 27077, 27085, 27088, 27097, 27105, 27115, 27117,
    27125, 27131, 27134, 27139, 27149, 27152, 27156, 27167, 27178, 27185, 27194,
    27202, 27206, 27208, 27210, 27216, 27223, 27229, 27238, 27241, 27248, 27252,
    27257, 27266, 27275, 27285, 27293, 27298, 27302, 27310, 27319, 27326, 27330,
    27334, 27343, 27349, 27357, 27363, 27369, 27377, 27384, 27393, 27398, 27404,
    27411, 27418, 27420, 27430, 27440, 27447, 27452, 27458, 27464, 27466, 27475,
    27482, 27484, 27489, 27499, 27501, 27506, 27508, 27510, 27514, 27516, 27522,
    27531, 27533, 27535, 27543, 27547, 27552, 27558, 27569, 27578, 27587, 27592,
    27595, 27606, 27614, 27616, 27624, 27630, 27635, 27646, 27650, 27661, 27671,
    27682, 27688, 27697, 27705, 27710, 27716, 27718, 27721, 27725, 27732, 27737,
    27743, 27753, 27759, 27762, 27771, 27778, 27785, 27787, 27797, 27808, 27814,
    27820, 27825, 27829, 27837, 27848, 27858, 27868, 27878, 27880, 27883, 27887,
    27895, 27902, 27910, 27918, 27922, 27924, 27935, 27937, 27943, 27951, 27961,
    27972, 27979, 27981, 27986, 27991, 27998, 28003, 28006, 28008, 28014, 28016,
    28020, 28030, 28038, 28044, 28053, 28063, 28065, 28067, 28074, 28078, 28089,
    28096, 28103, 28107, 28116, 28127, 28136, 28144, 28149, 28153, 28155, 28165,
    28176, 28178, 28186, 28188, 28191, 28197, 28205, 28208, 28213, 28224, 28235,
    28238, 28244, 28251, 28256, 28261, 28267, 28276, 28280, 28282, 28291, 28299,
    28302, 28304, 28315, 28326, 28328, 28330, 28339, 28346, 28356, 28366, 28369,
    28375, 28382, 28385, 28388, 28390, 28398, 28400, 28409, 28417, 28426, 28437,
    28444, 28452, 28462, 28471, 28479, 28481, 28485, 28488, 28490, 28497, 28503,
    28505, 28510, 28521, 28531, 28538, 28546, 28552, 28560, 28567, 28573, 28575,
    28579, 28583, 28586, 28588, 28592, 28601, 28604, 28608, 28615, 28617, 28628,
    28634, 28640, 28642, 28648, 28653, 28655, 28660, 28663, 28665, 28667, 28669,
    28677, 28688, 28696, 28704, 28715, 28718, 28726, 28729, 28738, 28747, 28749,
    28760, 28768, 28777, 28779, 28783, 28791, 28793, 28795, 28797, 28802, 28812,
    28814, 28817, 28821, 28823, 28830, 28832, 28840, 28842, 28847, 28849, 28851,
    28859, 28868, 28870, 28876, 28879, 28881, 28883, 28886, 28897, 28899, 28901,
    28903, 28909, 28914, 28916, 28926, 28929, 28931, 28937, 28940, 28942, 28951,
    28953, 28955, 28965, 28976, 28979, 28983, 28985, 28988, 28998, 29000, 29010,
    29014, 29022, 29025, 29032, 29041, 29044, 29052, 29055, 29065, 29070, 29075,
    29086, 29092, 29099, 29106, 29108, 29110, 29113, 29119, 29129, 29135, 29139,
    29145, 29147, 29150, 29154, 29164, 29167, 29174, 29182, 29193, 29195, 29201,
    29211, 29216, 29227, 29229, 29231, 29242, 29247, 29250, 29261, 29264, 29271,
    29275, 29283, 29291, 29295, 29299, 29309, 29312, 29320, 29328, 29330, 29337,
    29348, 29358, 29363, 29370, 29378, 29386, 29388, 29397, 29404, 29414, 29416,
    29423, 29432, 29436, 29442, 29447, 29457, 29462, 29467, 29469, 29477, 29486,
    29488, 29493, 29501, 29512, 29523, 29525, 29535, 29537, 29543, 29551, 29562,
    29565, 29573, 29575, 29586, 29592, 29602, 29604, 29609, 29619, 29622, 29632,
    29638, 29640, 29645, 29648, 29659, 29665, 29667, 29672, 29679, 29684, 29689,
    29691, 29693, 29697, 29701, 29703, 29712, 29717, 29727, 29732, 29735, 29741,
    29743, 29749, 29751, 29754, 29756, 29765, 29773, 29776, 29781, 29788, 29796,
    29803, 29814, 29819, 29828, 29830, 29836, 29840, 29842, 29846, 29849, 29858,
    29860, 29864, 29874, 29885, 29891, 29898, 29903, 29913, 29917, 29919, 29921,
    29927, 29930, 29939, 29945, 29952, 29963, 29965, 29975, 29982, 29987, 29997,
    30004, 30013, 30023, 30026, 30029, 30032, 30040, 30049, 30057, 30059, 30067,
    30069, 30074, 30085, 30093, 30100, 30111, 30115, 30118, 30120, 30129, 30140,
    30144, 30147, 30154, 30156, 30158, 30168, 30171, 30174, 30176, 30186, 30189,
    30191, 30201, 30207, 30218, 30222, 30231, 30236, 30240, 30244, 30251, 30253,
    30263, 30273, 30277, 30281, 30283, 30289, 30295, 30297, 30299, 30307, 30309,
    30316, 30321, 30325, 30329, 30331, 30334, 30338, 30343, 30351, 30353, 30355,
    30363, 30368, 30375, 30378, 30386, 30390, 30396, 30398, 30401, 30405, 30409,
    30412, 30416, 30421, 30426, 30430, 30432, 30443, 30447, 30458, 30469, 30474,
    30485, 30496, 30501, 30511, 30514, 30516, 30518, 30528, 30539, 30541, 30544,
    30554, 30556, 30567, 30574, 30579, 30585, 30587, 30598, 30606, 30614, 30621,
    30624, 30631, 30635, 30646, 30650, 30660, 30668, 30679, 30688, 30698, 30709,
    30711, 30713, 30716, 30724, 30731, 30734, 30736, 30743, 30745, 30753, 30755,
    30758, 30762, 30768, 30770, 30772, 30782, 30788, 30792, 30798, 30802, 30807,
    30816, 30826, 30828, 30837, 30840, 30842, 30849, 30857, 30860, 30863, 30865,
    30867, 30877, 30886, 30895, 30905, 30909, 30913, 30923, 30929, 30936, 30943,
    30946, 30948, 30951, 30956, 30958, 30965, 30973, 30977, 30980, 30982, 30985,
    30993, 30995, 31006, 31015, 31018, 31028, 31033, 31036, 31045, 31051, 31059,
    31068, 31077, 31080, 31082, 31086, 31092, 31096, 31101, 31106, 31115, 31123,
    31133, 31142, 31153, 31160, 31171, 31181, 31191, 31202, 31206, 31217, 31223,
    31232, 31243, 31245, 31247, 31255, 31266, 31277, 31281, 31288, 31291, 31297,
    31305, 31307, 31314, 31316, 31324, 31335, 31338, 31342, 31352, 31357, 31362,
    31372, 31383, 31386, 31388, 31390, 31401, 31404, 31415, 31418, 31422, 31424,
    31435, 31437, 31448, 31450, 31454, 31456, 31466, 31475, 31481, 31489, 31491,
    31493, 31500, 31505, 31509, 31511, 31516, 31524, 31527, 31535, 31538, 31541,
    31543, 31554, 31560, 31565, 31568, 31574, 31585, 31588, 31598, 31603, 31605,
    31616, 31625, 31635, 31646, 31654, 31662, 31669, 31677, 31684, 31686, 31693,
    31697, 31700, 31711, 31719, 31723, 31728, 31732, 31742, 31745, 31747, 31749,
    31754, 31756, 31760, 31762, 31766, 31771, 31774, 31778, 31787, 31790, 31800,
    31802, 31804, 31808, 31816, 31825, 31834, 31837, 31840, 31844, 31849, 31856,
    31866, 31873, 31876, 31885, 31891, 31893, 31904, 31912, 31921, 31926, 31928,
    31932, 31939, 31949, 31951, 31953, 31961, 31964, 31968, 31970, 31973, 31980,
    31986, 31993, 31997, 32004, 32014, 32022, 32032, 32039, 32048, 32050, 32058,
    32060, 32063, 32070, 32073, 32083, 32085, 32089, 32094, 32100, 32106, 32117,
    32122, 32125, 32129, 32135, 32145, 32147, 32150, 32161, 32166, 32177, 32184,
    32188, 32199, 32207, 32214, 32222, 32229, 32236, 32241, 32249, 32254, 32264,
    32268, 32275, 32280, 32288, 32291, 32296, 32299, 32303, 32306, 32314, 32318,
    32321, 32323, 32325, 32334, 32345, 32348, 32356, 32361, 32367, 32377, 32387,
    32392, 32400, 32402, 32409, 32417, 32419, 32422, 32424, 32434, 32439, 32441,
    32451, 32457, 32463, 32474, 32485, 32494, 32504, 32511, 32517, 32519, 32528,
    32530, 32539, 32541, 32544, 32555, 32557, 32561, 32565, 32576, 32578, 32587,
    32597, 32599, 32606, 32615, 32625, 32633, 32644, 32653, 32661, 32663, 32670,
    32679, 32690, 32701, 32704, 32707, 32713, 32715, 32719, 32721, 32727, 32732,
    32736, 32739, 32741, 32750, 32757, 32764, 32767, 32774, 32778, 32783, 32787,
    32797, 32807, 32814, 32821, 32827, 32834, 32842, 32851, 32861, 32870, 32878,
    32883, 32885, 32892, 32901, 32906, 32908, 32911, 32915, 32917, 32926, 32929,
    32933, 32938, 32946, 32952, 32963, 32968, 32978, 32984, 32989, 32991, 32993,
    32996, 33001, 33009, 33015, 33023, 33026, 33030, 33034, 33036, 33045, 33051,
    33053, 33060, 33068, 33070, 33076, 33080, 33086, 33092, 33101, 33105, 33114,
    33121, 33126, 33133, 33136, 33145, 33153, 33163, 33169, 33177, 33188, 33190,
    33192, 33194, 33203, 33206, 33208, 33216, 33224, 33226, 33228, 33238, 33240,
    33245, 33250, 33257, 33268, 33275, 33277, 33283, 33290, 33298, 33303, 33313,
    33317, 33321, 33324, 33333, 33344, 33351, 33360, 33367, 33373, 33377, 33380,
    33384, 33395, 33404, 33412, 33422, 33424, 33430, 33436, 33441, 33451, 33459,
    33461, 33469, 33480, 33482, 33485, 33492, 33503, 33509, 33516, 33525, 33536,
    33538, 33545, 33547, 33558, 33561, 33571, 33574, 33577, 33588, 33593, 33600,
    33604, 33611, 33615, 33617, 33620, 33622, 33632, 33643, 33650, 33655, 33659,
    33667, 33669, 33671, 33673, 33675, 33685, 33690, 33695, 33701, 33703, 33713,
    33717, 33724, 33726, 33730, 33735, 33738, 33749, 33751, 33760, 33769, 33779,
    33785, 33794, 33805, 33816, 33827, 33831, 33833, 33835, 33838, 33849, 33856,
    33858, 33864, 33866, 33868, 33871, 33876, 33881, 33891, 33893, 33901, 33906,
    33916, 33927, 33934, 33945, 33956, 33958, 33966, 33973, 33975, 33983, 33989,
    33991, 33999, 34001, 34003, 34014, 34024, 34026, 34033, 34041, 34043, 34052,
    34059, 34066, 34068, 34073, 34076, 34084, 34091, 34093, 34099, 34108, 34118,
    34129, 34139, 34149, 34157, 34168, 34179, 34189, 34191, 34202, 34213, 34221,
    34227, 34238, 34241, 34251, 34260, 34266, 34272, 34275, 34282, 34293, 34298,
    34305, 34308, 34316, 34325, 34328, 34339, 34341, 34343, 34345, 34347, 34349,
    34355, 34362, 34367, 34374, 34376, 34381, 34392, 34402, 34409, 34413, 34423,
    34434, 34436, 34438, 34443, 34453, 34456, 34458, 34460, 34462, 34464, 34468,
    34470, 34480, 34483, 34488, 34496, 34505, 34509, 34519, 34521, 34532, 34539,
    34547, 34555, 34564, 34566, 34568, 34570, 34572, 34581, 34586, 34589, 34595,
    34605, 34607, 34611, 34616, 34627, 34630, 34636, 34639, 34645, 34652, 34654,
    34659, 34669, 34677, 34684, 34692, 34700, 34703, 34705, 34716, 34727, 34731,
    34734, 34736, 34741, 34751, 34761, 34768, 34777, 34779, 34783, 34785, 34788,
    34793, 34803, 34812, 34815, 34825, 34831, 34841, 34848, 34858, 34866, 34871,
    34879, 34881, 34886, 34892, 34894, 34898, 34905, 34909, 34918, 34926, 34932,
    34938, 34943, 34951, 34957, 34965, 34967, 34976, 34987, 34997, 35003, 35014,
    35019, 35026, 35029, 35032, 35038, 35048, 35053, 35058, 35060, 35062, 35072,
    35076, 35080, 35082, 35087, 35094, 35103, 35114, 35120, 35123, 35129, 35137,
    35141, 35152, 35158, 35163, 35172, 35181, 35183, 35193, 35201, 35211, 35219,
    35222, 35231, 35233, 35235, 35243, 35249, 35251, 35261, 35271, 35273, 35276,
    35278, 35280, 35287, 35289, 35296, 35300, 35307, 35317, 35324, 35327, 35336,
    35341, 35351, 35353, 35356, 35360, 35368, 35379, 35386, 35390, 35395, 35404,
    35411, 35420, 35428, 35433, 35441, 35449, 35458, 35462, 35467, 35469, 35475,
    35479, 35483, 35491, 35493, 35502, 35513, 35524, 35535, 35545, 35550, 35556,
    35566, 35571, 35578, 35587, 35593, 35596, 35599, 35604, 35615, 35626, 35630,
    35634, 35644, 35650, 35653, 35660, 35666, 35673, 35684, 35686, 35692, 35703,
    35711, 35713, 35715, 35724, 35734, 35740, 35746, 35753, 35760, 35771, 35780,
    35783, 35789, 35796, 35801, 35807, 35814, 35818, 35821, 35823, 35830, 35834,
    35845, 35851, 35856, 35862, 35864, 35875, 35879, 35881, 35883, 35893, 35897,
    35906, 35909, 35915, 35920, 35931, 35935, 35940, 35942, 35944, 35952, 35959,
    35961, 35968, 35972, 35982, 35987, 35991, 35998, 36001, 36010, 36016, 36018,
    36025, 36035, 36039, 36043, 36050, 36057, 36065, 36067, 36069, 36073, 36081,
    36085, 36087, 36098, 36106, 36109, 36119, 36127, 36137, 36148, 36156, 36163,
    36166, 36168, 36171, 36181, 36192, 36199, 36206, 36209, 36211, 36218, 36226,
    36229, 36237, 36241, 36251, 36254, 36261, 36268, 36277, 36288, 36290, 36294,
    36297, 36301, 36304, 36310, 36319, 36321, 36332, 36335, 36337, 36343, 36346,
    36354, 36356, 36360, 36362, 36367, 36369, 36376, 36382, 36393, 36399, 36405,
    36413, 36419, 36430, 36433, 36440, 36451, 36459, 36467, 36476, 36485, 36496,
    36504, 36511, 36520, 36523, 36530, 36538, 36546, 36555, 36557, 36559, 36563,
    36570, 36578, 36582, 36585, 36587, 36589, 36595, 36597, 36605, 36611, 36613,
    36617, 36627, 36629, 36634, 36636, 36641, 36647, 36654, 36665, 36670, 36672,
    36675, 36686, 36692, 36696, 36705, 36712, 36720, 36728, 36736, 36741, 36748,
    36751, 36758, 36761, 36772, 36776, 36781, 36784, 36795, 36797, 36805, 36814,
    36822, 36824, 36835, 36841, 36850, 36858, 36861, 36865, 36871, 36876, 36886,
    36894, 36902, 36907, 36916, 36919, 36924, 36933, 36935, 36937, 36941, 36950,
    36957, 36963, 36965, 36974, 36979, 36981, 36988, 36991, 36993, 37002, 37011,
    37013, 37015, 37026, 37028, 37034, 37039, 37043, 37052, 37055, 37057, 37066,
    37075, 37077, 37085, 37087, 37098, 37105, 37113, 37121, 37123, 37128, 37136,
    37139, 37146, 37154, 37156, 37158, 37168, 37179, 37182, 37188, 37190, 37199,
    37201, 37206, 37208, 37218, 37227, 37233, 37239, 37241, 37245, 37247, 37257,
    37266, 37270, 37275, 37282, 37287, 37298, 37303, 37311, 37318, 37325, 37332,
    37334, 37336, 37344, 37354, 37365, 37373, 37376, 37381, 37387, 37390, 37392,
    37400, 37405, 37413, 37420, 37422, 37428, 37431, 37437, 37439, 37450, 37452,
    37457, 37464, 37466, 37471, 37473, 37475, 37481, 37491, 37499, 37505, 37510,
    37512, 37522, 37524, 37527, 37538, 37545, 37554, 37565, 37570, 37572, 37581,
    37588, 37595, 37597, 37607, 37616, 37619, 37623, 37625, 37629, 37636, 37644,
    37651, 37654, 37663, 37668, 37675, 37680, 37683, 37689, 37697, 37700, 37706,
    37712, 37720, 37730, 37739, 37746, 37752, 37759, 37764, 37768, 37778, 37783,
    37787, 37795, 37801, 37811, 37816, 37818, 37820, 37831, 37840, 37845, 37852,
    37860, 37867, 37875, 37877, 37879, 37890, 37893, 37899, 37901, 37906, 37914,
    37920, 37927, 37933, 37942, 37950, 37954, 37957, 37961, 37966, 37977, 37979,
    37983, 37990, 38001, 38006, 38010, 38013, 38023, 38030, 38039, 38041, 38043,
    38046, 38048, 38052, 38054, 38056, 38059, 38061, 38069, 38076, 38079, 38082,
    38084, 38086, 38093, 38099, 38106, 38109, 38111, 38113, 38115, 38117, 38123,
    38126, 38133, 38138, 38149, 38151, 38154, 38157, 38167, 38172, 38175, 38186,
    38188, 38194, 38196, 38198, 38201, 38208, 38218, 38220, 38230, 38234, 38238,
    38249, 38257, 38264, 38267, 38269, 38279, 38290, 38292, 38295, 38300, 38305,
    38309, 38317, 38325, 38329, 38335, 38337, 38342, 38353, 38355, 38362, 38372,
    38380, 38383, 38389, 38396, 38406, 38411, 38419, 38430, 38433, 38444, 38454,
    38461, 38464, 38466, 38468, 38475, 38477, 38481, 38490, 38496, 38507, 38510,
    38521, 38524, 38533, 38537, 38548, 38556, 38558, 38569, 38578, 38586, 38597,
    38603, 38605, 38607, 38609, 38614, 38621, 38631, 38635, 38637, 38641, 38651,
    38656, 38663, 38668, 38673, 38675, 38678, 38686, 38690, 38701, 38708, 38715,
    38717, 38724, 38731, 38740, 38742, 38744, 38755, 38757, 38765, 38770, 38779,
    38788, 38790, 38793, 38802, 38812, 38820, 38826, 38831, 38840, 38843, 38849,
    38855, 38857, 38859, 38864, 38874, 38878, 38882, 38888, 38891, 38893, 38896,
    38903, 38912, 38914, 38919, 38926, 38929, 38939, 38950, 38956, 38964, 38975,
    38986, 38991, 38996, 38998, 39000, 39004, 39011, 39018, 39020, 39026, 39030,
    39037, 39045, 39047, 39052, 39062, 39072, 39081, 39085, 39089, 39099, 39103,
    39106, 39116, 39122, 39124, 39131, 39133, 39135, 39142, 39144, 39153, 39159,
    39167, 39176, 39182, 39186, 39195, 39205, 39210, 39218, 39224, 39226, 39228,
    39237, 39246, 39250, 39254, 39265, 39267, 39269, 39278, 39282, 39285, 39287,
    39292, 39294, 39305, 39307, 39314, 39323, 39330, 39334, 39336, 39338, 39349,
    39356, 39365, 39368, 39370, 39372, 39380, 39385, 39396, 39398, 39404, 39409,
    39411, 39413, 39422, 39429, 39439, 39449, 39455, 39464, 39466, 39476, 39487,
    39496, 39498, 39504, 39510, 39521, 39529, 39532, 39543, 39548, 39550, 39553,
    39561, 39568, 39577, 39584, 39589, 39594, 39601, 39605, 39615, 39617, 39625,
    39634, 39640, 39643, 39652, 39662, 39669, 39673, 39675, 39680, 39687, 39693,
    39702, 39711, 39716, 39722, 39729, 39740, 39744, 39755, 39759, 39761, 39764,
    39771, 39777, 39782, 39790, 39796, 39804, 39807, 39813, 39822, 39824, 39826,
    39835, 39840, 39846, 39848, 39854, 39864, 39868, 39873, 39881, 39890, 39897,
    39903, 39912, 39919, 39925, 39929, 39936, 39940, 39948, 39957, 39962, 39965,
    39976, 39985, 39992, 39997, 40008, 40016, 40019, 40023, 40031, 40033, 40042,
    40049, 40051, 40057, 40064, 40072, 40074, 40080, 40085, 40088, 40097, 40099,
    40107, 40112, 40114, 40116, 40118, 40129, 40138, 40144, 40148, 40158, 40163,
    40170, 40172, 40174, 40179, 40184, 40195, 40201, 40205, 40208, 40211, 40218,
    40227, 40238, 40244, 40247, 40257, 40259, 40270, 40272, 40281, 40290, 40297,
    40300, 40303, 40308, 40310, 40314, 40325, 40331, 40338, 40340, 40351, 40359,
    40361, 40365, 40367, 40369, 40373, 40375, 40377, 40379, 40384, 40394, 40402,
    40405, 40408, 40413, 40423, 40429, 40431, 40433, 40441, 40444, 40453, 40459,
    40466, 40472, 40483, 40487, 40495, 40499, 40509, 40512, 40518, 40520, 40522,
    40530, 40536, 40544, 40554, 40557, 40563, 40574, 40581, 40590, 40592, 40594,
    40602, 40609, 40614, 40625, 40631, 40635, 40637, 40646, 40657, 40663, 40673,
    40680, 40691, 40693, 40695, 40704, 40715, 40721, 40725, 40727, 40734, 40742,
    40753, 40759, 40766, 40768, 40772, 40774, 40782, 40788, 40790, 40798, 40801,
    40803, 40805, 40814, 40818, 40820, 40824, 40828, 40838, 40840, 40843, 40854,
    40857, 40859, 40869, 40880, 40890, 40894, 40904, 40908, 40914, 40924, 40933,
    40941, 40947, 40950, 40960, 40968, 40978, 40985, 40989, 40995, 40998, 41003,
    41012, 41022, 41027, 41031, 41040, 41050, 41052, 41058, 41066, 41074, 41078,
    41083, 41094, 41100, 41107, 41113, 41120, 41129, 41139, 41144, 41150, 41159,
    41168, 41177, 41182, 41193, 41204, 41210, 41218, 41220, 41228, 41234, 41244,
    41248, 41259, 41267, 41270, 41280, 41289, 41300, 41302, 41304, 41309, 41315,
    41325, 41332, 41343, 41352, 41358, 41366, 41377, 41384, 41395, 41406, 41416,
    41418, 41428, 41436, 41445, 41454, 41457, 41466, 41474, 41478, 41488, 41490,
    41492, 41495, 41497, 41506, 41508, 41510, 41517, 41528, 41532, 41540, 41545,
    41552, 41558, 41562, 41572, 41574, 41583, 41590, 41592, 41597, 41606, 41610,
    41618, 41626, 41628, 41632, 41635, 41645, 41648, 41650, 41657, 41667, 41675,
    41681, 41687, 41692, 41696, 41698, 41702, 41706, 41709, 41711, 41713, 41717,
    41725, 41727, 41733, 41741, 41744, 41752, 41754, 41756, 41764, 41769, 41772,
    41777, 41788, 41796, 41803, 41808, 41814, 41824, 41835, 41844, 41855, 41860,
    41868, 41877, 41886, 41890, 41900, 41909, 41918, 41926, 41934, 41936, 41941,
    41947, 41951, 41954, 41958, 41963, 41966, 41976, 41985, 41995, 42006, 42012,
    42019, 42027, 42035, 42038, 42047, 42049, 42060, 42071, 42080, 42086, 42094,
    42099, 42110, 42112, 42114, 42122, 42130, 42136, 42143, 42153, 42162, 42165,
    42168, 42176, 42179, 42188, 42195, 42204, 42213, 42215, 42219, 42225, 42235,
    42242, 42248, 42251, 42255, 42258, 42261, 42271, 42282, 42284, 42286, 42297,
    42302, 42311, 42315, 42325, 42333, 42339, 42341, 42343, 42348, 42356, 42364,
    42374, 42382, 42386, 42390, 42401, 42406, 42413, 42424, 42427, 42429, 42434,
    42436, 42446, 42457, 42466, 42473, 42482, 42484, 42495, 42499, 42502, 42511,
    42518, 42520, 42524, 42526, 42531, 42533, 42539, 42541, 42545, 42547, 42556,
    42558, 42566, 42576, 42579, 42589, 42597, 42605, 42611, 42619, 42628, 42639,
    42648, 42652, 42662, 42669, 42672, 42677, 42684, 42686, 42696, 42704, 42714,
    42716, 42721, 42730, 42736, 42740, 42749, 42751, 42761, 42766, 42776, 42783,
    42793, 42796, 42806, 42810, 42821, 42831, 42840, 42844, 42847, 42851, 42861,
    42871, 42875, 42880, 42887, 42896, 42903, 42910, 42912, 42918, 42922, 42925,
    42927, 42935, 42937, 42947, 42957, 42967, 42974, 42976, 42984, 42988, 42994,
    43004, 43012, 43017, 43026, 43036, 43039, 43047, 43051, 43059, 43069, 43075,
    43078, 43089, 43093, 43095, 43097, 43101, 43103, 43105, 43114, 43122, 43133,
    43139, 43146, 43148, 43156, 43159, 43161, 43167, 43169, 43174, 43184, 43186,
    43193, 43203, 43211, 43214, 43222, 43229, 43231, 43235, 43240, 43245, 43255,
    43260, 43266, 43268, 43270, 43274, 43278, 43286, 43297, 43307, 43316, 43322,
    43324, 43333, 43336, 43342, 43350, 43352, 43362, 43369, 43378, 43387, 43393,
    43396, 43403, 43406, 43416, 43422, 43431, 43434, 43437, 43448, 43454, 43462,
    43467, 43478, 43482, 43484, 43490, 43495, 43501, 43512, 43518, 43520, 43525,
    43532, 43542, 43549, 43553, 43558, 43560, 43568, 43576, 43584, 43595, 43604,
    43612, 43623, 43632, 43636, 43645, 43647, 43658, 43669, 43680, 43687, 43696,
    43699, 43704, 43715, 43717, 43719, 43721, 43724, 43726, 43729, 43738, 43745,
    43750, 43753, 43755, 43758, 43766, 43776, 43779, 43786, 43797, 43799, 43802,
    43813, 43820, 43827, 43832, 43841, 43847, 43849, 43859, 43863, 43869, 43879,
    43888, 43895, 43897, 43904, 43906, 43909, 43918, 43927, 43934, 43945, 43953,
    43955, 43960, 43969, 43975, 43986, 43995, 43997, 44000, 44010, 44016, 44020,
    44028, 44038, 44040, 44045, 44054, 44057, 44066, 44068, 44077, 44083, 44093,
    44102, 44113, 44121, 44123, 44127, 44134, 44142, 44147, 44149, 44154, 44156,
    44163, 44166, 44172, 44176, 44186, 44188, 44196, 44204, 44213, 44222, 44226,
    44235, 44239, 44250, 44256, 44261, 44265, 44267, 44276, 44286, 44296, 44298,
    44304, 44312, 44322, 44330, 44334, 44343, 44345, 44349, 44359, 44365, 44373,
    44384, 44389, 44398, 44409, 44417, 44422, 44430, 44432, 44436, 44443, 44448,
    44452, 44457, 44468, 44479, 44488, 44492, 44497, 44507, 44514, 44520, 44522,
    44525, 44528, 44530, 44535, 44546, 44554, 44561, 44571, 44573, 44577, 44584,
    44586, 44595, 44597, 44601, 44610, 44612, 44619, 44628, 44632, 44642, 44651,
    44653, 44664, 44666, 44673, 44680, 44682, 44687, 44689, 44695, 44706, 44709,
    44718, 44727, 44736, 44744, 44755, 44760, 44770, 44774, 44780, 44782, 44793,
    44795, 44803, 44809, 44818, 44823, 44832, 44834, 44836, 44842, 44849, 44857,
    44866, 44875, 44877, 44879, 44889, 44892, 44898, 44900, 44902, 44904, 44912,
    44922, 44928, 44937, 44940, 44948, 44955, 44958, 44963, 44972, 44981, 44986,
    44988, 44992, 45003, 45008, 45017, 45025, 45033, 45041, 45051, 45055, 45063,
    45066, 45075, 45084, 45095, 45105, 45108, 45117, 45124, 45128, 45139, 45141,
    45150, 45153, 45162, 45172, 45177, 45179, 45181, 45183, 45185, 45193, 45195,
    45206, 45212, 45221, 45232, 45234, 45236, 45240, 45247, 45251, 45260, 45270,
    45275, 45278, 45289, 45294, 45301, 45308, 45312, 45318, 45327, 45337, 45346,
    45349, 45352, 45359, 45366, 45377, 45388, 45396, 45407, 45418, 45425, 45436,
    45446, 45449, 45458, 45466, 45471, 45477, 45487, 45496, 45498, 45501, 45503,
    45507, 45509, 45519, 45521, 45531, 45537, 45544, 45549, 45551, 45559, 45570,
    45573, 45578, 45580, 45588, 45599, 45608, 45612, 45618, 45625, 45629, 45640,
    45646, 45650, 45661, 45670, 45681, 45684, 45688, 45695, 45706, 45716, 45721,
    45726, 45734, 45741, 45744, 45746, 45754, 45763, 45765, 45767, 45775, 45780,
    45783, 45792, 45800, 45806, 45816, 45822, 45826, 45837, 45845, 45848, 45858,
    45868, 45875, 45886, 45892, 45903, 45911, 45917, 45924, 45927, 45931, 45941,
    45946, 45954, 45963, 45973, 45975, 45979, 45981, 45991, 46002, 46005, 46013,
    46024, 46035, 46039, 46048, 46052, 46055, 46058, 46060, 46068, 46070, 46072,
    46083, 46085, 46089, 46100, 46105, 46110, 46112, 46117, 46127, 46138, 46149,
    46159, 46161, 46171, 46175, 46180, 46189, 46197, 46199, 46201, 46208, 46210,
    46221, 46224, 46227, 46237, 46243, 46252, 46259, 46264, 46275, 46285, 46293,
    46304, 46315, 46318, 46325, 46334, 46339, 46349, 46355, 46357, 46360, 46369,
    46373, 46377, 46383, 46385, 46395, 46402, 46404, 46407, 46417, 46423, 46430,
    46439, 46443, 46445, 46455, 46459, 46464, 46468, 46470, 46472, 46483, 46493,
    46503, 46507, 46516, 46519, 46522, 46527, 46529, 46534, 46543, 46549, 46553,
    46557, 46564, 46574, 46580, 46584, 46590, 46592, 46603, 46613, 46618, 46622,
    46627, 46630, 46636, 46638, 46640, 46649, 46654, 46663, 46670, 46677, 46679,
    46687, 46690, 46696, 46704, 46713, 46718, 46728, 46739, 46743, 46746, 46750,
    46757, 46766, 46777, 46783, 46788, 46798, 46804, 46806, 46810, 46819, 46827,
    46829, 46831, 46842, 46849, 46855, 46860, 46864, 46870, 46872, 46880, 46889,
    46894, 46903, 46910, 46912, 46922, 46928, 46937, 46946, 46951, 46962, 46973,
    46976, 46978, 46982, 46991, 47002, 47008, 47010, 47021, 47031, 47036, 47046,
    47056, 47061, 47063, 47072, 47075, 47083, 47090, 47094, 47101, 47106, 47111,
    47121, 47129, 47131, 47133, 47142, 47151, 47160, 47167, 47176, 47186, 47188,
    47197, 47208, 47210, 47214, 47223, 47232, 47235, 47246, 47255, 47263, 47268,
    47278, 47285, 47289, 47300, 47306, 47311, 47315, 47320, 47322, 47325, 47331,
    47333, 47335, 47345, 47347, 47352, 47363, 47367, 47378, 47386, 47388, 47394,
    47398, 47408, 47411, 47413, 47421, 47423, 47432, 47441, 47446, 47455, 47458,
    47462, 47464, 47470, 47476, 47479, 47489, 47496, 47498, 47502, 47511, 47516,
    47526, 47532, 47538, 47540, 47548, 47552, 47554, 47562, 47569, 47573, 47575,
    47578, 47583, 47594, 47603, 47611, 47620, 47631, 47636, 47639, 47650, 47658,
    47661, 47663, 47665, 47676, 47681, 47690, 47694, 47702, 47711, 47719, 47726,
    47735, 47738, 47741, 47743, 47751, 47755, 47757, 47763, 47771, 47774, 47779,
    47787, 47797, 47804, 47810, 47815, 47826, 47828, 47830, 47840, 47846, 47854,
    47861, 47872, 47881, 47885, 47896, 47905, 47909, 47915, 47921, 47925, 47930,
    47937, 47940, 47951, 47962, 47968, 47975, 47977, 47985, 47990, 47996, 48005,
    48013, 48015, 48020, 48027, 48037, 48045, 48050, 48053, 48058, 48067, 48070,
    48078, 48088, 48090, 48098, 48109, 48120, 48129, 48137, 48140, 48145, 48152,
    48157, 48162};

  class pattern_doc_generator : public tests::doc_generator_base {
   public:
    pattern_doc_generator(std::string_view all_docs_field, std::string_view some_docs_field, size_t total_docs,
           std::span<irs::doc_id_t> with_field)
      : all_docs_field_{all_docs_field}, some_docs_field_{some_docs_field},
        with_field_ {with_field},
        max_doc_id_(total_docs + 1) {

    }
    const tests::document* next() override {
      if (produced_docs_ >= max_doc_id_) {
        return nullptr;
      }
      ++produced_docs_;
      doc_.clear();
      doc_.insert(
        std::make_shared<tests::string_field>(all_docs_field_, "all"));
      if (span_index_ < with_field_.size() &&
          with_field_[span_index_] == produced_docs_) {
        doc_.insert(
          std::make_shared<tests::string_field>(some_docs_field_, "some"), false, true);
        span_index_++;
      }
      return &doc_;
    }
    void reset() override {
      produced_docs_ = 0;
      span_index_ = 0;
    }

    tests::document doc_;
    std::string all_docs_field_;
    std::string some_docs_field_;
    std::span<irs::doc_id_t> with_field_;
    size_t produced_docs_{0};
    size_t max_doc_id_;
    size_t span_index_{0};
  };

  constexpr std::string_view target{"some_docs"};

  auto max_doc_id = with_fields[std::size(with_fields) - 1];
  {
    pattern_doc_generator gen("all_docs", target, max_doc_id, with_fields);
    irs::index_writer::init_options opts;
    opts.column_info =
      [target](irs::string_ref name) -> irs::column_info {
      // std::string to avoid ambigous comparison operator
      if (std::string(target) == name) {
        return {.compression = irs::type<irs::compression::lz4>::id()(),
                .options = {},
                .encryption = false,
                .track_prev_doc = true};
      } else {
        return {.compression = irs::type<irs::compression::lz4>::id()(),
                .options = {},
                .encryption = false,
                .track_prev_doc = false};
      }
    };
    add_segment(gen, irs::OM_CREATE, opts);
  }

  auto rdr = open_reader();
  using seek_type = std::tuple<irs::doc_id_t, irs::doc_id_t>;
  const seek_type seeks[] = {
    /*353, 359}, {1958, 1967},   {6042, 6045},   {6189, 6195},
    {7234, 7239},   {8998, 9000},   {9397, 9398},   {9700, 9703},
    {9826, 9831},   {11199, 11202}, {11733, 11734}, {13306, 13311},
    {16143, 16146}, {16410, 16411}, {16422, 16422}, {16572, 16573},
    {18016, 18026}, {22626, 22627}, {23907, 23911}, {24615, 24617},
    {24728, 24737}, {24904, 24905}, {25588, 25595}, {26693, 26699},
    {27402, 27404}, {27954, 27961}, {28023, 28030}, {28364, 28366},
    {28537, 28538}, {29009, 29010}, {29951, 29952}, {31417, 31418},
    {31561, 31565}, {34592, 34595}, {34988, 34997}, {35555, 35556},
    {36588, 36589}, {36618, 36627}, {37016, 37026}, {40120, 40129},
    {40661, 40663}, {42953, 42957}, {46908, 46910},*/ {48159, 48162}};
  // surrogate seek pattern check
  {
    // target, expected seek result
    irs::by_column_existence filter = make_filter(target, false);

    auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

    ASSERT_EQ(1, rdr->size());
    auto& segment = (*rdr)[0];

    auto column = segment.column(target);
    ASSERT_NE(nullptr, column);
    auto column_it = column->iterator(irs::ColumnHint::kPrevDoc);
    auto filter_it = prepared->execute(segment);

    auto* doc = irs::get<irs::document>(*filter_it);
    ASSERT_TRUE(bool(doc));

    ASSERT_EQ(column->size(), irs::cost::extract(*filter_it));
    for (auto& seek : seeks) {
      irs::seek(*filter_it, std::get<0>(seek));
      irs::seek(*column_it, std::get<0>(seek));
      ASSERT_EQ(filter_it->value(), column_it->value());
      ASSERT_EQ(filter_it->value(), doc->value);
      ASSERT_EQ(std::get<1>(seek), doc->value);
    }
  }
  // seek pattern check
  {
    irs::by_column_existence filter = make_filter(target, false);

    auto prepared = filter.prepare(*rdr, irs::Order::kUnordered);

    ASSERT_EQ(1, rdr->size());
    auto& segment = (*rdr)[0];

    auto column = segment.column(target);
    ASSERT_NE(nullptr, column);
    auto column_it = column->iterator(irs::ColumnHint::kPrevDoc);
    auto filter_it = prepared->execute(segment);

    auto* doc = irs::get<irs::document>(*filter_it);
    ASSERT_TRUE(bool(doc));

    ASSERT_EQ(column->size(), irs::cost::extract(*filter_it));
    for (auto& seek : seeks) {
      ASSERT_EQ(std::get<1>(seek), filter_it->seek(std::get<0>(seek)));
      ASSERT_EQ(std::get<1>(seek), column_it->seek(std::get<0>(seek)));
      ASSERT_EQ(filter_it->value(), column_it->value());
      ASSERT_EQ(filter_it->value(), doc->value);
      ASSERT_EQ(std::get<1>(seek), doc->value);
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
  column_existence_filter_test, column_existence_filter_test_case,
  ::testing::Combine(
    ::testing::Values(&tests::directory<&tests::memory_directory>,
                      &tests::directory<&tests::fs_directory>,
                      &tests::directory<&tests::mmap_directory>),
    ::testing::Values("1_0")),
  column_existence_filter_test_case::to_string);

INSTANTIATE_TEST_SUITE_P(
  column_existence_filter_test2, column_existence_filter_test_case2,
  ::testing::Combine(
    ::testing::Values(&tests::directory<&tests::memory_directory>,
                      &tests::directory<&tests::fs_directory>,
                      &tests::directory<&tests::mmap_directory>),
    ::testing::Values(//"1_3", "1_3simd",
                      "1_4", "1_4simd")),
  column_existence_filter_test_case2::to_string);

}  // namespace
