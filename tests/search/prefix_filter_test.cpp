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

#include "search/prefix_filter.hpp"

#include "filter_test_case_base.hpp"
#include "search/filter_visitor.hpp"
#include "tests_shared.hpp"

namespace {

irs::by_prefix make_filter(const std::string_view& field,
                           const std::string_view term,
                           size_t scored_terms_limit = 1024) {
  irs::by_prefix q;
  *q.mutable_field() = field;
  q.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  q.mutable_options()->scored_terms_limit = scored_terms_limit;
  return q;
}

class prefix_filter_test_case : public tests::FilterTestCaseBase {
 protected:
  void by_prefix_order() {
    // add segment
    {
      tests::json_doc_generator gen(resource("simple_sequential.json"),
                                    &tests::generic_json_field_factory);
      add_segment(gen);
    }

    auto rdr = open_reader();

    // empty query
    CheckQuery(irs::by_prefix(), Docs{}, Costs{0}, rdr);

    // empty prefix test collector call count for field/term/finish
    {
      Docs docs{1, 4, 9, 16, 21, 24, 26, 29, 31, 32};
      Costs costs{docs.size()};

      size_t collect_field_count = 0;
      size_t collect_term_count = 0;
      size_t finish_count = 0;

      std::array<irs::sort::ptr, 1> order{
        std::make_unique<tests::sort::custom_sort>()};

      auto& scorer = static_cast<tests::sort::custom_sort&>(*order.front());

      scorer.collector_collect_field = [&collect_field_count](
                                         const irs::SubReader&,
                                         const irs::term_reader&) -> void {
        ++collect_field_count;
      };
      scorer.collector_collect_term =
        [&collect_term_count](const irs::SubReader&, const irs::term_reader&,
                              const irs::attribute_provider&) -> void {
        ++collect_term_count;
      };
      scorer.collectors_collect_ = [&finish_count](
                                     irs::byte_type*, const irs::IndexReader&,
                                     const irs::sort::field_collector*,
                                     const irs::sort::term_collector*) -> void {
        ++finish_count;
      };
      scorer.prepare_field_collector_ =
        [&scorer]() -> irs::sort::field_collector::ptr {
        return std::make_unique<
          tests::sort::custom_sort::prepared::field_collector>(scorer);
      };
      scorer.prepare_term_collector_ =
        [&scorer]() -> irs::sort::term_collector::ptr {
        return std::make_unique<
          tests::sort::custom_sort::prepared::term_collector>(scorer);
      };
      CheckQuery(make_filter("prefix", ""), order, docs, rdr);
      ASSERT_EQ(9, collect_field_count);  // 9 fields (1 per term since treated
                                          // as a disjunction) in 1 segment
      ASSERT_EQ(9, collect_term_count);   // 9 different terms
      ASSERT_EQ(9, finish_count);         // 9 unque terms
    }

    // empty prefix
    {
      Docs docs{31, 32, 1, 4, 9, 16, 21, 24, 26, 29};
      Costs costs{docs.size()};

      irs::sort::ptr scorer{std::make_unique<tests::sort::frequency_sort>()};

      CheckQuery(make_filter("prefix", ""), std::span{&scorer, 1}, docs, rdr);
    }

    // FIXME
    //  empty prefix + scored_terms_limit
    //    {
    //      docs_t docs{ 31, 32, 1, 4, 9, 16, 21, 24, 26, 29 };
    //      costs_t costs{ docs.size() };
    //      irs::order order;
    //
    //      order.add<sort::frequency_sort>(false);
    //      CheckQuery(irs::by_prefix().field("prefix").scored_terms_limit(1),
    //      order, docs, rdr);
    //    }
    //
    // prefix
    {
      Docs docs{31, 32, 1, 4, 16, 21, 26, 29};
      Costs costs{docs.size()};

      std::array<irs::sort::ptr, 1> order{
        std::make_unique<tests::sort::frequency_sort>()};

      CheckQuery(make_filter("prefix", "a"), order, docs, rdr);
    }

    // prefix
    {
      Docs docs{31, 32, 1, 4, 16, 21, 26, 29};
      Costs costs{docs.size()};

      std::array<irs::sort::ptr, 2> order{
        std::make_unique<tests::sort::frequency_sort>(),
        std::make_unique<tests::sort::frequency_sort>()};

      CheckQuery(make_filter("prefix", "a"), order, docs, rdr);
    }
  }

  void by_prefix_sequential() {
    /* add segment */
    {
      tests::json_doc_generator gen(resource("simple_sequential.json"),
                                    &tests::generic_json_field_factory);
      add_segment(gen);
    }

    auto rdr = open_reader();

    // empty query
    CheckQuery(irs::by_prefix(), Docs{}, Costs{0}, rdr);

    // empty field
    CheckQuery(make_filter("", "xyz"), Docs{}, Costs{0}, rdr);

    // invalid field
    CheckQuery(make_filter("same1", "xyz"), Docs{}, Costs{0}, rdr);

    // invalid prefix
    CheckQuery(make_filter("same", "xyz_invalid"), Docs{}, Costs{0}, rdr);

    // valid prefix
    {
      Docs result;
      for (size_t i = 0; i < 32; ++i) {
        result.push_back(irs::doc_id_t((irs::doc_limits::min)() + i));
      }

      Costs costs{result.size()};

      CheckQuery(make_filter("same", "xyz"), result, costs, rdr);
    }

    // empty prefix : get all fields
    {
      Docs docs{1, 2, 3, 5, 8, 11, 14, 17, 19, 21, 24, 27, 31};
      Costs costs{docs.size()};

      CheckQuery(make_filter("duplicated", ""), docs, costs, rdr);
    }

    // single digit prefix
    {
      Docs docs{1, 5, 11, 21, 27, 31};
      Costs costs{docs.size()};

      CheckQuery(make_filter("duplicated", "a"), docs, costs, rdr);
    }

    CheckQuery(make_filter("name", "!"), Docs{28}, Costs{1}, rdr);
    CheckQuery(make_filter("prefix", "b"), Docs{9, 24}, Costs{2}, rdr);

    // multiple digit prefix
    {
      Docs docs{2, 3, 8, 14, 17, 19, 24};
      Costs costs{docs.size()};

      CheckQuery(make_filter("duplicated", "vcz"), docs, costs, rdr);
    }

    {
      Docs docs{1, 4, 21, 26, 31, 32};
      Costs costs{docs.size()};
      CheckQuery(make_filter("prefix", "abc"), docs, costs, rdr);
    }

    {
      Docs docs{1, 4, 21, 26, 31, 32};
      Costs costs{docs.size()};

      CheckQuery(make_filter("prefix", "abc"), docs, costs, rdr);
    }

    // whole word
    CheckQuery(make_filter("prefix", "bateradsfsfasdf"), Docs{24}, Costs{1},
               rdr);
  }

  void by_prefix_schemas() {
    // write segments
    {
      auto writer = open_writer(irs::OM_CREATE);

      std::vector<tests::doc_generator_base::ptr> gens;
      gens.emplace_back(
        new tests::json_doc_generator(resource("AdventureWorks2014.json"),
                                      &tests::generic_json_field_factory));
      gens.emplace_back(
        new tests::json_doc_generator(resource("AdventureWorks2014Edges.json"),
                                      &tests::generic_json_field_factory));
      gens.emplace_back(new tests::json_doc_generator(
        resource("Northwnd.json"), &tests::generic_json_field_factory));
      gens.emplace_back(new tests::json_doc_generator(
        resource("NorthwndEdges.json"), &tests::generic_json_field_factory));
      add_segments(*writer, gens);
    }

    auto rdr = open_reader();

    CheckQuery(make_filter("Name", "Addr"), Docs{1, 2, 77, 78}, rdr);
  }
};  // prefix_filter_test_case

TEST(by_prefix_test, options) {
  irs::by_prefix_options opts;
  ASSERT_TRUE(opts.term.empty());
  ASSERT_EQ(1024, opts.scored_terms_limit);
}

TEST(by_prefix_test, ctor) {
  irs::by_prefix q;
  ASSERT_EQ(irs::type<irs::by_prefix>::id(), q.type());
  ASSERT_EQ(irs::by_prefix_options{}, q.options());
  ASSERT_EQ("", q.field());
  ASSERT_EQ(irs::kNoBoost, q.boost());
}

TEST(by_prefix_test, equal) {
  {
    irs::by_prefix q = make_filter("field", "term");

    ASSERT_EQ(q, make_filter("field", "term"));
    ASSERT_EQ(q.hash(), make_filter("field", "term").hash());
    ASSERT_NE(q, make_filter("field1", "term"));
    ASSERT_NE(q, make_filter("field", "term", 100));
  }

  {
    irs::by_prefix q = make_filter("field", "term", 100);

    ASSERT_EQ(q, make_filter("field", "term", 100));
    ASSERT_EQ(q.hash(), make_filter("field", "term", 100).hash());
    ASSERT_NE(q, make_filter("field1", "term", 100));
    ASSERT_NE(q, make_filter("field", "term"));
  }
}

TEST(by_prefix_test, boost) {
  // no boost
  {
    irs::by_prefix q = make_filter("field", "term");

    auto prepared = q.prepare(irs::SubReader::empty());
    ASSERT_EQ(irs::kNoBoost, prepared->boost());
  }

  // with boost
  {
    irs::score_t boost = 1.5f;
    irs::by_prefix q = make_filter("field", "term");
    q.boost(boost);

    auto prepared = q.prepare(irs::SubReader::empty());
    ASSERT_EQ(boost, prepared->boost());
  }
}

TEST_P(prefix_filter_test_case, by_prefix) {
  by_prefix_order();
  by_prefix_sequential();
  by_prefix_schemas();
}

TEST_P(prefix_filter_test_case, visit) {
  // add segment
  {
    tests::json_doc_generator gen(resource("simple_sequential.json"),
                                  &tests::generic_json_field_factory);
    add_segment(gen);
  }

  const std::string_view field = "prefix";
  const irs::bytes_view term =
    irs::ViewCast<irs::byte_type>(std::string_view("ab"));

  tests::empty_filter_visitor visitor;
  // read segment
  auto index = open_reader();
  ASSERT_EQ(1, index.size());
  auto& segment = index[0];
  // get term dictionary for field
  const auto* reader = segment.field(field);
  ASSERT_NE(nullptr, reader);
  irs::by_prefix::visit(segment, *reader, term, visitor);
  ASSERT_EQ(1, visitor.prepare_calls_counter());
  ASSERT_EQ(6, visitor.visit_calls_counter());
  ASSERT_EQ((std::vector<std::pair<std::string_view, irs::score_t>>{
              {"abc", irs::kNoBoost},
              {"abcd", irs::kNoBoost},
              {"abcde", irs::kNoBoost},
              {"abcdrer", irs::kNoBoost},
              {"abcy", irs::kNoBoost},
              {"abde", irs::kNoBoost}}),
            visitor.term_refs<char>());

  visitor.reset();
}

INSTANTIATE_TEST_SUITE_P(
  prefix_filter_test, prefix_filter_test_case,
  ::testing::Combine(
    ::testing::Values(&tests::directory<&tests::memory_directory>,
                      &tests::directory<&tests::fs_directory>,
                      &tests::directory<&tests::mmap_directory>),
    ::testing::Values(tests::format_info{"1_0"},
                      tests::format_info{"1_1", "1_0"},
                      tests::format_info{"1_2", "1_0"},
                      tests::format_info{"1_3", "1_0"})),
  prefix_filter_test_case::to_string);

}  // namespace
