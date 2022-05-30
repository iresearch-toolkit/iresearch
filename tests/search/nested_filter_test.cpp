////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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

#include "search/nested_filter.hpp"

#include "search/all_filter.hpp"
#include "search/boolean_filter.hpp"
#include "search/column_existence_filter.hpp"
#include "search/filter_test_case_base.hpp"
#include "search/granular_range_filter.hpp"
#include "search/term_filter.hpp"
#include "tests_shared.hpp"

namespace {

struct DocIdScorer : irs::sort {
  DocIdScorer() : irs::sort(irs::type<DocIdScorer>::get()) {}

  struct Prepared final : irs::PreparedSortBase<void> {
    virtual irs::IndexFeatures features() const override {
      return irs::IndexFeatures::NONE;
    }

    virtual irs::ScoreFunction prepare_scorer(
        const irs::sub_reader&, const irs::term_reader&, const irs::byte_type*,
        const irs::attribute_provider& attrs, irs::score_t) const override {
      struct ScorerContext final : irs::score_ctx {
        explicit ScorerContext(const irs::document* doc) noexcept : doc{doc} {}

        const irs::document* doc;
      };

      auto* doc = irs::get<irs::document>(attrs);
      EXPECT_NE(nullptr, doc);

      return {std::make_unique<ScorerContext>(doc),
              [](irs::score_ctx* ctx, irs::score_t* res) noexcept {
                ASSERT_NE(nullptr, res);
                ASSERT_NE(nullptr, ctx);
                const auto& state = *reinterpret_cast<ScorerContext*>(ctx);
                *res = state.doc->value;
              }};
    }
  };

  irs::sort::prepared::ptr prepare() const override {
    return std::make_unique<Prepared>();
  }
};

// exists(name)
auto MakeByColumnExistence(std::string_view name) {
  auto filter = std::make_unique<irs::by_column_existence>();
  *filter->mutable_field() = name;
  return filter;
}

// name == value
auto MakeByTerm(std::string_view name, std::string_view value) {
  auto filter = std::make_unique<irs::by_term>();
  *filter->mutable_field() = name;
  filter->mutable_options()->term = irs::ref_cast<irs::byte_type>(value);
  return filter;
}

// name == value
auto MakeByNumericTerm(std::string_view name, int32_t value) {
  auto filter = std::make_unique<irs::by_term>();
  *filter->mutable_field() = name;

  irs::numeric_token_stream stream;
  irs::term_attribute const* token = irs::get<irs::term_attribute>(stream);
  stream.reset(value);
  stream.next();

  irs::assign(filter->mutable_options()->term, token->value);

  return filter;
}

irs::filter::ptr MakeOr(
    std::span<std::pair<std::string_view, std::string_view>> parts) {
  auto filter = std::make_unique<irs::Or>();
  for (const auto& [name, value] : parts) {
    filter->add<irs::by_term>() =
        std::move(static_cast<irs::by_term&>(*MakeByTerm(name, value)));
  }
  return filter;
}

irs::filter::ptr MakeAnd(
    std::span<std::pair<std::string_view, std::string_view>> parts) {
  auto filter = std::make_unique<irs::And>();
  for (const auto& [name, value] : parts) {
    filter->add<irs::by_term>() =
        std::move(static_cast<irs::by_term&>(*MakeByTerm(name, value)));
  }
  return filter;
}

// name == value && range_field <= upper_bound
auto MakeByTermAndRange(std::string_view name, std::string_view value,
                        std::string_view range_field, int32_t upper_bound) {
  auto root = std::make_unique<irs::And>();
  // name == value
  {
    auto& filter = root->add<irs::by_term>();
    *filter.mutable_field() = name;
    filter.mutable_options()->term = irs::ref_cast<irs::byte_type>(value);
  }
  // range_field <= upper_bound
  {
    auto& filter = root->add<irs::by_granular_range>();
    *filter.mutable_field() = range_field;

    irs::numeric_token_stream stream;
    auto& range = filter.mutable_options()->range;
    stream.reset(upper_bound);
    irs::set_granular_term(range.max, stream);
  }
  return root;
}

auto MakeOptions(std::string_view parent, std::string_view parent_value,
                 std::string_view child, std::string_view child_value,
                 irs::sort::MergeType merge_type = irs::sort::MergeType::kSum,
                 irs::Match match = irs::kMatchAny) {
  irs::ByNestedOptions opts;
  opts.match = match;
  opts.merge_type = merge_type;
  opts.parent = std::make_unique<irs::by_term>();
  auto& parent_filter = static_cast<irs::by_term&>(*opts.parent);
  *parent_filter.mutable_field() = parent;
  parent_filter.mutable_options()->term =
      irs::ref_cast<irs::byte_type>(parent_value);
  opts.child = std::make_unique<irs::by_term>();
  auto& child_filter = static_cast<irs::by_term&>(*opts.child);
  *child_filter.mutable_field() = child;
  child_filter.mutable_options()->term =
      irs::ref_cast<irs::byte_type>(child_value);

  return opts;
}

TEST(NestedFilterTest, CheckMatch) {
  static_assert(irs::Match{0, 0} == irs::kMatchNone);
  static_assert(irs::Match{1, irs::doc_limits::eof()} == irs::kMatchAny &&
                irs::kMatchAny.IsMinMatch());
  static_assert(irs::Match{irs::doc_limits::eof(), irs::doc_limits::eof()} ==
                    irs::kMatchAll &&
                !irs::kMatchAll.IsMinMatch());
}

TEST(NestedFilterTest, CheckOptions) {
  {
    irs::ByNestedOptions opts;
    ASSERT_EQ(nullptr, opts.parent);
    ASSERT_EQ(nullptr, opts.child);
    ASSERT_EQ(irs::sort::MergeType::kSum, opts.merge_type);
    ASSERT_EQ(irs::kMatchAny, opts.match);
    ASSERT_EQ(opts, irs::ByNestedOptions{});
    ASSERT_EQ(opts.hash(), irs::ByNestedOptions{}.hash());
  }

  {
    const auto opts0 = MakeOptions("parent", "42", "child", "442");
    const auto opts1 = MakeOptions("parent", "42", "child", "442");
    ASSERT_EQ(opts0, opts1);
    ASSERT_EQ(opts0.hash(), opts1.hash());

    ASSERT_NE(opts0, MakeOptions("parent", "43", "child", "442"));
    ASSERT_NE(opts0, MakeOptions("parent", "42", "child", "443"));
    ASSERT_NE(opts0, MakeOptions("parent", "42", "child", "442",
                                 irs::sort::MergeType::kMax));
    ASSERT_NE(opts0, MakeOptions("parent", "42", "child", "442",
                                 irs::sort::MergeType::kSum, irs::kMatchAll));
  }
}

TEST(NestedFilterTest, ConstructFilter) {
  irs::ByNestedFilter filter;
  ASSERT_EQ(irs::ByNestedOptions{}, filter.options());
  ASSERT_EQ(irs::kNoBoost, filter.boost());
}

class NestedFilterTestCase : public tests::FilterTestCaseBase {
 protected:
  struct Item {
    std::string name;
    int32_t price;
    int32_t count;
  };

  struct Order {
    std::string customer;
    std::string date;
    std::vector<Item> items;
  };

  static constexpr auto kIndexAndStore =
      irs::Action::INDEX | irs::Action::STORE;

  static void InsertItemDocument(irs::index_writer::documents_context& trx,
                                 std::string_view item, int32_t price,
                                 int32_t count) {
    auto doc = trx.insert();
    ASSERT_TRUE(doc.insert<kIndexAndStore>(tests::string_field{"item", item}));
    ASSERT_TRUE(doc.insert<kIndexAndStore>(tests::int_field{
        "price", price, irs::type<irs::granularity_prefix>::id()}));
    ASSERT_TRUE(doc.insert<kIndexAndStore>(tests::int_field{
        "count", count, irs::type<irs::granularity_prefix>::id()}));
    ASSERT_TRUE(doc);
  }

  static void InsertOrderDocument(irs::index_writer::documents_context& trx,
                                  std::string_view customer,
                                  std::string_view date) {
    auto doc = trx.insert();
    if (!customer.empty()) {
      ASSERT_TRUE(doc.insert<kIndexAndStore>(
          tests::string_field{"customer", customer}));
    }
    ASSERT_TRUE(doc.insert<kIndexAndStore>(tests::string_field{"date", date}));
    ASSERT_TRUE(doc);
  }

  static void InsertOrder(irs::index_writer& writer, const Order& order) {
    auto trx = writer.documents();
    InsertOrderDocument(trx, order.customer, order.date);
    for (const auto& [item, price, count] : order.items) {
      InsertItemDocument(trx, item, price, count);
    }
  }

  void InitDataSet();
};

void NestedFilterTestCase::InitDataSet() {
  irs::index_writer::init_options opts;
  auto writer = open_writer(irs::OM_CREATE, opts);
  ASSERT_NE(nullptr, writer);

  // Parent document: 1
  InsertOrder(*writer, {"ArangoDB",
                        "May",
                        {{"Keyboard", 100, 1},
                         {"Mouse", 50, 2},
                         {"Display", 1000, 2},
                         {"CPU", 5000, 1},
                         {"RAM", 5000, 1}}});

  // Parent document: 7
  InsertOrder(*writer, {"Quest", "June", {{"CPU", 1000, 3}}});

  // Parent document: 9
  InsertOrder(*writer, {"Dell",
                        "April",
                        {{"Mouse", 10, 2},
                         {"Display", 1000, 2},
                         {"CPU", 1000, 2},
                         {"RAM", 5000, 2}}});

  // Parent document: 14, missing "customer" field
  InsertOrder(*writer, {"", "April", {{"Mouse", 10, 2}}});

  // Parent document: 16
  InsertOrder(*writer, {"BAE",
                        "March",
                        {{"Stand", 10, 2},
                         {"Display", 1000, 2},
                         {"CPU", 1000, 2},
                         {"RAM", 5000, 2}}});

  ASSERT_TRUE(writer->commit());

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(1, reader.size());
}

TEST_P(NestedFilterTestCase, EmptyFilter) {
  InitDataSet();
  auto reader = open_reader();

  {
    irs::ByNestedFilter filter;
    CheckQuery(filter, Docs{}, Costs{0}, reader, SOURCE_LOCATION);
  }

  {
    irs::ByNestedFilter filter;
    auto& opts = *filter.mutable_options();
    opts.child = std::make_unique<irs::all>();
    CheckQuery(filter, Docs{}, Costs{0}, reader, SOURCE_LOCATION);
  }

  {
    irs::ByNestedFilter filter;
    auto& opts = *filter.mutable_options();
    opts.parent = std::make_unique<irs::all>();
    CheckQuery(filter, Docs{}, Costs{0}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinAny0) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByTerm("item", "Keyboard");
  opts.parent = MakeByColumnExistence("customer");

  CheckQuery(filter, Docs{1}, Costs{1}, reader, SOURCE_LOCATION);
}

TEST_P(NestedFilterTestCase, JoinAny1) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByTerm("item", "Mouse");
  opts.parent = MakeByColumnExistence("customer");

  CheckQuery(filter, Docs{1, 9}, Costs{3}, reader, SOURCE_LOCATION);

  {
    const Tests tests = {{Seek{1}, 1}, {Seek{2}, 9}, {Seek{2}, 9}};

    CheckQuery(filter, {}, {tests}, reader, SOURCE_LOCATION);
  }

  {
    std::array<irs::sort::ptr, 1> scorers{std::make_unique<DocIdScorer>()};

    const Tests tests = {
        {Seek{1}, 1, {3.f}},
        // FIXME(gnusi): should be 10, currently
        // fails due to the last parent is missing
        {Seek{2}, 9, {25.f}},
        // FIXME(gnusi): should be 10, currently
        // fails due to we don't cache score
        /*{Seek{2}, 9, {25.f}}*/
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    std::array<irs::sort::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                          std::make_unique<DocIdScorer>()};

    const Tests tests = {
        {Next{}, 1, {3.f, 3.f}},
        {Next{}, 9, {25.f, 25.f}},
        {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    const Tests tests = {{Seek{14}, irs::doc_limits::eof()},
                         {Next{}, irs::doc_limits::eof()},
                         {Next{}, irs::doc_limits::eof()}};
    CheckQuery(filter, {}, {tests}, reader, SOURCE_LOCATION);
  }

  {
    const Tests tests = {
        // Seek to doc_limits::invalid() is implementation specific
        {Seek{irs::doc_limits::invalid()}, irs::doc_limits::invalid()},
        {Seek{1}, 1},
        {Next{}, 9},
        {Next{}, irs::doc_limits::eof()},
        {Next{}, irs::doc_limits::eof()}};
    CheckQuery(filter, {}, {tests}, reader, SOURCE_LOCATION);
  }

  {
    const Tests tests = {
        {Seek{1}, 1}, {Next{}, 9}, {Next{}, irs::doc_limits::eof()}};

    CheckQuery(filter, {}, {tests}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinAny2) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByTermAndRange("item", "Mouse", "price", 11);
  opts.parent = MakeByColumnExistence("customer");

  CheckQuery(filter, Docs{9}, Costs{3}, reader, SOURCE_LOCATION);
}

TEST_P(NestedFilterTestCase, JoinAny3) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByNumericTerm("count", 2);
  opts.parent = MakeByColumnExistence("customer");

  CheckQuery(filter, Docs{1, 9, 16}, Costs{11}, reader, SOURCE_LOCATION);

  {
    opts.merge_type = irs::sort::MergeType::kMax;

    std::array<irs::sort::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                          std::make_unique<DocIdScorer>()};

    const Tests tests = {
        {Next{}, 1, {4.f, 4.f}},
        {Next{}, 9, {15.f, 15.f}},
        {Next{}, 16, {20.f, 20.f}},
        {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    opts.merge_type = irs::sort::MergeType::kMin;

    std::array<irs::sort::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                          std::make_unique<DocIdScorer>()};

    const Tests tests = {
        {Next{}, 1, {3.f, 3.f}},
        {Next{}, 9, {10.f, 10.f}},
        {Next{}, 16, {17.f, 17.f}},
        {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    opts.merge_type = irs::sort::MergeType::kNoop;

    std::array<irs::sort::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                          std::make_unique<DocIdScorer>()};

    const Tests tests = {
        {Next{}, 1, {}},
        {Next{}, 9, {}},
        {Next{}, 16, {}},
        {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinAll0) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByNumericTerm("count", 2);
  opts.parent = MakeByColumnExistence("customer");
  opts.match = irs::kMatchAll;

  CheckQuery(filter, Docs{16}, Costs{11}, reader, SOURCE_LOCATION);

  {
    opts.merge_type = irs::sort::MergeType::kMax;

    std::array<irs::sort::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                          std::make_unique<DocIdScorer>()};

    const Tests tests = {
        {Next{}, 16, {20.f, 20.f}},
        {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    opts.merge_type = irs::sort::MergeType::kMin;

    std::array<irs::sort::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                          std::make_unique<DocIdScorer>()};

    const Tests tests = {
        {Next{}, 16, {17.f, 17.f}},
        {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    opts.merge_type = irs::sort::MergeType::kNoop;

    std::array<irs::sort::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                          std::make_unique<DocIdScorer>()};

    const Tests tests = {
        {Next{}, 16, {}},
        {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinMin0) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByNumericTerm("count", 2);
  opts.parent = MakeByColumnExistence("customer");
  opts.match = irs::Match{3};

  CheckQuery(filter, Docs{9, 16}, Costs{11}, reader, SOURCE_LOCATION);

  {
    opts.merge_type = irs::sort::MergeType::kMax;

    std::array<irs::sort::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                          std::make_unique<DocIdScorer>()};

    const Tests tests = {
        {Next{}, 9, {15.f, 15.f}},
        {Next{}, 16, {20.f, 20.f}},
        {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    opts.merge_type = irs::sort::MergeType::kMin;

    std::array<irs::sort::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                          std::make_unique<DocIdScorer>()};

    const Tests tests = {
        {Next{}, 9, {10.f, 10.f}},
        {Next{}, 16, {17.f, 17.f}},
        {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    opts.merge_type = irs::sort::MergeType::kNoop;

    std::array<irs::sort::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                          std::make_unique<DocIdScorer>()};

    const Tests tests = {
        {Next{}, 9, {}},
        {Next{}, 16, {}},
        {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinRange0) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByNumericTerm("count", 2);
  opts.parent = MakeByColumnExistence("customer");
  opts.match = irs::Match{3, 5};

  CheckQuery(filter, Docs{9, 16}, Costs{11}, reader, SOURCE_LOCATION);

  {
    opts.merge_type = irs::sort::MergeType::kMax;

    std::array<irs::sort::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                          std::make_unique<DocIdScorer>()};

    const Tests tests = {
        {Next{}, 9, {15.f, 15.f}},
        {Next{}, 16, {20.f, 20.f}},
        {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    opts.merge_type = irs::sort::MergeType::kMin;

    std::array<irs::sort::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                          std::make_unique<DocIdScorer>()};

    const Tests tests = {
        {Next{}, 9, {10.f, 10.f}},
        {Next{}, 16, {17.f, 17.f}},
        {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    opts.merge_type = irs::sort::MergeType::kNoop;

    std::array<irs::sort::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                          std::make_unique<DocIdScorer>()};

    const Tests tests = {
        {Next{}, 9, {}},
        {Next{}, 16, {}},
        {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }
}

TEST_P(NestedFilterTestCase, JoinNone0) {
  InitDataSet();
  auto reader = open_reader();

  irs::ByNestedFilter filter;
  auto& opts = *filter.mutable_options();
  opts.child = MakeByTerm("item", "Mouse");
  opts.parent = MakeByColumnExistence("customer");
  opts.match = irs::kMatchNone;

  CheckQuery(filter, Docs{7, 16}, Costs{3}, reader, SOURCE_LOCATION);

  {
    opts.merge_type = irs::sort::MergeType::kMax;

    std::array<irs::sort::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                          std::make_unique<DocIdScorer>()};

    const Tests tests = {
        {Next{}, 7, {1.f, 1.f}},
        {Next{}, 16, {1.f, 1.f}},
        {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    opts.merge_type = irs::sort::MergeType::kMin;

    std::array<irs::sort::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                          std::make_unique<DocIdScorer>()};

    const Tests tests = {
        {Next{}, 7, {1.f, 1.f}},
        {Next{}, 16, {1.f, 1.f}},
        {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }

  {
    opts.merge_type = irs::sort::MergeType::kNoop;

    std::array<irs::sort::ptr, 2> scorers{std::make_unique<DocIdScorer>(),
                                          std::make_unique<DocIdScorer>()};

    const Tests tests = {
        {Next{}, 7, {}},
        {Next{}, 16, {}},
        {Next{}, irs::doc_limits::eof()},
    };

    CheckQuery(filter, scorers, {tests}, reader, SOURCE_LOCATION);
  }
}

INSTANTIATE_TEST_SUITE_P(
    NestedFilterTest, NestedFilterTestCase,
    ::testing::Combine(
        ::testing::Values(&tests::directory<&tests::memory_directory>,
                          &tests::directory<&tests::fs_directory>,
                          &tests::directory<&tests::mmap_directory>),
        ::testing::Values(tests::format_info{"1_0"},
                          tests::format_info{"1_1", "1_0"},
                          tests::format_info{"1_2", "1_0"},
                          tests::format_info{"1_3", "1_0"},
                          tests::format_info{"1_4", "1_0"},
                          tests::format_info{"1_5", "1_0"})),
    NestedFilterTestCase::to_string);

}  // namespace
