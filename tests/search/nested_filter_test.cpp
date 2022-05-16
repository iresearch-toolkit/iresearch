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

auto MakeOptions(
    std::string_view parent, std::string_view parent_value,
    std::string_view child, std::string_view child_value,
    irs::sort::MergeType merge_type = irs::sort::MergeType::AGGREGATE) {
  irs::ByNestedOptions opts;
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

TEST(NestedFilterTest, CheckOptions) {
  {
    irs::ByNestedOptions opts;
    ASSERT_EQ(nullptr, opts.parent);
    ASSERT_EQ(nullptr, opts.child);
    ASSERT_EQ(irs::sort::MergeType::AGGREGATE, opts.merge_type);
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
                                 irs::sort::MergeType::MAX));
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
    for (const auto& [item, price, count] : order.items) {
      InsertItemDocument(trx, item, price, count);
    }
    InsertOrderDocument(trx, order.customer, order.date);
  }
};

TEST_P(NestedFilterTestCase, BasicJoin) {
  irs::index_writer::init_options opts;
  auto writer = open_writer(irs::OM_CREATE, opts);
  ASSERT_NE(nullptr, writer);

  // Parent document: 6
  InsertOrder(*writer, {"ArangoDB",
                        "May",
                        {{"Keyboard", 100, 1},
                         {"Mouse", 50, 1},
                         {"Display", 1000, 1},
                         {"CPU", 5000, 1},
                         {"RAM", 5000, 1}}});

  // Parent document: 11
  InsertOrder(*writer, {"Dell",
                        "April",
                        {{"Mouse", 10, 1},
                         {"Display", 1000, 1},
                         {"CPU", 1000, 1},
                         {"RAM", 5000, 1}}});

  // Parent document: 13, missing "customer" field
  InsertOrder(*writer, {"", "April", {{"Mouse", 10, 1}}});

  ASSERT_TRUE(writer->commit());

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(1, reader.size());

  // Empty filter
  {
    irs::ByNestedFilter filter;
    CheckQuery(filter, Docs{}, Costs{0}, reader, SOURCE_LOCATION);
  }

  // Empty parent filter
  {
    irs::ByNestedFilter filter;
    auto& opts = *filter.mutable_options();
    opts.child = std::make_unique<irs::all>();
    CheckQuery(filter, Docs{}, Costs{0}, reader, SOURCE_LOCATION);
  }

  // Empty child filter
  {
    irs::ByNestedFilter filter;
    auto& opts = *filter.mutable_options();
    opts.parent = std::make_unique<irs::all>();
    CheckQuery(filter, Docs{}, Costs{0}, reader, SOURCE_LOCATION);
  }

  {
    irs::ByNestedFilter filter;
    auto& opts = *filter.mutable_options();
    opts.child = MakeByTerm("item", "Keyboard");
    opts.parent = MakeByColumnExistence("customer");
    CheckQuery(filter, Docs{6}, Costs{1}, reader, SOURCE_LOCATION);
  }

  {
    irs::ByNestedFilter filter;
    auto& opts = *filter.mutable_options();
    opts.child = MakeByTerm("item", "Mouse");
    opts.parent = MakeByColumnExistence("customer");
    CheckQuery(filter, Docs{6, 11}, Costs{3}, reader, SOURCE_LOCATION);
  }

  {
    irs::ByNestedFilter filter;
    auto& opts = *filter.mutable_options();
    opts.child = MakeByTermAndRange("item", "Mouse", "price", 11);
    opts.parent = MakeByColumnExistence("customer");
    CheckQuery(filter, Docs{11}, Costs{2}, reader, SOURCE_LOCATION);
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
