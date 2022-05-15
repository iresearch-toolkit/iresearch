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

#include "search/filter_test_case_base.hpp"
#include "search/term_filter.hpp"
#include "tests_shared.hpp"

namespace {

class NestedFilterTestCase : public tests::filter_test_case_base {};

irs::ByNestedOptions MakeOptions(
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

TEST(NestedFilterTestCase, CheckOptions) {
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

}  // namespace
