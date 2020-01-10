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
////////////////////////////////////////////////////////////////////////////////

#include "tests_shared.hpp"

#include "store/memory_directory.hpp"
#include "utils/automaton.hpp"
#include "utils/levenshtein_utils.hpp"
#include "utils/fst_table_matcher.hpp"

#include "index/index_tests.hpp"

using namespace irs::literals;

namespace {

void assert_index(const irs::index_reader& reader,
                  const irs::parametric_description& description,
                  const irs::bytes_ref& target) {
  auto acceptor = irs::make_levenshtein_automaton(description, target);
  irs::automaton_table_matcher matcher(acceptor, fst::fsa::kRho);

  for (auto& segment : reader) {
    auto fields = segment.fields();
    ASSERT_NE(nullptr, fields);

    while (fields->next()) {
      auto expected_terms = fields->value().iterator();
      ASSERT_NE(nullptr, expected_terms);
      auto actual_terms = fields->value().iterator(matcher);
      ASSERT_NE(nullptr, actual_terms);

      while (expected_terms->next()) {
        auto& expected_term = expected_terms->value();
        auto edit_distance = irs::edit_distance(expected_term, target);
        if (edit_distance > description.max_distance) {
          continue;
        }

        ASSERT_TRUE(actual_terms->next());
        auto& actual_term = actual_terms->value();
        ASSERT_EQ(expected_term, actual_term);
      }
    }
  }
}

}

TEST(levenshtein_utils_test, test_distance) {
  {
    const irs::string_ref lhs = "aec";
    const irs::string_ref rhs = "abcd";

    ASSERT_EQ(2, irs::edit_distance(lhs.c_str(), lhs.size(), rhs.c_str(), rhs.size()));
    ASSERT_EQ(2, irs::edit_distance(rhs.c_str(), rhs.size(), lhs.c_str(), lhs.size()));
  }

  {
    const irs::string_ref lhs = "elephant";
    const irs::string_ref rhs = "relevant";

    ASSERT_EQ(3, irs::edit_distance(lhs.c_str(), lhs.size(), rhs.c_str(), rhs.size()));
    ASSERT_EQ(3, irs::edit_distance(rhs.c_str(), rhs.size(), lhs.c_str(), lhs.size()));
  }

  {
    const irs::string_ref lhs = "";
    const irs::string_ref rhs = "aec";

    ASSERT_EQ(3, irs::edit_distance(lhs.c_str(), lhs.size(), rhs.c_str(), rhs.size()));
    ASSERT_EQ(3, irs::edit_distance(rhs.c_str(), rhs.size(), lhs.c_str(), lhs.size()));
  }

  {
    const irs::string_ref lhs = "";
    const irs::string_ref rhs = "";

    ASSERT_EQ(0, irs::edit_distance(lhs.c_str(), lhs.size(), rhs.c_str(), rhs.size()));
    ASSERT_EQ(0, irs::edit_distance(rhs.c_str(), rhs.size(), lhs.c_str(), lhs.size()));
  }

  {
    const irs::string_ref lhs;
    const irs::string_ref rhs;

    ASSERT_EQ(0, irs::edit_distance(lhs.c_str(), lhs.size(), rhs.c_str(), rhs.size()));
    ASSERT_EQ(0, irs::edit_distance(rhs.c_str(), rhs.size(), lhs.c_str(), lhs.size()));
  }
}

class levenshtein_automaton_index_test_case : public tests::index_test_base { };

TEST_P(levenshtein_automaton_index_test_case, test_lev_automaton_distance_1) {
  const irs::parametric_description DESCRIPTIONS[] {
    irs::make_parametric_description(1, false),
    irs::make_parametric_description(2, false),
    irs::make_parametric_description(3, false)
  };

  const irs::string_ref TARGETS[] {
//    "del"
    "atlas", "bloom", "burden", "del",
    "survenius", "surbenus", ""
  };

  // add data
  {
    tests::templates::europarl_doc_template doc;
    tests::delim_doc_generator gen(resource("europarl.subset.txt"), doc);
    add_segment(gen);
  }

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  for (auto& description : DESCRIPTIONS) {
    for (auto& target : TARGETS) {
      ::assert_index(reader, description, irs::ref_cast<irs::byte_type>(target));
    }
  }
}

INSTANTIATE_TEST_CASE_P(
  levenshtein_automaton_index_test,
  levenshtein_automaton_index_test_case,
  ::testing::Combine(
    ::testing::Values(
      &tests::memory_directory
    ),
    ::testing::Values("1_2")
  ),
  tests::to_string
);
