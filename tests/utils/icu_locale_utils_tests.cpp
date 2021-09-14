////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
/// @author Alexey Bakharew
////////////////////////////////////////////////////////////////////////////////

#include "gtest/gtest.h"
#include "utils/locale_utils.hpp"
#include "utils/icu_locale_utils.hpp"
#include "utils/misc.hpp"
#include "velocypack/Parser.h"
#include "velocypack/velocypack-aliases.h"

namespace tests {
  class IcuLocaleUtilsTestSuite: public ::testing::Test {
    virtual void SetUp() {
      // Code here will be called immediately after the constructor (right before each test).
    }

    virtual void TearDown() {
      // Code here will be called immediately after each test (right before the destructor).
    }
  };
}

using namespace tests;
using namespace iresearch::icu_locale_utils;

// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------


TEST_F(IcuLocaleUtilsTestSuite, test_get_locale_from_vpack) {

  {
    std::string config = R"({"locale":{"language" : "de", "country" : "DE"}})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

    icu::Locale expected_locale("de", "DE");
    ASSERT_EQ(actual_locale, expected_locale);
  }

  {
    std::string config = R"({"locale":{"language" : "EN", "country" : "US", "variant" : "_phonebook"}})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

    icu::Locale expected_locale("EN", "US", "phonebook");
    ASSERT_EQ(actual_locale, expected_locale);
  }

  {
    std::string config = R"({"locale":{"language" : "EN", "country" : "EN", "variant" : "_pinyan"}})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

    icu::Locale expected_locale("de", "DE", "phonebook");
    ASSERT_NE(actual_locale, expected_locale);
  }

  {
    std::string config = R"({"locale":{"language" : "EN", "country" : "US", "variant" : "_pinyan", "encoding" : "utf-16"}})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

    icu::Locale expected_locale("en", "US", "pinyan");
    ASSERT_EQ(actual_locale, expected_locale);
  }

  {
    std::string config = R"({"locale":{"language" : "EN", "variant" : "_pinyan"}})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

    icu::Locale expected_locale("en", NULL, "pinyan");
    ASSERT_EQ(actual_locale, expected_locale);
  }

  {
    std::string config = R"({"locale":{"language" : "DE@collation=phonebook"}})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

    icu::Locale expected_locale("DE", NULL, NULL, "collation=phonebook");
    ASSERT_EQ(actual_locale, expected_locale);
  }
}

TEST_F(IcuLocaleUtilsTestSuite, test_locale_to_vpack) {

  {
    icu::Locale icu_locale("UK");
    VPackBuilder builder;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder));

    auto expected_config = VPackParser::fromJson(R"({"locale":{"language":"uk"}})")->slice();
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config.toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE");
    VPackBuilder builder;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder));

    auto expected_config = VPackParser::fromJson(R"({"locale":{"language":"de","country":"DE"}})")->slice();
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config.toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE", "phonebook");
    VPackBuilder builder;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder));

    auto expected_config = VPackParser::fromJson(R"({"locale":{"language":"de","country":"DE","variant":"PHONEBOOK"}})")->slice();
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config.toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE", NULL, "phonebook");
    VPackBuilder builder;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder));

    auto expected_config = VPackParser::fromJson(R"({"locale":{"language":"de","country":"DE","variant":"PHONEBOOK"}})")->slice();
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config.toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE", NULL, "collation=phonebook");
    VPackBuilder builder;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder));

    auto expected_config = VPackParser::fromJson(R"({"locale":{"language":"de","country":"DE"}})")->slice();
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config.toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE", "pinyan", "collation=phonebook");
    VPackBuilder builder;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder));

    auto expected_config = VPackParser::fromJson(R"({"locale":{"language":"de","country":"DE","variant":"PINYAN"}})")->slice();
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config.toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE", "pinyan", "phonebook");
    VPackBuilder builder;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder));

    auto expected_config = VPackParser::fromJson(R"({"locale":{"language":"de","country":"DE","variant":"PINYAN_PHONEBOOK"}})")->slice();
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config.toString(), actual_config.toString());
  }
}
