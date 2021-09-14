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
    std::string config = R"({"language" : "de", "country" : "DE"})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

    icu::Locale expected_locale("de", "DE");
    ASSERT_EQ(actual_locale, expected_locale);
    ASSERT_TRUE(verify_icu_locale(actual_locale));
  }

  {
    std::string config = R"({"language" : "EN", "country" : "US", "variant" : "phonebook"})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

    icu::Locale expected_locale("EN", "US", "phonebook");
    ASSERT_EQ(actual_locale, expected_locale);
  }

  {
    std::string config = R"({"language" : "EN", "country" : "EN", "variant" : "pinyan"})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

    icu::Locale expected_locale("de", "DE", "phonebook");
    ASSERT_NE(actual_locale, expected_locale);

    ASSERT_FALSE(verify_icu_locale(actual_locale));
  }

  {
    std::string config = R"({"language" : "EN", "country" : "US", "variant" : "pinyan", "encoding" : "utf-16"})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

    icu::Locale expected_locale("en", "US", "pinyan");
    ASSERT_EQ(actual_locale, expected_locale);

    ASSERT_TRUE(verify_icu_locale(actual_locale));
  }

  {
    std::string config = R"({"language" : "EN", "variant" : "pinyan"})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

    icu::Locale expected_locale("en", NULL, "pinyan");
    ASSERT_EQ(actual_locale, expected_locale);

    ASSERT_TRUE(verify_icu_locale(actual_locale));
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

TEST_F(IcuLocaleUtilsTestSuite, test_verify_icu_locale) {
  {
    ASSERT_FALSE(verify_icu_locale(icu::Locale("de", "RU")));
    ASSERT_TRUE(verify_icu_locale(icu::Locale("de_DE")));
    ASSERT_TRUE(verify_icu_locale(icu::Locale("RU", "UA")));
    ASSERT_TRUE(verify_icu_locale(icu::Locale::createFromName("shi_Tfng_MA"))); // because we can't create such locale by default ctor
    ASSERT_TRUE(verify_icu_locale(icu::Locale("fr", "SN", "wrong arg")));
    ASSERT_TRUE(verify_icu_locale(icu::Locale("fr", "SN", "wrong arg", "wrong arg")));
    ASSERT_TRUE(verify_icu_locale(icu::Locale("fr", "SN", NULL, "wrong arg")));
    ASSERT_TRUE(verify_icu_locale(icu::Locale::createFromName("fr_SN")));
    ASSERT_TRUE(verify_icu_locale(icu::Locale::createFromName("fr_SN_wrong_arg")));
    ASSERT_FALSE(verify_icu_locale(icu::Locale("wrong arg")));
    ASSERT_TRUE(verify_icu_locale(icu::Locale("RU", "wrong arg")));
  }
}
