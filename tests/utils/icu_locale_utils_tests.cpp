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
  class LocaleUtils2TestSuite: public ::testing::Test {
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


TEST_F(LocaleUtils2TestSuite, test_get_locale_from_vpack) {

//  {
//    std::string config = R"({"language" : "de", "country" : "DE"})";
//    auto vpack = VPackParser::fromJson(config.c_str(), config.size());

//    std::locale actual_locale;
//    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

//    std::locale expected_locale;
//    std::string expected_locale_name = "de_DE";
//    expected_locale = locale(expected_locale_name);

//    ASSERT_EQ(actual_locale, expected_locale);
//  }

}

TEST_F(LocaleUtils2TestSuite, test_verify_icu_locale) {

  {

  }

}
