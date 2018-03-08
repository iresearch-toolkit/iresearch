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

#include "gtest/gtest.h"
#include "utils/locale_utils.hpp"

#if defined (__GNUC__)
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif

  #include <boost/locale.hpp>

#if defined (__GNUC__)
  #pragma GCC diagnostic pop
#endif

namespace tests {
  class LocaleUtilsTestSuite: public ::testing::Test {
    virtual void SetUp() {
      // Code here will be called immediately after the constructor (right before each test).
    }

    virtual void TearDown() {
      // Code here will be called immediately after each test (right before the destructor).
    }
  };
}

using namespace tests;
using namespace iresearch::locale_utils;

// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------

TEST_F(LocaleUtilsTestSuite, test_locale_create) {
  {
    std::locale locale = iresearch::locale_utils::locale(nullptr);

    ASSERT_EQ(std::locale::classic(), locale);
  }

  {
    std::locale locale = iresearch::locale_utils::locale(nullptr, true);

    ASSERT_EQ(std::string("c.UTF-8"), std::use_facet<boost::locale::info>(locale).name());
    ASSERT_TRUE(std::use_facet<boost::locale::info>(locale).utf8());
  }

  {
    std::locale locale = iresearch::locale_utils::locale("*");

    ASSERT_EQ(std::string("*"), std::use_facet<boost::locale::info>(locale).name());
    ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
  }

  {
    std::locale locale = iresearch::locale_utils::locale("C");

    ASSERT_EQ(std::string("C"), std::use_facet<boost::locale::info>(locale).name());
    ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
  }

  {
    std::locale locale = iresearch::locale_utils::locale("en");

    ASSERT_EQ(std::string("en"), std::use_facet<boost::locale::info>(locale).name());
    ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
  }

  {
    std::locale locale = iresearch::locale_utils::locale("en_US");

    ASSERT_EQ(std::string("en_US"), std::use_facet<boost::locale::info>(locale).name());
    ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
  }

  {
    std::locale locale = iresearch::locale_utils::locale("en_US.UTF-8");

    ASSERT_EQ(std::string("en_US.UTF-8"), std::use_facet<boost::locale::info>(locale).name());
    ASSERT_TRUE(std::use_facet<boost::locale::info>(locale).utf8());
  }

  {
    std::locale locale = iresearch::locale_utils::locale("ru_RU.KOI8-R");

    ASSERT_EQ(std::string("ru_RU.KOI8-R"), std::use_facet<boost::locale::info>(locale).name());
    ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
  }

  {
    std::locale locale = iresearch::locale_utils::locale("ru_RU.KOI8-R", true);

    ASSERT_EQ(std::string("ru_RU.UTF-8"), std::use_facet<boost::locale::info>(locale).name());
    ASSERT_TRUE(std::use_facet<boost::locale::info>(locale).utf8());
  }

  {
    std::locale locale = iresearch::locale_utils::locale("InvalidString");

    ASSERT_EQ(std::string("InvalidString"), std::use_facet<boost::locale::info>(locale).name());
  }
}

TEST_F(LocaleUtilsTestSuite, test_locale_build) {
  {
    {
      std::locale expected = boost::locale::generator().generate("");
      std::locale locale = iresearch::locale_utils::locale("", "", "");

      ASSERT_EQ(std::use_facet<boost::locale::info>(expected).name(), std::use_facet<boost::locale::info>(locale).name());
      ASSERT_EQ(std::use_facet<boost::locale::info>(expected).language(), std::use_facet<boost::locale::info>(locale).language());
      ASSERT_EQ(std::use_facet<boost::locale::info>(expected).country(), std::use_facet<boost::locale::info>(locale).country());
      ASSERT_EQ(std::use_facet<boost::locale::info>(expected).encoding(), std::use_facet<boost::locale::info>(locale).encoding());
      ASSERT_EQ(std::use_facet<boost::locale::info>(expected).variant(), std::use_facet<boost::locale::info>(locale).variant());
      ASSERT_EQ(std::use_facet<boost::locale::info>(expected).utf8(), std::use_facet<boost::locale::info>(locale).utf8());
    }

    {
      std::locale locale = iresearch::locale_utils::locale("en", "", "");

      ASSERT_EQ(std::string("en"), std::use_facet<boost::locale::info>(locale).name());
      ASSERT_EQ(std::string("en"), std::use_facet<boost::locale::info>(locale).language());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).country());
      ASSERT_EQ(std::string("us-ascii"), std::use_facet<boost::locale::info>(locale).encoding());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).variant());
      ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
    }

    {
      std::locale locale = iresearch::locale_utils::locale("en", "US", "");

      ASSERT_EQ(std::string("en_US"), std::use_facet<boost::locale::info>(locale).name());
      ASSERT_EQ(std::string("en"), std::use_facet<boost::locale::info>(locale).language());
      ASSERT_EQ(std::string("US"), std::use_facet<boost::locale::info>(locale).country());
      ASSERT_EQ(std::string("us-ascii"), std::use_facet<boost::locale::info>(locale).encoding());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).variant());
      ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
    }

    {
      std::locale locale = iresearch::locale_utils::locale("en", "US", "ISO-8859-1");

      ASSERT_EQ(std::string("en_US.ISO-8859-1"), std::use_facet<boost::locale::info>(locale).name());
      ASSERT_EQ(std::string("en"), std::use_facet<boost::locale::info>(locale).language());
      ASSERT_EQ(std::string("US"), std::use_facet<boost::locale::info>(locale).country());
      ASSERT_EQ(std::string("iso-8859-1"), std::use_facet<boost::locale::info>(locale).encoding());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).variant());
      ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
    }

    {
      std::locale locale = iresearch::locale_utils::locale("en", "US", "ISO-8859-1", "args");

      ASSERT_EQ(std::string("en_US.ISO-8859-1@args"), std::use_facet<boost::locale::info>(locale).name());
      ASSERT_EQ(std::string("en"), std::use_facet<boost::locale::info>(locale).language());
      ASSERT_EQ(std::string("US"), std::use_facet<boost::locale::info>(locale).country());
      ASSERT_EQ(std::string("iso-8859-1"), std::use_facet<boost::locale::info>(locale).encoding());
      ASSERT_EQ(std::string("args"), std::use_facet<boost::locale::info>(locale).variant());
      ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
    }

    {
      std::locale locale = iresearch::locale_utils::locale("en", "US", "UTF-8");

      ASSERT_EQ(std::string("en_US.UTF-8"), std::use_facet<boost::locale::info>(locale).name());
      ASSERT_EQ(std::string("en"), std::use_facet<boost::locale::info>(locale).language());
      ASSERT_EQ(std::string("US"), std::use_facet<boost::locale::info>(locale).country());
      ASSERT_EQ(std::string("utf-8"), std::use_facet<boost::locale::info>(locale).encoding());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).variant());
      ASSERT_TRUE(std::use_facet<boost::locale::info>(locale).utf8());
    }

    // ...........................................................................
    // invalid
    // ...........................................................................

    {
      std::locale locale = iresearch::locale_utils::locale("", "US", "");

      ASSERT_EQ(std::string("_US"), std::use_facet<boost::locale::info>(locale).name());
      ASSERT_EQ(std::string("C"), std::use_facet<boost::locale::info>(locale).language());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).country());
      ASSERT_EQ(std::string("us-ascii"), std::use_facet<boost::locale::info>(locale).encoding());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).variant());
      ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
    }

    {
      std::locale locale = iresearch::locale_utils::locale("", "US", "ISO-8859-1");

      ASSERT_EQ(std::string("_US.ISO-8859-1"), std::use_facet<boost::locale::info>(locale).name());
      ASSERT_EQ(std::string("C"), std::use_facet<boost::locale::info>(locale).language());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).country());
      ASSERT_EQ(std::string("us-ascii"), std::use_facet<boost::locale::info>(locale).encoding());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).variant());
      ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
    }

    {
      std::locale locale = iresearch::locale_utils::locale("", "US", "ISO-8859-1", "args");

      ASSERT_EQ(std::string("_US.ISO-8859-1@args"), std::use_facet<boost::locale::info>(locale).name());
      ASSERT_EQ(std::string("C"), std::use_facet<boost::locale::info>(locale).language());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).country());
      ASSERT_EQ(std::string("us-ascii"), std::use_facet<boost::locale::info>(locale).encoding());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).variant());
      ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
    }

    {
      std::locale locale = iresearch::locale_utils::locale("", "US", "UTF-8");

      ASSERT_EQ(std::string("_US.UTF-8"), std::use_facet<boost::locale::info>(locale).name());
      ASSERT_EQ(std::string("C"), std::use_facet<boost::locale::info>(locale).language());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).country());
      ASSERT_EQ(std::string("us-ascii"), std::use_facet<boost::locale::info>(locale).encoding());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).variant());
      ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
    }

    {
      std::locale locale = iresearch::locale_utils::locale("", "", "ISO-8859-1");

      ASSERT_EQ(std::string(".ISO-8859-1"), std::use_facet<boost::locale::info>(locale).name());
      ASSERT_EQ(std::string("C"), std::use_facet<boost::locale::info>(locale).language());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).country());
      ASSERT_EQ(std::string("us-ascii"), std::use_facet<boost::locale::info>(locale).encoding());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).variant());
      ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
    }

    {
      std::locale locale = iresearch::locale_utils::locale("", "", "ISO-8859-1", "args");

      ASSERT_EQ(std::string(".ISO-8859-1@args"), std::use_facet<boost::locale::info>(locale).name());
      ASSERT_EQ(std::string("C"), std::use_facet<boost::locale::info>(locale).language());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).country());
      ASSERT_EQ(std::string("us-ascii"), std::use_facet<boost::locale::info>(locale).encoding());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).variant());
      ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
    }

    {
      std::locale locale = iresearch::locale_utils::locale("", "", "UTF-8");

      ASSERT_EQ(std::string(".UTF-8"), std::use_facet<boost::locale::info>(locale).name());
      ASSERT_EQ(std::string("C"), std::use_facet<boost::locale::info>(locale).language());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).country());
      ASSERT_EQ(std::string("us-ascii"), std::use_facet<boost::locale::info>(locale).encoding());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).variant());
      ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
    }

    {
      std::locale locale = iresearch::locale_utils::locale("", "", "UTF-8", "args");

      ASSERT_EQ(std::string(".UTF-8@args"), std::use_facet<boost::locale::info>(locale).name());
      ASSERT_EQ(std::string("C"), std::use_facet<boost::locale::info>(locale).language());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).country());
      ASSERT_EQ(std::string("us-ascii"), std::use_facet<boost::locale::info>(locale).encoding());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).variant());
      ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
    }

    // ...........................................................................
    // invalid
    // ...........................................................................

    {
      std::locale locale = iresearch::locale_utils::locale("_.@", "", "", "");

      ASSERT_EQ(std::string("_.@_.@"), std::use_facet<boost::locale::info>(locale).name());
      ASSERT_EQ(std::string("C"), std::use_facet<boost::locale::info>(locale).language());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).country());
      ASSERT_EQ(std::string("us-ascii"), std::use_facet<boost::locale::info>(locale).encoding());
      ASSERT_EQ(std::string(""), std::use_facet<boost::locale::info>(locale).variant());
      ASSERT_FALSE(std::use_facet<boost::locale::info>(locale).utf8());
    }
  }
}

TEST_F(LocaleUtilsTestSuite, test_locale_info) {
  {
    std::locale locale = std::locale::classic();

    ASSERT_EQ(std::string(""), iresearch::locale_utils::country(locale));
    ASSERT_EQ(std::string("us-ascii"), iresearch::locale_utils::encoding(locale));
    ASSERT_EQ(std::string("c"), iresearch::locale_utils::language(locale));
    ASSERT_EQ(std::string("C"), iresearch::locale_utils::name(locale));
  }

  {
    std::locale locale = iresearch::locale_utils::locale(nullptr);

    ASSERT_EQ(std::string(""), iresearch::locale_utils::country(locale));
    ASSERT_EQ(std::string("us-ascii"), iresearch::locale_utils::encoding(locale));
    ASSERT_EQ(std::string("c"), iresearch::locale_utils::language(locale));
    ASSERT_EQ(std::string("C"), iresearch::locale_utils::name(locale));
  }

  {
    std::locale locale = iresearch::locale_utils::locale("*");

    ASSERT_EQ(std::string(""), iresearch::locale_utils::country(locale));
    ASSERT_EQ(std::string("us-ascii"), iresearch::locale_utils::encoding(locale));
    ASSERT_EQ(std::string("*"), iresearch::locale_utils::language(locale));
    ASSERT_EQ(std::string("*"), iresearch::locale_utils::name(locale));
  }

  {
    std::locale locale = iresearch::locale_utils::locale("C");

    ASSERT_EQ(std::string(""), iresearch::locale_utils::country(locale));
    ASSERT_EQ(std::string("us-ascii"), iresearch::locale_utils::encoding(locale));
    ASSERT_EQ(std::string("c"), iresearch::locale_utils::language(locale));
    ASSERT_EQ(std::string("C"), iresearch::locale_utils::name(locale));
  }

  {
    std::locale locale = iresearch::locale_utils::locale("en");

    ASSERT_EQ(std::string(""), iresearch::locale_utils::country(locale));
    ASSERT_EQ(std::string("us-ascii"), iresearch::locale_utils::encoding(locale));
    ASSERT_EQ(std::string("en"), iresearch::locale_utils::language(locale));
    ASSERT_EQ(std::string("en"), iresearch::locale_utils::name(locale));
  }

  {
    std::locale locale = iresearch::locale_utils::locale("en_US");

    ASSERT_EQ(std::string("US"), iresearch::locale_utils::country(locale));
    ASSERT_EQ(std::string("us-ascii"), iresearch::locale_utils::encoding(locale));
    ASSERT_EQ(std::string("en"), iresearch::locale_utils::language(locale));
    ASSERT_EQ(std::string("en_US"), iresearch::locale_utils::name(locale));
  }

  {
    std::locale locale = iresearch::locale_utils::locale("en_US.UTF-8");

    ASSERT_EQ(std::string("US"), iresearch::locale_utils::country(locale));
    ASSERT_EQ(std::string("utf-8"), iresearch::locale_utils::encoding(locale));
    ASSERT_EQ(std::string("en"), iresearch::locale_utils::language(locale));
    ASSERT_EQ(std::string("en_US.UTF-8"), iresearch::locale_utils::name(locale));
  }

  {
    std::locale locale = iresearch::locale_utils::locale("ru_RU.KOI8-R");

    ASSERT_EQ(std::string("RU"), iresearch::locale_utils::country(locale));
    ASSERT_EQ(std::string("koi8-r"), iresearch::locale_utils::encoding(locale));
    ASSERT_EQ(std::string("ru"), iresearch::locale_utils::language(locale));
    ASSERT_EQ(std::string("ru_RU.KOI8-R"), iresearch::locale_utils::name(locale));
  }

  {
    std::locale locale = iresearch::locale_utils::locale("InvalidString");

    ASSERT_EQ(std::string(""), iresearch::locale_utils::country(locale));
    ASSERT_EQ(std::string("us-ascii"), iresearch::locale_utils::encoding(locale));
    ASSERT_EQ(std::string("invalidstring"), iresearch::locale_utils::language(locale));
    ASSERT_EQ(std::string("InvalidString"), iresearch::locale_utils::name(locale));
  }
}

TEST_F(LocaleUtilsTestSuite, test_locale_num_put) {
  auto de = irs::locale_utils::locale("de");
  auto en = irs::locale_utils::locale("en.IBM-943"); // EBCDIC
  auto ru = irs::locale_utils::locale("ru_RU.KOI8-R");

  // bool
  {
    std::ostringstream de_out;
    std::ostringstream en_out;
    std::ostringstream ru_out;
    std::vector<std::ostringstream*> v = { &de_out, &en_out, &ru_out };

    de_out.imbue(de);
    en_out.imbue(en);
    ru_out.imbue(ru);

    for (auto out: v) {
      *out << "|" << std::boolalpha << false
           << "|" << std::boolalpha << true
           << "|" << std::noboolalpha << std::dec << false
           << "|" << std::noboolalpha << std::hex << false
           << "|" << std::noboolalpha << std::oct << false
           << "|" << std::noboolalpha << std::dec << true
           << "|" << std::noboolalpha << std::hex << true
           << "|" << std::noboolalpha << std::oct << true
           << "|" << std::showbase << std::showpos << std::internal << std::boolalpha << std::setw(10) << false
           << "|" << std::showbase << std::showpos << std::internal << std::boolalpha << std::setw(10) << true
           << "|" << std::showbase << std::showpos << std::internal << std::noboolalpha << std::dec << std::setw(10) << false
           << "|" << std::showbase << std::showpos << std::internal << std::noboolalpha << std::hex << std::setw(10) << false
           << "|" << std::showbase << std::showpos << std::internal << std::noboolalpha << std::oct << std::setw(10) << false
           << "|" << std::showbase << std::showpos << std::internal << std::noboolalpha << std::dec << std::setw(10) << true
           << "|" << std::showbase << std::showpos << std::internal << std::noboolalpha << std::hex << std::setw(10) << true
           << "|" << std::showbase << std::showpos << std::internal << std::noboolalpha << std::oct << std::setw(10) << true
           << "|" << std::showbase << std::showpos << std::left << std::boolalpha << std::setw(10) << false
           << "|" << std::showbase << std::showpos << std::left << std::boolalpha << std::setw(10) << true
           << "|" << std::showbase << std::showpos << std::left << std::noboolalpha << std::dec << std::setw(10) << false
           << "|" << std::showbase << std::showpos << std::left << std::noboolalpha << std::hex << std::setw(10) << false
           << "|" << std::showbase << std::showpos << std::left << std::noboolalpha << std::oct << std::setw(10) << false
           << "|" << std::showbase << std::showpos << std::left << std::noboolalpha << std::dec << std::setw(10) << true
           << "|" << std::showbase << std::showpos << std::left << std::noboolalpha << std::hex << std::setw(10) << true
           << "|" << std::showbase << std::showpos << std::left << std::noboolalpha << std::oct << std::setw(10) << true
           << "|" << std::showbase << std::showpos << std::right << std::boolalpha << std::setw(10) << false
           << "|" << std::showbase << std::showpos << std::right << std::boolalpha << std::setw(10) << true
           << "|" << std::showbase << std::showpos << std::right << std::noboolalpha << std::dec << std::setw(10) << false
           << "|" << std::showbase << std::showpos << std::right << std::noboolalpha << std::hex << std::setw(10) << false
           << "|" << std::showbase << std::showpos << std::right << std::noboolalpha << std::oct << std::setw(10) << false
           << "|" << std::showbase << std::showpos << std::right << std::noboolalpha << std::dec << std::setw(10) << true
           << "|" << std::showbase << std::showpos << std::right << std::noboolalpha << std::hex << std::setw(10) << true
           << "|" << std::showbase << std::showpos << std::right << std::noboolalpha << std::oct << std::setw(10) << true
           << "|" << std::endl;
    }

    ASSERT_EQ(std::string("|false|true|0|0|0|1|1|1|     false|      true|+        0|         0|         0|+        1|0x       1|        01|false     |true      |+0        |0         |0         |+1        |0x1       |01        |     false|      true|        +0|         0|         0|        +1|       0x1|        01|\n"), de_out.str());
    ASSERT_EQ(std::string("|false|true|0|0|0|1|1|1|     false|      true|+        0|         0|         0|+        1|0x       1|        01|false     |true      |+0        |0         |0         |+1        |0x1       |01        |     false|      true|        +0|         0|         0|        +1|       0x1|        01|\n"), en_out.str());
    ASSERT_EQ(std::string("|false|true|0|0|0|1|1|1|     false|      true|+        0|         0|         0|+        1|0x       1|        01|false     |true      |+0        |0         |0         |+1        |0x1       |01        |     false|      true|        +0|         0|         0|        +1|       0x1|        01|\n"), ru_out.str());
  }

  // long
  {
    std::ostringstream de_out;
    std::ostringstream en_out;
    std::ostringstream ru_out;
    std::vector<std::ostringstream*> v = { &de_out, &en_out, &ru_out };

    de_out.imbue(de);
    en_out.imbue(en);
    ru_out.imbue(ru);

    for (auto out: v) {
      *out << "|" << std::dec << (long)(-1234)
           << "|" << std::hex << (long)(-1234)
           << "|" << std::oct << (long)(-1234)
           << "|" << std::dec << (long)(0)
           << "|" << std::hex << (long)(0)
           << "|" << std::oct << (long)(0)
           << "|" << std::dec << (long)(1234)
           << "|" << std::hex << (long)(1234)
           << "|" << std::oct << (long)(1234)
           << "|" << std::showbase << std::showpos << std::internal << std::dec << std::setw(10) << (long)(-1234)
           << "|" << std::showbase << std::showpos << std::internal << std::hex << std::setw(10) << (long)(-1234)
           << "|" << std::showbase << std::showpos << std::internal << std::oct << std::setw(10) << (long)(-1234)
           << "|" << std::showbase << std::showpos << std::internal << std::dec << std::setw(10) << (long)(0)
           << "|" << std::showbase << std::showpos << std::internal << std::hex << std::setw(10) << (long)(0)
           << "|" << std::showbase << std::showpos << std::internal << std::oct << std::setw(10) << (long)(0)
           << "|" << std::showbase << std::showpos << std::internal << std::dec << std::setw(10) << (long)(1234)
           << "|" << std::showbase << std::showpos << std::internal << std::hex << std::setw(10) << (long)(1234)
           << "|" << std::showbase << std::showpos << std::internal << std::oct << std::setw(10) << (long)(1234)
           << "|" << std::showbase << std::showpos << std::left << std::dec << std::setw(10) << (long)(-1234)
           << "|" << std::showbase << std::showpos << std::left << std::hex << std::setw(10) << (long)(-1234)
           << "|" << std::showbase << std::showpos << std::left << std::oct << std::setw(10) << (long)(-1234)
           << "|" << std::showbase << std::showpos << std::left << std::dec << std::setw(10) << (long)(0)
           << "|" << std::showbase << std::showpos << std::left << std::hex << std::setw(10) << (long)(0)
           << "|" << std::showbase << std::showpos << std::left << std::oct << std::setw(10) << (long)(0)
           << "|" << std::showbase << std::showpos << std::left << std::dec << std::setw(10) << (long)(1234)
           << "|" << std::showbase << std::showpos << std::left << std::hex << std::setw(10) << (long)(1234)
           << "|" << std::showbase << std::showpos << std::left << std::oct << std::setw(10) << (long)(1234)
           << "|" << std::showbase << std::showpos << std::right << std::dec << std::setw(10) << (long)(-1234)
           << "|" << std::showbase << std::showpos << std::right << std::hex << std::setw(10) << (long)(-1234)
           << "|" << std::showbase << std::showpos << std::right << std::oct << std::setw(10) << (long)(-1234)
           << "|" << std::showbase << std::showpos << std::right << std::dec << std::setw(10) << (long)(0)
           << "|" << std::showbase << std::showpos << std::right << std::hex << std::setw(10) << (long)(0)
           << "|" << std::showbase << std::showpos << std::right << std::oct << std::setw(10) << (long)(0)
           << "|" << std::showbase << std::showpos << std::right << std::dec << std::setw(10) << (long)(1234)
           << "|" << std::showbase << std::showpos << std::right << std::hex << std::setw(10) << (long)(1234)
           << "|" << std::showbase << std::showpos << std::right << std::oct << std::setw(10) << (long)(1234)
           << "|" << std::endl;
    }

    ASSERT_EQ(std::string("|-1234|fffffffffffffb2e|1777777777777777775456|0|0|0|1234|4d2|2322|-     1234|0xfffffffffffffb2e|01777777777777777775456|+        0|         0|         0|+     1234|0x     4d2|     02322|-1234     |0xfffffffffffffb2e|01777777777777777775456|+0        |0         |0         |+1234     |0x4d2     |02322     |     -1234|0xfffffffffffffb2e|01777777777777777775456|        +0|         0|         0|     +1234|     0x4d2|     02322|\n"), de_out.str());
    ASSERT_EQ(std::string("|-1234|fffffffffffffb2e|1777777777777777775456|0|0|0|1234|4d2|2322|-     1234|0xfffffffffffffb2e|01777777777777777775456|+        0|         0|         0|+     1234|0x     4d2|     02322|-1234     |0xfffffffffffffb2e|01777777777777777775456|+0        |0         |0         |+1234     |0x4d2     |02322     |     -1234|0xfffffffffffffb2e|01777777777777777775456|        +0|         0|         0|     +1234|     0x4d2|     02322|\n"), en_out.str());
    ASSERT_EQ(std::string("|-1234|fffffffffffffb2e|1777777777777777775456|0|0|0|1234|4d2|2322|-     1234|0xfffffffffffffb2e|01777777777777777775456|+        0|         0|         0|+     1234|0x     4d2|     02322|-1234     |0xfffffffffffffb2e|01777777777777777775456|+0        |0         |0         |+1234     |0x4d2     |02322     |     -1234|0xfffffffffffffb2e|01777777777777777775456|        +0|         0|         0|     +1234|     0x4d2|     02322|\n"), ru_out.str());
  }

  // long long
  {
    std::ostringstream de_out;
    std::ostringstream en_out;
    std::ostringstream ru_out;
    std::vector<std::ostringstream*> v = { &de_out, &en_out, &ru_out };

    de_out.imbue(de);
    en_out.imbue(en);
    ru_out.imbue(ru);

    for (auto out: v) {
      *out << "|" << std::dec << (long long)(-1234)
           << "|" << std::hex << (long long)(-1234)
           << "|" << std::oct << (long long)(-1234)
           << "|" << std::dec << (long long)(0)
           << "|" << std::hex << (long long)(0)
           << "|" << std::oct << (long long)(0)
           << "|" << std::dec << (long long)(1234)
           << "|" << std::hex << (long long)(1234)
           << "|" << std::oct << (long long)(1234)
           << "|" << std::showbase << std::showpos << std::internal << std::dec << std::setw(10) << (long long)(-1234)
           << "|" << std::showbase << std::showpos << std::internal << std::hex << std::setw(10) << (long long)(-1234)
           << "|" << std::showbase << std::showpos << std::internal << std::oct << std::setw(10) << (long long)(-1234)
           << "|" << std::showbase << std::showpos << std::internal << std::dec << std::setw(10) << (long long)(0)
           << "|" << std::showbase << std::showpos << std::internal << std::hex << std::setw(10) << (long long)(0)
           << "|" << std::showbase << std::showpos << std::internal << std::oct << std::setw(10) << (long long)(0)
           << "|" << std::showbase << std::showpos << std::internal << std::dec << std::setw(10) << (long long)(1234)
           << "|" << std::showbase << std::showpos << std::internal << std::hex << std::setw(10) << (long long)(1234)
           << "|" << std::showbase << std::showpos << std::internal << std::oct << std::setw(10) << (long long)(1234)
           << "|" << std::showbase << std::showpos << std::left << std::dec << std::setw(10) << (long long)(-1234)
           << "|" << std::showbase << std::showpos << std::left << std::hex << std::setw(10) << (long long)(-1234)
           << "|" << std::showbase << std::showpos << std::left << std::oct << std::setw(10) << (long long)(-1234)
           << "|" << std::showbase << std::showpos << std::left << std::dec << std::setw(10) << (long long)(0)
           << "|" << std::showbase << std::showpos << std::left << std::hex << std::setw(10) << (long long)(0)
           << "|" << std::showbase << std::showpos << std::left << std::oct << std::setw(10) << (long long)(0)
           << "|" << std::showbase << std::showpos << std::left << std::dec << std::setw(10) << (long long)(1234)
           << "|" << std::showbase << std::showpos << std::left << std::hex << std::setw(10) << (long long)(1234)
           << "|" << std::showbase << std::showpos << std::left << std::oct << std::setw(10) << (long long)(1234)
           << "|" << std::showbase << std::showpos << std::right << std::dec << std::setw(10) << (long long)(-1234)
           << "|" << std::showbase << std::showpos << std::right << std::hex << std::setw(10) << (long long)(-1234)
           << "|" << std::showbase << std::showpos << std::right << std::oct << std::setw(10) << (long long)(-1234)
           << "|" << std::showbase << std::showpos << std::right << std::dec << std::setw(10) << (long long)(0)
           << "|" << std::showbase << std::showpos << std::right << std::hex << std::setw(10) << (long long)(0)
           << "|" << std::showbase << std::showpos << std::right << std::oct << std::setw(10) << (long long)(0)
           << "|" << std::showbase << std::showpos << std::right << std::dec << std::setw(10) << (long long)(1234)
           << "|" << std::showbase << std::showpos << std::right << std::hex << std::setw(10) << (long long)(1234)
           << "|" << std::showbase << std::showpos << std::right << std::oct << std::setw(10) << (long long)(1234)
           << "|" << std::endl;
    }

    ASSERT_EQ(std::string("|-1234|fffffffffffffb2e|1777777777777777775456|0|0|0|1234|4d2|2322|-     1234|0xfffffffffffffb2e|01777777777777777775456|+        0|         0|         0|+     1234|0x     4d2|     02322|-1234     |0xfffffffffffffb2e|01777777777777777775456|+0        |0         |0         |+1234     |0x4d2     |02322     |     -1234|0xfffffffffffffb2e|01777777777777777775456|        +0|         0|         0|     +1234|     0x4d2|     02322|\n"), de_out.str());
    ASSERT_EQ(std::string("|-1234|fffffffffffffb2e|1777777777777777775456|0|0|0|1234|4d2|2322|-     1234|0xfffffffffffffb2e|01777777777777777775456|+        0|         0|         0|+     1234|0x     4d2|     02322|-1234     |0xfffffffffffffb2e|01777777777777777775456|+0        |0         |0         |+1234     |0x4d2     |02322     |     -1234|0xfffffffffffffb2e|01777777777777777775456|        +0|         0|         0|     +1234|     0x4d2|     02322|\n"), en_out.str());
    ASSERT_EQ(std::string("|-1234|fffffffffffffb2e|1777777777777777775456|0|0|0|1234|4d2|2322|-     1234|0xfffffffffffffb2e|01777777777777777775456|+        0|         0|         0|+     1234|0x     4d2|     02322|-1234     |0xfffffffffffffb2e|01777777777777777775456|+0        |0         |0         |+1234     |0x4d2     |02322     |     -1234|0xfffffffffffffb2e|01777777777777777775456|        +0|         0|         0|     +1234|     0x4d2|     02322|\n"), ru_out.str());
  }

  // unsigned long
  {
    std::ostringstream de_out;
    std::ostringstream en_out;
    std::ostringstream ru_out;
    std::vector<std::ostringstream*> v = { &de_out, &en_out, &ru_out };

    de_out.imbue(de);
    en_out.imbue(en);
    ru_out.imbue(ru);

    for (auto out: v) {
      *out << "|" << std::dec << (unsigned long)(0)
           << "|" << std::hex << (unsigned long)(0)
           << "|" << std::oct << (unsigned long)(0)
           << "|" << std::dec << (unsigned long)(1234)
           << "|" << std::hex << (unsigned long)(1234)
           << "|" << std::oct << (unsigned long)(1234)
           << "|" << std::showbase << std::showpos << std::internal << std::dec << std::setw(10) << (unsigned long)(0)
           << "|" << std::showbase << std::showpos << std::internal << std::hex << std::setw(10) << (unsigned long)(0)
           << "|" << std::showbase << std::showpos << std::internal << std::oct << std::setw(10) << (unsigned long)(0)
           << "|" << std::showbase << std::showpos << std::internal << std::dec << std::setw(10) << (unsigned long)(1234)
           << "|" << std::showbase << std::showpos << std::internal << std::hex << std::setw(10) << (unsigned long)(1234)
           << "|" << std::showbase << std::showpos << std::internal << std::oct << std::setw(10) << (unsigned long)(1234)
           << "|" << std::showbase << std::showpos << std::left << std::dec << std::setw(10) << (unsigned long)(0)
           << "|" << std::showbase << std::showpos << std::left << std::hex << std::setw(10) << (unsigned long)(0)
           << "|" << std::showbase << std::showpos << std::left << std::oct << std::setw(10) << (unsigned long)(0)
           << "|" << std::showbase << std::showpos << std::left << std::dec << std::setw(10) << (unsigned long)(1234)
           << "|" << std::showbase << std::showpos << std::left << std::hex << std::setw(10) << (unsigned long)(1234)
           << "|" << std::showbase << std::showpos << std::left << std::oct << std::setw(10) << (unsigned long)(1234)
           << "|" << std::showbase << std::showpos << std::right << std::dec << std::setw(10) << (unsigned long)(0)
           << "|" << std::showbase << std::showpos << std::right << std::hex << std::setw(10) << (unsigned long)(0)
           << "|" << std::showbase << std::showpos << std::right << std::oct << std::setw(10) << (unsigned long)(0)
           << "|" << std::showbase << std::showpos << std::right << std::dec << std::setw(10) << (unsigned long)(1234)
           << "|" << std::showbase << std::showpos << std::right << std::hex << std::setw(10) << (unsigned long)(1234)
           << "|" << std::showbase << std::showpos << std::right << std::oct << std::setw(10) << (unsigned long)(1234)
           << "|" << std::endl;
    }

    ASSERT_EQ(std::string("|0|0|0|1234|4d2|2322|         0|         0|         0|      1234|0x     4d2|     02322|0         |0         |0         |1234      |0x4d2     |02322     |         0|         0|         0|      1234|     0x4d2|     02322|\n"), de_out.str());
    ASSERT_EQ(std::string("|0|0|0|1234|4d2|2322|         0|         0|         0|      1234|0x     4d2|     02322|0         |0         |0         |1234      |0x4d2     |02322     |         0|         0|         0|      1234|     0x4d2|     02322|\n"), en_out.str());
    ASSERT_EQ(std::string("|0|0|0|1234|4d2|2322|         0|         0|         0|      1234|0x     4d2|     02322|0         |0         |0         |1234      |0x4d2     |02322     |         0|         0|         0|      1234|     0x4d2|     02322|\n"), ru_out.str());
  }

  // unsigned long long
  {
    std::ostringstream de_out;
    std::ostringstream en_out;
    std::ostringstream ru_out;
    std::vector<std::ostringstream*> v = { &de_out, &en_out, &ru_out };

    de_out.imbue(de);
    en_out.imbue(en);
    ru_out.imbue(ru);

    for (auto out: v) {
      *out << "|" << std::dec << (unsigned long long)(0)
           << "|" << std::hex << (unsigned long long)(0)
           << "|" << std::oct << (unsigned long long)(0)
           << "|" << std::dec << (unsigned long long)(1234)
           << "|" << std::hex << (unsigned long long)(1234)
           << "|" << std::oct << (unsigned long long)(1234)
           << "|" << std::showbase << std::showpos << std::internal << std::dec << std::setw(10) << (unsigned long long)(0)
           << "|" << std::showbase << std::showpos << std::internal << std::hex << std::setw(10) << (unsigned long long)(0)
           << "|" << std::showbase << std::showpos << std::internal << std::oct << std::setw(10) << (unsigned long long)(0)
           << "|" << std::showbase << std::showpos << std::internal << std::dec << std::setw(10) << (unsigned long long)(1234)
           << "|" << std::showbase << std::showpos << std::internal << std::hex << std::setw(10) << (unsigned long long)(1234)
           << "|" << std::showbase << std::showpos << std::internal << std::oct << std::setw(10) << (unsigned long long)(1234)
           << "|" << std::showbase << std::showpos << std::left << std::dec << std::setw(10) << (unsigned long long)(0)
           << "|" << std::showbase << std::showpos << std::left << std::hex << std::setw(10) << (unsigned long long)(0)
           << "|" << std::showbase << std::showpos << std::left << std::oct << std::setw(10) << (unsigned long long)(0)
           << "|" << std::showbase << std::showpos << std::left << std::dec << std::setw(10) << (unsigned long long)(1234)
           << "|" << std::showbase << std::showpos << std::left << std::hex << std::setw(10) << (unsigned long long)(1234)
           << "|" << std::showbase << std::showpos << std::left << std::oct << std::setw(10) << (unsigned long long)(1234)
           << "|" << std::showbase << std::showpos << std::right << std::dec << std::setw(10) << (unsigned long long)(0)
           << "|" << std::showbase << std::showpos << std::right << std::hex << std::setw(10) << (unsigned long long)(0)
           << "|" << std::showbase << std::showpos << std::right << std::oct << std::setw(10) << (unsigned long long)(0)
           << "|" << std::showbase << std::showpos << std::right << std::dec << std::setw(10) << (unsigned long long)(1234)
           << "|" << std::showbase << std::showpos << std::right << std::hex << std::setw(10) << (unsigned long long)(1234)
           << "|" << std::showbase << std::showpos << std::right << std::oct << std::setw(10) << (unsigned long long)(1234)
           << "|" << std::endl;
    }

    ASSERT_EQ(std::string("|0|0|0|1234|4d2|2322|         0|         0|         0|      1234|0x     4d2|     02322|0         |0         |0         |1234      |0x4d2     |02322     |         0|         0|         0|      1234|     0x4d2|     02322|\n"), de_out.str());
    ASSERT_EQ(std::string("|0|0|0|1234|4d2|2322|         0|         0|         0|      1234|0x     4d2|     02322|0         |0         |0         |1234      |0x4d2     |02322     |         0|         0|         0|      1234|     0x4d2|     02322|\n"), en_out.str());
    ASSERT_EQ(std::string("|0|0|0|1234|4d2|2322|         0|         0|         0|      1234|0x     4d2|     02322|0         |0         |0         |1234      |0x4d2     |02322     |         0|         0|         0|      1234|     0x4d2|     02322|\n"), ru_out.str());
  }

  // double
  {
    std::ostringstream de_out;
    std::ostringstream en_out;
    std::ostringstream ru_out;
    std::vector<std::ostringstream*> v = { &de_out, &en_out, &ru_out };

    de_out.imbue(de);
    en_out.imbue(en);
    ru_out.imbue(ru);

    for (auto out: v) {
      *out << "|" << std::defaultfloat << std::dec << (double)(-1003.1415)
           << "|" << std::defaultfloat << std::hex << (double)(-1003.1415)
           << "|" << std::defaultfloat << std::oct << (double)(-1003.1415)
           << "|" << std::defaultfloat << std::dec << (double)(0.)
           << "|" << std::defaultfloat << std::hex << (double)(0.)
           << "|" << std::defaultfloat << std::oct << (double)(0.)
           << "|" << std::defaultfloat << std::dec << (double)(1002.71828)
           << "|" << std::defaultfloat << std::hex << (double)(1002.71828)
           << "|" << std::defaultfloat << std::oct << (double)(1002.71828)
           << "|" << std::fixed << std::dec << (double)(-1003.1415)
           << "|" << std::fixed << std::hex << (double)(-1003.1415)
           << "|" << std::fixed << std::oct << (double)(-1003.1415)
           << "|" << std::fixed << std::dec << (double)(0.)
           << "|" << std::fixed << std::hex << (double)(0.)
           << "|" << std::fixed << std::oct << (double)(0.)
           << "|" << std::fixed << std::dec << (double)(1002.71828)
           << "|" << std::fixed << std::hex << (double)(1002.71828)
           << "|" << std::fixed << std::oct << (double)(1002.71828)
           << "|" << std::hexfloat << std::dec << (double)(-1003.1415)
           << "|" << std::hexfloat << std::hex << (double)(-1003.1415)
           << "|" << std::hexfloat << std::oct << (double)(-1003.1415)
           << "|" << std::hexfloat << std::dec << (double)(0.)
           << "|" << std::hexfloat << std::hex << (double)(0.)
           << "|" << std::hexfloat << std::oct << (double)(0.)
           << "|" << std::hexfloat << std::dec << (double)(1002.71828)
           << "|" << std::hexfloat << std::hex << (double)(1002.71828)
           << "|" << std::hexfloat << std::oct << (double)(1002.71828)
           << "|" << std::scientific << std::dec << (double)(-1003.1415)
           << "|" << std::scientific << std::hex << (double)(-1003.1415)
           << "|" << std::scientific << std::oct << (double)(-1003.1415)
           << "|" << std::scientific << std::dec << (double)(0.)
           << "|" << std::scientific << std::hex << (double)(0.)
           << "|" << std::scientific << std::oct << (double)(0.)
           << "|" << std::scientific << std::dec << (double)(1002.71828)
           << "|" << std::scientific << std::hex << (double)(1002.71828)
           << "|" << std::scientific << std::oct << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::dec << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::hex << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::oct << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::dec << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::hex << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::oct << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::dec << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::hex << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::oct << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::dec << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::hex << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::oct << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::dec << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::hex << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::oct << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::dec << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::hex << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::oct << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::dec << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::hex << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::oct << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::dec << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::hex << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::oct << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::dec << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::hex << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::oct << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::dec << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::hex << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::oct << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::dec << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::hex << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::oct << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::dec << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::hex << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::oct << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::dec << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::hex << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::oct << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::dec << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::hex << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::oct << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::dec << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::hex << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::oct << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::dec << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::hex << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::oct << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::dec << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::hex << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::oct << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::dec << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::hex << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::oct << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::dec << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::hex << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::oct << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::dec << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::hex << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::oct << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::dec << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::hex << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::oct << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::dec << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::hex << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::oct << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::dec << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::hex << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::oct << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::dec << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::hex << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::oct << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::dec << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::hex << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::oct << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::dec << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::hex << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::oct << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::dec << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::hex << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::oct << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::dec << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::hex << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::oct << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::dec << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::hex << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::oct << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::dec << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::hex << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::oct << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::dec << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::hex << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::oct << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::dec << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::hex << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::oct << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::dec << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::hex << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::oct << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::dec << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::hex << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::oct << std::setw(10) << (double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::dec << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::hex << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::oct << std::setw(10) << (double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::dec << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::hex << std::setw(10) << (double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::oct << std::setw(10) << (double)(1002.71828)
           << "|" << std::endl;
    }

    ASSERT_EQ(std::string("|-1003.14|-1003.14|-1003.14|0|0|0|1002.72|1002.72|1002.72|-1003.141500|-1003.141500|-1003.141500|0.000000|0.000000|0.000000|1002.718280|1002.718280|1002.718280|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|0x0p+0|0x0p+0|0x0p+0|0x1.f55bf0995aaf8p+9|0x1.f55bf0995aaf8p+9|0x1.f55bf0995aaf8p+9|-1.003141e+03|-1.003141e+03|-1.003141e+03|0.000000e+00|0.000000e+00|0.000000e+00|1.002718e+03|1.002718e+03|1.002718e+03|-  1003.14|-  1003.14|-  1003.14|+        0|+        0|+        0|+  1002.72|+  1002.72|+  1002.72|-1003.14  |-1003.14  |-1003.14  |+0        |+0        |+0        |+1002.72  |+1002.72  |+1002.72  |  -1003.14|  -1003.14|  -1003.14|        +0|        +0|        +0|  +1002.72|  +1002.72|  +1002.72|-1003.141500|-1003.141500|-1003.141500|+ 0.000000|+ 0.000000|+ 0.000000|+1002.718280|+1002.718280|+1002.718280|-1003.141500|-1003.141500|-1003.141500|+0.000000 |+0.000000 |+0.000000 |+1002.718280|+1002.718280|+1002.718280|-1003.141500|-1003.141500|-1003.141500| +0.000000| +0.000000| +0.000000|+1002.718280|+1002.718280|+1002.718280|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|+   0x0p+0|+   0x0p+0|+   0x0p+0|+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|+0x0p+0   |+0x0p+0   |+0x0p+0   |+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|   +0x0p+0|   +0x0p+0|   +0x0p+0|+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|\n"), de_out.str());
    ASSERT_EQ(std::string("|-1003.14|-1003.14|-1003.14|0|0|0|1002.72|1002.72|1002.72|-1003.141500|-1003.141500|-1003.141500|0.000000|0.000000|0.000000|1002.718280|1002.718280|1002.718280|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|0x0p+0|0x0p+0|0x0p+0|0x1.f55bf0995aaf8p+9|0x1.f55bf0995aaf8p+9|0x1.f55bf0995aaf8p+9|-1.003141e+03|-1.003141e+03|-1.003141e+03|0.000000e+00|0.000000e+00|0.000000e+00|1.002718e+03|1.002718e+03|1.002718e+03|-  1003.14|-  1003.14|-  1003.14|+        0|+        0|+        0|+  1002.72|+  1002.72|+  1002.72|-1003.14  |-1003.14  |-1003.14  |+0        |+0        |+0        |+1002.72  |+1002.72  |+1002.72  |  -1003.14|  -1003.14|  -1003.14|        +0|        +0|        +0|  +1002.72|  +1002.72|  +1002.72|-1003.141500|-1003.141500|-1003.141500|+ 0.000000|+ 0.000000|+ 0.000000|+1002.718280|+1002.718280|+1002.718280|-1003.141500|-1003.141500|-1003.141500|+0.000000 |+0.000000 |+0.000000 |+1002.718280|+1002.718280|+1002.718280|-1003.141500|-1003.141500|-1003.141500| +0.000000| +0.000000| +0.000000|+1002.718280|+1002.718280|+1002.718280|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|+   0x0p+0|+   0x0p+0|+   0x0p+0|+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|+0x0p+0   |+0x0p+0   |+0x0p+0   |+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|   +0x0p+0|   +0x0p+0|   +0x0p+0|+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|\n"), en_out.str());
    ASSERT_EQ(std::string("|-1003.14|-1003.14|-1003.14|0|0|0|1002.72|1002.72|1002.72|-1003.141500|-1003.141500|-1003.141500|0.000000|0.000000|0.000000|1002.718280|1002.718280|1002.718280|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|0x0p+0|0x0p+0|0x0p+0|0x1.f55bf0995aaf8p+9|0x1.f55bf0995aaf8p+9|0x1.f55bf0995aaf8p+9|-1.003141e+03|-1.003141e+03|-1.003141e+03|0.000000e+00|0.000000e+00|0.000000e+00|1.002718e+03|1.002718e+03|1.002718e+03|-  1003.14|-  1003.14|-  1003.14|+        0|+        0|+        0|+  1002.72|+  1002.72|+  1002.72|-1003.14  |-1003.14  |-1003.14  |+0        |+0        |+0        |+1002.72  |+1002.72  |+1002.72  |  -1003.14|  -1003.14|  -1003.14|        +0|        +0|        +0|  +1002.72|  +1002.72|  +1002.72|-1003.141500|-1003.141500|-1003.141500|+ 0.000000|+ 0.000000|+ 0.000000|+1002.718280|+1002.718280|+1002.718280|-1003.141500|-1003.141500|-1003.141500|+0.000000 |+0.000000 |+0.000000 |+1002.718280|+1002.718280|+1002.718280|-1003.141500|-1003.141500|-1003.141500| +0.000000| +0.000000| +0.000000|+1002.718280|+1002.718280|+1002.718280|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|+   0x0p+0|+   0x0p+0|+   0x0p+0|+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|+0x0p+0   |+0x0p+0   |+0x0p+0   |+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|-0x1.f5921cac08312p+9|   +0x0p+0|   +0x0p+0|   +0x0p+0|+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|+0x1.f55bf0995aaf8p+9|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|\n"), ru_out.str());
  }

  // long double
  {
    std::ostringstream de_out;
    std::ostringstream en_out;
    std::ostringstream ru_out;
    std::vector<std::ostringstream*> v = { &de_out, &en_out, &ru_out };

    de_out.imbue(de);
    en_out.imbue(en);
    ru_out.imbue(ru);

    for (auto out: v) {
      *out << "|" << std::defaultfloat << std::dec << (long double)(-1003.1415)
           << "|" << std::defaultfloat << std::hex << (long double)(-1003.1415)
           << "|" << std::defaultfloat << std::oct << (long double)(-1003.1415)
           << "|" << std::defaultfloat << std::dec << (long double)(0.)
           << "|" << std::defaultfloat << std::hex << (long double)(0.)
           << "|" << std::defaultfloat << std::oct << (long double)(0.)
           << "|" << std::defaultfloat << std::dec << (long double)(1002.71828)
           << "|" << std::defaultfloat << std::hex << (long double)(1002.71828)
           << "|" << std::defaultfloat << std::oct << (long double)(1002.71828)
           << "|" << std::fixed << std::dec << (long double)(-1003.1415)
           << "|" << std::fixed << std::hex << (long double)(-1003.1415)
           << "|" << std::fixed << std::oct << (long double)(-1003.1415)
           << "|" << std::fixed << std::dec << (long double)(0.)
           << "|" << std::fixed << std::hex << (long double)(0.)
           << "|" << std::fixed << std::oct << (long double)(0.)
           << "|" << std::fixed << std::dec << (long double)(1002.71828)
           << "|" << std::fixed << std::hex << (long double)(1002.71828)
           << "|" << std::fixed << std::oct << (long double)(1002.71828)
           << "|" << std::hexfloat << std::dec << (long double)(-1003.1415)
           << "|" << std::hexfloat << std::hex << (long double)(-1003.1415)
           << "|" << std::hexfloat << std::oct << (long double)(-1003.1415)
           << "|" << std::hexfloat << std::dec << (long double)(0.)
           << "|" << std::hexfloat << std::hex << (long double)(0.)
           << "|" << std::hexfloat << std::oct << (long double)(0.)
           << "|" << std::hexfloat << std::dec << (long double)(1002.71828)
           << "|" << std::hexfloat << std::hex << (long double)(1002.71828)
           << "|" << std::hexfloat << std::oct << (long double)(1002.71828)
           << "|" << std::scientific << std::dec << (long double)(-1003.1415)
           << "|" << std::scientific << std::hex << (long double)(-1003.1415)
           << "|" << std::scientific << std::oct << (long double)(-1003.1415)
           << "|" << std::scientific << std::dec << (long double)(0.)
           << "|" << std::scientific << std::hex << (long double)(0.)
           << "|" << std::scientific << std::oct << (long double)(0.)
           << "|" << std::scientific << std::dec << (long double)(1002.71828)
           << "|" << std::scientific << std::hex << (long double)(1002.71828)
           << "|" << std::scientific << std::oct << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::dec << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::hex << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::oct << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::dec << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::hex << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::oct << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::dec << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::hex << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::defaultfloat << std::oct << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::dec << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::hex << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::oct << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::dec << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::hex << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::oct << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::dec << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::hex << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::defaultfloat << std::oct << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::dec << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::hex << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::oct << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::dec << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::hex << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::oct << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::dec << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::hex << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::defaultfloat << std::oct << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::dec << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::hex << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::oct << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::dec << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::hex << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::oct << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::dec << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::hex << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::fixed << std::oct << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::dec << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::hex << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::oct << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::dec << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::hex << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::oct << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::dec << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::hex << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::fixed << std::oct << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::dec << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::hex << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::oct << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::dec << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::hex << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::oct << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::dec << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::hex << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::fixed << std::oct << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::dec << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::hex << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::oct << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::dec << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::hex << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::oct << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::dec << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::hex << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::hexfloat << std::oct << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::dec << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::hex << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::oct << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::dec << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::hex << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::oct << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::dec << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::hex << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::hexfloat << std::oct << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::dec << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::hex << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::oct << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::dec << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::hex << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::oct << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::dec << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::hex << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::hexfloat << std::oct << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::dec << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::hex << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::oct << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::dec << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::hex << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::oct << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::dec << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::hex << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::internal << std::scientific << std::oct << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::dec << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::hex << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::oct << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::dec << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::hex << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::oct << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::dec << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::hex << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::left << std::scientific << std::oct << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::dec << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::hex << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::oct << std::setw(10) << (long double)(-1003.1415)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::dec << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::hex << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::oct << std::setw(10) << (long double)(0.)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::dec << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::hex << std::setw(10) << (long double)(1002.71828)
           << "|" << std::showbase << std::showpos << std::right << std::scientific << std::oct << std::setw(10) << (long double)(1002.71828)
           << "|" << std::endl;
    }

    ASSERT_EQ(std::string("|-1003.14|-1003.14|-1003.14|0|0|0|1002.72|1002.72|1002.72|-1003.141500|-1003.141500|-1003.141500|0.000000|0.000000|0.000000|1002.718280|1002.718280|1002.718280|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|0x0p+0|0x0p+0|0x0p+0|0xf.aadf84cad57cp+6|0xf.aadf84cad57cp+6|0xf.aadf84cad57cp+6|-1.003141e+03|-1.003141e+03|-1.003141e+03|0.000000e+00|0.000000e+00|0.000000e+00|1.002718e+03|1.002718e+03|1.002718e+03|-  1003.14|-  1003.14|-  1003.14|+        0|+        0|+        0|+  1002.72|+  1002.72|+  1002.72|-1003.14  |-1003.14  |-1003.14  |+0        |+0        |+0        |+1002.72  |+1002.72  |+1002.72  |  -1003.14|  -1003.14|  -1003.14|        +0|        +0|        +0|  +1002.72|  +1002.72|  +1002.72|-1003.141500|-1003.141500|-1003.141500|+ 0.000000|+ 0.000000|+ 0.000000|+1002.718280|+1002.718280|+1002.718280|-1003.141500|-1003.141500|-1003.141500|+0.000000 |+0.000000 |+0.000000 |+1002.718280|+1002.718280|+1002.718280|-1003.141500|-1003.141500|-1003.141500| +0.000000| +0.000000| +0.000000|+1002.718280|+1002.718280|+1002.718280|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|+   0x0p+0|+   0x0p+0|+   0x0p+0|+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|+0x0p+0   |+0x0p+0   |+0x0p+0   |+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|   +0x0p+0|   +0x0p+0|   +0x0p+0|+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|\n"), de_out.str());
    ASSERT_EQ(std::string("|-1003.14|-1003.14|-1003.14|0|0|0|1002.72|1002.72|1002.72|-1003.141500|-1003.141500|-1003.141500|0.000000|0.000000|0.000000|1002.718280|1002.718280|1002.718280|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|0x0p+0|0x0p+0|0x0p+0|0xf.aadf84cad57cp+6|0xf.aadf84cad57cp+6|0xf.aadf84cad57cp+6|-1.003141e+03|-1.003141e+03|-1.003141e+03|0.000000e+00|0.000000e+00|0.000000e+00|1.002718e+03|1.002718e+03|1.002718e+03|-  1003.14|-  1003.14|-  1003.14|+        0|+        0|+        0|+  1002.72|+  1002.72|+  1002.72|-1003.14  |-1003.14  |-1003.14  |+0        |+0        |+0        |+1002.72  |+1002.72  |+1002.72  |  -1003.14|  -1003.14|  -1003.14|        +0|        +0|        +0|  +1002.72|  +1002.72|  +1002.72|-1003.141500|-1003.141500|-1003.141500|+ 0.000000|+ 0.000000|+ 0.000000|+1002.718280|+1002.718280|+1002.718280|-1003.141500|-1003.141500|-1003.141500|+0.000000 |+0.000000 |+0.000000 |+1002.718280|+1002.718280|+1002.718280|-1003.141500|-1003.141500|-1003.141500| +0.000000| +0.000000| +0.000000|+1002.718280|+1002.718280|+1002.718280|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|+   0x0p+0|+   0x0p+0|+   0x0p+0|+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|+0x0p+0   |+0x0p+0   |+0x0p+0   |+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|   +0x0p+0|   +0x0p+0|   +0x0p+0|+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|\n"), en_out.str());
    ASSERT_EQ(std::string("|-1003.14|-1003.14|-1003.14|0|0|0|1002.72|1002.72|1002.72|-1003.141500|-1003.141500|-1003.141500|0.000000|0.000000|0.000000|1002.718280|1002.718280|1002.718280|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|0x0p+0|0x0p+0|0x0p+0|0xf.aadf84cad57cp+6|0xf.aadf84cad57cp+6|0xf.aadf84cad57cp+6|-1.003141e+03|-1.003141e+03|-1.003141e+03|0.000000e+00|0.000000e+00|0.000000e+00|1.002718e+03|1.002718e+03|1.002718e+03|-  1003.14|-  1003.14|-  1003.14|+        0|+        0|+        0|+  1002.72|+  1002.72|+  1002.72|-1003.14  |-1003.14  |-1003.14  |+0        |+0        |+0        |+1002.72  |+1002.72  |+1002.72  |  -1003.14|  -1003.14|  -1003.14|        +0|        +0|        +0|  +1002.72|  +1002.72|  +1002.72|-1003.141500|-1003.141500|-1003.141500|+ 0.000000|+ 0.000000|+ 0.000000|+1002.718280|+1002.718280|+1002.718280|-1003.141500|-1003.141500|-1003.141500|+0.000000 |+0.000000 |+0.000000 |+1002.718280|+1002.718280|+1002.718280|-1003.141500|-1003.141500|-1003.141500| +0.000000| +0.000000| +0.000000|+1002.718280|+1002.718280|+1002.718280|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|+   0x0p+0|+   0x0p+0|+   0x0p+0|+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|+0x0p+0   |+0x0p+0   |+0x0p+0   |+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|-0xf.ac90e5604189p+6|   +0x0p+0|   +0x0p+0|   +0x0p+0|+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|+0xf.aadf84cad57cp+6|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|-1.003141e+03|-1.003141e+03|-1.003141e+03|+0.000000e+00|+0.000000e+00|+0.000000e+00|+1.002718e+03|+1.002718e+03|+1.002718e+03|\n"), ru_out.str());
  }

  // const void*
  {
    std::ostringstream de_out;
    std::ostringstream en_out;
    std::ostringstream ru_out;
    std::vector<std::ostringstream*> v = { &de_out, &en_out, &ru_out };

    de_out.imbue(de);
    en_out.imbue(en);
    ru_out.imbue(ru);

    for (auto out: v) {
      *out << "|" << std::dec << (const void*)(0)
           << "|" << std::hex << (const void*)(0)
           << "|" << std::oct << (const void*)(0)
           << "|" << std::dec << (const void*)(1234)
           << "|" << std::hex << (const void*)(1234)
           << "|" << std::oct << (const void*)(1234)
           << "|" << std::showbase << std::showpos << std::internal << std::dec << std::setw(10) << (const void*)(0)
           << "|" << std::showbase << std::showpos << std::internal << std::hex << std::setw(10) << (const void*)(0)
           << "|" << std::showbase << std::showpos << std::internal << std::oct << std::setw(10) << (const void*)(0)
           << "|" << std::showbase << std::showpos << std::internal << std::dec << std::setw(10) << (const void*)(1234)
           << "|" << std::showbase << std::showpos << std::internal << std::hex << std::setw(10) << (const void*)(1234)
           << "|" << std::showbase << std::showpos << std::internal << std::oct << std::setw(10) << (const void*)(1234)
           << "|" << std::showbase << std::showpos << std::left << std::dec << std::setw(10) << (const void*)(0)
           << "|" << std::showbase << std::showpos << std::left << std::hex << std::setw(10) << (const void*)(0)
           << "|" << std::showbase << std::showpos << std::left << std::oct << std::setw(10) << (const void*)(0)
           << "|" << std::showbase << std::showpos << std::left << std::dec << std::setw(10) << (const void*)(1234)
           << "|" << std::showbase << std::showpos << std::left << std::hex << std::setw(10) << (const void*)(1234)
           << "|" << std::showbase << std::showpos << std::left << std::oct << std::setw(10) << (const void*)(1234)
           << "|" << std::showbase << std::showpos << std::right << std::dec << std::setw(10) << (const void*)(0)
           << "|" << std::showbase << std::showpos << std::right << std::hex << std::setw(10) << (const void*)(0)
           << "|" << std::showbase << std::showpos << std::right << std::oct << std::setw(10) << (const void*)(0)
           << "|" << std::showbase << std::showpos << std::right << std::dec << std::setw(10) << (const void*)(1234)
           << "|" << std::showbase << std::showpos << std::right << std::hex << std::setw(10) << (const void*)(1234)
           << "|" << std::showbase << std::showpos << std::right << std::oct << std::setw(10) << (const void*)(1234)
           << "|" << std::endl;
    }

    ASSERT_EQ(std::string("|0|0|0|0x4d2|0x4d2|0x4d2|         0|         0|         0|0x     4d2|0x     4d2|0x     4d2|0         |0         |0         |0x4d2     |0x4d2     |0x4d2     |         0|         0|         0|     0x4d2|     0x4d2|     0x4d2|\n"), de_out.str());
    ASSERT_EQ(std::string("|0|0|0|0x4d2|0x4d2|0x4d2|         0|         0|         0|0x     4d2|0x     4d2|0x     4d2|0         |0         |0         |0x4d2     |0x4d2     |0x4d2     |         0|         0|         0|     0x4d2|     0x4d2|     0x4d2|\n"), en_out.str());
    ASSERT_EQ(std::string("|0|0|0|0x4d2|0x4d2|0x4d2|         0|         0|         0|0x     4d2|0x     4d2|0x     4d2|0         |0         |0         |0x4d2     |0x4d2     |0x4d2     |         0|         0|         0|     0x4d2|     0x4d2|     0x4d2|\n"), ru_out.str());
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------