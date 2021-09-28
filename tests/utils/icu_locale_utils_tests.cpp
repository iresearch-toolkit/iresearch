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
#include "utils/icu_locale_utils.hpp"
#include "utils/locale_utils.hpp"
#include "utils/misc.hpp"
#include "velocypack/Parser.h"
#include "velocypack/velocypack-aliases.h"

using namespace iresearch::icu_locale_utils;

// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------


TEST(icu_locale_utils_test_suite, test_get_locale_from_vpack) {

  {
    std::string config = R"({"language" : "de", "country" : "DE"})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

    icu::Locale expected_locale("de", "DE");
    ASSERT_EQ(actual_locale, expected_locale);
  }

  {
    std::string config = R"({"language" : "EN", "country" : "US", "variant" : "_phonebook"})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

    icu::Locale expected_locale("EN", "US", "phonebook");
    ASSERT_EQ(actual_locale, expected_locale);
  }

  {
    std::string config = R"({"language" : "EN", "country" : "EN", "variant" : "_pinyan"})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

    icu::Locale expected_locale("de", "DE", "phonebook");
    ASSERT_NE(actual_locale, expected_locale);
  }

  {
    std::string config = R"({"language" : "EN", "country" : "EN", "variant" : "_pinyan", "encoding" : "utf-16"})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_FALSE(get_locale_from_vpack(vpack->slice(), actual_locale));
  }

  {
    std::string config = R"({"language" : "EN", "country" : "US", "variant" : "_pinyan", "encoding" : "utf-8"})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

    icu::Locale expected_locale("en", "US", "pinyan");
    ASSERT_EQ(actual_locale, expected_locale);
  }

  {
    std::string config = R"({"language" : "EN", "variant" : "_pinyan"})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

    icu::Locale expected_locale("en", NULL, "pinyan");
    ASSERT_EQ(actual_locale, expected_locale);
  }

  {
    std::string config = R"({"language" : "DE@collation=phonebook"})";
    auto vpack = VPackParser::fromJson(config.c_str(), config.size());
    icu::Locale actual_locale;
    ASSERT_TRUE(get_locale_from_vpack(vpack->slice(), actual_locale));

    icu::Locale expected_locale("DE", NULL, NULL, "collation=phonebook");
    ASSERT_EQ(actual_locale, expected_locale);
  }
}

TEST(icu_locale_utils_test_suite, test_get_locale_from_str) {

  // new format. Keywords are considered
  {
    irs::string_ref locale_name = "de@collation=phonebook";
    icu::Locale locale;
    Unicode unicode;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode));
    ASSERT_TRUE(unicode == Unicode::UTF8);
    irs::string_ref actual = locale.getName();
    ASSERT_EQ(locale_name, actual);
  }


  // new format. Keywords are considered. utf-8 encoding
  {
    irs::string_ref locale_name = "de_DE.utf-8@collation=phonebook";
    icu::Locale locale;
    Unicode unicode;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode));
    ASSERT_TRUE(unicode == Unicode::UTF8);
    irs::string_ref actual = locale.getName();
    ASSERT_EQ(locale_name, actual);
  }

  // new format. Keywords are considered. non utf-8 encoding
  {
    irs::string_ref locale_name = "de_DE.utf-16@collation=phonebook";
    icu::Locale locale;
    Unicode unicode;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode));
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
    irs::string_ref actual = locale.getName();
    ASSERT_EQ(locale_name, actual);
  }

  // old format. Keywords are ignored. utf-8 encoding
  {
    irs::string_ref locale_name = "de.UTF-8@collation=phonebook";
    icu::Locale locale;
    Unicode unicode;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, false, &unicode));
    ASSERT_TRUE(unicode == Unicode::UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de.UTF-8";
    ASSERT_EQ(expected, actual);
  }

  // old format. Keywords are ignored
  {
    irs::string_ref locale_name = "de-DE@collation=phonebook";
    icu::Locale locale;
    Unicode unicode;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, false, &unicode));
    ASSERT_TRUE(unicode == Unicode::UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de_DE";
    ASSERT_EQ(expected, actual);
  }

  // old format. Keywords are ignored
  {
    irs::string_ref locale_name = "de-DE_phonebook";
    icu::Locale locale;
    Unicode unicode;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, false, &unicode));
    ASSERT_TRUE(unicode == Unicode::UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de_DE_PHONEBOOK";
    ASSERT_EQ(expected, actual);
  }

  // old format. Keywords are ignored
  {
    irs::string_ref locale_name = "de-DE_phonebook@wrong=keywords";
    icu::Locale locale;
    Unicode unicode;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, false, &unicode));
    ASSERT_TRUE(unicode == Unicode::UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de_DE_PHONEBOOK";
    ASSERT_EQ(expected, actual);
  }

  // old format. Keywords are ignored
  {
    irs::string_ref locale_name = "";
    icu::Locale locale;
    Unicode unicode;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, false, &unicode));
    ASSERT_TRUE(unicode == Unicode::UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "";
    ASSERT_EQ(expected, actual);
  }

  // old format. Keywords are ignored. Non utf8 encoding
  {
    irs::string_ref locale_name = "de-DE_phonebook.UTF-16@wrong=keywords";
    icu::Locale locale;
    Unicode unicode;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, false, &unicode));
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de_DE_PHONEBOOK.UTF-16";
    ASSERT_EQ(expected, actual);
  }

  // old format. Keywords are ignored. Non utf8 encoding
  {
    irs::string_ref locale_name = "de-DE_phonebook.utf-32@wrong=keywords";
    icu::Locale locale;
    Unicode unicode;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, false, &unicode));
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de_DE_PHONEBOOK.utf-32";
    ASSERT_EQ(expected, actual);
  }

  // old format. Keywords are ignored. Non utf8 encoding
  {
    irs::string_ref locale_name = "de-DE_phonebook.utf-32";
    icu::Locale locale;
    Unicode unicode;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, false, &unicode));
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de_DE_PHONEBOOK.utf-32";
    ASSERT_EQ(expected, actual);
  }

  // try to break it
  {
    irs::string_ref locale_name = "ru_RU@let.s.break.it";
    icu::Locale locale;
    Unicode unicode;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode));
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "ru_RU@let.s.break.it";
    ASSERT_EQ(expected, actual);
  }

  // try to break it
  {
    irs::string_ref locale_name = "ru_RU@let.s.break.it";
    icu::Locale locale;
    Unicode unicode;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, false, &unicode));
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "ru_RU";
    ASSERT_EQ(expected, actual);
  }

  // try to break it
  {
    irs::string_ref locale_name = "@ru_De.utf-8";
    icu::Locale locale;
    Unicode unicode;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode));
    ASSERT_TRUE(unicode == Unicode::UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "@ru_De.utf-8";
    ASSERT_EQ(expected, actual);
  }

  // try to break it
  {
    irs::string_ref locale_name = "@ru_De.utf-8";
    icu::Locale locale;
    Unicode unicode;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, false, &unicode));
    ASSERT_TRUE(unicode == Unicode::UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "";
    ASSERT_EQ(expected, actual);
  }

  // get also encoding name
  {
    irs::string_ref locale_name = "ru_De.utf-8";
    icu::Locale locale;
    Unicode unicode;
    std::string encoding_name;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode, &encoding_name));
    ASSERT_TRUE(unicode == Unicode::UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "ru_DE.utf-8";
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(encoding_name, "utf-8");
  }

  // get also encoding name
  {
    irs::string_ref locale_name = "de-DE_phonebook.utf-32";
    icu::Locale locale;
    Unicode unicode;
    std::string encoding_name;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode, &encoding_name));
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de_DE_PHONEBOOK.utf-32";
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(encoding_name, "utf-32");
  }

  // get also encoding name
  {
    irs::string_ref locale_name = "de-DE.utf-32@collation=phonebook";
    icu::Locale locale;
    Unicode unicode;
    std::string encoding_name;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode, &encoding_name));
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de_DE.utf-32@collation=phonebook";
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(encoding_name, "utf-32");
  }

  // get also encoding name
  {
    irs::string_ref locale_name = "de.UTF-8@phonebook";
    icu::Locale locale;
    Unicode unicode;
    std::string encoding_name;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode, &encoding_name));
    ASSERT_TRUE(unicode == Unicode::UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de.UTF-8@phonebook";
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(encoding_name, "utf-8");
  }

  // get also encoding name
  {
    irs::string_ref locale_name = "de-DE.AsCiI@collation=phonebook";
    icu::Locale locale;
    Unicode unicode;
    std::string encoding_name;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode, &encoding_name));
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de_DE.AsCiI@collation=phonebook";
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(encoding_name, "ascii");
  }


  // get also encoding name
  {
    irs::string_ref locale_name = "de-DE_phonebook.ucs-2_utf8_ascii";
    icu::Locale locale;
    Unicode unicode;
    std::string encoding_name;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, false, &unicode, &encoding_name));
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de";
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(encoding_name, "ucs-2_utf8_ascii");
  }

  // Try to break it. get also encoding name
  {
    irs::string_ref locale_name = "de@DE._phon@ebook.ucs-2_utf8_.@scii..";
    icu::Locale locale;
    Unicode unicode;
    std::string encoding_name;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, false, &unicode, &encoding_name));
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de";
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(encoding_name, "_phon@ebook.ucs-2_utf8_.@scii..");
  }

  // Try to break it. get also encoding name
  {
    irs::string_ref locale_name = "de@DE._phon@ebook.ucs-2_utf8_.@scii..";
    icu::Locale locale;
    Unicode unicode;
    std::string encoding_name;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode, &encoding_name));
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de@DE._phon@ebook.ucs-2_utf8_.@scii..";
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(encoding_name, "_phon@ebook.ucs-2_utf8_.@scii..");
  }

  // get also encoding name. Default 'utf-8' value
  {
    irs::string_ref locale_name = "de";
    icu::Locale locale;
    Unicode unicode;
    std::string encoding_name;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode, &encoding_name));
    ASSERT_TRUE(unicode == Unicode::UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de";
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(encoding_name, "utf-8");
  }

  // get also encoding name. Default 'utf-8' value
  {
    irs::string_ref locale_name = "de@collation=phonebook";
    icu::Locale locale;
    Unicode unicode;
    std::string encoding_name;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode, &encoding_name));
    ASSERT_TRUE(unicode == Unicode::UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de@collation=phonebook";
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(encoding_name, "utf-8");
  }

  // get also encoding name.
  {
    irs::string_ref locale_name = "de.utf-8";
    icu::Locale locale;
    Unicode unicode;
    std::string encoding_name;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode, &encoding_name));
    ASSERT_TRUE(unicode == Unicode::UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de.utf-8";
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(encoding_name, "utf-8");
  }

}

TEST(icu_locale_utils_test_suite, test_locale_to_vpack) {

  {
    icu::Locale icu_locale("UK");
    VPackBuilder builder;
    Unicode unicode = Unicode::UTF8;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder, &unicode));

    auto expected_config = VPackParser::fromJson(R"({"language":"uk","encoding":"utf-8"})")->slice();
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config.toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE");
    VPackBuilder builder;
    Unicode unicode = Unicode::UTF8;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder, &unicode));

    auto expected_config = VPackParser::fromJson(R"({"language":"de","country":"DE","encoding":"utf-8"})")->slice();
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config.toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE", "phonebook");
    VPackBuilder builder;
    Unicode unicode = Unicode::UTF8;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder, &unicode));

    auto expected_config = VPackParser::fromJson(R"({"language":"de","country":"DE","variant":"PHONEBOOK","encoding":"utf-8"})")->slice();
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config.toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE", NULL, "phonebook");
    VPackBuilder builder;
    Unicode unicode = Unicode::UTF8;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder, &unicode));

    auto expected_config = VPackParser::fromJson(R"({"language":"de","country":"DE","variant":"PHONEBOOK","encoding":"utf-8"})")->slice();
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config.toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE", NULL, "collation=phonebook");
    VPackBuilder builder;
    Unicode unicode = Unicode::UTF8;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder, &unicode));

    auto expected_config = VPackParser::fromJson(R"({"language":"de","country":"DE","encoding":"utf-8"})")->slice();
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config.toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE", "pinyan", "collation=phonebook");
    VPackBuilder builder;
    Unicode unicode = Unicode::UTF8;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder, &unicode));

    auto expected_config = VPackParser::fromJson(R"({"language":"de","country":"DE","variant":"PINYAN","encoding":"utf-8"})")->slice();
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config.toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE", "pinyan", "phonebook");
    VPackBuilder builder;
    Unicode unicode = Unicode::UTF8;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder, &unicode));

    auto expected_config = VPackParser::fromJson(R"({"language":"de","country":"DE","variant":"PINYAN_PHONEBOOK","encoding":"utf-8"})")->slice();
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config.toString(), actual_config.toString());
  }
}

TEST(icu_locale_utils_test_suite, test_convert_to_utf16) {

  // from ascii to utf16
  {
    std::string data_ascii("\x54\x68\x65\x20\x6F\x6C\x64\x20\x6C\x61\x64\x79\x20"); // "the old lady "
    std::u16string data_utf16;

    ASSERT_TRUE(convert_to_utf16("ascii", data_ascii, data_utf16));
    ASSERT_EQ(data_utf16, std::u16string(u"The old lady "));

  }

  // from koi8-r to utf16
  {
    std::string data_koi8r("\xC5\xD6\xC9\xCB");
    std::u16string data_utf16;

    ASSERT_TRUE(convert_to_utf16("koi8-r", data_koi8r, data_utf16));
    ASSERT_EQ(data_utf16, std::u16string(u"ежик"));
  }

  // from utf8 to utf16
  {
    std::string data_utf8("the old lady");
    std::u16string data_utf16;

    ASSERT_TRUE(convert_to_utf16("utf8", data_utf8, data_utf16));
    ASSERT_EQ(data_utf16, std::u16string(u"the old lady"));
  }

  // from utf32 to utf16
  {
    std::u32string data_utf32(U"\U00000074\U00000068\U00000065\U00000020\U0000006f\U0000006c\U00000064\U00000020\U0000006c\U00000061\U00000064\U00000079");
    std::u16string data_utf16;

    ASSERT_TRUE(convert_to_utf16("utf32", data_utf32, data_utf16));
    ASSERT_EQ(data_utf16, std::u16string(u"the old lady"));
  }
}

TEST(icu_locale_utils_test_suite, test_convert_from_utf16) {

  // from utf16 to ascii
  {
    std::u16string data_utf16 = u"The old lady ";
    std::string data_ascii;

    ASSERT_TRUE(convert_from_utf16("ascii", data_utf16, data_ascii));

    ASSERT_EQ(data_ascii, std::string("\x54\x68\x65\x20\x6F\x6C\x64\x20\x6C\x61\x64\x79\x20"));
  }

  // from utf16 to koi8-r
  {
    std::u16string data_utf16u(u"ежик");
    std::string data_koi8r;

    ASSERT_TRUE(convert_from_utf16("koi8-r", data_utf16u, data_koi8r));
    ASSERT_EQ(data_koi8r, std::string("\xC5\xD6\xC9\xCB"));
  }

  // from utf16 to utf8
  {
    std::u16string data_utf16(u"the old lady");
    std::string data_utf8;

    ASSERT_TRUE(convert_from_utf16("utf8", data_utf16, data_utf8));
    ASSERT_EQ(data_utf8, std::string(u8"the old lady"));
  }

  // from utf16 to utf32
  {
    std::u16string data_utf16(u"the old lady");
    std::u32string data_utf32;

    ASSERT_TRUE(convert_from_utf16("utf32", data_utf16, data_utf32));
    ASSERT_EQ(data_utf32, std::u32string(U"\U00000074\U00000068\U00000065\U00000020\U0000006f\U0000006c\U00000064\U00000020\U0000006c\U00000061\U00000064\U00000079"));
  }
}




























