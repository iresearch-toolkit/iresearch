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
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
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
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
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
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
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
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
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
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
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
    ASSERT_EQ(encoding_name, "utf8");
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
    ASSERT_EQ(encoding_name, "utf32");
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
    ASSERT_EQ(encoding_name, "utf32");
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
    ASSERT_EQ(encoding_name, "utf8");
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
    irs::string_ref locale_name = "C";
    icu::Locale locale;
    Unicode unicode;
    std::string encoding_name;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode, &encoding_name));
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "c";
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(encoding_name, "ascii");
  }

  // get also encoding name
  {
    irs::string_ref locale_name = "en";
    icu::Locale locale;
    Unicode unicode;
    std::string encoding_name;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode, &encoding_name));
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "en";
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
    ASSERT_EQ(encoding_name, "ucs2utf8ascii");
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
    ASSERT_EQ(encoding_name, "phonebookucs2utf8scii");
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
    ASSERT_EQ(encoding_name, "phonebookucs2utf8scii");
  }

  // get also encoding name. Default 'ascii' value
  {
    irs::string_ref locale_name = "de";
    icu::Locale locale;
    Unicode unicode;
    std::string encoding_name;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode, &encoding_name));
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de";
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(encoding_name, "ascii");
  }

  // get also encoding name. Default 'ascii' value
  {
    irs::string_ref locale_name = "de@collation=phonebook";
    icu::Locale locale;
    Unicode unicode;
    std::string encoding_name;
    ASSERT_TRUE(get_locale_from_str(locale_name, locale, true, &unicode, &encoding_name));
    ASSERT_TRUE(unicode == Unicode::NON_UTF8);
    irs::string_ref actual = locale.getName();
    irs::string_ref expected = "de@collation=phonebook";
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(encoding_name, "ascii");
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
    ASSERT_EQ(encoding_name, "utf8");
  }

}

TEST(icu_locale_utils_test_suite, test_locale_to_vpack) {

  {
    icu::Locale icu_locale("UK");
    VPackBuilder builder;
    Unicode unicode = Unicode::UTF8;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder, &unicode));

    auto expected_config = VPackParser::fromJson(R"({"language":"uk","encoding":"utf8"})");
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config->toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE");
    VPackBuilder builder;
    Unicode unicode = Unicode::UTF8;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder, &unicode));

    auto expected_config = VPackParser::fromJson(R"({"language":"de","country":"DE","encoding":"utf8"})");
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config->toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE", "phonebook");
    VPackBuilder builder;
    Unicode unicode = Unicode::UTF8;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder, &unicode));

    auto expected_config = VPackParser::fromJson(R"({"language":"de","country":"DE","variant":"PHONEBOOK","encoding":"utf8"})");
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config->toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE", NULL, "phonebook");
    VPackBuilder builder;
    Unicode unicode = Unicode::UTF8;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder, &unicode));

    auto expected_config = VPackParser::fromJson(R"({"language":"de","country":"DE","variant":"PHONEBOOK","encoding":"utf8"})");
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config->toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE", NULL, "collation=phonebook");
    VPackBuilder builder;
    Unicode unicode = Unicode::UTF8;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder, &unicode));

    auto expected_config = VPackParser::fromJson(R"({"language":"de","country":"DE","encoding":"utf8"})");
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config->toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE", "pinyan", "collation=phonebook");
    VPackBuilder builder;
    Unicode unicode = Unicode::UTF8;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder, &unicode));

    auto expected_config = VPackParser::fromJson(R"({"language":"de","country":"DE","variant":"PINYAN","encoding":"utf8"})");
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config->toString(), actual_config.toString());
  }

  {
    icu::Locale icu_locale("de", "DE", "pinyan", "phonebook");
    VPackBuilder builder;
    Unicode unicode = Unicode::UTF8;
    ASSERT_TRUE(locale_to_vpack(icu_locale, &builder, &unicode));

    auto expected_config = VPackParser::fromJson(R"({"language":"de","country":"DE","variant":"PINYAN_PHONEBOOK","encoding":"utf8"})");
    auto actual_config = builder.slice();
    ASSERT_EQ(expected_config->toString(), actual_config.toString());
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
    ASSERT_EQ(data_utf16, std::u16string(u"\u0435\u0436\u0438\u043a"));
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

    ASSERT_TRUE(convert_to_utf16("utf32", irs::string_ref((char*)data_utf32.c_str(), data_utf32.size() * 4), data_utf16));
    ASSERT_EQ(data_utf16, std::u16string(u"the old lady"));
  }

  // from koi8-r to utf16. Also get converter
  {
    std::string data_koi8r("\xC5\xD6\xC9\xCB");
    std::u16string data_utf16;

    irs::locale_utils::converter_pool* cvt = nullptr;

    ASSERT_TRUE(convert_to_utf16("koi8-r", data_koi8r, data_utf16, &cvt));
    ASSERT_EQ(data_utf16, std::u16string(u"\u0435\u0436\u0438\u043a"));

    auto* actual_cvt = &irs::locale_utils::get_converter("koi8-r");
    ASSERT_EQ(cvt, actual_cvt);
  }

  // from utf8 to utf16. Also get converter
  {
    std::string data_utf8("the old lady");
    std::u16string data_utf16;

    irs::locale_utils::converter_pool* cvt = nullptr;

    ASSERT_TRUE(convert_to_utf16("utf8", data_utf8, data_utf16, &cvt));
    ASSERT_EQ(data_utf16, std::u16string(u"the old lady"));

    auto* actual_cvt = &irs::locale_utils::get_converter("utf8");
    ASSERT_EQ(cvt, actual_cvt);
  }

  // from utf32 to utf16. Also get converter
  {
    std::u32string data_utf32(U"\U00000074\U00000068\U00000065\U00000020\U0000006f\U0000006c\U00000064\U00000020\U0000006c\U00000061\U00000064\U00000079");
    std::u16string data_utf16;

    irs::locale_utils::converter_pool* cvt = nullptr;

    ASSERT_TRUE(convert_to_utf16("utf32", irs::string_ref((char*)data_utf32.c_str(), data_utf32.size() * 4), data_utf16, &cvt));
    ASSERT_EQ(data_utf16, std::u16string(u"the old lady"));

    // because we don't use converter for utf32
    ASSERT_EQ(cvt, nullptr);
  }

  // wrong original encoding
  {
    std::u16string data_utf8(u"the old lady");
    std::u16string data_utf16;

    ASSERT_TRUE(convert_to_utf16("utf8", irs::string_ref((char*)data_utf8.c_str(), data_utf8.size() * 2), data_utf16));
    ASSERT_NE(data_utf16, std::u16string(u"the old lady"));
  }


  // wrong original encoding
  {
    std::u32string data_utf8(U"the old lady");
    std::u16string data_utf16;

    ASSERT_TRUE(convert_to_utf16("utf8", irs::string_ref((char*)data_utf8.c_str(), data_utf8.size() * 2), data_utf16));
    ASSERT_NE(data_utf16, std::u16string(u"the old lady"));
  }
}

TEST(icu_locale_utils_test_suite, test_create_unicode_string) {

  // create Unicode string from utf8
//  {
//    std::string data_utf8("The old lady pulled her spectacles down and looked over them about the room");

//    icu::UnicodeString actual;
//    ASSERT_TRUE(create_unicode_string("utf8", data_utf8, actual));

//    icu::UnicodeString expected = icu::UnicodeString::fromUTF8(data_utf8);

//    ASSERT_EQ(actual, expected);
//  }

//  // create Unicode string from utf16
//  {
//    std::u16string data_utf16(u"The old lady pulled her spectacles down and looked over them about the room");

//    icu::UnicodeString actual;
//    ASSERT_TRUE(create_unicode_string("utf16", irs::string_ref((char*)data_utf16.c_str(), data_utf16.size() * 2), actual));

//    // utf16 is base for icu::UnicodeString
//    icu::UnicodeString expected(data_utf16.c_str(), data_utf16.size());

//    ASSERT_EQ(actual, expected);
//  }

//  // create Unicode string from utf32
//  {
//    std::u32string data_utf32(U"The old lady pulled her spectacles down and looked over them about the room");

//    icu::UnicodeString actual;
//    ASSERT_TRUE(create_unicode_string("utf32", irs::string_ref((char*)data_utf32.c_str(), data_utf32.size() * 4), actual));

//    icu::UnicodeString expected = icu::UnicodeString::fromUTF32((UChar32*)data_utf32.c_str(), data_utf32.size());

//    ASSERT_EQ(actual, expected);
//  }

//  // create Unicode string from ascii
//  {
//    std::string data_ascii("\x54\x68\x65\x20\x6F\x6C\x64\x20\x6C\x61\x64\x79\x20");

//    icu::UnicodeString actual;
//    ASSERT_TRUE(create_unicode_string("ascii", data_ascii, actual));

//    icu::UnicodeString expected = icu::UnicodeString(data_ascii.c_str());

//    ASSERT_EQ(actual, expected);
//  }

//  // create Unicode string from utf8. Also get converter
//  {
//    std::string data_utf8("The old lady pulled her spectacles down and looked over them about the room");

//    icu::UnicodeString actual;
//    irs::locale_utils::converter_pool* cvt = nullptr;
//    ASSERT_TRUE(create_unicode_string("utf8", data_utf8, actual, &cvt));

//    icu::UnicodeString expected = icu::UnicodeString::fromUTF8(data_utf8);
//    auto* actual_cvt = &irs::locale_utils::get_converter("utf8");
//    ASSERT_EQ(cvt, actual_cvt);
//    ASSERT_EQ(actual, expected);
//  }

//  // create Unicode string from utf16. Also get converter
//  {
//    std::u16string data_utf16(u"The old lady pulled her spectacles down and looked over them about the room");

//    icu::UnicodeString actual;
//    irs::locale_utils::converter_pool* cvt = nullptr;
//    ASSERT_TRUE(create_unicode_string("utf16", irs::string_ref((char*)data_utf16.c_str(), data_utf16.size() * 2), actual, &cvt));

//    // utf16 is base for icu::UnicodeString
//    icu::UnicodeString expected(data_utf16.c_str(), data_utf16.size());

//    // because we don't do any conversion with utf-16 encoding
//    ASSERT_EQ(cvt, nullptr);
//    ASSERT_EQ(actual, expected);
//  }

//  // create Unicode string from utf32. Also get converter
//  {
//    std::u32string data_utf32(U"The old lady pulled her spectacles down and looked over them about the room");

//    icu::UnicodeString actual;
//    irs::locale_utils::converter_pool* cvt = nullptr;
//    ASSERT_TRUE(create_unicode_string("utf32", irs::string_ref((char*)data_utf32.c_str(), data_utf32.size() * 4), actual, &cvt));

//    icu::UnicodeString expected = icu::UnicodeString::fromUTF32((UChar32*)data_utf32.c_str(), data_utf32.size());

//    // because we don't use converter for conversion with utf-32
//    ASSERT_EQ(cvt, nullptr);
//    ASSERT_EQ(actual, expected);
//  }

//  // create Unicode string from ascii. Also get converter
//  {
//    std::string data_ascii("\x54\x68\x65\x20\x6F\x6C\x64\x20\x6C\x61\x64\x79\x20");

//    icu::UnicodeString actual;
//    irs::locale_utils::converter_pool* cvt = nullptr;
//    ASSERT_TRUE(create_unicode_string("ascii", data_ascii, actual, &cvt));

//    icu::UnicodeString expected = icu::UnicodeString(data_ascii.c_str());
//    auto* actual_cvt = &irs::locale_utils::get_converter("ascii");
//    ASSERT_EQ(cvt, actual_cvt);
//    ASSERT_EQ(actual, expected);
//  }
}




























