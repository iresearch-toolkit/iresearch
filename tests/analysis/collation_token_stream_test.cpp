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
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "tests_shared.hpp"

#include <unicode/coll.h>
#include <unicode/sortkey.h>
#include <cstring.h>
#include <unicode/locid.h>
#include <charstr.h>

#include "analysis/collation_token_stream.hpp"
#include "utils/locale_utils.hpp"

TEST(collation_token_stream_test, consts) {
  static_assert("collation" == irs::type<irs::analysis::collation_token_stream>::name());
}

TEST(collation_token_stream_test, construct) {
  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({ "locale": {"language" : "en", "country": "US", "variant" : "UTF-8"}})");
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::type<irs::analysis::collation_token_stream>::id(), stream->type());
  }

  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::text>::get(),
      R"({ "locale": {"language" : "en-US"}})");
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::type<irs::analysis::collation_token_stream>::id(), stream->type());
  }

  {
    ASSERT_EQ(nullptr, irs::analysis::analyzers::get("collation", irs::type<irs::text_format::json>::get(), irs::string_ref::NIL));
    ASSERT_EQ(nullptr, irs::analysis::analyzers::get("collation", irs::type<irs::text_format::json>::get(), "1"));
    ASSERT_EQ(nullptr, irs::analysis::analyzers::get("collation", irs::type<irs::text_format::json>::get(), "[]"));
    ASSERT_EQ(nullptr, irs::analysis::analyzers::get("collation", irs::type<irs::text_format::json>::get(), "{}"));
    ASSERT_EQ(nullptr, irs::analysis::analyzers::get("collation", irs::type<irs::text_format::json>::get(), "{\"locale\":1}"));
    ASSERT_EQ(nullptr, irs::analysis::analyzers::get("collation", irs::type<irs::text_format::json>::get(),
                                                     R"({ "locale": {"language" : "en", "country": "EN", "variant" : "UTF-8"}})")); // no such locale
  }
}

void MyLocale( const   char * newLanguage,
                const   char * newCountry,
                const   char * newVariant,
                const   char * newKeywords)
{
//    if( (newLanguage==NULL) && (newCountry == NULL) && (newVariant == NULL) )
//    {
//        icu::init(NULL, FALSE); /* shortcut */
//    }
    //else
    {
        UErrorCode status = U_ZERO_ERROR;
        int32_t size = 0;
        int32_t lsize = 0;
        int32_t csize = 0;
        int32_t vsize = 0;
        int32_t ksize = 0;

        // Calculate the size of the resulting string.

        // Language
        if ( newLanguage != NULL )
        {
            lsize = (int32_t)uprv_strlen(newLanguage);
            if ( lsize < 0 || lsize > 357913941 ) { // int32 wrap
                //setToBogus();
                return;
            }
            size = lsize;
        }

        icu::CharString togo(newLanguage, lsize, status); // start with newLanguage

        // _Country
        if ( newCountry != NULL )
        {
            csize = (int32_t)uprv_strlen(newCountry);
            if ( csize < 0 || csize > 357913941 ) { // int32 wrap
                //setToBogus();
                return;
            }
            size += csize;
        }

        // _Variant
        if ( newVariant != NULL )
        {
            // remove leading _'s
            while(newVariant[0] == '_')
            {
                newVariant++;
            }

            // remove trailing _'s
            vsize = (int32_t)uprv_strlen(newVariant);
            if ( vsize < 0 || vsize > 357913941 ) { // int32 wrap
                //setToBogus();
                return;
            }
            while( (vsize>1) && (newVariant[vsize-1] == '_') )
            {
                vsize--;
            }
        }

        if( vsize > 0 )
        {
            size += vsize;
        }

        // Separator rules:
        if ( vsize > 0 )
        {
            size += 2;  // at least: __v
        }
        else if ( csize > 0 )
        {
            size += 1;  // at least: _v
        }

        if ( newKeywords != NULL)
        {
            ksize = (int32_t)uprv_strlen(newKeywords);
            if ( ksize < 0 || ksize > 357913941 ) {
              //setToBogus();
              return;
            }
            size += ksize + 1;
        }

        //  NOW we have the full locale string..
        // Now, copy it back.

        // newLanguage is already copied

        if ( ( vsize != 0 ) || (csize != 0) )  // at least:  __v
        {                                      //            ^
            togo.append('_', status);
        }

        if ( csize != 0 )
        {
            togo.append(newCountry, status);
        }

        if ( vsize != 0)
        {
            togo.append('_', status)
                .append(newVariant, vsize, status);
        }

        if ( ksize != 0)
        {
            if (uprv_strchr(newKeywords, '=')) {
                togo.append('@', status); /* keyword parsing */
            }
            else {
                togo.append('_', status); /* Variant parsing with a script */
                if ( vsize == 0) {
                    togo.append('_', status); /* No country found */
                }
            }
            togo.append(newKeywords, status);
        }

        if (U_FAILURE(status)) {
            // Something went wrong with appending, etc.
            //setToBogus();
            return;
        }
        // Parse it, because for example 'language' might really be a complete
        // string.
//        init(togo.data(), FALSE);
        std::cout << togo.data() << std::endl;
    }
}


TEST(collation_token_stream_test, check_collation) {

  auto err = UErrorCode::U_ZERO_ERROR;

  constexpr irs::string_ref locale_name = R"(en)";
  const icu::Locale icu_locale = icu::Locale::createFromName(locale_name.c_str());

  icu::CollationKey key;
  std::unique_ptr<icu::Collator> coll{
    icu::Collator::createInstance(icu_locale, err) };
  ASSERT_NE(nullptr, coll);
  ASSERT_TRUE(U_SUCCESS(err));

  auto get_collation_key = [&](irs::string_ref data) -> irs::bytes_ref {
    err = UErrorCode::U_ZERO_ERROR;
    coll->getCollationKey(
      icu::UnicodeString::fromUTF8(
        icu::StringPiece{data.c_str(), static_cast<int32_t>(data.size())}),
      key, err);
    EXPECT_TRUE(U_SUCCESS(err));

    int32_t size = 0;
    const irs::byte_type* p = key.getByteArray(size);
    EXPECT_NE(nullptr, p);
    EXPECT_NE(0, size);

    return { p, static_cast<size_t>(size)-1 };
  };

  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale": {"language" : "en"}})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr irs::string_ref data{"å b z a"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale": {"language" : "sv"}})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr irs::string_ref data{"a å b z"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_NE(get_collation_key(data), term->value);

      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }
}

TEST(collation_token_stream_test, check_tokens_utf8) {
  auto err = UErrorCode::U_ZERO_ERROR;

  constexpr irs::string_ref locale_name = "en-EN.UTF-8";

  const auto icu_locale =icu::Locale::createFromName(locale_name.c_str());

  icu::CollationKey key;
  std::unique_ptr<icu::Collator> coll{
    icu::Collator::createInstance(icu_locale, err) };
  ASSERT_NE(nullptr, coll);
  ASSERT_TRUE(U_SUCCESS(err));

  auto get_collation_key = [&](irs::string_ref data) -> irs::bytes_ref {
    err = UErrorCode::U_ZERO_ERROR;
    coll->getCollationKey(
      icu::UnicodeString::fromUTF8(
        icu::StringPiece{data.c_str(), static_cast<int32_t>(data.size())}),
      key, err);
    EXPECT_TRUE(U_SUCCESS(err));

    int32_t size = 0;
    const irs::byte_type* p = key.getByteArray(size);
    EXPECT_NE(nullptr, p);
    EXPECT_NE(0, size);

    return { p, static_cast<size_t>(size)-1 };
  };

  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({ "locale" : {"language" : "en" }})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      const irs::string_ref data{irs::string_ref::NIL};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }

    {
      const irs::string_ref data{irs::string_ref::EMPTY};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }

    {
      constexpr irs::string_ref data{"quick"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }

    {
      constexpr irs::string_ref data{"foo"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }

    {
      constexpr irs::string_ref data{"the quick Brown fox jumps over the lazy dog"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }
}

TEST(collation_token_stream_test, check_tokens) {
  auto err = UErrorCode::U_ZERO_ERROR;

  constexpr irs::string_ref locale_name = "de-DE";

  const auto icu_locale =icu::Locale::createFromName(locale_name.c_str());

  icu::CollationKey key;
  std::unique_ptr<icu::Collator> coll{
    icu::Collator::createInstance(icu_locale, err) };

  ASSERT_NE(nullptr, coll);
  ASSERT_TRUE(U_SUCCESS(err));

  auto get_collation_key = [&](irs::string_ref data) -> irs::bytes_ref {
    err = UErrorCode::U_ZERO_ERROR;
    coll->getCollationKey(
      icu::UnicodeString::fromUTF8(
        icu::StringPiece{data.c_str(), static_cast<int32_t>(data.size())}),
      key, err);
    EXPECT_TRUE(U_SUCCESS(err));

    int32_t size = 0;
    const irs::byte_type* p = key.getByteArray(size);
    EXPECT_NE(nullptr, p);
    EXPECT_NE(0, size);

    return { p, static_cast<size_t>(size)-1 };
  };

//  {
//    auto stream = irs::analysis::analyzers::get(
//      "collation", irs::type<irs::text_format::json>::get(),
//      R"({ "locale" : {"language" : "de_DE" }})");

//    ASSERT_NE(nullptr, stream);

//    auto* offset = irs::get<irs::offset>(*stream);
//    ASSERT_NE(nullptr, offset);
//    auto* term = irs::get<irs::term_attribute>(*stream);
//    ASSERT_NE(nullptr, term);
//    auto* inc = irs::get<irs::increment>(*stream);
//    ASSERT_NE(nullptr, inc);

//    {
//      std::wstring unicodeData = L"\u00F6\u00F5";

//      std::string data;
//      ASSERT_TRUE(irs::locale_utils::append_external<wchar_t>(data, unicodeData, locale));

//      ASSERT_TRUE(stream->reset(data));
//      ASSERT_TRUE(stream->next());
//      ASSERT_EQ(0, offset->start);
//      ASSERT_EQ(data.size(), offset->end);
//      ASSERT_EQ(get_collation_key(data), term->value);
//      ASSERT_EQ(1, inc->value);
//      ASSERT_FALSE(stream->next());
//    }
//  }
}
