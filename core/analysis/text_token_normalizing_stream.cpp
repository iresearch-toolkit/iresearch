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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////
#include "velocypack/Slice.h"
#include "velocypack/Builder.h"
#include "velocypack/Parser.h"
#include "velocypack/velocypack-aliases.h"

#include <frozen/unordered_map.h>

#include <unicode/locid.h> // for icu::Locale

#if defined(_MSC_VER)
  #pragma warning(disable: 4512)
#endif

  #include <unicode/normalizer2.h> // for icu::Normalizer2

#if defined(_MSC_VER)
  #pragma warning(default: 4512)
#endif

#include <unicode/translit.h> // for icu::Transliterator

#if defined(_MSC_VER)
  #pragma warning(disable: 4229)
#endif

  #include <unicode/uclean.h> // for u_cleanup

#if defined(_MSC_VER)
  #pragma warning(default: 4229)
#endif

#include "utils/hash_utils.hpp"
#include "utils/locale_utils.hpp"
#include "utils/vpack_utils.hpp"

#include "text_token_normalizing_stream.hpp"

namespace iresearch {
namespace analysis {

// -----------------------------------------------------------------------------
// --SECTION--                                                     private types
// -----------------------------------------------------------------------------

struct text_token_normalizing_stream::state_t {
  icu::UnicodeString data;
  icu::Locale icu_locale;
  std::shared_ptr<const icu::Normalizer2> normalizer;
  const options_t options;
  std::string term_buf; // used by reset()
  std::shared_ptr<icu::Transliterator> transliterator;
  state_t(const options_t& opts): icu_locale("C"), options(opts) {
    // NOTE: use of the default constructor for Locale() or
    //       use of Locale::createFromName(nullptr)
    //       causes a memory leak with Boost 1.58, as detected by valgrind
    icu_locale.setToBogus(); // set to uninitialized
  }
};

} // analysis
} // ROOT

namespace {


bool make_locale_from_name(const irs::string_ref& name,
                         std::locale& locale) {
  try {
    locale = irs::locale_utils::locale(
        name, irs::string_ref::NIL,
        true);  // true == convert to unicode, required for ICU and Snowball
                // check if ICU supports locale
    auto icu_locale =
        icu::Locale(std::string(irs::locale_utils::language(locale)).c_str(),
                    std::string(irs::locale_utils::country(locale)).c_str());
    return !icu_locale.isBogus();
  } catch (...) {
    IR_FRMT_ERROR(
        "Caught error while constructing locale from "
        "name: %s",
        name.c_str());
  }
  return false;
}

constexpr irs::string_ref LOCALE_PARAM_NAME       = "locale";
constexpr irs::string_ref CASE_CONVERT_PARAM_NAME = "case";
constexpr irs::string_ref ACCENT_PARAM_NAME       = "accent";

constexpr frozen::unordered_map<
    irs::string_ref,
    irs::analysis::text_token_normalizing_stream::options_t::case_convert_t, 3> CASE_CONVERT_MAP = {
  { "lower", irs::analysis::text_token_normalizing_stream::options_t::case_convert_t::LOWER },
  { "none", irs::analysis::text_token_normalizing_stream::options_t::case_convert_t::NONE },
  { "upper", irs::analysis::text_token_normalizing_stream::options_t::case_convert_t::UPPER },
};


bool parse_vpack_options(
    const irs::string_ref& args,
    irs::analysis::text_token_normalizing_stream::options_t& options) {

  VPackSlice slice(reinterpret_cast<uint8_t const*>(args.c_str()));
  if (!slice.isObject() && !slice.isString()) {
    std::string slice_as_str = iresearch::get_string(slice);
    IR_FRMT_ERROR("Slice for delimited_token_stream is not an object or string: %s",
                  slice_as_str.c_str());
    return false;
  }

  try {
    switch (slice.type()) {
      case VPackValueType::String:
        return make_locale_from_name(slice.toString(), options.locale);  // required
      case VPackValueType::Object:
        if (slice.hasKey(LOCALE_PARAM_NAME.c_str()) &&
            slice.get(LOCALE_PARAM_NAME.c_str()).isString()) {
          if (!make_locale_from_name(slice.get(LOCALE_PARAM_NAME).toString(), options.locale)) {
            return false;
          }
          if (slice.hasKey(CASE_CONVERT_PARAM_NAME.c_str())) {
            auto case_convert_slice = slice.get(CASE_CONVERT_PARAM_NAME.c_str());  // optional string enum

            if (!case_convert_slice.isString()) {
              IR_FRMT_WARN(
                  "Non-string value in '%s' while constructing "
                  "text_token_normalizing_stream from jSON arguments: %s",
                  CASE_CONVERT_PARAM_NAME.c_str(), args.c_str());

              return false;
            }

            auto itr = CASE_CONVERT_MAP.find(case_convert_slice.toString());

            if (itr == CASE_CONVERT_MAP.end()) {
              IR_FRMT_WARN(
                  "Invalid value in '%s' while constructing "
                  "text_token_normalizing_stream from jSON arguments: %s",
                  CASE_CONVERT_PARAM_NAME.c_str(), args.c_str());

              return false;
            }

            options.case_convert = itr->second;
          }

          if (slice.hasKey(ACCENT_PARAM_NAME.c_str())) {
            auto accent_slice = slice.get(ACCENT_PARAM_NAME);  // optional bool

            if (!accent_slice.isBool()) {
              IR_FRMT_WARN(
                  "Non-boolean value in '%s' while constructing "
                  "text_token_normalizing_stream from jSON arguments: %s",
                  ACCENT_PARAM_NAME.c_str(), args.c_str());

              return false;
            }

            options.accent = accent_slice.getBool();
          }

          return true;
        }
      [[fallthrough]];
      default:
        IR_FRMT_ERROR(
            "Missing '%s' while constructing text_token_normalizing_stream "
            "from jSON arguments: %s",
            LOCALE_PARAM_NAME.c_str(), args.c_str());
    }
  } catch (...) {
    IR_FRMT_ERROR(
        "Caught error while constructing text_token_normalizing_stream from "
        "jSON arguments: %s",
        args.c_str());
  }
  return false;
}
    ////////////////////////////////////////////////////////////////////////////////
/// @brief args is a jSON encoded object with the following attributes:
///        "locale"(string): the locale to use for stemming <required>
///        "case"(string enum): modify token case using "locale"
///        "accent"(bool): leave accents
////////////////////////////////////////////////////////////////////////////////
irs::analysis::analyzer::ptr make_vpack(const irs::string_ref& args) {
  irs::analysis::text_token_normalizing_stream::options_t options;
  if (parse_vpack_options(args, options)) {
    return irs::memory::make_shared<
        irs::analysis::text_token_normalizing_stream>(std::move(options));
  } else {
    return nullptr;
  }
}

///////////////////////////////////////////////////////////////////////////////
/// @brief builds analyzer config from internal options in json format
/// @param options reference to analyzer options storage
/// @param definition string for storing json document with config 
///////////////////////////////////////////////////////////////////////////////
bool make_vpack_config(
    const irs::analysis::text_token_normalizing_stream::options_t& options,
    std::string& definition) {

  VPackBuilder builder;
  {
    VPackObjectBuilder object(&builder);
    {
      // locale
      const auto& locale_name = irs::locale_utils::name(options.locale);
      VPackStringRef locale_param_name(LOCALE_PARAM_NAME.c_str(), LOCALE_PARAM_NAME.size());
      VPackValue value_local(irs::string_ref(locale_name.c_str(), locale_name.length()));
      builder.add(locale_param_name, value_local);

      // case convert
      auto case_value = std::find_if(CASE_CONVERT_MAP.begin(), CASE_CONVERT_MAP.end(),
        [&options](const decltype(CASE_CONVERT_MAP)::value_type& v) {
            return v.second == options.case_convert;
        });
      if (case_value != CASE_CONVERT_MAP.end()) {
        VPackStringRef case_convert_param_name(CASE_CONVERT_PARAM_NAME.c_str(), CASE_CONVERT_PARAM_NAME.size());
        VPackValue value(irs::string_ref(case_value->first.c_str(), case_value->first.size()));
        builder.add(case_convert_param_name, value);
      }
      else {
        IR_FRMT_ERROR(
          "Invalid case_convert value in text analyzer options: %d",
          static_cast<int>(options.case_convert));
        return false;
      }

      // Accent
      VPackStringRef accent_param_name(ACCENT_PARAM_NAME.c_str(), ACCENT_PARAM_NAME.size());
      VPackValue value_accent(options.accent);
      builder.add(accent_param_name, value_accent);
    }
  }

  definition.assign(builder.slice().startAs<char>(), builder.slice().byteSize());
  return true;
}

bool normalize_vpack_config(const irs::string_ref& args,
                           std::string& definition) {
  irs::analysis::text_token_normalizing_stream::options_t options;
  if (parse_vpack_options(args, options)) {
    return make_vpack_config(options, definition);
  } else {
    return false;
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief args is a language to use for normalizing
////////////////////////////////////////////////////////////////////////////////
irs::analysis::analyzer::ptr make_text(const irs::string_ref& args) {
  try {
    irs::analysis::text_token_normalizing_stream::options_t options;

    if (make_locale_from_name(args, options.locale)) {// interpret 'args' as a locale name
      return irs::memory::make_shared<irs::analysis::text_token_normalizing_stream>(
          std::move(options) );
    }
  } catch (...) {
    IR_FRMT_ERROR(
      "Caught error while constructing text_token_normalizing_stream TEXT arguments: %s",
      args.c_str()
    );
  }

  return nullptr;
}

bool normalize_text_config(const irs::string_ref& args,
                           std::string& definition) {
  std::locale locale;
  if (make_locale_from_name(args, locale)){
    definition = irs::locale_utils::name(locale);
    return true;
  }
  return false;
}

irs::analysis::analyzer::ptr make_json(const irs::string_ref& args) {
  try {
    if (args.null()) {
      IR_FRMT_ERROR("Null arguments while constructing ngram_token_stream");
      return nullptr;
    }
    auto vpack = VPackParser::fromJson(args.c_str());
    return make_vpack(
        irs::string_ref(reinterpret_cast<const char*>(vpack->data()), vpack->size()));
  } catch(const VPackException& ex) {
    IR_FRMT_ERROR("Caught error '%s' while constructing ngram_token_stream from json: %s",
                  ex.what(), args.c_str());
  } catch (...) {
    IR_FRMT_ERROR("Caught error while constructing ngram_token_stream from json: %s",
                  args.c_str());
  }
  return nullptr;
}

bool normalize_json_config(const irs::string_ref& args, std::string& definition) {
  try {
    if (args.null()) {
      IR_FRMT_ERROR("Null arguments while normalizing ngram_token_stream");
      return false;
    }
    auto vpack = VPackParser::fromJson(args.c_str());
    std::string vpack_container;
    if (normalize_vpack_config(
        irs::string_ref(reinterpret_cast<const char*>(vpack->data()), vpack->size()),
        vpack_container)) {
      VPackSlice slice(
          reinterpret_cast<const uint8_t*>(vpack_container.c_str()));
      definition = iresearch::get_string(slice);
      if (definition.empty()) {
          return false;
      }
      return true;
    }
  } catch(const VPackException& ex) {
    IR_FRMT_ERROR("Caught error '%s' while normalizing ngram_token_stream from json: %s",
                  ex.what(), args.c_str());
  } catch (...) {
    IR_FRMT_ERROR("Caught error while normalizing ngram_token_stream from json: %s",
                  args.c_str());
  }
  return false;
}

REGISTER_ANALYZER_JSON(irs::analysis::text_token_normalizing_stream, make_json, 
                       normalize_json_config);
REGISTER_ANALYZER_TEXT(irs::analysis::text_token_normalizing_stream, make_text, 
                       normalize_text_config);
REGISTER_ANALYZER_TEXT(irs::analysis::text_token_normalizing_stream, make_vpack,
                       normalize_vpack_config);

}

namespace iresearch {
namespace analysis {

text_token_normalizing_stream::text_token_normalizing_stream(
    const options_t& options)
  : analyzer{irs::type<text_token_normalizing_stream>::get()},
    state_(memory::make_unique<state_t>(options)),
    term_eof_(true) {
}

/*static*/ void text_token_normalizing_stream::init() {
  REGISTER_ANALYZER_JSON(text_token_normalizing_stream, make_json,
                         normalize_json_config); // match registration above
  REGISTER_ANALYZER_TEXT(text_token_normalizing_stream, make_text, 
                         normalize_text_config); // match registration above
  REGISTER_ANALYZER_TEXT(irs::analysis::text_token_normalizing_stream, make_vpack,
                         normalize_vpack_config); // match registration above
}

/*static*/ analyzer::ptr text_token_normalizing_stream::make(
    const string_ref& locale) {
  return make_text(locale);
}

bool text_token_normalizing_stream::next() {
  if (term_eof_) {
    return false;
  }

  term_eof_ = true;

  return true;
}

bool text_token_normalizing_stream::reset(const irs::string_ref& data) {
  if (state_->icu_locale.isBogus()) {
    state_->icu_locale = icu::Locale(
      std::string(irs::locale_utils::language(state_->options.locale)).c_str(),
      std::string(irs::locale_utils::country(state_->options.locale)).c_str()
    );

    if (state_->icu_locale.isBogus()) {
      return false;
    }
  }

  auto err = UErrorCode::U_ZERO_ERROR; // a value that passes the U_SUCCESS() test

  if (!state_->normalizer) {
    // reusable object owned by ICU
    state_->normalizer.reset(
      icu::Normalizer2::getNFCInstance(err), [](const icu::Normalizer2*)->void{}
    );

    if (!U_SUCCESS(err) || !state_->normalizer) {
      state_->normalizer.reset();

      return false;
    }
  }

  if (!state_->options.accent && !state_->transliterator) {
    // transliteration rule taken verbatim from: http://userguide.icu-project.org/transforms/general
    // do not allocate statically since it causes memory leaks in ICU
    icu::UnicodeString collationRule("NFD; [:Nonspacing Mark:] Remove; NFC"); 

    // reusable object owned by *this
    state_->transliterator.reset(icu::Transliterator::createInstance(
      collationRule, UTransDirection::UTRANS_FORWARD, err
    ));

    if (!U_SUCCESS(err) || !state_->transliterator) {
      state_->transliterator.reset();

      return false;
    }
  }

  // ...........................................................................
  // convert encoding to UTF8 for use with ICU
  // ...........................................................................
  std::string data_utf8;
  irs::string_ref data_utf8_ref;
  if (irs::locale_utils::is_utf8(state_->options.locale)) {
    data_utf8_ref = data;
  } else {
    // valid conversion since 'locale_' was created with internal unicode encoding
    if (!irs::locale_utils::append_internal(data_utf8, data, state_->options.locale)) {
      return false; // UTF8 conversion failure
    }
    data_utf8_ref = data_utf8;
  }

  if (data_utf8_ref.size() > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
    return false; // ICU UnicodeString signatures can handle at most INT32_MAX
  }

  state_->data = icu::UnicodeString::fromUTF8(
    icu::StringPiece(data_utf8_ref.c_str(), static_cast<int32_t>(data_utf8_ref.size())));

  // ...........................................................................
  // normalize unicode
  // ...........................................................................
  icu::UnicodeString term_icu;

  state_->normalizer->normalize(state_->data, term_icu, err);

  if (!U_SUCCESS(err)) {
    term_icu = state_->data; // use non-normalized value if normalization failure
  }

  // ...........................................................................
  // case-convert unicode
  // ...........................................................................
  switch (state_->options.case_convert) {
   case options_t::case_convert_t::LOWER:
    term_icu.toLower(state_->icu_locale); // inplace case-conversion
    break;
   case options_t::case_convert_t::UPPER:
    term_icu.toUpper(state_->icu_locale); // inplace case-conversion
    break;
   default:
    {} // NOOP
  };

  // ...........................................................................
  // collate value, e.g. remove accents
  // ...........................................................................
  if (state_->transliterator) {
    state_->transliterator->transliterate(term_icu); // inplace translitiration
  }

  state_->term_buf.clear();
  term_icu.toUTF8String(state_->term_buf);

  // ...........................................................................
  // use the normalized value
  // ...........................................................................
  static_assert(sizeof(irs::byte_type) == sizeof(char), "sizeof(irs::byte_type) != sizeof(char)");
  std::get<term_attribute>(attrs_).value = irs::ref_cast<irs::byte_type>(state_->term_buf);
  auto& offset = std::get<irs::offset>(attrs_);
  offset.start = 0;
  offset.end = static_cast<uint32_t>(data.size());
  std::get<payload>(attrs_).value = ref_cast<uint8_t>(data);
  term_eof_ = false;

  return true;
}


} // analysis
} // ROOT
