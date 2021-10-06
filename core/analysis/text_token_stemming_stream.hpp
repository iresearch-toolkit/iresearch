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

#ifndef IRESEARCH_TEXT_TOKEN_STEMMING_STREAM_H
#define IRESEARCH_TEXT_TOKEN_STEMMING_STREAM_H

#include "analyzers.hpp"
#include "token_attributes.hpp"
#include "utils/frozen_attributes.hpp"
#include "utils/icu_locale_utils.hpp"

struct sb_stemmer; // forward declaration

namespace iresearch {
namespace analysis {

////////////////////////////////////////////////////////////////////////////////
/// @class text_token_stemming_stream
/// @brief an analyser capable of stemming the text, treated as a single token,
///        for supported languages
////////////////////////////////////////////////////////////////////////////////
class text_token_stemming_stream final
  : public analyzer,
    private util::noncopyable {
 public:
  struct options_t {
    icu::Locale locale;
    icu_locale_utils::Unicode unicode{icu_locale_utils::Unicode::UTF8};

    options_t() {
      locale.setToBogus();
    }
  };

  static constexpr string_ref type_name() noexcept { return "stem"; }
  static void init(); // for trigering registration in a static build
  static ptr make(const string_ref& locale);

  explicit text_token_stemming_stream(const options_t& options);
  virtual attribute* get_mutable(irs::type_info::type_id type) noexcept override final {
    return irs::get_mutable(attrs_, type);
  }
  virtual bool next() override;
  virtual bool reset(const string_ref& data) override;

 private:
  struct stemmer_deleter {
    void operator()(sb_stemmer*) const noexcept;
  };

  using attributes = std::tuple<
    increment,
    offset,
    payload,         // raw token value
    term_attribute>; // token value with evaluated quotes

   attributes attrs_;
   options_t options_;
   std::string buf_;
   std::unique_ptr<sb_stemmer, stemmer_deleter> stemmer_;
   bool term_eof_;
};

} // analysis
} // ROOT

#endif
