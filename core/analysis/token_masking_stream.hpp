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

#ifndef IRESEARCH_TOKEN_MASKING_STREAM_H
#define IRESEARCH_TOKEN_MASKING_STREAM_H

#include "analyzers.hpp"
#include "token_attributes.hpp"

NS_ROOT
NS_BEGIN(analysis)

////////////////////////////////////////////////////////////////////////////////
/// @brief an analyzer capable of masking the input, treated as a single token,
///        if it is present in the configured list
////////////////////////////////////////////////////////////////////////////////
class token_masking_stream: public analyzer, util::noncopyable {
 public:
  static constexpr string_ref type_name() noexcept { return "mask"; }
  static void init(); // for trigering registration in a static build

  // for use with irs::order::add<T>() and default args (static build)
  DECLARE_FACTORY(const string_ref& mask);

  explicit token_masking_stream(std::unordered_set<irs::bstring>&& mask);
  virtual const irs::attribute_view& attributes() const noexcept override {
    return attrs_;
  }
  virtual bool next() override;
  virtual bool reset(const string_ref& data) override;

  private:
   irs::attribute_view attrs_;
   irs::increment inc_;
   std::unordered_set<irs::bstring> mask_;
   irs::offset offset_;
   irs::payload payload_; // raw token value
   irs::term_attribute term_; // token value with evaluated quotes
   bool term_eof_;
};

NS_END // analysis
NS_END // ROOT

#endif
