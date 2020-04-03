////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_GEO_TOKEN_STREAM_H
#define IRESEARCH_GEO_TOKEN_STREAM_H

#include "shared.hpp"
#include "analyzers.hpp"
#include "token_stream.hpp"
#include "token_attributes.hpp"
#include "s2/s2region_term_indexer.h"

NS_ROOT
NS_BEGIN(analysis)

class geo_token_stream final : public analyzer,
                               private util::noncopyable {
 private:
  class term : public term_attribute {
   public:
    void value(const std::string& v) noexcept {
      value_ = bytes_ref(
        reinterpret_cast<const byte_type*>(v.c_str()), 
        v.size());
    }
  }; // term

 public:
  DECLARE_ANALYZER_TYPE();
  DECLARE_FACTORY(const S2RegionTermIndexer::Options& opts);

  static void init(); // for triggering registration in a static build

  explicit geo_token_stream(const S2RegionTermIndexer::Options& opts,
                            const string_ref& prefix);

  virtual const irs::attribute_view& attributes() const noexcept override {
    return attrs_;
  }

  virtual bool next() noexcept override;
  virtual bool reset(const string_ref& data) override;

  S2RegionTermIndexer indexer_;
  std::vector<std::string> terms_;
  const std::string* begin_{ terms_.data() };
  const std::string* end_{ begin_ };
  std::string prefix_;
  attribute_view attrs_;
  offset offset_;
  increment inc_;
  term term_;
}; // geo_token_stream

NS_END // analysis
NS_END // ROOT

#endif // IRESEARCH_GEO_TOKEN_STREAM_H

