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

#include <absl/container/flat_hash_set.h>

#include "analyzers.hpp"
#include "token_attributes.hpp"
#include "utils/frozen_attributes.hpp"
#include "utils/hash_utils.hpp"

namespace iresearch {
namespace analysis {

////////////////////////////////////////////////////////////////////////////////
/// @brief an analyzer capable of masking the input, treated as a single token,
///        if it is present in the configured list
////////////////////////////////////////////////////////////////////////////////
class token_masking_stream final
  : public analyzer,
    private util::noncopyable {
 public:
  using MaskSet  = absl::flat_hash_set<std::string>;

  static constexpr string_ref type_name() noexcept { return "mask"; }

  static void init(); // for trigering registration in a static build
  static ptr make(const string_ref& mask);

  explicit token_masking_stream(MaskSet&& mask);
  virtual attribute* get_mutable(irs::type_info::type_id type) noexcept override {
    return irs::get_mutable(attrs_, type);
  }
  virtual bool next() override;
  virtual bool reset(const string_ref& data) override;

 private:
  using attributes = std::tuple<
    increment,
    offset,
    payload,         // raw token value
    term_attribute>; // token value with evaluated quotes

  MaskSet mask_;
  attributes attrs_;
  bool term_eof_;
};

} // analysis
} // ROOT

#endif
