////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "analyzers.hpp"
#include "token_attributes.hpp"
#include "utils/attribute_helper.hpp"

namespace irs {
namespace analysis {

////////////////////////////////////////////////////////////////////////////////
/// @brief an analyzer capable of breaking up delimited text into tokens
///        separated by a set of strings.
////////////////////////////////////////////////////////////////////////////////
class multi_delimited_token_stream
  : public TypedAnalyzer<multi_delimited_token_stream>,
    private util::noncopyable {
 public:
  struct Options {
    std::vector<std::string> delimiters;
  };

  static constexpr std::string_view type_name() noexcept { return "multi-delimiter"; }
  static void init();

  static analyzer::ptr make(Options&&);

  attribute* get_mutable(irs::type_info::type_id type) noexcept final {
    return irs::get_mutable(attrs_, type);
  }

 protected:
  using attributes = std::tuple<increment,
                                offset,  // token value with evaluated quotes
                                term_attribute>;
  bytes_view data_;
  attributes attrs_;
};

}  // namespace analysis
}  // namespace irs
