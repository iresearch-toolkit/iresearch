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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////
#ifndef IRESEARCH_SEGMENTATION_TOKEN_STREAM_H
#define IRESEARCH_SEGMENTATION_TOKEN_STREAM_H

#include "shared.hpp"
#include "analyzers.hpp"
#include "token_stream.hpp"
#include "token_attributes.hpp"
#include "utils/frozen_attributes.hpp"


namespace iresearch {
namespace analysis {
class segmentation_token_stream final
  : public analyzer,
    private util::noncopyable {
 public:
  static constexpr string_ref type_name() noexcept { return "segmentation"; }
  static void init(); // for triggering registration in a static build
  
  struct options_t {
  };

  virtual attribute* get_mutable(irs::type_info::type_id type) noexcept override final {
    return irs::get_mutable(attrs_, type);
  }
  explicit segmentation_token_stream(options_t&&);
  virtual bool next() override;
  virtual bool reset(const string_ref& data) override;


 private:
  using attributes = std::tuple<
    increment,
    offset,
    term_attribute>;

  bstring term_buf_; // buffer for value if value cannot be referenced directly
  attributes attrs_;
 //std::unique_ptr<RS::Unicorn::WordIterator> word_range_; // it should stay before src_ !
 // std::string src_;
  
};
} // namespace analysis
} // namespace iresearch

#endif // IRESEARCH_SEGMENTATION_TOKEN_STREAM_H