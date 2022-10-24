////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_MINHASH_TOKEN_STREAM_H
#define IRESEARCH_MINHASH_TOKEN_STREAM_H

#include "analysis/analyzer.hpp"
#include "analysis/token_attributes.hpp"
#include "utils/attribute_helper.hpp"
#include "utils/minhash_utils.hpp"
#include "utils/noncopyable.hpp"

namespace iresearch::analysis {

class MinHashTokenStream final : public analyzer, private util::noncopyable {
 public:
  struct Options {
    // Analyzer used for hashing set generation
    analysis::analyzer::ptr analyzer;
    // Number of min hashes to maintain
    uint32_t num_hashes{1};
  };

  // Return analyzer type name.
  static constexpr std::string_view type_name() noexcept { return "minhash"; }

  // For triggering registration in a static build.
  static void init();

  explicit MinHashTokenStream(Options&& opts);

  // Advance stream to the next token.
  bool next() override;

  // Reset stream to a specified value.
  bool reset(std::string_view value) override;

  // Return a stream attribute denoted by `id`.
  attribute* get_mutable(irs::type_info::type_id id) noexcept override {
    return irs::get_mutable(attrs_, id);
  }

  // Return analyzer options.
  const Options& options() const noexcept { return opts_; }

  // Return accumulated MinHash signature.
  const MinHash& signature() const noexcept { return minhash_; }

 private:
  using attributes = std::tuple<term_attribute, increment, offset>;
  using iterator = std::vector<size_t>::const_iterator;

  void ComputeSignature();

  Options opts_;
  MinHash minhash_;
  attributes attrs_;
  increment next_inc_;
  const term_attribute* term_{};
  const offset* offset_{};
  iterator begin_{};
  iterator end_{};
  std::array<char, 11> buf_{};
};

}  // namespace iresearch::analysis

#endif
