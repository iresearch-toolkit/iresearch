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
/// @author Alex Geenen
////////////////////////////////////////////////////////////////////////////////


#ifndef IRESEARCH_NEAREST_NEIGHBORS_STREAM_H
#define IRESEARCH_NEAREST_NEIGHBORS_STREAM_H

#include "fasttext.h"

#include "analysis/analyzers.hpp"
#include "analysis/token_attributes.hpp"
#include "utils/frozen_attributes.hpp"

namespace iresearch {
namespace analysis {

class nearest_neighbors_stream final
  : public analyzer,
    private util::noncopyable {
  public:
    struct Options {
      Options() : model_location{}, top_k{1} {}
      explicit Options(std::string& model_location) : model_location{model_location}, top_k{1} {}
      explicit Options(std::string& model_location, int32_t top_k) : model_location{model_location}, top_k{top_k} {}

      std::string model_location;
      int32_t top_k;
    };

    static constexpr string_ref type_name() noexcept { return "nearest_neighbors"; }

    static void init(); // for registration in a static build

    explicit nearest_neighbors_stream(Options& options, std::function<std::shared_ptr<fasttext::FastText>()> model_provider);

    virtual attribute* get_mutable(irs::type_info::type_id type) noexcept override {
      return irs::get_mutable(attrs_, type);
    }

    virtual bool next() override;
    virtual bool reset(const string_ref& data) override;

  private:
    using attributes = std::tuple<
      increment,
      offset,
      term_attribute>;

    std::shared_ptr<fasttext::FastText> model_container_;
    std::vector<std::pair<float, std::string>> neighbors_;
    std::vector<std::pair<float, std::string>>::iterator neighbors_it_;
    attributes attrs_;
    int32_t top_k_;
};
} // analysis
} // iresearch


#endif //IRESEARCH_NEAREST_NEIGHBORS_STREAM_H