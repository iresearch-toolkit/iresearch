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

#ifndef IRESEARCH_EMBEDDING_CLASSIFICATION_STREAM_H
#define IRESEARCH_EMBEDDING_CLASSIFICATION_STREAM_H

#include "fasttext.h"

#include "analysis/analyzers.hpp"
#include "analysis/token_attributes.hpp"
#include "utils/frozen_attributes.hpp"

namespace iresearch {
namespace analysis {

class classification_stream final
    : public analyzer,
      private util::noncopyable {
  public:
    struct Options {
      Options() : model_location{}, top_k{1}, threshold{0.0} {}
      explicit Options(std::string& model_location) : model_location{model_location}, top_k{1}, threshold{0.0} {}
      explicit Options(std::string& model_location, int32_t top_k) : model_location{model_location}, top_k{top_k}, threshold{0.0} {}
      explicit Options(std::string& model_location, double threshold): model_location{model_location}, top_k{1}, threshold{threshold} {}
      explicit Options(std::string& model_location, int32_t top_k, double threshold) :
        model_location{model_location},
        top_k{top_k},
        threshold{threshold} {}

      std::string model_location;
      int32_t top_k;
      double threshold;
    };

    static constexpr string_ref type_name() noexcept { return "classification"; }

    static void init(); // for registration in a static build

    explicit classification_stream(const Options& options, std::function<std::shared_ptr<fasttext::FastText>()> model_provider);

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

    attributes attrs_;
    std::shared_ptr<fasttext::FastText> model_container_;
    int32_t top_k_;
    double threshold_;
    std::vector<std::pair<float, std::string>> predictions_;
    std::vector<std::pair<float, std::string>>::iterator predictions_it_;
};
} // analysis
} // iresearch


#endif //IRESEARCH_EMBEDDING_CLASSIFICATION_STREAM_H
