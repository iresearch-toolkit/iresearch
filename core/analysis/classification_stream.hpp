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
      Options() : model_location{} {}
      explicit Options(std::string& model_location) : model_location{model_location} {}

      std::string model_location;
    };

    static constexpr string_ref type_name() noexcept { return "classification"; }

    static void init(); // for registration in a static build

    explicit classification_stream(const Options& options);
    ~classification_stream();

    virtual attribute* get_mutable(irs::type_info::type_id type) noexcept override {
      return irs::get_mutable(attrs_, type);
    }

    virtual bool next() override;
    virtual bool reset(const string_ref& data) override;

  private:
    using attributes = std::tuple<
      increment,
      offset,
      payload,
      term_attribute>;

    attributes attrs_;
    std::shared_ptr<fasttext::FastText> model_container;
    const std::string& model_location;
    bool term_eof_;
};

class EmbeddingsModelLoader {
  public:
    static EmbeddingsModelLoader& getInstance() {
      static EmbeddingsModelLoader instance{};
      return instance;
    }

    std::shared_ptr<fasttext::FastText> get_model_and_increment_count(const std::string& model_location);

    void decrement_model_usage_count(const std::string& model_location);

    EmbeddingsModelLoader(EmbeddingsModelLoader const&) = delete;
    void operator=(EmbeddingsModelLoader const&) = delete;

  private:
    EmbeddingsModelLoader() : model_map{}, model_usage_count{}, map_mutex{} {}


    std::unordered_map<std::string, std::shared_ptr<fasttext::FastText>> model_map;
    std::unordered_map<std::string, uint32_t> model_usage_count;
    std::mutex map_mutex;
};

} // analysis
} // iresearch


#endif //IRESEARCH_EMBEDDING_CLASSIFICATION_STREAM_H
