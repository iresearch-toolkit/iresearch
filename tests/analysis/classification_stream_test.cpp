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

#include "analysis/classification_stream.hpp"

#include "tests_shared.hpp"
#include "velocypack/Parser.h"
#include "velocypack/velocypack-aliases.h"

#ifndef IRESEARCH_DLL

TEST(classification_stream_test, consts) {
  static_assert("classification" == irs::type<irs::analysis::classification_stream>::name());
}

TEST(classification_stream_test, load_model) {
  auto model_loc = test_base::resource("ag_news.bin").u8string();
  irs::analysis::classification_stream::Options options{model_loc};
  auto load_model= [&options]() {
    auto ft = std::make_shared<fasttext::FastText>();
    ft->loadModel(options.model_location);
    return ft;
  };
  ASSERT_NO_THROW(irs::analysis::classification_stream{load_model});
}

#endif

TEST(classification_stream_test, test_load) {
  // load json string
  {
    auto model_loc = test_base::resource("ag_news.bin").u8string();
    irs::string_ref data{"tests"};
    auto input_json = "{\"model_location\": \"" + model_loc + "\"}";
    auto stream = irs::analysis::analyzers::get("classification", irs::type<irs::text_format::json>::get(), input_json);

    ASSERT_NE(nullptr, stream);
    ASSERT_FALSE(stream->next());
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::offset>(*stream);
    auto* term = irs::get<irs::term_attribute>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(5, offset->end);
    ASSERT_EQ("__label__4", irs::ref_cast<char>(term->value));
    ASSERT_FALSE(stream->next());
  }

  // multi-word input
  {
    auto model_loc = test_base::resource("ag_news.bin").u8string();
    irs::string_ref data{"tests are interesting."};
    auto input_json = "{\"model_location\": \"" + model_loc + "\"}";
    auto stream = irs::analysis::analyzers::get("classification", irs::type<irs::text_format::json>::get(), input_json);

    ASSERT_NE(nullptr, stream);
    ASSERT_FALSE(stream->next());
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::offset>(*stream);
    auto* term = irs::get<irs::term_attribute>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(22, offset->end);
    ASSERT_EQ("__label__4", irs::ref_cast<char>(term->value));
    ASSERT_FALSE(stream->next());
  }

  // Multi line input
  {
    auto model_loc = test_base::resource("ag_news.bin").u8string();
    irs::string_ref data{"tests are interesting.\nwhat about that?"};
    auto input_json = "{\"model_location\": \"" + model_loc + "\"}";
    auto stream = irs::analysis::analyzers::get("classification", irs::type<irs::text_format::json>::get(), input_json);

    ASSERT_NE(nullptr, stream);
    ASSERT_FALSE(stream->next());
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::offset>(*stream);
    auto* term = irs::get<irs::term_attribute>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(39, offset->end);
    ASSERT_EQ("__label__4", irs::ref_cast<char>(term->value));
    ASSERT_FALSE(stream->next());
  }

  {
    auto model_loc = test_base::resource("ag_news.bin").u8string();
    irs::string_ref data{"karzai visits rival ' s stronghold , the afghan president makes a rare visit to the north just two weeks before the country ' s first presidential elections ."};
    auto input_json = "{\"model_location\": \"" + model_loc + "\"}";
    auto stream = irs::analysis::analyzers::get("classification", irs::type<irs::text_format::json>::get(), input_json);

    ASSERT_NE(nullptr, stream);
    ASSERT_FALSE(stream->next());
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::offset>(*stream);
    auto* term = irs::get<irs::term_attribute>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(158, offset->end);
    ASSERT_EQ("__label__1", irs::ref_cast<char>(term->value));
    ASSERT_FALSE(stream->next());
  }

}

TEST(classification_stream_test, test_make_config_json) {
  // random extra param
  {
    auto model_loc = test_base::resource("ag_news.bin").u8string();
    std::string config = "{\"model_location\": \"" + model_loc + "\", \"not_valid\": false}";
    std::string expected_conf = "{\"model_location\": \"" + model_loc + "\"}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::normalize(actual, "classification", irs::type<irs::text_format::json>::get(), config));
    ASSERT_EQ(VPackParser::fromJson(expected_conf)->toString(), actual);
  }

  // test VPack
  {
    auto model_loc = test_base::resource("ag_news.bin").u8string();
    std::string config = "{\"model_location\":\"" + model_loc + "\", \"not_valid\": false}";
    auto expected_conf = "{\"model_location\": \"" + model_loc + "\"}";
    auto in_vpack = VPackParser::fromJson(config);
    std::string in_str;
    in_str.assign(in_vpack->slice().startAs<char>(), in_vpack->slice().byteSize());
    std::string out_str;
    ASSERT_TRUE(irs::analysis::analyzers::normalize(out_str, "classification", irs::type<irs::text_format::vpack>::get(), in_str));
    VPackSlice out_slice(reinterpret_cast<const uint8_t*>(out_str.c_str()));
    ASSERT_EQ(VPackParser::fromJson(expected_conf)->toString(), out_slice.toString());
  }
}
