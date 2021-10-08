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

#include "analysis/nearest_neighbors_stream.hpp"

#include "tests_shared.hpp"
#include "velocypack/Parser.h"
#include "velocypack/velocypack-aliases.h"

#ifndef IRESEARCH_DLL

TEST(nearest_neighbors_stream_test, consts) {
  static_assert("nearest_neighbors" == irs::type<irs::analysis::nearest_neighbors_stream>::name());
}

TEST(nearest_neighbors_stream_test, load_model) {
  auto model_loc = test_base::resource("ag_news.bin").u8string();
  irs::analysis::nearest_neighbors_stream::Options options{model_loc};
  auto load_model= [&options]() {
    auto ft = std::make_shared<fasttext::FastText>();
    ft->loadModel(options.model_location);
    return ft;
  };
  ASSERT_NO_THROW(irs::analysis::nearest_neighbors_stream(options, load_model));
}

#endif

TEST(nearest_neighbors_stream_test, test_load) {
  // load json string
  {
    auto model_loc = test_base::resource("ag_news.bin").u8string();
    irs::string_ref data{"sad"};
    auto input_json = "{\"model_location\": \"" + model_loc + "\"}";
    auto stream = irs::analysis::analyzers::get("nearest_neighbors", irs::type<irs::text_format::json>::get(), input_json);

    ASSERT_NE(nullptr, stream);
    ASSERT_FALSE(stream->next());
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::offset>(*stream);
    auto* term = irs::get<irs::term_attribute>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("determined", irs::ref_cast<char>(term->value));
    ASSERT_FALSE(stream->next());
  }


  {
    auto model_loc = test_base::resource("ag_news.bin").u8string();
    irs::string_ref data{"sad"};
    auto input_json = "{\"model_location\": \"" + model_loc + "\", \"top_k\": 2}";
    auto stream = irs::analysis::analyzers::get("nearest_neighbors", irs::type<irs::text_format::json>::get(), input_json);

    ASSERT_NE(nullptr, stream);
    ASSERT_FALSE(stream->next());
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::offset>(*stream);
    auto* term = irs::get<irs::term_attribute>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("determined", irs::ref_cast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ("postseason", irs::ref_cast<char>(term->value));
    ASSERT_FALSE(stream->next());
  }

  // test longer string
  {
    auto model_loc = test_base::resource("ag_news.bin").u8string();
    irs::string_ref data{"sad postseason"};
    auto input_json = "{\"model_location\": \"" + model_loc + "\", \"top_k\": 2}";
    auto stream = irs::analysis::analyzers::get("nearest_neighbors", irs::type<irs::text_format::json>::get(), input_json);

    ASSERT_NE(nullptr, stream);
    ASSERT_FALSE(stream->next());
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::offset>(*stream);
    auto* term = irs::get<irs::term_attribute>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(14, offset->end);
    ASSERT_EQ("135\\previously", irs::ref_cast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ("108-", irs::ref_cast<char>(term->value));
    ASSERT_FALSE(stream->next());
  }}

TEST(nearest_neighbors_stream_test, test_make_config_json) {
  // random extra param
  {
    auto model_loc = test_base::resource("ag_news.bin").u8string();
    std::string config = "{\"model_location\": \"" + model_loc + "\", \"not_valid\": false}";
    std::string expected_conf = "{\"model_location\": \"" + model_loc + "\", \"top_k\": 1}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::normalize(actual, "nearest_neighbors", irs::type<irs::text_format::json>::get(), config));
    ASSERT_EQ(VPackParser::fromJson(expected_conf)->toString(), actual);
  }

  // test top k
  {
    auto model_loc = test_base::resource("ag_news.bin").u8string();
    std::string config = "{\"model_location\": \"" + model_loc + "\", \"top_k\": 2}";
    std::string expected_conf = "{\"model_location\": \"" + model_loc + "\", \"top_k\": 2}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::normalize(actual, "nearest_neighbors", irs::type<irs::text_format::json>::get(), config));
    ASSERT_EQ(VPackParser::fromJson(expected_conf)->toString(), actual);
  }

  // test VPack
  {
    auto model_loc = test_base::resource("ag_news.bin").u8string();
    std::string config = "{\"model_location\":\"" + model_loc + "\", \"not_valid\": false}";
    auto expected_conf = "{\"model_location\": \"" + model_loc + "\", \"top_k\": 1}";
    auto in_vpack = VPackParser::fromJson(config);
    std::string in_str;
    in_str.assign(in_vpack->slice().startAs<char>(), in_vpack->slice().byteSize());
    std::string out_str;
    ASSERT_TRUE(irs::analysis::analyzers::normalize(out_str, "nearest_neighbors", irs::type<irs::text_format::vpack>::get(), in_str));
    VPackSlice out_slice(reinterpret_cast<const uint8_t*>(out_str.c_str()));
    ASSERT_EQ(VPackParser::fromJson(expected_conf)->toString(), out_slice.toString());
  }
}

