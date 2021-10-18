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

TEST(classification_stream_test, consts) {
  static_assert("classification" == irs::type<irs::analysis::classification_stream>::name());
}

TEST(classification_stream_test, test_load) {
  // load json string
  {
    auto model_loc = test_base::resource("model_cooking.bin").u8string();
    irs::string_ref data{"baking"};
    auto input_json = "{\"model_location\": \"" + model_loc + "\"}";
    auto stream = irs::analysis::analyzers::get("classification", irs::type<irs::text_format::json>::get(), input_json);

    ASSERT_NE(nullptr, stream);
    ASSERT_FALSE(stream->next());
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::offset>(*stream);
    auto* term = irs::get<irs::term_attribute>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(6, offset->end);
    ASSERT_EQ("__label__baking", irs::ref_cast<char>(term->value));
    ASSERT_FALSE(stream->next());
  }

  // multi-word input
  {
    auto model_loc = test_base::resource("model_cooking.bin").u8string();
    irs::string_ref data{"Why not put knives in the dishwasher?"};
    auto input_json = "{\"model_location\": \"" + model_loc + "\"}";
    auto stream = irs::analysis::analyzers::get("classification", irs::type<irs::text_format::json>::get(), input_json);

    ASSERT_NE(nullptr, stream);
    ASSERT_FALSE(stream->next());
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::offset>(*stream);
    auto* term = irs::get<irs::term_attribute>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(37, offset->end);
    ASSERT_EQ("__label__knives", irs::ref_cast<char>(term->value));
    ASSERT_FALSE(stream->next());
  }

  // Multi line input
  {
    auto model_loc = test_base::resource("model_cooking.bin").u8string();
    irs::string_ref data{"Why not put knives in the dishwasher?\nOr one knife?"};
    auto input_json = "{\"model_location\": \"" + model_loc + "\"}";
    auto stream = irs::analysis::analyzers::get("classification", irs::type<irs::text_format::json>::get(), input_json);

    ASSERT_NE(nullptr, stream);
    ASSERT_FALSE(stream->next());
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::offset>(*stream);
    auto* term = irs::get<irs::term_attribute>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(51, offset->end);
    ASSERT_EQ("__label__oil", irs::ref_cast<char>(term->value));
    ASSERT_FALSE(stream->next());
  }

  // top 2 labels
  {
    auto model_loc = test_base::resource("model_cooking.bin").u8string();
    irs::string_ref data{"Why not put knives in the dishwasher?"};
    auto input_json = "{\"model_location\": \"" + model_loc + "\", \"top_k\": 2}";
    auto stream = irs::analysis::analyzers::get("classification", irs::type<irs::text_format::json>::get(), input_json);

    ASSERT_NE(nullptr, stream);
    ASSERT_FALSE(stream->next());
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::offset>(*stream);
    auto* term = irs::get<irs::term_attribute>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(37, offset->end);
    ASSERT_EQ("__label__knives", irs::ref_cast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ("__label__oil", irs::ref_cast<char>(term->value));
    ASSERT_FALSE(stream->next());
  }

}

TEST(classification_stream_test, test_make_config_json) {
  // random extra param
  {
    auto model_loc = test_base::resource("model_cooking.bin").u8string();
    std::string config = "{\"model_location\": \"" + model_loc + "\", \"not_valid\": false}";
    std::string expected_conf = "{\"model_location\": \"" + model_loc + "\", \"top_k\": 1, \"threshold\": 0.0}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::normalize(actual, "classification", irs::type<irs::text_format::json>::get(), config));
    ASSERT_EQ(VPackParser::fromJson(expected_conf)->toString(), actual);
  }

  // test top k
  {
    auto model_loc = test_base::resource("model_cooking.bin").u8string();
    std::string config = "{\"model_location\": \"" + model_loc + "\", \"top_k\": 2}";
    std::string expected_conf = "{\"model_location\": \"" + model_loc + "\", \"top_k\": 2, \"threshold\": 0.0}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::normalize(actual, "classification", irs::type<irs::text_format::json>::get(), config));
    ASSERT_EQ(VPackParser::fromJson(expected_conf)->toString(), actual);
  }

  // test threshold
  {
    auto model_loc = test_base::resource("model_cooking.bin").u8string();
    std::string config = "{\"model_location\": \"" + model_loc + "\", \"threshold\": 0.1}";
    std::string expected_conf = "{\"model_location\": \"" + model_loc + "\", \"top_k\": 1, \"threshold\": 0.1}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::normalize(actual, "classification", irs::type<irs::text_format::json>::get(), config));
    ASSERT_EQ(VPackParser::fromJson(expected_conf)->toString(), actual);
  }

  // test all 3 params
  {
    auto model_loc = test_base::resource("model_cooking.bin").u8string();
    std::string config = "{\"model_location\": \"" + model_loc + "\", \"threshold\": 0.1, \"top_k\": 2}";
    std::string expected_conf = "{\"model_location\": \"" + model_loc + "\", \"top_k\": 2, \"threshold\": 0.1}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::normalize(actual, "classification", irs::type<irs::text_format::json>::get(), config));
    ASSERT_EQ(VPackParser::fromJson(expected_conf)->toString(), actual);
  }

  // test VPack
  {
    auto model_loc = test_base::resource("model_cooking.bin").u8string();
    std::string config = "{\"model_location\":\"" + model_loc + "\", \"not_valid\": false}";
    auto expected_conf = "{\"model_location\": \"" + model_loc + "\", \"top_k\": 1, \"threshold\": 0.0}";
    auto in_vpack = VPackParser::fromJson(config);
    std::string in_str;
    in_str.assign(in_vpack->slice().startAs<char>(), in_vpack->slice().byteSize());
    std::string out_str;
    ASSERT_TRUE(irs::analysis::analyzers::normalize(out_str, "classification", irs::type<irs::text_format::vpack>::get(), in_str));
    VPackSlice out_slice(reinterpret_cast<const uint8_t*>(out_str.c_str()));
    ASSERT_EQ(VPackParser::fromJson(expected_conf)->toString(), out_slice.toString());
  }
}
