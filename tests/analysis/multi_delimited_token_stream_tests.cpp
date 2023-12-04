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

#include "analysis/multi_delimited_token_stream.hpp"
#include "gtest/gtest.h"
#include "tests_config.hpp"

namespace {

class multi_delimited_token_stream_tests : public ::testing::Test {
  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right before
    // each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right before the
    // destructor).
  }
};

}  // namespace

// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------

#ifndef IRESEARCH_DLL

TEST_F(multi_delimited_token_stream_tests, consts) {
  static_assert("multi-delimiter" ==
                irs::type<irs::analysis::multi_delimited_token_stream>::name());
}

TEST_F(multi_delimited_token_stream_tests, test_delimiter) {
  // test delimiter std::string_view{}
  {
    auto stream =
      irs::analysis::multi_delimited_token_stream::make({.delimiters = {"a"}});
    ASSERT_EQ(irs::type<irs::analysis::multi_delimited_token_stream>::id(),
              stream->type());

    ASSERT_TRUE(stream->reset("baccaad"));

    auto* payload = irs::get<irs::payload>(*stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::term_attribute>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ("b", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ("cc", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ("d", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
  }
}

TEST_F(multi_delimited_token_stream_tests, test_delimiter_empty_match) {
  // test delimiter std::string_view{}
  {
    auto stream =
      irs::analysis::multi_delimited_token_stream::make({.delimiters = {"."}});
    ASSERT_EQ(irs::type<irs::analysis::multi_delimited_token_stream>::id(),
              stream->type());

    ASSERT_TRUE(stream->reset(".."));

    auto* payload = irs::get<irs::payload>(*stream);
    ASSERT_EQ(nullptr, payload);

    ASSERT_FALSE(stream->next());
  }
}

TEST_F(multi_delimited_token_stream_tests, test_delimiter_5) {
  // test delimiter std::string_view{}
  {
    auto stream =
      irs::analysis::multi_delimited_token_stream::make({.delimiters = {";", ",", "|", ".", ":"}});
    ASSERT_EQ(irs::type<irs::analysis::multi_delimited_token_stream>::id(),
              stream->type());

    ASSERT_TRUE(stream->reset("a:b||c.d,ff."));

    auto* payload = irs::get<irs::payload>(*stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::term_attribute>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ("a", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ("b", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ("c", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ("d", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ("ff", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
  }
}

#endif
