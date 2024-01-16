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

irs::bstring operator""_b(const char* ptr, std::size_t size) {
  return irs::bstring{
    irs::ViewCast<irs::byte_type>(std::string_view{ptr, size})};
}

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

TEST_F(multi_delimited_token_stream_tests, consts) {
  static_assert("multi_delimiter" ==
                irs::type<irs::analysis::MultiDelimitedAnalyser>::name());
}

TEST_F(multi_delimited_token_stream_tests, test_delimiter) {
  auto stream =
    irs::analysis::MultiDelimitedAnalyser::Make({.delimiters = {"a"_b}});
  ASSERT_EQ(irs::type<irs::analysis::MultiDelimitedAnalyser>::id(),
            stream->type());

  ASSERT_TRUE(stream->reset("baccaad"));

  auto* payload = irs::get<irs::payload>(*stream);
  ASSERT_EQ(nullptr, payload);
  auto* term = irs::get<irs::term_attribute>(*stream);
  auto* inc = irs::get<irs::increment>(*stream);
  auto* offset = irs::get<irs::offset>(*stream);
  ASSERT_EQ(inc->value, 1);

  ASSERT_TRUE(stream->next());
  ASSERT_EQ("b", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 0);
  ASSERT_EQ(offset->end, 1);
  ASSERT_TRUE(stream->next());
  ASSERT_EQ("cc", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 2);
  ASSERT_EQ(offset->end, 4);
  ASSERT_TRUE(stream->next());
  ASSERT_EQ("d", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 6);
  ASSERT_EQ(offset->end, 7);
  ASSERT_FALSE(stream->next());
}

TEST_F(multi_delimited_token_stream_tests, test_delimiter_empty_match) {
  auto stream =
    irs::analysis::MultiDelimitedAnalyser::Make({.delimiters = {"."_b}});
  ASSERT_EQ(irs::type<irs::analysis::MultiDelimitedAnalyser>::id(),
            stream->type());

  ASSERT_TRUE(stream->reset(".."));

  auto* payload = irs::get<irs::payload>(*stream);
  ASSERT_EQ(nullptr, payload);

  ASSERT_FALSE(stream->next());
}

TEST_F(multi_delimited_token_stream_tests, test_delimiter_3) {
  auto stream = irs::analysis::MultiDelimitedAnalyser::Make(
    {.delimiters = {";"_b, ","_b, "|"_b}});
  ASSERT_EQ(irs::type<irs::analysis::MultiDelimitedAnalyser>::id(),
            stream->type());

  ASSERT_TRUE(stream->reset("a;b||c|d,ff"));

  auto* payload = irs::get<irs::payload>(*stream);
  ASSERT_EQ(nullptr, payload);
  auto* term = irs::get<irs::term_attribute>(*stream);
  auto* offset = irs::get<irs::offset>(*stream);

  ASSERT_TRUE(stream->next());
  ASSERT_EQ("a", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 0);
  ASSERT_EQ(offset->end, 1);
  ASSERT_TRUE(stream->next());
  ASSERT_EQ("b", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 2);
  ASSERT_EQ(offset->end, 3);
  ASSERT_TRUE(stream->next());
  ASSERT_EQ("c", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 5);
  ASSERT_EQ(offset->end, 6);
  ASSERT_TRUE(stream->next());
  ASSERT_EQ("d", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 7);
  ASSERT_EQ(offset->end, 8);
  ASSERT_TRUE(stream->next());
  ASSERT_EQ("ff", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 9);
  ASSERT_EQ(offset->end, 11);
  ASSERT_FALSE(stream->next());
}

TEST_F(multi_delimited_token_stream_tests, test_delimiter_5) {
  auto stream = irs::analysis::MultiDelimitedAnalyser::Make(
    {.delimiters = {";"_b, ","_b, "|"_b, "."_b, ":"_b}});
  ASSERT_EQ(irs::type<irs::analysis::MultiDelimitedAnalyser>::id(),
            stream->type());

  ASSERT_TRUE(stream->reset("a:b||c.d,ff."));

  auto* payload = irs::get<irs::payload>(*stream);
  ASSERT_EQ(nullptr, payload);
  auto* term = irs::get<irs::term_attribute>(*stream);
  auto* offset = irs::get<irs::offset>(*stream);

  ASSERT_TRUE(stream->next());
  ASSERT_EQ("a", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 0);
  ASSERT_EQ(offset->end, 1);
  ASSERT_TRUE(stream->next());
  ASSERT_EQ("b", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 2);
  ASSERT_EQ(offset->end, 3);
  ASSERT_TRUE(stream->next());
  ASSERT_EQ("c", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 5);
  ASSERT_EQ(offset->end, 6);
  ASSERT_TRUE(stream->next());
  ASSERT_EQ("d", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 7);
  ASSERT_EQ(offset->end, 8);
  ASSERT_TRUE(stream->next());
  ASSERT_EQ("ff", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 9);
  ASSERT_EQ(offset->end, 11);
  ASSERT_FALSE(stream->next());
}

TEST_F(multi_delimited_token_stream_tests, test_delimiter_single_long) {
  auto stream =
    irs::analysis::MultiDelimitedAnalyser::Make({.delimiters = {"foo"_b}});
  ASSERT_EQ(irs::type<irs::analysis::MultiDelimitedAnalyser>::id(),
            stream->type());

  ASSERT_TRUE(stream->reset("foobarfoobazbarfoobar"));

  auto* payload = irs::get<irs::payload>(*stream);
  ASSERT_EQ(nullptr, payload);
  auto* term = irs::get<irs::term_attribute>(*stream);
  auto* offset = irs::get<irs::offset>(*stream);

  ASSERT_TRUE(stream->next());
  ASSERT_EQ("bar", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 3);
  ASSERT_EQ(offset->end, 6);
  ASSERT_TRUE(stream->next());
  ASSERT_EQ("bazbar", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 9);
  ASSERT_EQ(offset->end, 15);
  ASSERT_TRUE(stream->next());
  ASSERT_EQ("bar", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 18);
  ASSERT_EQ(offset->end, 21);
  ASSERT_FALSE(stream->next());
}

TEST_F(multi_delimited_token_stream_tests, no_delimiter) {
  auto stream = irs::analysis::MultiDelimitedAnalyser::Make({.delimiters = {}});
  ASSERT_EQ(irs::type<irs::analysis::MultiDelimitedAnalyser>::id(),
            stream->type());

  ASSERT_TRUE(stream->reset("foobar"));

  auto* payload = irs::get<irs::payload>(*stream);
  ASSERT_EQ(nullptr, payload);
  auto* term = irs::get<irs::term_attribute>(*stream);
  auto* offset = irs::get<irs::offset>(*stream);

  ASSERT_TRUE(stream->next());
  ASSERT_EQ("foobar", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 0);
  ASSERT_EQ(offset->end, 6);
  ASSERT_FALSE(stream->next());
}

TEST_F(multi_delimited_token_stream_tests, multi_words) {
  auto stream = irs::analysis::MultiDelimitedAnalyser::Make(
    {.delimiters = {"foo"_b, "bar"_b, "baz"_b}});
  ASSERT_EQ(irs::type<irs::analysis::MultiDelimitedAnalyser>::id(),
            stream->type());

  ASSERT_TRUE(stream->reset("fooxyzbarbazz"));

  auto* payload = irs::get<irs::payload>(*stream);
  ASSERT_EQ(nullptr, payload);
  auto* term = irs::get<irs::term_attribute>(*stream);
  auto* offset = irs::get<irs::offset>(*stream);

  ASSERT_TRUE(stream->next());
  ASSERT_EQ("xyz", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 3);
  ASSERT_EQ(offset->end, 6);
  ASSERT_TRUE(stream->next());
  ASSERT_EQ("z", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 12);
  ASSERT_EQ(offset->end, 13);
  ASSERT_FALSE(stream->next());
}

TEST_F(multi_delimited_token_stream_tests, multi_words_2) {
  auto stream = irs::analysis::MultiDelimitedAnalyser::Make(
    {.delimiters = {"foo"_b, "bar"_b, "baz"_b}});
  ASSERT_EQ(irs::type<irs::analysis::MultiDelimitedAnalyser>::id(),
            stream->type());

  ASSERT_TRUE(stream->reset("foobarbaz"));

  auto* payload = irs::get<irs::payload>(*stream);
  ASSERT_EQ(nullptr, payload);

  ASSERT_FALSE(stream->next());
}

TEST_F(multi_delimited_token_stream_tests, trick_matching_1) {
  auto stream = irs::analysis::MultiDelimitedAnalyser::Make(
    {.delimiters = {"foo"_b, "ffa"_b}});
  ASSERT_EQ(irs::type<irs::analysis::MultiDelimitedAnalyser>::id(),
            stream->type());

  ASSERT_TRUE(stream->reset("abcffoobar"));

  auto* payload = irs::get<irs::payload>(*stream);
  ASSERT_EQ(nullptr, payload);
  auto* term = irs::get<irs::term_attribute>(*stream);
  auto* offset = irs::get<irs::offset>(*stream);

  ASSERT_TRUE(stream->next());
  ASSERT_EQ("abcf", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 0);
  ASSERT_EQ(offset->end, 4);
  ASSERT_TRUE(stream->next());
  ASSERT_EQ("bar", irs::ViewCast<char>(term->value));
  ASSERT_EQ(offset->start, 7);
  ASSERT_EQ(offset->end, 10);
  ASSERT_FALSE(stream->next());
}
