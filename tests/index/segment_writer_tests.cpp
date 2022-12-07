////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <array>

#include "analysis/token_attributes.hpp"
#include "index/comparer.hpp"
#include "index/index_tests.hpp"
#include "index/segment_writer.hpp"
#include "store/memory_directory.hpp"
#include "store/store_utils.hpp"
#include "tests_shared.hpp"
#include "utils/lz4compression.hpp"

namespace {

class segment_writer_tests : public test_base {
 protected:
  static irs::column_info_provider_t default_column_info() {
    return [](const std::string_view&) {
      return irs::column_info{
        .compression = irs::type<irs::compression::lz4>::get(),
        .options = {},
        .encryption = true,
        .track_prev_doc = false};
    };
  }

  static irs::feature_info_provider_t default_feature_info() {
    return [](irs::type_info::type_id) {
      return std::make_pair(
        irs::column_info{.compression = irs::type<irs::compression::lz4>::get(),
                         .options = {},
                         .encryption = true,
                         .track_prev_doc = false},
        irs::feature_writer_factory_t{});
    };
  }

  static auto default_codec() { return irs::formats::get("1_5"); }
};

struct token_stream_mock final : public irs::token_stream {
  std::map<irs::type_info::type_id, irs::attribute*> attrs;
  size_t token_count;
  irs::attribute* get_mutable(irs::type_info::type_id type) noexcept override {
    const auto it = attrs.find(type);
    return it == attrs.end() ? nullptr : it->second;
  }
  bool next() override { return --token_count; }
};

}  // namespace

#ifndef IRESEARCH_DEBUG

TEST_F(segment_writer_tests, invalid_actions) {
  struct field_t {
    irs::token_stream& token_stream;
    field_t(irs::token_stream& stream) : token_stream(stream) {}
    float_t boost() const { return 1.f; }
    irs::features_t features() const { return {}; }
    irs::token_stream& get_tokens() { return token_stream; }
    std::string_view& name() const {
      static std::string_view value("test_field");
      return value;
    }
    bool write(irs::data_output& out) {
      irs::write_string(out, name());
      return true;
    }
  };

  irs::boolean_token_stream stream;
  stream.reset(true);
  field_t field(stream);

  auto column_info = default_column_info();
  auto feature_info = default_feature_info();

  irs::memory_directory dir;
  auto writer =
    irs::segment_writer::make(dir, column_info, feature_info, nullptr);
  ASSERT_EQ(0, writer->memory_active());

  // store + store sorted
  {
    irs::segment_writer::update_context ctx;
    writer->begin(ctx);
    ASSERT_TRUE(writer->valid());
    ASSERT_FALSE(
      writer->insert<irs::Action(int(irs::Action::STORE) |
                                 int(irs::Action::STORE_SORTED))>(field));
    ASSERT_FALSE(writer->valid());
    writer->commit();
  }

  // store + store sorted
  {
    irs::segment_writer::update_context ctx;
    writer->begin(ctx);
    ASSERT_TRUE(writer->valid());
    ASSERT_FALSE(
      writer
        ->insert<irs::Action(int(irs::Action::INDEX) | int(irs::Action::STORE) |
                             int(irs::Action::STORE_SORTED))>(field));
    ASSERT_FALSE(writer->valid());
    writer->commit();
  }

  ASSERT_LT(0, writer->memory_active());

  writer->reset();

  ASSERT_EQ(0, writer->memory_active());
}

#endif

TEST_F(segment_writer_tests, memory_sorted_vs_unsorted) {
  struct field_t {
    const std::string_view& name() const {
      static const std::string_view value("test_field");
      return value;
    }

    bool write(irs::data_output& out) const {
      irs::write_string(out, name());
      return true;
    }
  } field;

  struct comparator final : irs::comparer {
    bool less(irs::bytes_view lhs,
              irs::bytes_view rhs) const noexcept override {
      return lhs < rhs;
    }
  } less;

  auto column_info = default_column_info();
  auto feature_info = default_feature_info();

  irs::memory_directory dir;

  auto writer_sorted =
    irs::segment_writer::make(dir, column_info, feature_info, &less);
  ASSERT_EQ(0, writer_sorted->memory_active());
  auto writer_unsorted =
    irs::segment_writer::make(dir, column_info, feature_info, nullptr);
  ASSERT_EQ(0, writer_unsorted->memory_active());

  irs::segment_meta segment;
  segment.name = "foo";
  segment.codec = irs::formats::get("1_1", "1_0");
  writer_sorted->reset(segment);
  ASSERT_EQ(0, writer_sorted->memory_active());
  writer_unsorted->reset(segment);
  ASSERT_EQ(0, writer_unsorted->memory_active());

  for (size_t i = 0; i < 100; ++i) {
    irs::segment_writer::update_context ctx;
    writer_sorted->begin(ctx);
    ASSERT_TRUE(writer_sorted->valid());
    ASSERT_TRUE(writer_sorted->insert<irs::Action::STORE>(field));
    ASSERT_TRUE(writer_sorted->valid());
    writer_sorted->commit();

    writer_unsorted->begin(ctx);
    ASSERT_TRUE(writer_unsorted->valid());
    ASSERT_TRUE(writer_unsorted->insert<irs::Action::STORE>(field));
    ASSERT_TRUE(writer_unsorted->valid());
    writer_unsorted->commit();
  }

  ASSERT_GT(writer_sorted->memory_active(), 0);
  ASSERT_GT(writer_unsorted->memory_active(), 0);

  // we don't count stored field without comparator
  ASSERT_LT(writer_unsorted->memory_active(), writer_sorted->memory_active());

  writer_sorted->reset();
  ASSERT_EQ(0, writer_sorted->memory_active());

  writer_unsorted->reset();
  ASSERT_EQ(0, writer_unsorted->memory_active());
}

TEST_F(segment_writer_tests, insert_sorted_without_comparator) {
  struct field_t {
    const std::string_view& name() const {
      static const std::string_view value("test_field");
      return value;
    }

    bool write(irs::data_output& out) const {
      irs::write_string(out, name());
      return true;
    }
  } field;

  decltype(default_column_info()) column_info = [](const std::string_view&) {
    return irs::column_info{
      irs::type<irs::compression::lz4>::get(),
      irs::compression::options(irs::compression::options::Hint::SPEED), true};
  };
  auto feature_info = default_feature_info();

  irs::memory_directory dir;
  auto writer =
    irs::segment_writer::make(dir, column_info, feature_info, nullptr);
  ASSERT_EQ(0, writer->memory_active());

  irs::segment_meta segment;
  segment.name = "foo";
  segment.codec = irs::formats::get("1_1", "1_0");
  writer->reset(segment);
  ASSERT_EQ(0, writer->memory_active());

  for (size_t i = 0; i < 100; ++i) {
    irs::segment_writer::update_context ctx;
    writer->begin(ctx);
    ASSERT_TRUE(writer->valid());
    ASSERT_FALSE(writer->insert<irs::Action::STORE_SORTED>(field));
    ASSERT_FALSE(writer->valid());
    writer->commit();
  }

  // we don't count stored field without comparator
  ASSERT_GT(writer->memory_active(), 0);

  writer->reset();

  ASSERT_EQ(0, writer->memory_active());
}

TEST_F(segment_writer_tests, memory_store_sorted_field) {
  struct field_t {
    const std::string_view& name() const {
      static const std::string_view value("test_field");
      return value;
    }

    bool write(irs::data_output& out) const {
      irs::write_string(out, name());
      return true;
    }
  } field;

  struct comparator final : irs::comparer {
    bool less(irs::bytes_view lhs,
              irs::bytes_view rhs) const noexcept override {
      return lhs < rhs;
    }
  } less;

  auto column_info = default_column_info();
  auto feature_info = default_feature_info();

  irs::memory_directory dir;
  auto writer =
    irs::segment_writer::make(dir, column_info, feature_info, &less);
  ASSERT_EQ(0, writer->memory_active());

  irs::segment_meta segment;
  segment.name = "foo";
  segment.codec = irs::formats::get("1_1", "1_0");
  writer->reset(segment);
  ASSERT_EQ(0, writer->memory_active());

  for (size_t i = 0; i < 100; ++i) {
    irs::segment_writer::update_context ctx;
    writer->begin(ctx);
    ASSERT_TRUE(writer->valid());
    ASSERT_TRUE(writer->insert<irs::Action::STORE_SORTED>(field));
    ASSERT_TRUE(writer->valid());
    writer->commit();
  }

  // we don't count stored field without comparator
  ASSERT_GT(writer->memory_active(), 0);

  writer->reset();

  ASSERT_EQ(0, writer->memory_active());
}

TEST_F(segment_writer_tests, memory_store_field_sorted) {
  struct field_t {
    const std::string_view& name() const {
      static const std::string_view value("test_field");
      return value;
    }

    bool write(irs::data_output& out) const {
      irs::write_string(out, name());
      return true;
    }
  } field;

  struct comparator final : irs::comparer {
    bool less(irs::bytes_view lhs,
              irs::bytes_view rhs) const noexcept override {
      return lhs < rhs;
    }
  } less;

  auto column_info = default_column_info();
  auto feature_info = default_feature_info();

  irs::memory_directory dir;
  auto writer =
    irs::segment_writer::make(dir, column_info, feature_info, &less);
  ASSERT_EQ(0, writer->memory_active());

  irs::segment_meta segment;
  segment.name = "foo";
  segment.codec = irs::formats::get("1_1", "1_0");
  writer->reset(segment);
  ASSERT_EQ(0, writer->memory_active());

  for (size_t i = 0; i < 100; ++i) {
    irs::segment_writer::update_context ctx;
    writer->begin(ctx);
    ASSERT_TRUE(writer->valid());
    ASSERT_TRUE(writer->insert<irs::Action::STORE>(field));
    ASSERT_TRUE(writer->valid());
    writer->commit();
  }

  // we don't count stored field without comparator
  ASSERT_GT(writer->memory_active(), 0);

  writer->reset();

  ASSERT_EQ(0, writer->memory_active());
}

TEST_F(segment_writer_tests, memory_store_field_unsorted) {
  struct field_t {
    const std::string_view& name() const {
      static const std::string_view value("test_field");
      return value;
    }

    bool write(irs::data_output& out) const {
      irs::write_string(out, name());
      return true;
    }
  } field;

  auto column_info = default_column_info();
  auto feature_info = default_feature_info();

  irs::memory_directory dir;
  auto writer =
    irs::segment_writer::make(dir, column_info, feature_info, nullptr);
  ASSERT_EQ(0, writer->memory_active());

  irs::segment_meta segment;
  segment.name = "foo";
  segment.codec = irs::formats::get("1_1", "1_0");
  writer->reset(segment);
  ASSERT_EQ(0, writer->memory_active());

  for (size_t i = 0; i < 100; ++i) {
    irs::segment_writer::update_context ctx;
    writer->begin(ctx);
    ASSERT_TRUE(writer->valid());
    ASSERT_TRUE(writer->insert<irs::Action::STORE>(field));
    ASSERT_TRUE(writer->valid());
    writer->commit();
  }

  ASSERT_GT(writer->memory_active(), 0);

  writer->reset();

  ASSERT_EQ(0, writer->memory_active());
}

TEST_F(segment_writer_tests, memory_index_field) {
  struct field_t {
    irs::token_stream& token_stream;
    field_t(irs::token_stream& stream) : token_stream(stream) {}
    irs::features_t features() const { return {}; }
    irs::IndexFeatures index_features() const {
      return irs::IndexFeatures::NONE;
    }
    irs::token_stream& get_tokens() { return token_stream; }
    std::string_view& name() const {
      static std::string_view value("test_field");
      return value;
    }
  };

  irs::boolean_token_stream stream;
  stream.reset(true);
  field_t field(stream);

  auto column_info = default_column_info();
  auto feature_info = default_feature_info();

  irs::segment_meta segment;
  segment.name = "tmp";
  segment.codec = irs::formats::get("1_0");
  ASSERT_NE(nullptr, segment.codec);

  irs::memory_directory dir;
  auto writer =
    irs::segment_writer::make(dir, column_info, feature_info, nullptr);
  writer->reset(segment);

  ASSERT_EQ(0, writer->memory_active());

  for (size_t i = 0; i < 100; ++i) {
    irs::segment_writer::update_context ctx;
    writer->begin(ctx);
    ASSERT_TRUE(writer->valid());
    ASSERT_TRUE(writer->insert<irs::Action::INDEX>(field));
    ASSERT_TRUE(writer->valid());
    writer->commit();
  }

  ASSERT_LT(0, writer->memory_active());

  writer->reset();

  ASSERT_EQ(0, writer->memory_active());
}

TEST_F(segment_writer_tests, index_field) {
  struct field_t {
    irs::token_stream& token_stream;
    field_t(irs::token_stream& stream) : token_stream(stream) {}
    float_t boost() const { return 1.f; }
    irs::IndexFeatures index_features() const {
      return irs::IndexFeatures::NONE;
    }
    irs::features_t features() const { return {}; }
    irs::token_stream& get_tokens() { return token_stream; }
    std::string_view& name() const {
      static std::string_view value("test_field");
      return value;
    }
  };

  auto column_info = default_column_info();
  auto feature_info = default_feature_info();

  // test missing token_stream attributes (increment)
  {
    irs::segment_meta segment;
    segment.name = "tmp";
    segment.codec = irs::formats::get("1_0");
    ASSERT_NE(nullptr, segment.codec);

    irs::memory_directory dir;
    auto writer =
      irs::segment_writer::make(dir, column_info, feature_info, nullptr);
    writer->reset(segment);

    irs::segment_writer::update_context ctx;
    token_stream_mock stream;
    field_t field(stream);
    irs::term_attribute term;

    stream.attrs[irs::type<irs::term_attribute>::id()] = &term;
    stream.token_count = 10;

    writer->begin(ctx);
    ASSERT_TRUE(writer->valid());
    ASSERT_FALSE(writer->insert<irs::Action::INDEX>(field));
    ASSERT_FALSE(writer->valid());
    writer->commit();
  }

  // test missing token_stream attributes (term_attribute)
  {
    irs::segment_meta segment;
    segment.name = "tmp";
    segment.codec = irs::formats::get("1_0");
    ASSERT_NE(nullptr, segment.codec);

    irs::memory_directory dir;
    auto writer =
      irs::segment_writer::make(dir, column_info, feature_info, nullptr);
    writer->reset(segment);

    irs::segment_writer::update_context ctx;
    token_stream_mock stream;
    field_t field(stream);
    irs::increment inc;

    stream.attrs[irs::type<irs::increment>::id()] = &inc;
    stream.token_count = 10;

    writer->begin(ctx);
    ASSERT_TRUE(writer->valid());
    ASSERT_FALSE(writer->insert<irs::Action::INDEX>(field));
    ASSERT_FALSE(writer->valid());
    writer->commit();
  }
}

struct StringComparer final : irs::comparer {
  bool less(irs::bytes_view lhs, irs::bytes_view rhs) const final {
    if (lhs.empty() && rhs.empty()) {
      return true;
    } else if (rhs.empty()) {
      return false;
    } else if (lhs.empty()) {
      return true;
    }

    const auto lhs_value = irs::to_string<irs::bytes_view>(lhs.data());
    const auto rhs_value = irs::to_string<irs::bytes_view>(rhs.data());

    return lhs_value < rhs_value;
  }
};

void reorder(std::span<tests::document const*> docs,
             std::span<irs::segment_writer::update_context> ctxs,
             std::vector<size_t> order) {
  for (size_t i = 0; i < order.size(); ++i) {
    auto new_i = order[i];
    while (i != new_i) {
      std::swap(docs[i], docs[new_i]);
      std::swap(ctxs[i], ctxs[new_i]);
      std::swap(new_i, order[new_i]);
    }
  }
}

TEST_F(segment_writer_tests, reorder) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [](tests::document& doc, std::string_view name,
       const tests::json_doc_generator::json_value& data) {
      if (name == "name" && data.is_string()) {
        auto field = std::make_shared<tests::string_field>(name, data.str);
        doc.sorted = field;
        doc.insert(field);
      }
    });
  static constexpr size_t kLen = 5;
  std::array<tests::document const*, kLen> docs;
  std::array<irs::segment_writer::update_context, kLen> ctxs;
  for (size_t i = 0; i < kLen; ++i) {
    docs[i] = gen.next();
    ctxs[i] = {i, i};
  }
  const std::vector<size_t> expected{0, 1, 2, 3, 4};
  auto cases = std::array<std::vector<size_t>, 5>{
    std::vector<size_t>{0, 1, 2, 3, 4},  // no reorder
    std::vector<size_t>{2, 3, 1, 4, 0},  // single cycle
    std::vector<size_t>{3, 0, 4, 1, 2},  // two intersected cycles
    std::vector<size_t>{4, 0, 3, 2, 1},  // two nested cycles
    std::vector<size_t>{2, 0, 1, 4, 3},  // two not intersected cycles
  };

  for (auto& order : cases) {
    reorder(docs, ctxs, order);

    auto column_info = default_column_info();
    auto feature_info = default_feature_info();
    StringComparer less;

    irs::memory_directory dir;
    auto writer =
      irs::segment_writer::make(dir, column_info, feature_info, &less);
    ASSERT_EQ(0, writer->memory_active());

    irs::segment_meta segment;
    segment.name = "foo";
    segment.codec = default_codec();
    writer->reset(segment);
    ASSERT_EQ(0, writer->memory_active());

    for (size_t i = 0; i < kLen; ++i) {
      writer->begin(ctxs[i]);
      ASSERT_TRUE(writer->valid());
      ASSERT_TRUE(writer->insert<irs::Action::STORE_SORTED>(*docs[i]->sorted));
      ASSERT_TRUE(writer->valid());
      writer->commit();
    }

    // we don't count stored field without comparator
    ASSERT_GT(writer->memory_active(), 0);
    irs::index_meta::index_segment_t index_segment;
    index_segment.meta.codec = default_codec();
    writer->flush(index_segment);
    auto docs_context = writer->docs_context();
    ASSERT_EQ(docs_context.size(), kLen);
    for (size_t i = 0; i < kLen; ++i) {
      EXPECT_EQ(expected[i], docs_context[i].generation);
      EXPECT_EQ(expected[i], docs_context[i].update_id);
    }

    writer->reset();

    ASSERT_EQ(0, writer->memory_active());
  }
}
