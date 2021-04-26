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
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "tests_shared.hpp"
#include "tests_param.hpp"

#include "formats/columnstore.hpp"

class columnstore_test_case : public virtual tests::directory_test_case_base<bool> {
 public:
  static std::string to_string(
      const testing::TestParamInfo<std::tuple<tests::dir_factory_f, bool>>& info) {
    tests::dir_factory_f factory;
    bool consolidation;
    std::tie(factory, consolidation) = info.param;

    if (consolidation) {
      return (*factory)(nullptr).second + "___consolidation";
    }

    return (*factory)(nullptr).second;
  }

  bool consolidation() const noexcept {
    auto& p = this->GetParam();
    return std::get<bool>(p);
  }
};

TEST_P(columnstore_test_case, reader_ctor) {
  irs::columns::reader reader;
  ASSERT_EQ(0, reader.size());
  ASSERT_EQ(nullptr, reader.column(0));
}

TEST_P(columnstore_test_case, empty_columnstore) {
  constexpr irs::doc_id_t MAX = 1;
  const irs::segment_meta meta("test", nullptr);

  irs::flush_state state;
  state.doc_count = MAX;
  state.name = meta.name;
  state.features = &irs::flags::empty_instance();

  irs::columns::writer writer(this->consolidation());
  writer.prepare(dir(), meta);
  writer.push_column({ irs::type<irs::compression::none>::get(), {}, false });
  writer.push_column({ irs::type<irs::compression::none>::get(), {}, false });
  ASSERT_FALSE(writer.commit(state));

  irs::columns::reader reader;
  ASSERT_FALSE(reader.prepare(dir(), meta));
}

TEST_P(columnstore_test_case, empty_column) {
  constexpr irs::doc_id_t MAX = 1;
  const irs::segment_meta meta("test", nullptr);

  irs::flush_state state;
  state.doc_count = MAX;
  state.name = meta.name;
  state.features = &irs::flags::empty_instance();

  irs::columns::writer writer(this->consolidation());
  writer.prepare(dir(), meta);
  auto [id0, handle0] = writer.push_column({ irs::type<irs::compression::none>::get(), {}, false });
  auto [id1, handle1] = writer.push_column({ irs::type<irs::compression::none>::get(), {}, false });
  auto [id2, handle2] = writer.push_column({ irs::type<irs::compression::none>::get(), {}, false });
  handle1(42).write_byte(42);
  ASSERT_TRUE(writer.commit(state));

  irs::columns::reader reader;
  ASSERT_TRUE(reader.prepare(dir(), meta));
  ASSERT_EQ(2, reader.size());

  // column 0
  {
    auto column = reader.column(0);
    ASSERT_NE(nullptr, column);
    auto it = column->iterator();
    ASSERT_NE(nullptr, it);
    ASSERT_TRUE(irs::doc_limits::eof(it->value()));
  }

  // column 1
  {
    auto column = reader.column(1);
    ASSERT_NE(nullptr, column);
    auto it = column->iterator();
    auto* document = irs::get<irs::document>(*it);
    ASSERT_NE(nullptr, document);
    auto* payload = irs::get<irs::payload>(*it);
    ASSERT_NE(nullptr, payload);
    auto* cost = irs::get<irs::cost>(*it);
    ASSERT_NE(nullptr, cost);
    ASSERT_EQ(column->size(), cost->estimate());
    ASSERT_NE(nullptr, it);
    ASSERT_FALSE(irs::doc_limits::valid(it->value()));
    ASSERT_TRUE(it->next());
    ASSERT_EQ(42, it->value());
    ASSERT_EQ(1, payload->value.size());
    ASSERT_EQ(42, payload->value[0]);
    ASSERT_FALSE(it->next());
    ASSERT_FALSE(it->next());
  }
}

TEST_P(columnstore_test_case, sparse_mask_column) {
  constexpr irs::doc_id_t MAX = 1000000;
  const irs::segment_meta meta("test", nullptr);

  irs::flush_state state;
  state.doc_count = MAX;
  state.name = meta.name;
  state.features = &irs::flags::empty_instance();

  {
    irs::columns::writer writer(this->consolidation());
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column({
      irs::type<irs::compression::none>::get(),
      {}, false });

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= MAX; doc += 2) {
      column(doc);
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columns::reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta));
    ASSERT_EQ(1, reader.size());

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(MAX/2, column->size());

    {
      auto it = column->iterator();
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_EQ(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= MAX; doc += 2) {
        ASSERT_EQ(doc, it->seek(doc));
      }
    }

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= MAX; doc += 2) {
      auto it = column->iterator();
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_EQ(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());

      ASSERT_EQ(doc, it->seek(doc));
    }
  }
}

TEST_P(columnstore_test_case, sparse_column) {
  constexpr irs::doc_id_t MAX = 1000000;
  const irs::segment_meta meta("test", nullptr);

  irs::flush_state state;
  state.doc_count = MAX;
  state.name = meta.name;
  state.features = &irs::flags::empty_instance();

  {
    irs::columns::writer writer(this->consolidation());
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column({
      irs::type<irs::compression::none>::get(),
      {}, false });

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= MAX; doc += 2) {
      auto& stream = column(doc);
      const auto str = std::to_string(doc);
      stream.write_bytes(reinterpret_cast<const irs::byte_type*>(str.c_str()), str.size());
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columns::reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta));
    ASSERT_EQ(1, reader.size());

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(MAX/2, column->size());

    {
      auto it = column->iterator();
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= MAX; doc += 2) {
        ASSERT_EQ(doc, it->seek(doc));
        const auto str = std::to_string(doc);
        EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
      }
    }

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= MAX; doc += 2) {
      auto it = column->iterator();
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());

      ASSERT_EQ(doc, it->seek(doc));
      const auto str = std::to_string(doc);
      EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
    }
  }
}

TEST_P(columnstore_test_case, dense_mask_column) {
  constexpr irs::doc_id_t MAX = 1000000;
  const irs::segment_meta meta("test", nullptr);

  irs::flush_state state;
  state.doc_count = MAX;
  state.name = meta.name;
  state.features = &irs::flags::empty_instance();

  {
    irs::columns::writer writer(this->consolidation());
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column({
      irs::type<irs::compression::none>::get(),
      {}, false });

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= MAX; ++doc) {
      column(doc);
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columns::reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta));
    ASSERT_EQ(1, reader.size());

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(MAX, column->size());

    {
      auto it = column->iterator();
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      ASSERT_TRUE(payload->value.null());
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= MAX; ++doc) {
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_TRUE(payload->value.null());
      }
    }

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= MAX; ++doc) {
      auto it = column->iterator();
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      ASSERT_TRUE(payload->value.null());
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());

      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_TRUE(payload->value.null());
    }
  }
}

TEST_P(columnstore_test_case, dense_column) {
  constexpr irs::doc_id_t MAX = 1000000;
  const irs::segment_meta meta("test", nullptr);

  irs::flush_state state;
  state.doc_count = MAX;
  state.name = meta.name;
  state.features = &irs::flags::empty_instance();

  {
    irs::columns::writer writer(this->consolidation());
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column({
      irs::type<irs::compression::none>::get(),
      {}, false });

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= MAX; ++doc) {
      auto& stream = column(doc);
      const auto str = std::to_string(doc);
      stream.write_bytes(reinterpret_cast<const irs::byte_type*>(str.c_str()), str.size());
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columns::reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta));
    ASSERT_EQ(1, reader.size());

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(MAX, column->size());

    {
      auto it = column->iterator();
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= MAX; ++doc) {
        ASSERT_EQ(doc, it->seek(doc));
        const auto str = std::to_string(doc);
        EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
      }
    }

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= MAX; ++doc) {
      auto it = column->iterator();
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());

      ASSERT_EQ(doc, it->seek(doc));
      const auto str = std::to_string(doc);
      EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
    }
  }
}

TEST_P(columnstore_test_case, dense_fixed_length_column) {
  constexpr irs::doc_id_t MAX = 1000000;
  const irs::segment_meta meta("test", nullptr);

  irs::flush_state state;
  state.doc_count = MAX;
  state.name = meta.name;
  state.features = &irs::flags::empty_instance();

  {
    irs::columns::writer writer(this->consolidation());
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column({
      irs::type<irs::compression::none>::get(),
      {}, false });

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= MAX; ++doc) {
      auto& stream = column(doc);
      const auto str = std::to_string(doc);
      stream.write_bytes(reinterpret_cast<const irs::byte_type*>(&doc), sizeof doc);
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columns::reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta));
    ASSERT_EQ(1, reader.size());

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(MAX, column->size());

    {
      auto it = column->iterator();
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= MAX; ++doc) {
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(sizeof doc, payload->value.size());
        const irs::doc_id_t actual_doc = *reinterpret_cast<const irs::doc_id_t*>(payload->value.c_str());
        EXPECT_EQ(doc, actual_doc);
      }
    }

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= MAX; ++doc) {
      auto it = column->iterator();
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());

      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_EQ(sizeof doc, payload->value.size());
      const irs::doc_id_t actual_doc = *reinterpret_cast<const irs::doc_id_t*>(payload->value.c_str());
      EXPECT_EQ(doc, actual_doc);
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
  columnstore_test,
  columnstore_test_case,
  ::testing::Combine(
    ::testing::Values(
      &tests::memory_directory,
      &tests::fs_directory,
      &tests::mmap_directory),
    ::testing::Values(false, true)),
  &columnstore_test_case::to_string
);
