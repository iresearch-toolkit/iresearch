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

#include "formats/columnstore2.hpp"

#include "search/score.hpp"
#include "tests_param.hpp"
#include "tests_shared.hpp"

using namespace irs::columnstore2;

class columnstore2_test_case
    : public virtual tests::directory_test_case_base<
          irs::ColumnHint, irs::columnstore2::Version> {
 public:
  static std::string to_string(const testing::TestParamInfo<ParamType>& info) {
    auto [factory, hint, version] = info.param;

    std::string name = (*factory)(nullptr).second;

    switch (hint) {
      case irs::ColumnHint::kNormal:
        break;
      case irs::ColumnHint::kConsolidation:
        name += "___consolidation";
        break;
      case irs::ColumnHint::kMask:
        name += "___mask";
        break;
      default:
        EXPECT_FALSE(true);
        break;
    }

    return name + "___" + std::to_string(static_cast<uint32_t>(version));
  }

  irs::columnstore2::Version version() const noexcept {
    auto& p = this->GetParam();
    return std::get<irs::columnstore2::Version>(p);
  }

  irs::ColumnHint hint() const noexcept {
    auto& p = this->GetParam();
    return std::get<irs::ColumnHint>(p);
  }

  void assert_prev_doc(irs::doc_iterator& it, irs::doc_iterator& prev_it) {
    auto prev_doc = [](irs::doc_iterator& it, irs::doc_id_t target) {
      auto doc = it.value();
      auto prev = 0;
      while (doc < target && it.next()) {
        prev = doc;
        doc = it.value();
      }
      return prev;
    };

    auto* prev = irs::get<irs::seek_prev>(it);
    ASSERT_EQ(version() >= irs::columnstore2::Version::kPrevSeek,
              nullptr != prev);
    if (prev) {
      ASSERT_EQ(prev_doc(prev_it, it.value()), (*prev)());
    }
  }
};

TEST_P(columnstore2_test_case, reader_ctor) {
  irs::columnstore2::reader reader;
  ASSERT_EQ(0, reader.size());
  ASSERT_EQ(nullptr, reader.column(0));
  ASSERT_EQ(nullptr, reader.header(0));
}

TEST_P(columnstore2_test_case, empty_columnstore) {
  constexpr irs::doc_id_t kMax = 1;
  const irs::segment_meta meta("test", nullptr);

  irs::flush_state state;
  state.doc_count = kMax;
  state.name = meta.name;

  auto finalizer = [](auto&) {
    // Must not be called
    EXPECT_FALSE(true);
    return irs::string_ref::NIL;
  };

  irs::columnstore2::writer writer(
      version(), this->hint() == irs::ColumnHint::kConsolidation);
  writer.prepare(dir(), meta);
  writer.push_column({irs::type<irs::compression::none>::get(), {}, false},
                     finalizer);
  writer.push_column({irs::type<irs::compression::none>::get(), {}, false},
                     finalizer);
  ASSERT_FALSE(writer.commit(state));

  irs::columnstore2::reader reader;
  ASSERT_FALSE(reader.prepare(dir(), meta));
}

TEST_P(columnstore2_test_case, empty_column) {
  constexpr irs::doc_id_t kMax = 1;
  const irs::segment_meta meta("test", nullptr);
  const bool has_encryption = bool(dir().attributes().encryption());

  irs::flush_state state;
  state.doc_count = kMax;
  state.name = meta.name;

  const irs::column_info info{
      irs::type<irs::compression::none>::get(), {}, has_encryption};

  irs::columnstore2::writer writer(
      version(), this->hint() == irs::ColumnHint::kConsolidation);
  writer.prepare(dir(), meta);
  [[maybe_unused]] auto [id0, handle0] =
      writer.push_column(info, [](irs::bstring& out) {
        EXPECT_TRUE(out.empty());
        out += 1;
        return "foobar";
      });
  [[maybe_unused]] auto [id1, handle1] =
      writer.push_column(info, [](irs::bstring& out) {
        EXPECT_TRUE(out.empty());
        out += 2;
        return irs::string_ref::NIL;
      });
  [[maybe_unused]] auto [id2, handle2] = writer.push_column(info, [](auto&) {
    // Must no be called
    EXPECT_TRUE(false);
    return irs::string_ref::NIL;
  });
  handle1(42).write_byte(42);
  ASSERT_TRUE(writer.commit(state));

  irs::columnstore2::reader reader;
  ASSERT_TRUE(reader.prepare(dir(), meta));
  ASSERT_EQ(2, reader.size());

  // column 0
  {
    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(0, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::invalid(), header->min);
    ASSERT_EQ(ColumnType::kMask, header->type);
    ASSERT_EQ(
        has_encryption ? ColumnProperty::kEncrypt : ColumnProperty::kNormal,
        header->props);

    auto column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(0, column->id());
    ASSERT_EQ("foobar", column->name());
    ASSERT_EQ(0, column->size());
    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(1, header_payload[0]);
    auto it = column->iterator(hint());
    ASSERT_NE(nullptr, it);
    ASSERT_EQ(0, irs::cost::extract(*it));
    ASSERT_TRUE(irs::doc_limits::eof(it->value()));
  }

  // column 1
  {
    auto* header = reader.header(1);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(1, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(42, header->min);
    ASSERT_EQ(ColumnType::kSparse, header->type);  // FIXME why sparse?
    ASSERT_EQ(has_encryption
                  ? (ColumnProperty::kEncrypt | ColumnProperty::kNoName)
                  : ColumnProperty::kNoName,
              header->props);

    auto column = reader.column(1);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(1, column->id());
    ASSERT_TRUE(column->name().null());
    ASSERT_EQ(1, column->size());
    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(2, header_payload[0]);
    auto it = column->iterator(hint());
    auto* document = irs::get<irs::document>(*it);
    ASSERT_NE(nullptr, document);
    auto* payload = irs::get<irs::payload>(*it);
    ASSERT_NE(nullptr, payload);
    auto* cost = irs::get<irs::cost>(*it);
    auto* prev = irs::get<irs::seek_prev>(*it);
    ASSERT_NE(nullptr, prev);
    ASSERT_NE(nullptr, cost);
    ASSERT_EQ(column->size(), cost->estimate());
    ASSERT_NE(nullptr, it);
    ASSERT_FALSE(irs::doc_limits::valid(it->value()));
    ASSERT_TRUE(it->next());
    if (prev) {
      ASSERT_EQ(0, (*prev)());
    }
    ASSERT_EQ(42, it->value());
    ASSERT_EQ(1, payload->value.size());
    ASSERT_EQ(42, payload->value[0]);
    ASSERT_FALSE(it->next());
    ASSERT_FALSE(it->next());
  }
}

TEST_P(columnstore2_test_case, sparse_mask_column) {
  constexpr irs::doc_id_t kMax = 1000000;
  const irs::segment_meta meta("test", nullptr);
  const bool has_encryption = bool(dir().attributes().encryption());

  irs::flush_state state;
  state.doc_count = kMax;
  state.name = meta.name;

  {
    irs::columnstore2::writer writer(
        version(), this->hint() == irs::ColumnHint::kConsolidation);
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column(
        {irs::type<irs::compression::none>::get(), {}, has_encryption},
        [](irs::bstring& out) {
          EXPECT_TRUE(out.empty());
          out += 42;
          return irs::string_ref::NIL;
        });

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
      column(doc);
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columnstore2::reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta));
    ASSERT_EQ(1, reader.size());

    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax / 2, header->docs_count);
    ASSERT_NE(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::kMask, header->type);
    ASSERT_EQ(has_encryption
                  ? (ColumnProperty::kEncrypt | ColumnProperty::kNoName)
                  : ColumnProperty::kNoName,
              header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax / 2, column->size());
    ASSERT_EQ(0, column->id());
    ASSERT_TRUE(column->name().null());

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    // seek stateful
    {
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_EQ(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(doc, it->seek(doc));
      }
    }

    // seek stateless
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
      auto it = column->iterator(hint());
      auto* prev = irs::get<irs::seek_prev>(*it);
      ASSERT_EQ(version() >= irs::columnstore2::Version::kPrevSeek,
                nullptr != prev);
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_EQ(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_EQ(doc, it->seek(doc));
    }

    // seek + next
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 5000) {
      auto prev_it = column->iterator(hint());
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_EQ(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_EQ(doc, it->seek(doc - 1));

      assert_prev_doc(*it, *prev_it);

      auto next_it = column->iterator(hint());
      auto* prev = irs::get<irs::seek_prev>(*next_it);
      ASSERT_EQ(version() >= irs::columnstore2::Version::kPrevSeek,
                nullptr != prev);
      ASSERT_EQ(doc, next_it->seek(doc));
      for (auto next_doc = doc + 2; next_doc <= kMax; next_doc += 2) {
        ASSERT_TRUE(next_it->next());
        ASSERT_EQ(next_doc, next_it->value());
        assert_prev_doc(*next_it, *prev_it);
      }
    }

    // next + seek
    {
      auto prev_it = column->iterator(hint());
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_EQ(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);
      ASSERT_TRUE(it->next());
      assert_prev_doc(*it, *prev_it);
      ASSERT_EQ(irs::doc_limits::min(), document->value);
      ASSERT_EQ(118775, it->seek(118774));
      assert_prev_doc(*it, *prev_it);
      ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
    }
  }
}

TEST_P(columnstore2_test_case, sparse_column) {
  constexpr irs::doc_id_t kMax = 1000000;
  const irs::segment_meta meta("test", nullptr);
  const bool has_encryption = bool(dir().attributes().encryption());

  irs::flush_state state;
  state.doc_count = kMax;
  state.name = meta.name;

  {
    irs::columnstore2::writer writer(
        version(), this->hint() == irs::ColumnHint::kConsolidation);
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column(
        {irs::type<irs::compression::none>::get(), {}, has_encryption},
        [](irs::bstring& out) {
          EXPECT_TRUE(out.empty());
          out += 42;
          return "foobaz";
        });

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
      auto& stream = column(doc);
      const auto str = std::to_string(doc);
      stream.write_bytes(reinterpret_cast<const irs::byte_type*>(str.c_str()),
                         str.size());
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columnstore2::reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta));
    ASSERT_EQ(1, reader.size());

    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax / 2, header->docs_count);
    ASSERT_NE(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::kSparse, header->type);
    ASSERT_EQ(
        has_encryption ? ColumnProperty::kEncrypt : ColumnProperty::kNormal,
        header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax / 2, column->size());
    ASSERT_EQ(0, column->id());
    ASSERT_EQ("foobaz", column->name());

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    auto assert_iterator = [&](irs::ColumnHint hint) {
      {
        auto prev_it = column->iterator(hint);
        auto it = column->iterator(hint);
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        const irs::payload* payload = nullptr;
        if (hint != irs::ColumnHint::kMask) {
          payload = irs::get<irs::payload>(*it);
          ASSERT_NE(nullptr, payload);
        } else {
          ASSERT_EQ(nullptr, irs::get<irs::payload>(*it));
        }
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::score>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

        for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
             doc += 2) {
          SCOPED_TRACE(doc);
          ASSERT_EQ(doc, it->seek(doc));
          ASSERT_EQ(doc, it->seek(doc));
          const auto str = std::to_string(doc);
          if (payload) {
            ASSERT_EQ(str, irs::ref_cast<char>(payload->value));
          }
          assert_prev_doc(*it, *prev_it);
        }
      }

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
        auto it = column->iterator(hint);
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        const irs::payload* payload = nullptr;
        if (hint != irs::ColumnHint::kMask) {
          payload = irs::get<irs::payload>(*it);
          ASSERT_NE(nullptr, payload);
        } else {
          ASSERT_EQ(nullptr, irs::get<irs::payload>(*it));
        }
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::score>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

        const auto str = std::to_string(doc);
        ASSERT_EQ(doc, it->seek(doc));
        if (payload) {
          EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
        }
        ASSERT_EQ(doc, it->seek(doc));
        if (payload) {
          EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
        }
        ASSERT_EQ(doc, it->seek(doc - 1));
        if (payload) {
          EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
        }
      }

      // seek + next
      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
           doc += 5000) {
        auto prev_it = column->iterator(hint);
        auto it = column->iterator(hint);
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        const irs::payload* payload = nullptr;
        if (hint != irs::ColumnHint::kMask) {
          payload = irs::get<irs::payload>(*it);
          ASSERT_NE(nullptr, payload);
        } else {
          ASSERT_EQ(nullptr, irs::get<irs::payload>(*it));
        }
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::score>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

        const auto str = std::to_string(doc);
        ASSERT_EQ(doc, it->seek(doc));
        if (payload) {
          EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
        }
        ASSERT_EQ(doc, it->seek(doc));
        if (payload) {
          EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
        }
        ASSERT_EQ(doc, it->seek(doc - 1));
        if (payload) {
          EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
        }
        assert_prev_doc(*it, *prev_it);

        auto next_it = column->iterator(hint);
        ASSERT_NE(nullptr, next_it);
        const irs::payload* next_payload = nullptr;
        if (hint != irs::ColumnHint::kMask) {
          next_payload = irs::get<irs::payload>(*next_it);
          ASSERT_NE(nullptr, next_payload);
        } else {
          ASSERT_EQ(nullptr, irs::get<irs::payload>(*next_it));
        }
        ASSERT_EQ(doc, next_it->seek(doc));
        if (next_payload) {
          EXPECT_EQ(str, irs::ref_cast<char>(next_payload->value));
        }
        for (auto next_doc = doc + 2; next_doc <= kMax; next_doc += 2) {
          ASSERT_TRUE(next_it->next());
          ASSERT_EQ(next_doc, next_it->value());
          const auto str = std::to_string(next_doc);
          if (next_payload) {
            EXPECT_EQ(str, irs::ref_cast<char>(next_payload->value));
          }
          assert_prev_doc(*next_it, *prev_it);
        }
      }

      // next + seek
      {
        auto prev_it = column->iterator(hint);
        auto it = column->iterator(hint);
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        const irs::payload* payload = nullptr;
        if (hint != irs::ColumnHint::kMask) {
          payload = irs::get<irs::payload>(*it);
          ASSERT_NE(nullptr, payload);
        } else {
          ASSERT_EQ(nullptr, irs::get<irs::payload>(*it));
        }
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::score>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);
        ASSERT_TRUE(it->next());
        ASSERT_EQ(irs::doc_limits::min(), document->value);
        assert_prev_doc(*it, *prev_it);
        ASSERT_EQ(118775, it->seek(118774));
        const auto str = std::to_string(it->value());
        if (payload) {
          EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
        }
        assert_prev_doc(*it, *prev_it);
        ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
      }
    };

    assert_iterator(hint());
    assert_iterator(irs::ColumnHint::kMask);
  }
}

TEST_P(columnstore2_test_case, sparse_column_gap) {
  static constexpr irs::doc_id_t kMax = 500000;
  static constexpr auto kBlockSize = irs::sparse_bitmap_writer::kBlockSize;
  static constexpr auto kGapBegin = ((kMax / kBlockSize) - 4) * kBlockSize;
  const irs::segment_meta meta("test", nullptr);
  const bool has_encryption = bool(dir().attributes().encryption());

  irs::flush_state state;
  state.doc_count = kMax;
  state.name = meta.name;

  {
    irs::columnstore2::writer writer(
        version(), this->hint() == irs::ColumnHint::kConsolidation);
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column(
        {irs::type<irs::compression::none>::get(), {}, has_encryption},
        [](irs::bstring& out) {
          EXPECT_TRUE(out.empty());
          out += 42;
          return "foobarbaz";
        });

    auto write_payload = [](irs::doc_id_t doc, irs::data_output& stream) {
      if (doc <= kGapBegin || doc > (kGapBegin + kBlockSize)) {
        stream.write_bytes(reinterpret_cast<const irs::byte_type*>(&doc),
                           sizeof doc);
      }
    };

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      write_payload(doc, column(doc));
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    auto assert_payload = [](irs::doc_id_t doc, irs::bytes_ref payload) {
      SCOPED_TRACE(doc);
      if (doc <= kGapBegin || doc > (kGapBegin + kBlockSize)) {
        ASSERT_EQ(sizeof doc, payload.size());
        const irs::doc_id_t actual_doc =
            *reinterpret_cast<const irs::doc_id_t*>(payload.c_str());
        ASSERT_EQ(doc, actual_doc);
      } else {
        ASSERT_TRUE(payload.empty());
      }
    };

    irs::columnstore2::reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta));
    ASSERT_EQ(1, reader.size());

    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::kSparse, header->type);
    ASSERT_EQ(
        has_encryption ? ColumnProperty::kEncrypt : ColumnProperty::kNormal,
        header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax, column->size());

    ASSERT_EQ(0, column->id());
    ASSERT_EQ("foobarbaz", column->name());

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    {
      auto prev_it = column->iterator(hint());
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        SCOPED_TRACE(doc);
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(doc, it->seek(doc));
        assert_payload(doc, payload->value);
        assert_prev_doc(*it, *prev_it);
      }
    }

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      const auto str = std::to_string(doc);
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc - 1));
      assert_payload(doc, payload->value);
    }

    // seek + next
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 5000) {
      auto prev_it = column->iterator(hint());
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      const auto str = std::to_string(doc);
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc - 1));
      assert_payload(doc, payload->value);
      assert_prev_doc(*it, *prev_it);

      auto next_it = column->iterator(hint());
      auto* next_payload = irs::get<irs::payload>(*next_it);
      ASSERT_NE(nullptr, next_payload);
      ASSERT_EQ(doc, next_it->seek(doc));
      assert_payload(doc, next_payload->value);
      for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
        ASSERT_TRUE(next_it->next());
        ASSERT_EQ(next_doc, next_it->value());
        assert_payload(next_doc, next_payload->value);
        assert_prev_doc(*next_it, *prev_it);
      }
    }

    // next + seek
    {
      auto prev_it = column->iterator(hint());
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);
      ASSERT_TRUE(it->next());
      ASSERT_EQ(irs::doc_limits::min(), document->value);
      assert_prev_doc(*it, *prev_it);
      ASSERT_EQ(118775, it->seek(118775));
      assert_payload(118775, payload->value);
      assert_prev_doc(*it, *prev_it);
      ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
    }
  }
}

TEST_P(columnstore2_test_case, sparse_column_tail_block) {
  static constexpr irs::doc_id_t kMax = 500000;
  static constexpr auto kBlockSize = irs::sparse_bitmap_writer::kBlockSize;
  static constexpr auto kTailBegin = (kMax / kBlockSize) * kBlockSize;
  const irs::segment_meta meta("test", nullptr);
  const bool has_encryption = bool(dir().attributes().encryption());

  irs::flush_state state;
  state.doc_count = kMax;
  state.name = meta.name;

  {
    auto write_payload = [](irs::doc_id_t doc, irs::data_output& stream) {
      stream.write_bytes(reinterpret_cast<const irs::byte_type*>(&doc),
                         sizeof doc);
      if (doc > kTailBegin) {
        stream.write_bytes(reinterpret_cast<const irs::byte_type*>(&doc),
                           sizeof doc);
      }
    };

    irs::columnstore2::writer writer(
        version(), this->hint() == irs::ColumnHint::kConsolidation);
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column(
        {irs::type<irs::compression::none>::get(), {}, has_encryption},
        [](irs::bstring& out) {
          EXPECT_TRUE(out.empty());
          out += 42;
          return irs::string_ref::NIL;
        });

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      write_payload(doc, column(doc));
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columnstore2::reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta));
    ASSERT_EQ(1, reader.size());

    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::kSparse, header->type);
    ASSERT_EQ(has_encryption
                  ? (ColumnProperty::kEncrypt | ColumnProperty::kNoName)
                  : ColumnProperty::kNoName,
              header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax, column->size());

    ASSERT_EQ(0, column->id());
    ASSERT_TRUE(column->name().null());

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    auto assert_payload = [](irs::doc_id_t doc, irs::bytes_ref payload) {
      SCOPED_TRACE(doc);
      ASSERT_FALSE(payload.empty());
      if (doc > kTailBegin) {
        ASSERT_EQ(2 * sizeof doc, payload.size());
        const irs::doc_id_t* actual_doc =
            reinterpret_cast<const irs::doc_id_t*>(payload.c_str());
        ASSERT_EQ(doc, actual_doc[0]);
        ASSERT_EQ(doc, actual_doc[1]);
      } else {
        ASSERT_EQ(sizeof doc, payload.size());
        const irs::doc_id_t actual_doc =
            *reinterpret_cast<const irs::doc_id_t*>(payload.c_str());
        ASSERT_EQ(doc, actual_doc);
      }
    };

    {
      auto prev_it = column->iterator(hint());
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(doc, it->seek(doc));
        assert_payload(doc, payload->value);
        assert_prev_doc(*it, *prev_it);
      }
    }

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
    }

    // seek + next
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
         doc += 10000) {
      auto prev_it = column->iterator(hint());
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc - 1));
      assert_payload(doc, payload->value);
      assert_prev_doc(*it, *prev_it);

      auto next_it = column->iterator(hint());
      auto* next_payload = irs::get<irs::payload>(*next_it);
      ASSERT_NE(nullptr, next_payload);
      ASSERT_EQ(doc, next_it->seek(doc));
      assert_payload(doc, next_payload->value);
      for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
        ASSERT_TRUE(next_it->next());
        assert_payload(next_doc, next_payload->value);
        assert_prev_doc(*next_it, *prev_it);
      }
    }

    // next + seek
    {
      constexpr irs::doc_id_t doc = 118774;

      auto prev_it = column->iterator(hint());
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      ASSERT_TRUE(it->next());
      ASSERT_EQ(irs::doc_limits::min(), document->value);
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      assert_prev_doc(*it, *prev_it);
      ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
    }
  }
}

TEST_P(columnstore2_test_case, sparse_column_tail_block_last_value) {
  static constexpr irs::doc_id_t kMax = 500000;
  // last value has different length
  static constexpr auto kTailBegin = kMax - 1;
  const irs::segment_meta meta("test", nullptr);
  const bool has_encryption = bool(dir().attributes().encryption());

  irs::flush_state state;
  state.doc_count = kMax;
  state.name = meta.name;

  {
    auto write_payload = [](irs::doc_id_t doc, irs::data_output& stream) {
      stream.write_bytes(reinterpret_cast<const irs::byte_type*>(&doc),
                         sizeof doc);
      if (doc > kTailBegin) {
        stream.write_byte(42);
      }
    };

    irs::columnstore2::writer writer(
        version(), this->hint() == irs::ColumnHint::kConsolidation);
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column(
        {irs::type<irs::compression::none>::get(), {}, has_encryption},
        [](irs::bstring& out) {
          EXPECT_TRUE(out.empty());
          out += 42;
          return irs::string_ref::NIL;
        });

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      write_payload(doc, column(doc));
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columnstore2::reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta));
    ASSERT_EQ(1, reader.size());

    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::kSparse, header->type);
    ASSERT_EQ(has_encryption
                  ? (ColumnProperty::kEncrypt | ColumnProperty::kNoName)
                  : ColumnProperty::kNoName,
              header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax, column->size());

    ASSERT_EQ(0, column->id());
    ASSERT_TRUE(column->name().null());

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    auto assert_payload = [](irs::doc_id_t doc, irs::bytes_ref payload) {
      SCOPED_TRACE(doc);
      ASSERT_FALSE(payload.empty());
      if (doc > kTailBegin) {
        ASSERT_EQ(1 + sizeof doc, payload.size());
        const irs::doc_id_t actual_doc =
            *reinterpret_cast<const irs::doc_id_t*>(payload.c_str());
        ASSERT_EQ(doc, actual_doc);
        ASSERT_EQ(42, payload[sizeof doc]);
      } else {
        ASSERT_EQ(sizeof doc, payload.size());
        const irs::doc_id_t actual_doc =
            *reinterpret_cast<const irs::doc_id_t*>(payload.c_str());
        ASSERT_EQ(doc, actual_doc);
      }
    };

    {
      auto prev_it = column->iterator(hint());
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(doc, it->seek(doc));
        assert_payload(doc, payload->value);
        assert_prev_doc(*it, *prev_it);
      }
    }

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
    }

    // seek + next
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
         doc += 10000) {
      auto prev_it = column->iterator(hint());
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc - 1));
      assert_payload(doc, payload->value);
      assert_prev_doc(*it, *prev_it);

      auto next_it = column->iterator(hint());
      auto* next_payload = irs::get<irs::payload>(*next_it);
      ASSERT_NE(nullptr, next_payload);
      ASSERT_EQ(doc, next_it->seek(doc));
      assert_payload(doc, next_payload->value);
      for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
        ASSERT_TRUE(next_it->next());
        assert_payload(next_doc, next_payload->value);
        assert_prev_doc(*next_it, *prev_it);
      }
    }

    // next + seek
    {
      constexpr irs::doc_id_t doc = 118774;

      auto prev_it = column->iterator(hint());
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      ASSERT_TRUE(it->next());
      ASSERT_EQ(irs::doc_limits::min(), document->value);
      assert_prev_doc(*it, *prev_it);
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      assert_prev_doc(*it, *prev_it);
      ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
    }
  }
}

TEST_P(columnstore2_test_case, dense_mask_column) {
  constexpr irs::doc_id_t kMax = 1000000;
  const irs::segment_meta meta("test", nullptr);
  const bool has_encryption = bool(dir().attributes().encryption());

  irs::flush_state state;
  state.doc_count = kMax;
  state.name = meta.name;

  {
    irs::columnstore2::writer writer(
        version(), this->hint() == irs::ColumnHint::kConsolidation);
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column(
        {irs::type<irs::compression::none>::get(), {}, has_encryption},
        [](irs::bstring& out) {
          EXPECT_TRUE(out.empty());
          out += 42;
          return "foobar";
        });

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      column(doc);
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columnstore2::reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta));
    ASSERT_EQ(1, reader.size());

    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::kMask, header->type);
    ASSERT_EQ(
        has_encryption ? ColumnProperty::kEncrypt : ColumnProperty::kNormal,
        header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax, column->size());

    ASSERT_EQ(0, column->id());
    ASSERT_EQ("foobar", column->name());

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    {
      auto prev_it = column->iterator(hint());
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      ASSERT_TRUE(payload->value.null());
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_TRUE(payload->value.null());
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_TRUE(payload->value.null());
        ASSERT_EQ(doc, it->seek(doc - 1));
        ASSERT_TRUE(payload->value.null());
        assert_prev_doc(*it, *prev_it);
      }
    }

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      const auto hint = doc % 2 ? this->hint() : irs::ColumnHint::kMask;
      auto it = column->iterator(hint);
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      ASSERT_TRUE(payload->value.null());
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_TRUE(payload->value.null());
      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_TRUE(payload->value.null());
      ASSERT_EQ(doc, it->seek(doc - 1));
      ASSERT_TRUE(payload->value.null());
    }

    // seek + next
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
         doc += 10000) {
      auto prev_it = column->iterator(hint());
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_EQ(doc, it->seek(doc));
      assert_prev_doc(*it, *prev_it);

      auto next_it = column->iterator(hint());
      ASSERT_EQ(doc, next_it->seek(doc));
      for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
        ASSERT_TRUE(next_it->next());
        ASSERT_EQ(next_doc, next_it->value());
        assert_prev_doc(*next_it, *prev_it);
      }
    }

    // next + seek
    {
      auto prev_it = column->iterator(hint());
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);
      ASSERT_TRUE(it->next());
      ASSERT_EQ(irs::doc_limits::min(), document->value);
      assert_prev_doc(*it, *prev_it);
      ASSERT_TRUE(payload->value.null());
      ASSERT_EQ(118774, it->seek(118774));
      assert_prev_doc(*it, *prev_it);
      ASSERT_TRUE(payload->value.null());
      ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
      ASSERT_TRUE(irs::doc_limits::eof(it->seek(irs::doc_limits::eof())));
    }
  }
}

TEST_P(columnstore2_test_case, dense_column) {
  constexpr irs::doc_id_t kMax = 1000000;
  const irs::segment_meta meta("test", nullptr);
  const bool has_encryption = bool(dir().attributes().encryption());

  irs::flush_state state;
  state.doc_count = kMax;
  state.name = meta.name;

  {
    irs::columnstore2::writer writer(
        version(), this->hint() == irs::ColumnHint::kConsolidation);
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column(
        {irs::type<irs::compression::none>::get(), {}, has_encryption},
        [](irs::bstring& out) {
          EXPECT_TRUE(out.empty());
          out += 42;
          return "foobar";
        });

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      auto& stream = column(doc);
      const auto str = std::to_string(doc);
      stream.write_bytes(reinterpret_cast<const irs::byte_type*>(str.c_str()),
                         str.size());
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columnstore2::reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta));
    ASSERT_EQ(1, reader.size());

    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::kSparse, header->type);
    ASSERT_EQ(
        has_encryption ? ColumnProperty::kEncrypt : ColumnProperty::kNormal,
        header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax, column->size());

    ASSERT_EQ(0, column->id());
    ASSERT_EQ("foobar", column->name());

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    auto assert_iterator = [&](irs::ColumnHint hint) {
      {
        auto prev_it = column->iterator(hint);
        auto it = column->iterator(hint);
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::payload>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::score>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

        for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
          ASSERT_EQ(doc, it->seek(doc));
          ASSERT_EQ(doc, it->seek(doc));
          assert_prev_doc(*it, *prev_it);
          const auto str = std::to_string(doc);
          if (hint == irs::ColumnHint::kMask) {
            EXPECT_TRUE(payload->value.null());
          } else {
            EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
          }
        }
      }

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto it = column->iterator(hint);
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::payload>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::score>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

        const auto str = std::to_string(doc);
        ASSERT_EQ(doc, it->seek(doc));
        if (hint == irs::ColumnHint::kMask) {
          EXPECT_TRUE(payload->value.null());
        } else {
          EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
        }
        ASSERT_EQ(doc, it->seek(doc));
        if (hint == irs::ColumnHint::kMask) {
          EXPECT_TRUE(payload->value.null());
        } else {
          EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
        }
        ASSERT_EQ(doc, it->seek(doc - 1));
        if (hint == irs::ColumnHint::kMask) {
          EXPECT_TRUE(payload->value.null());
        } else {
          EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
        }
      }

      // seek + next
      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
           doc += 10000) {
        auto prev_it = column->iterator(hint);
        auto it = column->iterator(hint);
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::payload>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::score>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

        const auto str = std::to_string(doc);
        ASSERT_EQ(doc, it->seek(doc));
        if (hint == irs::ColumnHint::kMask) {
          EXPECT_TRUE(payload->value.null());
        } else {
          EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
        }
        ASSERT_EQ(doc, it->seek(doc));
        if (hint == irs::ColumnHint::kMask) {
          EXPECT_TRUE(payload->value.null());
        } else {
          EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
        }
        assert_prev_doc(*it, *prev_it);

        auto next_it = column->iterator(hint);
        auto* next_payload = irs::get<irs::payload>(*next_it);
        ASSERT_NE(nullptr, next_payload);
        ASSERT_EQ(doc, next_it->seek(doc));
        if (hint == irs::ColumnHint::kMask) {
          EXPECT_TRUE(next_payload->value.null());
        } else {
          EXPECT_EQ(str, irs::ref_cast<char>(next_payload->value));
        }
        for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
          ASSERT_TRUE(next_it->next());
          ASSERT_EQ(next_doc, next_it->value());
          const auto str = std::to_string(next_doc);
          if (hint == irs::ColumnHint::kMask) {
            EXPECT_TRUE(next_payload->value.null());
          } else {
            EXPECT_EQ(str, irs::ref_cast<char>(next_payload->value));
          }
          assert_prev_doc(*next_it, *prev_it);
        }
      }

      // next + seek
      {
        auto prev_it = column->iterator(hint);
        auto it = column->iterator(hint);
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::payload>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::score>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);
        ASSERT_TRUE(it->next());
        ASSERT_EQ(irs::doc_limits::min(), document->value);
        assert_prev_doc(*it, *prev_it);
        const auto str = std::to_string(118774);
        ASSERT_EQ(118774, it->seek(118774));
        if (hint == irs::ColumnHint::kMask) {
          EXPECT_TRUE(payload->value.null());
        } else {
          EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
        }
        assert_prev_doc(*it, *prev_it);
        ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
      }
    };

    assert_iterator(hint());
    assert_iterator(irs::ColumnHint::kMask);
  }
}

TEST_P(columnstore2_test_case, dense_column_range) {
  constexpr irs::doc_id_t kMin = 500000;
  constexpr irs::doc_id_t kMax = 1000000;
  const irs::segment_meta meta("test", nullptr);
  const bool has_encryption = bool(dir().attributes().encryption());

  irs::flush_state state;
  state.doc_count = kMax;
  state.name = meta.name;

  {
    irs::columnstore2::writer writer(
        version(), this->hint() == irs::ColumnHint::kConsolidation);
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column(
        {irs::type<irs::compression::none>::get(), {}, has_encryption},
        [](irs::bstring& out) {
          EXPECT_TRUE(out.empty());
          out += 42;
          return irs::string_ref::NIL;
        });

    for (irs::doc_id_t doc = kMin; doc <= kMax; ++doc) {
      auto& stream = column(doc);
      const auto str = std::to_string(doc);
      stream.write_bytes(reinterpret_cast<const irs::byte_type*>(str.c_str()),
                         str.size());
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columnstore2::reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta));
    ASSERT_EQ(1, reader.size());

    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax - kMin + 1, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(kMin, header->min);
    ASSERT_EQ(ColumnType::kSparse, header->type);
    ASSERT_EQ(has_encryption
                  ? (ColumnProperty::kEncrypt | ColumnProperty::kNoName)
                  : ColumnProperty::kNoName,
              header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax - kMin + 1, column->size());

    ASSERT_EQ(0, column->id());
    ASSERT_TRUE(column->name().null());

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    // seek before range
    {
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      const auto str = std::to_string(kMin);
      ASSERT_EQ(kMin, it->seek(42));
      EXPECT_EQ(str, irs::ref_cast<char>(payload->value));

      irs::doc_id_t expected_doc = kMin + 1;
      for (; expected_doc <= kMax; ++expected_doc) {
        const auto str = std::to_string(expected_doc);
        ASSERT_EQ(expected_doc, it->seek(expected_doc));
      }
      ASSERT_FALSE(it->next());
      ASSERT_TRUE(irs::doc_limits::eof(it->value()));
    }

    {
      auto prev_it = column->iterator(hint());
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        const auto expected_doc = (doc <= kMin ? kMin : doc);
        const auto str = std::to_string(expected_doc);
        ASSERT_EQ(expected_doc, it->seek(doc));
        EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
        ASSERT_EQ(expected_doc, it->seek(doc));
        EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
        ASSERT_EQ(expected_doc, it->seek(doc - 1));
        EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
        assert_prev_doc(*it, *prev_it);
      }
    }

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      const auto expected_doc = (doc <= kMin ? kMin : doc);
      const auto str = std::to_string(expected_doc);
      ASSERT_EQ(expected_doc, it->seek(doc));
      EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
      ASSERT_EQ(expected_doc, it->seek(doc));
      EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
    }

    // seek + next
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
         doc += 10000) {
      auto prev_it = column->iterator(hint());
      auto it = column->iterator(hint());
      auto* document = irs::get<irs::document>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::payload>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::cost>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::score>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

      const auto expected_doc = (doc <= kMin ? kMin : doc);
      const auto str = std::to_string(expected_doc);
      ASSERT_EQ(expected_doc, it->seek(doc));
      EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
      ASSERT_EQ(expected_doc, it->seek(doc));
      EXPECT_EQ(str, irs::ref_cast<char>(payload->value));
      assert_prev_doc(*it, *prev_it);

      auto next_it = column->iterator(hint());
      ASSERT_EQ(expected_doc, next_it->seek(doc));
      auto* next_payload = irs::get<irs::payload>(*next_it);
      ASSERT_NE(nullptr, next_payload);
      EXPECT_EQ(str, irs::ref_cast<char>(next_payload->value));
      for (auto next_doc = expected_doc + 1; next_doc <= kMax; ++next_doc) {
        ASSERT_TRUE(next_it->next());
        ASSERT_EQ(next_doc, next_it->value());
        const auto str = std::to_string(next_doc);
        EXPECT_EQ(str, irs::ref_cast<char>(next_payload->value));
        assert_prev_doc(*next_it, *prev_it);
      }
    }
  }
}

TEST_P(columnstore2_test_case, dense_fixed_length_column) {
  constexpr irs::doc_id_t kMax = 1000000;
  const irs::segment_meta meta("test", nullptr);
  const bool has_encryption = bool(dir().attributes().encryption());

  irs::flush_state state;
  state.doc_count = kMax;
  state.name = meta.name;

  {
    irs::columnstore2::writer writer(
        version(), this->hint() == irs::ColumnHint::kConsolidation);
    writer.prepare(dir(), meta);

    {
      auto [id, column] = writer.push_column(
          {irs::type<irs::compression::none>::get(), {}, has_encryption},
          [](irs::bstring& out) {
            EXPECT_TRUE(out.empty());
            out += 42;
            return irs::string_ref::NIL;
          });

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto& stream = column(doc);
        stream.write_bytes(reinterpret_cast<const irs::byte_type*>(&doc),
                           sizeof doc);
      }
    }

    {
      auto [id, column] = writer.push_column(
          {irs::type<irs::compression::none>::get(), {}, has_encryption},
          [](irs::bstring& out) {
            EXPECT_TRUE(out.empty());
            out += 43;
            return irs::string_ref::NIL;
          });

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto& stream = column(doc);
        stream.write_byte(static_cast<irs::byte_type>(doc & 0xFF));
      }
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columnstore2::reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta));
    ASSERT_EQ(2, reader.size());

    {
      constexpr irs::field_id kColumnId = 0;

      auto* header = reader.header(kColumnId);
      ASSERT_NE(nullptr, header);
      ASSERT_EQ(kMax, header->docs_count);
      ASSERT_EQ(0, header->docs_index);
      ASSERT_EQ(irs::doc_limits::min(), header->min);
      ASSERT_EQ(this->hint() == irs::ColumnHint::kConsolidation
                    ? ColumnType::kDenseFixed
                    : ColumnType::kFixed,
                header->type);
      ASSERT_EQ(has_encryption
                    ? (ColumnProperty::kEncrypt | ColumnProperty::kNoName)
                    : ColumnProperty::kNoName,
                header->props);

      auto* column = reader.column(kColumnId);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(kMax, column->size());

      ASSERT_EQ(0, column->id());
      ASSERT_TRUE(column->name().null());

      const auto header_payload = column->payload();
      ASSERT_EQ(1, header_payload.size());
      ASSERT_EQ(42, header_payload[0]);

      {
        auto it = column->iterator(hint());
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::payload>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::score>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

        for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
          ASSERT_EQ(doc, it->seek(doc));
          ASSERT_EQ(doc, it->seek(doc));
          ASSERT_EQ(sizeof doc, payload->value.size());
          const irs::doc_id_t actual_doc =
              *reinterpret_cast<const irs::doc_id_t*>(payload->value.c_str());
          EXPECT_EQ(doc, actual_doc);
        }
      }

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto it = column->iterator(hint());
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::payload>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::score>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(sizeof doc, payload->value.size());
        const irs::doc_id_t actual_doc =
            *reinterpret_cast<const irs::doc_id_t*>(payload->value.c_str());
        EXPECT_EQ(doc, actual_doc);
      }

      // seek + next
      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
           doc += 10000) {
        auto prev_it = column->iterator(hint());
        auto it = column->iterator(hint());
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::payload>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::score>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(sizeof doc, payload->value.size());
        EXPECT_EQ(doc, *reinterpret_cast<const irs::doc_id_t*>(
                           payload->value.c_str()));
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(sizeof doc, payload->value.size());
        EXPECT_EQ(doc, *reinterpret_cast<const irs::doc_id_t*>(
                           payload->value.c_str()));
        ASSERT_EQ(doc, it->seek(doc - 1));
        ASSERT_EQ(sizeof doc, payload->value.size());
        EXPECT_EQ(doc, *reinterpret_cast<const irs::doc_id_t*>(
                           payload->value.c_str()));
        assert_prev_doc(*it, *prev_it);

        auto next_it = column->iterator(hint());
        auto* next_payload = irs::get<irs::payload>(*next_it);
        ASSERT_NE(nullptr, next_payload);
        ASSERT_EQ(doc, next_it->seek(doc));
        ASSERT_EQ(sizeof doc, next_payload->value.size());
        EXPECT_EQ(doc, *reinterpret_cast<const irs::doc_id_t*>(
                           next_payload->value.c_str()));
        for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
          ASSERT_TRUE(next_it->next());
          ASSERT_EQ(next_doc, next_it->value());
          ASSERT_EQ(sizeof next_doc, next_payload->value.size());
          ASSERT_EQ(next_doc, *reinterpret_cast<const irs::doc_id_t*>(
                                  next_payload->value.c_str()));
          assert_prev_doc(*next_it, *prev_it);
        }
      }

      // next + seek
      {
        auto prev_it = column->iterator(hint());
        auto it = column->iterator(hint());
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::payload>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        ASSERT_TRUE(it->next());
        ASSERT_EQ(irs::doc_limits::min(), document->value);
        assert_prev_doc(*it, *prev_it);
        ASSERT_EQ(118774, it->seek(118774));
        ASSERT_EQ(sizeof(irs::doc_id_t), payload->value.size());
        EXPECT_EQ(118774, *reinterpret_cast<const irs::doc_id_t*>(
                              payload->value.c_str()));
        assert_prev_doc(*it, *prev_it);
        ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
      }
    }

    {
      constexpr irs::field_id kColumnId = 1;

      auto* header = reader.header(kColumnId);
      ASSERT_NE(nullptr, header);
      ASSERT_EQ(kMax, header->docs_count);
      ASSERT_EQ(0, header->docs_index);
      ASSERT_EQ(irs::doc_limits::min(), header->min);
      ASSERT_EQ(this->hint() == irs::ColumnHint::kConsolidation
                    ? ColumnType::kDenseFixed
                    : ColumnType::kFixed,
                header->type);
      ASSERT_EQ(has_encryption
                    ? (ColumnProperty::kEncrypt | ColumnProperty::kNoName)
                    : ColumnProperty::kNoName,
                header->props);

      auto* column = reader.column(kColumnId);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(kMax, column->size());

      ASSERT_EQ(1, column->id());
      ASSERT_TRUE(column->name().null());

      const auto header_payload = column->payload();
      ASSERT_EQ(1, header_payload.size());
      ASSERT_EQ(43, header_payload[0]);

      {
        auto prev_it = column->iterator(hint());
        auto it = column->iterator(hint());
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::payload>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::score>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

        for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
          ASSERT_EQ(doc, it->seek(doc));
          ASSERT_EQ(doc, it->seek(doc));
          ASSERT_EQ(1, payload->value.size());
          EXPECT_EQ(static_cast<irs::byte_type>(doc & 0xFF), payload->value[0]);
          assert_prev_doc(*it, *prev_it);
        }
      }

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto it = column->iterator(hint());
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::payload>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::score>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(1, payload->value.size());
        EXPECT_EQ(static_cast<irs::byte_type>(doc & 0xFF), payload->value[0]);
      }

      // seek + next
      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
           doc += 10000) {
        auto prev_it = column->iterator(hint());
        auto it = column->iterator(hint());
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::payload>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::score>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(1, payload->value.size());
        EXPECT_EQ(static_cast<irs::byte_type>(doc & 0xFF), payload->value[0]);
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(1, payload->value.size());
        EXPECT_EQ(static_cast<irs::byte_type>(doc & 0xFF), payload->value[0]);
        ASSERT_EQ(doc, it->seek(doc - 1));
        ASSERT_EQ(1, payload->value.size());
        EXPECT_EQ(static_cast<irs::byte_type>(doc & 0xFF), payload->value[0]);
        assert_prev_doc(*it, *prev_it);

        auto next_it = column->iterator(hint());
        auto* next_payload = irs::get<irs::payload>(*next_it);
        ASSERT_NE(nullptr, next_payload);
        ASSERT_EQ(doc, next_it->seek(doc));
        ASSERT_EQ(1, next_payload->value.size());
        EXPECT_EQ(static_cast<irs::byte_type>(doc & 0xFF),
                  next_payload->value[0]);
        for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
          ASSERT_TRUE(next_it->next());
          ASSERT_EQ(next_doc, next_it->value());
          ASSERT_EQ(1, next_payload->value.size());
          EXPECT_EQ(static_cast<irs::byte_type>(next_doc & 0xFF),
                    next_payload->value[0]);
          assert_prev_doc(*next_it, *prev_it);
        }
      }

      // next + seek
      {
        auto prev_it = column->iterator(hint());
        auto it = column->iterator(hint());
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::payload>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        ASSERT_TRUE(it->next());
        ASSERT_EQ(irs::doc_limits::min(), document->value);
        assert_prev_doc(*it, *prev_it);
        ASSERT_EQ(118774, it->seek(118774));
        ASSERT_EQ(1, payload->value.size());
        EXPECT_EQ(static_cast<irs::byte_type>(118774 & 0xFF),
                  payload->value[0]);
        assert_prev_doc(*it, *prev_it);
        ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
      }
    }
  }
}

TEST_P(columnstore2_test_case, dense_fixed_length_column_empty_tail) {
  constexpr irs::doc_id_t kMax = 1000000;
  const irs::segment_meta meta("test", nullptr);
  const bool has_encryption = bool(dir().attributes().encryption());

  irs::flush_state state;
  state.doc_count = kMax;
  state.name = meta.name;

  {
    irs::columnstore2::writer writer(
        version(), this->hint() == irs::ColumnHint::kConsolidation);
    writer.prepare(dir(), meta);

    {
      auto [id, column] = writer.push_column(
          {irs::type<irs::compression::none>::get(), {}, has_encryption},
          [](irs::bstring& out) {
            EXPECT_TRUE(out.empty());
            out += 42;
            return irs::string_ref::NIL;
          });

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto& stream = column(doc);
        stream.write_bytes(reinterpret_cast<const irs::byte_type*>(&doc),
                           sizeof doc);
      }
    }

    {
      // empty column has to be removed
      auto [id, column] = writer.push_column(
          {irs::type<irs::compression::none>::get(), {}, has_encryption},
          [](auto&) {
            // Must not be called
            EXPECT_FALSE(true);
            return irs::string_ref::NIL;
          });
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columnstore2::reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta));
    ASSERT_EQ(1, reader.size());

    {
      constexpr irs::field_id kColumnId = 0;

      auto* header = reader.header(kColumnId);
      ASSERT_NE(nullptr, header);
      ASSERT_EQ(kMax, header->docs_count);
      ASSERT_EQ(0, header->docs_index);
      ASSERT_EQ(irs::doc_limits::min(), header->min);
      ASSERT_EQ(this->hint() == irs::ColumnHint::kConsolidation
                    ? ColumnType::kDenseFixed
                    : ColumnType::kFixed,
                header->type);
      ASSERT_EQ(has_encryption
                    ? (ColumnProperty::kEncrypt | ColumnProperty::kNoName)
                    : ColumnProperty::kNoName,
                header->props);

      auto* column = reader.column(kColumnId);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(kMax, column->size());

      ASSERT_EQ(0, column->id());
      ASSERT_TRUE(column->name().null());

      const auto header_payload = column->payload();
      ASSERT_EQ(1, header_payload.size());
      ASSERT_EQ(42, header_payload[0]);

      {
        auto prev_it = column->iterator(hint());
        auto it = column->iterator(hint());
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::payload>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::score>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

        for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
          ASSERT_EQ(doc, it->seek(doc));
          ASSERT_EQ(doc, it->seek(doc));
          ASSERT_EQ(sizeof doc, payload->value.size());
          const irs::doc_id_t actual_doc =
              *reinterpret_cast<const irs::doc_id_t*>(payload->value.c_str());
          EXPECT_EQ(doc, actual_doc);
          assert_prev_doc(*it, *prev_it);
        }
      }

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto it = column->iterator(hint());
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::payload>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::score>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(sizeof doc, payload->value.size());
        const irs::doc_id_t actual_doc =
            *reinterpret_cast<const irs::doc_id_t*>(payload->value.c_str());
        EXPECT_EQ(doc, actual_doc);
      }

      // seek + next
      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
           doc += 10000) {
        auto prev_it = column->iterator(hint());
        auto it = column->iterator(hint());
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::payload>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::score>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == irs::ScoreFunction::kDefault);

        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(sizeof doc, payload->value.size());
        EXPECT_EQ(doc, *reinterpret_cast<const irs::doc_id_t*>(
                           payload->value.c_str()));
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(sizeof doc, payload->value.size());
        EXPECT_EQ(doc, *reinterpret_cast<const irs::doc_id_t*>(
                           payload->value.c_str()));
        ASSERT_EQ(doc, it->seek(doc - 1));
        ASSERT_EQ(sizeof doc, payload->value.size());
        EXPECT_EQ(doc, *reinterpret_cast<const irs::doc_id_t*>(
                           payload->value.c_str()));
        assert_prev_doc(*it, *prev_it);

        auto next_it = column->iterator(hint());
        auto* next_payload = irs::get<irs::payload>(*next_it);
        ASSERT_NE(nullptr, next_payload);
        ASSERT_EQ(doc, next_it->seek(doc));
        ASSERT_EQ(sizeof doc, next_payload->value.size());
        EXPECT_EQ(doc, *reinterpret_cast<const irs::doc_id_t*>(
                           next_payload->value.c_str()));
        for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
          ASSERT_TRUE(next_it->next());
          ASSERT_EQ(next_doc, next_it->value());
          ASSERT_EQ(sizeof next_doc, next_payload->value.size());
          ASSERT_EQ(next_doc, *reinterpret_cast<const irs::doc_id_t*>(
                                  next_payload->value.c_str()));
          assert_prev_doc(*next_it, *prev_it);
        }
      }

      // next + seek
      {
        auto prev_it = column->iterator(hint());
        auto it = column->iterator(hint());
        auto* document = irs::get<irs::document>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::payload>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::cost>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        ASSERT_TRUE(it->next());
        ASSERT_EQ(irs::doc_limits::min(), document->value);
        assert_prev_doc(*it, *prev_it);
        ASSERT_EQ(118774, it->seek(118774));
        ASSERT_EQ(sizeof(irs::doc_id_t), payload->value.size());
        EXPECT_EQ(118774, *reinterpret_cast<const irs::doc_id_t*>(
                              payload->value.c_str()));
        assert_prev_doc(*it, *prev_it);
        ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
      }
    }
  }
}

TEST_P(columnstore2_test_case, empty_columns) {
  constexpr irs::doc_id_t kMax = 1000000;
  const irs::segment_meta meta("test", nullptr);
  const bool has_encryption = bool(dir().attributes().encryption());

  irs::flush_state state;
  state.doc_count = kMax;
  state.name = meta.name;

  {
    irs::columnstore2::writer writer(
        version(), this->hint() == irs::ColumnHint::kConsolidation);
    writer.prepare(dir(), meta);

    {
      // empty column must be removed
      auto [id, column] = writer.push_column(
          {irs::type<irs::compression::none>::get(), {}, has_encryption},
          [](auto&) {
            // Must not be called
            EXPECT_FALSE(true);
            return irs::string_ref::NIL;
          });
    }

    {
      // empty column must be removed
      auto [id, column] = writer.push_column(
          {irs::type<irs::compression::none>::get(), {}, has_encryption},
          [](auto&) {
            // Must not be called
            EXPECT_FALSE(true);
            return irs::string_ref::NIL;
          });
    }

    ASSERT_FALSE(writer.commit(state));
  }

  size_t count = 0;
  ASSERT_TRUE(dir().visit([&count](auto) {
    ++count;
    return false;
  }));

  ASSERT_EQ(0, count);
}

static_assert(irs::columnstore2::Version::kMax ==
              irs::columnstore2::Version::kPrevSeek);

INSTANTIATE_TEST_SUITE_P(
    columnstore2_test, columnstore2_test_case,
    ::testing::Combine(
        ::testing::Values(&tests::directory<&tests::memory_directory>,
                          &tests::directory<&tests::fs_directory>,
                          &tests::directory<&tests::mmap_directory>,
                          &tests::rot13_directory<&tests::memory_directory, 16>,
                          &tests::rot13_directory<&tests::fs_directory, 16>,
                          &tests::rot13_directory<&tests::mmap_directory, 16>),
        ::testing::Values(irs::ColumnHint::kNormal,
                          irs::ColumnHint::kConsolidation,
                          irs::ColumnHint::kMask,
                          irs::ColumnHint::kPrevDoc),
        ::testing::Values(irs::columnstore2::Version::kMin,
                          irs::columnstore2::Version::kPrevSeek)),
    &columnstore2_test_case::to_string);
