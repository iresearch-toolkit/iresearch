////////////////////////////////////////////////////////////////////////////////
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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

#include <random>

#include "formats/formats_10.hpp"
#include "formats/formats_10_attributes.hpp"
#include "formats_test_case_base.hpp"
#include "store/directory_attributes.hpp"
#include "tests_shared.hpp"

namespace {

class FreqThresholdDocIterator : public irs::doc_iterator {
 public:
  FreqThresholdDocIterator(irs::doc_iterator& impl, uint32_t threshold)
    : impl_{&impl},
      freq_{irs::get<irs::frequency>(impl)},
      threshold_{threshold} {}

  irs::attribute* get_mutable(irs::type_info::type_id id) final {
    return impl_->get_mutable(id);
  }

  irs::doc_id_t value() const final { return impl_->value(); }

  bool next() final {
    while (impl_->next()) {
      if (freq_ && freq_->value < threshold_) {
        continue;
      }

      return true;
    }
    return false;
  }

  irs::doc_id_t seek(irs::doc_id_t target) final {
    target = impl_->seek(target);

    if (irs::doc_limits::eof(target)) {
      return irs::doc_limits::eof();
    }

    if (freq_ && freq_->value < threshold_) {
      next();
    }

    return value();
  }

 private:
  irs::doc_iterator* impl_;
  const irs::frequency* freq_;
  uint32_t threshold_;
};

class SkipList {
 public:
  struct Step {
    irs::doc_id_t key;
    irs::score_t freq;
  };

  struct Level {
    const irs::doc_id_t step;
    std::vector<Step> steps;
  };

  static SkipList Make(irs::doc_iterator& it, irs::doc_id_t skip_0,
                       irs::doc_id_t skip_n, irs::doc_id_t count);

  SkipList() = default;

  bool Empty() const noexcept { return skip_list_.empty(); }
  size_t Size() const noexcept { return skip_list_.size(); }
  irs::score_t At(size_t level, irs::doc_id_t doc) const noexcept {
    EXPECT_LT(level, skip_list_.size());

    auto& [_, data] = skip_list_[level];
    auto it = std::lower_bound(
      std::begin(data), std::end(data), Step{doc, 0.f},
      [](const auto& lhs, const auto& rhs) { return lhs.key < rhs.key; });

    EXPECT_NE(it, std::end(data));
    return it->freq;
  }

 private:
  explicit SkipList(std::vector<Level>&& skip_list)
    : skip_list_{std::move(skip_list)} {
    for (auto& [_, level] : skip_list) {
      EXPECT_TRUE(std::is_sorted(
        std::begin(level), std::end(level),
        [](const auto& lhs, const auto& rhs) { return lhs.key < rhs.key; }));
    }
  }

  std::vector<Level> skip_list_;
};

SkipList SkipList::Make(irs::doc_iterator& it, irs::doc_id_t skip_0,
                        irs::doc_id_t skip_n, irs::doc_id_t count) {
  size_t num_levels =
    skip_0 < count ? 1 + irs::math::log(count / skip_0, skip_n) : 0;
  EXPECT_GT(num_levels, 0);

  std::vector<Level> skip_list;
  skip_list.reserve(num_levels);

  auto step = static_cast<irs::doc_id_t>(
    skip_0 * static_cast<size_t>(std::pow(skip_n, num_levels - 1)));

  for (; num_levels; --num_levels) {
    skip_list.emplace_back(Level{step, std::vector{Step{0U, 0.f}}});
    step /= skip_n;
  }

  auto add = [&](irs::doc_id_t i, irs::doc_id_t doc, irs::score_t freq) {
    for (auto& [step, level] : skip_list) {
      if (level.size() * step < count) {
        ASSERT_FALSE(level.empty());
        level.back() = {doc, std::max(level.back().freq, freq)};
        if (0 == (i % step)) {
          level.emplace_back(Step{0, 0.f});
        }
      }
    }
  };

  auto* freq = irs::get<irs::frequency>(it);

  if (freq) {
    for (irs::doc_id_t i = 1; it.next(); ++i) {
      add(i, it.value(), freq->value);
    }

    for (auto& [step, level] : skip_list) {
      level.back() = {irs::doc_limits::eof(),
                      std::numeric_limits<irs::score_t>::max()};
    }
  }

  return SkipList{std::move(skip_list)};
}

void AssertSkipList(const SkipList& expected_freqs, irs::doc_id_t doc,
                    std::span<const irs::score_t> actual_freqs) {
  ASSERT_EQ(expected_freqs.Size(), actual_freqs.size());
  for (size_t i = 0, size = expected_freqs.Size(); i < size; ++i) {
    const auto expected_freq = expected_freqs.At(i, doc);
    ASSERT_EQ(expected_freq, actual_freqs[i]);
  }
}

class Format15TestCase : public tests::format_test_case {
 protected:
  static constexpr size_t kVersion10PostingsWriterBlockSize = 128;

  void PostingsWandSeek(
    std::span<const std::pair<irs::doc_id_t, uint32_t>> docs,
    irs::IndexFeatures features, uint32_t threshold);
};

void Format15TestCase::PostingsWandSeek(
  std::span<const std::pair<irs::doc_id_t, uint32_t>> docs,
  irs::IndexFeatures features, uint32_t threshold) {
  irs::field_meta field;
  field.index_features = features;
  auto dir = get_directory(*this);

  auto codec =
    std::dynamic_pointer_cast<const irs::version10::format>(get_codec());
  ASSERT_NE(nullptr, codec);
  auto writer = codec->get_postings_writer(false);
  ASSERT_NE(nullptr, writer);
  irs::postings_writer::state term_meta;  // must be destroyed before the writer

  {
    irs::flush_state state;
    state.dir = dir.get();
    state.doc_count = docs.back().first + 1;
    state.name = "segment_name";
    state.index_features = field.index_features;

    auto out = dir->create("attributes");
    ASSERT_FALSE(!out);
    irs::write_string(*out, std::string_view("file_header"));

    writer->prepare(*out, state);
    writer->begin_field(features);

    postings it(docs, field.index_features);
    term_meta = writer->write(it);

    writer->encode(*out, *term_meta);
    writer->end();
  }

  {
    irs::SegmentMeta meta;
    meta.name = "segment_name";

    irs::reader_state state;
    state.dir = dir.get();
    state.meta = &meta;

    auto in = dir->open("attributes", irs::IOAdvice::NORMAL);
    ASSERT_FALSE(!in);
    [[maybe_unused]] const auto tmp = irs::read_string<std::string>(*in);

    auto reader = codec->get_postings_reader();
    ASSERT_NE(nullptr, reader);
    reader->prepare(*in, state, field.index_features);

    irs::bstring in_data(in->length() - in->file_pointer(), 0);
    in->read_bytes(&in_data[0], in_data.size());
    const auto* begin = in_data.c_str();

    auto factory = [](const irs::attribute_provider& attrs) {
      auto* freq = irs::get<irs::frequency>(attrs);
      EXPECT_NE(nullptr, freq);

      return irs::ScoreFunction{
        reinterpret_cast<irs::score_ctx&>(const_cast<irs::frequency&>(*freq)),
        [](irs::score_ctx* ctx, irs::score_t* res) noexcept {
          *res = reinterpret_cast<irs::frequency*>(ctx)->value;
        }};
    };

    {
      irs::version10::term_meta read_meta;
      begin += reader->decode(begin, field.index_features, read_meta);

      {
        auto& typed_meta = static_cast<irs::version10::term_meta&>(*term_meta);
        ASSERT_EQ(typed_meta.docs_count, read_meta.docs_count);
        ASSERT_EQ(typed_meta.doc_start, read_meta.doc_start);
        ASSERT_EQ(typed_meta.pos_start, read_meta.pos_start);
        ASSERT_EQ(typed_meta.pos_end, read_meta.pos_end);
        ASSERT_EQ(typed_meta.pay_start, read_meta.pay_start);
        ASSERT_EQ(typed_meta.e_single_doc, read_meta.e_single_doc);
        ASSERT_EQ(typed_meta.e_skip_start, read_meta.e_skip_start);
      }

      auto assert_docs_seq = [&]() {
        postings expected_postings{docs, field.index_features};
        FreqThresholdDocIterator expected{expected_postings, threshold};
        SkipList skip_list;

        auto actual = reader->wanderator(field.index_features, features,
                                         factory, read_meta);
        ASSERT_NE(nullptr, actual);

        auto* threshold_value =
          irs::get_mutable<irs::score_threshold>(actual.get());
        if (docs.size() <= kVersion10PostingsWriterBlockSize ||
            irs::IndexFeatures::NONE == (features & irs::IndexFeatures::FREQ)) {
          ASSERT_EQ(nullptr, threshold_value);
        } else {
          ASSERT_NE(nullptr, threshold_value);
          threshold_value->value = threshold;

          postings tmp{docs, field.index_features};
          skip_list = SkipList::Make(tmp, kVersion10PostingsWriterBlockSize, 8,
                                     irs::doc_id_t(docs.size()));
        }

        ASSERT_FALSE(irs::doc_limits::valid(actual->value()));

        while (expected.next()) {
          const auto expected_doc_id = expected.value();
          ASSERT_TRUE(actual->next());

          ASSERT_EQ(expected_doc_id, actual->value());
          ASSERT_EQ(expected_doc_id, actual->seek(expected_doc_id));
          // seek to the same doc
          ASSERT_EQ(expected_doc_id, actual->seek(expected_doc_id));
          // seek to the smaller doc
          ASSERT_EQ(expected_doc_id, actual->seek(irs::doc_limits::invalid()));

          if (!skip_list.Empty()) {
            AssertSkipList(skip_list, expected_doc_id,
                           threshold_value->skip_scores);
          }
          assert_frequency_and_positions(expected, *actual);
        }

        ASSERT_FALSE(actual->next());
        ASSERT_TRUE(irs::doc_limits::eof(actual->value()));

        // seek after the existing documents
        ASSERT_TRUE(irs::doc_limits::eof(actual->seek(docs.back().first + 42)));
      };

      auto assert_docs_random = [&](size_t seed, size_t inc) {
        postings expected_postings{docs, field.index_features};
        FreqThresholdDocIterator expected{expected_postings, threshold};

        auto actual = reader->wanderator(field.index_features, features,
                                         factory, read_meta);
        ASSERT_NE(nullptr, actual);

        auto* threshold_value =
          irs::get_mutable<irs::score_threshold>(actual.get());
        if (docs.size() <= kVersion10PostingsWriterBlockSize ||
            irs::IndexFeatures::NONE == (features & irs::IndexFeatures::FREQ)) {
          ASSERT_EQ(nullptr, threshold_value);
        } else {
          ASSERT_NE(nullptr, threshold_value);
          threshold_value->value = threshold;
        }

        ASSERT_FALSE(irs::doc_limits::valid(actual->value()));

        for (size_t i = seed, size = docs.size(); i < size; i += inc) {
          const auto& doc = docs[i];
          const auto expected_doc_id = expected.seek(doc.first);
          ASSERT_EQ(expected_doc_id, actual->seek(expected_doc_id));
          // seek to the same doc
          ASSERT_EQ(expected_doc_id, actual->seek(expected_doc_id));
          // seek to the smaller doc
          ASSERT_EQ(expected_doc_id, actual->seek(irs::doc_limits::invalid()));

          assert_frequency_and_positions(expected, *actual);
        }

        if (inc == 1) {
          ASSERT_FALSE(actual->next());
          ASSERT_TRUE(irs::doc_limits::eof(actual->value()));

          // seek after the existing documents
          ASSERT_TRUE(
            irs::doc_limits::eof(actual->seek(docs.back().first + 42)));
        }
      };

      // next + seek to eof
      {
        auto it = reader->wanderator(
          field.index_features, irs::IndexFeatures::NONE, factory, read_meta);
        ASSERT_FALSE(irs::doc_limits::valid(it->value()));
        ASSERT_TRUE(it->next());
        ASSERT_EQ(docs.front().first, it->value());
        ASSERT_TRUE(irs::doc_limits::eof(it->seek(docs.back().first + 42)));
      }

      assert_docs_seq();

      // seek for every document 127th document in a block
      assert_docs_random(kVersion10PostingsWriterBlockSize - 1,
                         kVersion10PostingsWriterBlockSize);

      // seek for every 128th document in a block
      assert_docs_random(kVersion10PostingsWriterBlockSize,
                         kVersion10PostingsWriterBlockSize);

      // seek for every document
      assert_docs_random(0, 1);

      // seek to every 5th document
      assert_docs_random(0, 5);

      // seek backwards && next
      {
        for (auto doc = docs.rbegin(), end = docs.rend(); doc != end; ++doc) {
          if (doc->second < threshold) {
            continue;
          }

          postings expected_postings{docs, field.index_features};
          FreqThresholdDocIterator expected{expected_postings, threshold};

          auto actual = reader->wanderator(field.index_features, features,
                                           factory, read_meta);
          ASSERT_NE(nullptr, actual);

          auto* threshold_value =
            irs::get_mutable<irs::score_threshold>(actual.get());
          if (docs.size() <= kVersion10PostingsWriterBlockSize ||
              irs::IndexFeatures::NONE ==
                (features & irs::IndexFeatures::FREQ)) {
            ASSERT_EQ(nullptr, threshold_value);
          } else {
            ASSERT_NE(nullptr, threshold_value);
            threshold_value->value = threshold;
          }

          ASSERT_FALSE(irs::doc_limits::valid(actual->value()));
          ASSERT_EQ(doc->first, actual->seek(doc->first));

          ASSERT_EQ(doc->first, expected.seek(doc->first));
          assert_frequency_and_positions(expected, *actual);
          if (doc != docs.rbegin()) {
            ASSERT_TRUE(expected.next());
            ASSERT_EQ((doc - 1)->first, expected.value());

            ASSERT_TRUE(actual->next());
            ASSERT_EQ((doc - 1)->first, actual->value());

            assert_frequency_and_positions(expected, *actual);
          }
        }
      }

      // seek to irs::doc_limits::invalid()
      {
        auto it = reader->wanderator(
          field.index_features, irs::IndexFeatures::NONE, factory, read_meta);
        ASSERT_FALSE(irs::doc_limits::valid(it->value()));
        ASSERT_FALSE(
          irs::doc_limits::valid(it->seek(irs::doc_limits::invalid())));
        ASSERT_TRUE(it->next());
        ASSERT_EQ(docs.front().first, it->value());
      }

      // seek to irs::doc_limits::eof()
      {
        auto it = reader->wanderator(
          field.index_features, irs::IndexFeatures::NONE, factory, read_meta);
        ASSERT_FALSE(irs::doc_limits::valid(it->value()));
        ASSERT_TRUE(irs::doc_limits::eof(it->seek(irs::doc_limits::eof())));
        ASSERT_FALSE(it->next());
        ASSERT_TRUE(irs::doc_limits::eof(it->value()));
      }
    }

    ASSERT_EQ(begin, in_data.data() + in_data.size());
  }
}

static constexpr auto kTestDirs =
  tests::getDirectories<tests::kTypesDefault | tests::kTypesRot13_16 |
                        tests::kTypesRot13_7>();

static const auto kTestValues =
  ::testing::Combine(::testing::ValuesIn(kTestDirs),
                     ::testing::Values(tests::format_info{"1_5", "1_0"},
                                       tests::format_info{"1_5simd", "1_0"}));

// Generic tests
using tests::format_test_case;

INSTANTIATE_TEST_SUITE_P(Format15Test, format_test_case, kTestValues,
                         format_test_case::to_string);

// 1.5 specific tests

TEST_P(Format15TestCase, PostingsWandSeek) {
  auto generate_docs = [](size_t count, float_t mean, float_t dev,
                          size_t step) {
    std::vector<std::pair<irs::doc_id_t, uint32_t>> docs;
    docs.reserve(count);
    std::generate_n(
      std::back_inserter(docs), count,
      [i = (irs::doc_limits::min)(), gen = std::mt19937{},
       distr = std::normal_distribution<float_t>{mean, dev}, step]() mutable {
        const irs::doc_id_t doc = i;
        const auto freq = static_cast<uint32_t>(std::roundf(distr(gen)));
        i += step;

        return std::make_pair(doc, freq);
      });
    return docs;
  };

  auto check_docs = [](const auto& docs) {
    return std::is_sorted(
             std::begin(docs), std::end(docs),
             [](auto& lhs, auto& rhs) { return lhs.first < rhs.first; }) &&
           std::all_of(std::begin(docs), std::end(docs), [](auto& v) {
             return static_cast<int32_t>(v.second) > 0;
           });
  };

  constexpr auto kNone = irs::IndexFeatures::NONE;
  constexpr auto kFreq = irs::IndexFeatures::FREQ;
  constexpr auto kPos = irs::IndexFeatures::FREQ | irs::IndexFeatures::POS;
  constexpr auto kOffs = irs::IndexFeatures::FREQ | irs::IndexFeatures::POS |
                         irs::IndexFeatures::OFFS;
  constexpr auto kPay = irs::IndexFeatures::FREQ | irs::IndexFeatures::POS |
                        irs::IndexFeatures::PAY;
  constexpr auto kAll = irs::IndexFeatures::FREQ | irs::IndexFeatures::POS |
                        irs::IndexFeatures::OFFS | irs::IndexFeatures::PAY;

  // singleton doc
  {
    constexpr size_t kCount = 1;
    constexpr uint32_t kThreshold = 0;
    static_assert(kCount < kVersion10PostingsWriterBlockSize);

    const auto docs = generate_docs(kCount, 50.f, 14.f, 1);
    ASSERT_TRUE(check_docs(docs));

    PostingsWandSeek(docs, kNone, kThreshold);
    PostingsWandSeek(docs, kFreq, kThreshold);
    PostingsWandSeek(docs, kPos, kThreshold);
    PostingsWandSeek(docs, kOffs, kThreshold);
    PostingsWandSeek(docs, kPay, kThreshold);
    PostingsWandSeek(docs, kAll, kThreshold);
  }

  // short list (< postings_writer::BLOCK_SIZE)
  {
    constexpr size_t kCount = 117;
    constexpr uint32_t kThreshold = 0;
    static_assert(kCount < kVersion10PostingsWriterBlockSize);

    const auto docs = generate_docs(kCount, 50.f, 14.f, 1);
    ASSERT_TRUE(check_docs(docs));

    PostingsWandSeek(docs, kNone, kThreshold);
    PostingsWandSeek(docs, kFreq, kThreshold);
    PostingsWandSeek(docs, kPos, kThreshold);
    PostingsWandSeek(docs, kOffs, kThreshold);
    PostingsWandSeek(docs, kPay, kThreshold);
    PostingsWandSeek(docs, kAll, kThreshold);
  }

  // equals to postings_writer::BLOCK_SIZE
  {
    constexpr uint32_t kThreshold = 0;
    const auto docs =
      generate_docs(kVersion10PostingsWriterBlockSize, 50.f, 14.f, 1);
    ASSERT_TRUE(check_docs(docs));

    PostingsWandSeek(docs, kNone, kThreshold);
    PostingsWandSeek(docs, kFreq, kThreshold);
    PostingsWandSeek(docs, kPos, kThreshold);
    PostingsWandSeek(docs, kOffs, kThreshold);
    PostingsWandSeek(docs, kPay, kThreshold);
    PostingsWandSeek(docs, kAll, kThreshold);
  }

  // long list
  {
    constexpr size_t kCount = 10000;
    constexpr uint32_t kThreshold = 0;
    const auto docs = generate_docs(kCount, 50.f, 13.f, 1);
    ASSERT_TRUE(check_docs(docs));

    PostingsWandSeek(docs, kNone, kThreshold);
    PostingsWandSeek(docs, kFreq, kThreshold);
    PostingsWandSeek(docs, kPos, kThreshold);
    PostingsWandSeek(docs, kOffs, kThreshold);
    PostingsWandSeek(docs, kPay, kThreshold);
    PostingsWandSeek(docs, kAll, kThreshold);
  }

  // 2^15
  {
    constexpr size_t kCount = size_t{1} << 15;
    constexpr uint32_t kThreshold = 0;
    const auto docs = generate_docs(kCount, 1000.f, 20.f, 2);
    ASSERT_TRUE(check_docs(docs));

    PostingsWandSeek(docs, kNone, kThreshold);
    PostingsWandSeek(docs, kFreq, kThreshold);
    PostingsWandSeek(docs, kPos, kThreshold);
    PostingsWandSeek(docs, kOffs, kThreshold);
    PostingsWandSeek(docs, kPay, kThreshold);
    PostingsWandSeek(docs, kAll, kThreshold);
  }
}

INSTANTIATE_TEST_SUITE_P(Format15Test, Format15TestCase, kTestValues,
                         Format15TestCase::to_string);

}  // namespace
