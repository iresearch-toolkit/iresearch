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
/// @author Andrei Abramov
////////////////////////////////////////////////////////////////////////////////

#include "formats_test_case_base.hpp"
#include "store/directory_attributes.hpp"
#include "formats/formats_10.hpp"
#include "formats/formats_10_attributes.hpp"
#include "tests_shared.hpp"

namespace {

class FreqThresholdDocIterator final : public irs::doc_iterator {
 public:
  FreqThresholdDocIterator(irs::doc_iterator& impl, uint32_t threshold)
      : impl_{&impl}, freq_{irs::get<irs::frequency>(impl)},
        threshold_{threshold} {
  }

  irs::attribute* get_mutable(irs::type_info::type_id id) final {
    return impl_->get_mutable(id);
  }

  irs::doc_id_t value() const final {
    return impl_->value();
  }

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

class Format15TestCase : public tests::format_test_case {
 protected:
  static constexpr size_t kVersion10PostingsWriterBlockSize = 128;

  void PostingsWandSeek(
      std::span<const std::pair<irs::doc_id_t, uint32_t>> docs,
      irs::IndexFeatures features,
      uint32_t threshold);
};

void Format15TestCase::PostingsWandSeek(
    std::span<const std::pair<irs::doc_id_t, uint32_t>> docs,
    irs::IndexFeatures features,
    uint32_t threshold) {
  irs::field_meta field;
  field.index_features = features;
  auto dir = get_directory(*this);

  // attributes for term
  auto codec = std::dynamic_pointer_cast<const irs::version10::format>(get_codec());
  ASSERT_NE(nullptr, codec);
  auto writer = codec->get_postings_writer(false);
  ASSERT_NE(nullptr, writer);
  irs::postings_writer::state term_meta; // must be destroyed before the writer

  // write postings for field
  {
    irs::flush_state state;
    state.dir = dir.get();
    state.doc_count = docs.back().first+1;
    state.name = "segment_name";
    state.index_features = field.index_features;

    auto out = dir->create("attributes");
    ASSERT_FALSE(!out);
    irs::write_string(*out, irs::string_ref("file_header"));

    // prepare writer
    writer->prepare(*out, state);

    writer->begin_field(features);

    // write postings for term
    {
      postings it(docs, field.index_features);
      term_meta = writer->write(it);

      // write attributes to out
      writer->encode(*out, *term_meta);
    }

    writer->end();
  }

  // read postings
  {
    irs::segment_meta meta;
    meta.name = "segment_name";

    irs::reader_state state;
    state.dir = dir.get();
    state.meta = &meta;

    auto in = dir->open("attributes", irs::IOAdvice::NORMAL);
    ASSERT_FALSE(!in);
    const auto tmp = irs::read_string<std::string>(*in);

    // prepare reader
    auto reader = codec->get_postings_reader();
    ASSERT_NE(nullptr, reader);
    reader->prepare(*in, state, field.index_features);

    irs::bstring in_data(in->length() - in->file_pointer(), 0);
    in->read_bytes(&in_data[0], in_data.size());
    const auto* begin = in_data.c_str();

    // read term attributes
    {
      irs::version10::term_meta read_meta;
      begin += reader->decode(begin, field.index_features, read_meta);

      // check term_meta
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

      auto assert_docs = [&](size_t seed, size_t inc) {
        postings expected_postings{docs, field.index_features};
        FreqThresholdDocIterator expected{expected_postings, threshold};

        auto actual = reader->wanderator(field.index_features, features, read_meta);
        ASSERT_FALSE(irs::doc_limits::valid(actual->value()));

        for (size_t i = seed, size = docs.size(); i < size; i += inc) {
          const auto& doc = docs[i];
          const auto expected_doc_id = expected.seek(doc.first);
          ASSERT_EQ(expected_doc_id, actual->seek(expected_doc_id));
          ASSERT_EQ(expected_doc_id, actual->seek(expected_doc_id)); // seek to the same doc
          ASSERT_EQ(expected_doc_id, actual->seek(irs::doc_limits::invalid())); // seek to the smaller doc

          assert_positions(expected, *actual);
        }

        if (inc == 1) {
          ASSERT_FALSE(actual->next());
          ASSERT_TRUE(irs::doc_limits::eof(actual->value()));

          // seek after the existing documents
          ASSERT_TRUE(irs::doc_limits::eof(actual->seek(docs.back().first + 42)));
        }
      };

      // seek for every document 127th document in a block
      assert_docs(kVersion10PostingsWriterBlockSize-1, kVersion10PostingsWriterBlockSize);

      // seek for every 128th document in a block
      assert_docs(kVersion10PostingsWriterBlockSize, kVersion10PostingsWriterBlockSize);

      // seek for every document
      assert_docs(0, 1);

      // seek to every 5th document
      assert_docs(0, 5);

      // FIXME(gnusi): implement
      // seek for backwards && next
      //{
      //  for (auto doc = docs.rbegin(), end = docs.rend(); doc != end; ++doc) {
      //    if (doc->second < threshold) {
      //      continue;
      //    }

      //    postings expected(docs.begin(), docs.end(), field.index_features);
      //    auto it = reader->iterator(field.index_features, features, read_meta);
      //    ASSERT_FALSE(irs::doc_limits::valid(it->value()));
      //    ASSERT_EQ(doc->first, it->seek(doc->first));

      //    ASSERT_EQ(doc->first, expected.seek(doc->first));
      //    assert_positions(expected, *it);
      //    if (doc != docs.rbegin()) {
      //      ASSERT_TRUE(expected.next());
      //      ASSERT_EQ((doc - 1)->first, expected.value());



      //      ASSERT_TRUE(it->next());
      //      ASSERT_EQ((doc-1)->first, it->value());

      //      assert_positions(expected, *it);
      //    }
      //  }
      //}

      // seek to irs::doc_limits::invalid()
      {
        auto it = reader->wanderator(field.index_features, irs::IndexFeatures::NONE, read_meta);
        ASSERT_FALSE(irs::doc_limits::valid(it->value()));
        ASSERT_FALSE(irs::doc_limits::valid(it->seek(irs::doc_limits::invalid())));
        ASSERT_TRUE(it->next());
        ASSERT_EQ(docs.front().first, it->value());
      }

      // seek to irs::doc_limits::eof()
      {
        auto it = reader->wanderator(field.index_features, irs::IndexFeatures::NONE, read_meta);
        ASSERT_FALSE(irs::doc_limits::valid(it->value()));
        ASSERT_TRUE(irs::doc_limits::eof(it->seek(irs::doc_limits::eof())));
        ASSERT_FALSE(it->next());
        ASSERT_TRUE(irs::doc_limits::eof(it->value()));
      }
    }

    ASSERT_EQ(begin, in_data.data() + in_data.size());
  }
}

const auto kTestValues =
    ::testing::Combine(
        ::testing::Values(&tests::directory<&tests::memory_directory>,
                          &tests::directory<&tests::fs_directory>,
                          &tests::directory<&tests::mmap_directory>,
                          &tests::rot13_directory<&tests::fs_directory, 16>,
                          &tests::rot13_directory<&tests::mmap_directory, 16>,
                          &tests::rot13_directory<&tests::memory_directory, 7>,
                          &tests::rot13_directory<&tests::fs_directory, 7>,
                          &tests::rot13_directory<&tests::mmap_directory, 7>),
        ::testing::Values(tests::format_info{"1_5", "1_0"},
                          tests::format_info{"1_5simd", "1_0"}));

// Generic tests
using tests::format_test_case;

INSTANTIATE_TEST_SUITE_P(
    Format15Test,
    format_test_case,
    kTestValues,
    format_test_case::to_string);

// 1.5 specific tests

TEST_P(Format15TestCase, PostingsWandSeek) {
  auto generate_docs = [](size_t count, size_t step) {
    std::vector<std::pair<irs::doc_id_t, uint32_t>> docs;
    docs.reserve(count);
    irs::doc_id_t i = (irs::doc_limits::min)();
    std::generate_n(std::back_inserter(docs), count,
                    [&i, step]() {
                      const irs::doc_id_t doc = i;
                      const uint32_t freq = std::max(1U, doc % 7);
                      i+= step;

                      return std::make_pair(doc, freq);
                    });
    return docs;
  };

  constexpr auto kNone = irs::IndexFeatures::NONE;
  constexpr auto kFreq = irs::IndexFeatures::FREQ;
  constexpr auto kPos = irs::IndexFeatures::FREQ | irs::IndexFeatures::POS;
  constexpr auto kOffs = irs::IndexFeatures::FREQ | irs::IndexFeatures::POS | irs::IndexFeatures::OFFS;
  constexpr auto kPay = irs::IndexFeatures::FREQ | irs::IndexFeatures::POS | irs::IndexFeatures::PAY;
  constexpr auto kAll = irs::IndexFeatures::FREQ | irs::IndexFeatures::POS | irs::IndexFeatures::OFFS | irs::IndexFeatures::PAY;

  // short list (< postings_writer::BLOCK_SIZE)
  {
    constexpr size_t kCount = 117;
    constexpr uint32_t kThreshold = 0;
    static_assert(kCount < kVersion10PostingsWriterBlockSize);

    const auto docs = generate_docs(kCount, 1);

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
    const auto docs = generate_docs(kVersion10PostingsWriterBlockSize, 1);

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
    const auto docs = generate_docs(kCount, 1);

    PostingsWandSeek(docs, kNone, kThreshold);
    PostingsWandSeek(docs, kFreq, kThreshold);
    PostingsWandSeek(docs, kPos, kThreshold);
    PostingsWandSeek(docs, kOffs, kThreshold);
    PostingsWandSeek(docs, kPay, kThreshold);
    PostingsWandSeek(docs, kAll, kThreshold);
  }

  // 2^15
  {
    constexpr size_t kCount = 32768;
    constexpr uint32_t kThreshold = 0;
    const auto docs = generate_docs(kCount, 2);

    PostingsWandSeek(docs, kNone, kThreshold);
    PostingsWandSeek(docs, kFreq, kThreshold);
    PostingsWandSeek(docs, kPos, kThreshold);
    PostingsWandSeek(docs, kOffs, kThreshold);
    PostingsWandSeek(docs, kPay, kThreshold);
    PostingsWandSeek(docs, kAll, kThreshold);
  }
}

INSTANTIATE_TEST_SUITE_P(
    Format15Test,
    Format15TestCase,
    kTestValues,
    Format15TestCase::to_string);

}  // namespace
