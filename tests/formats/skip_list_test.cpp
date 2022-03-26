////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "tests_shared.hpp"
#include "formats/skip_list.hpp"
#include "store/memory_directory.hpp"
#include "index/iterators.hpp"
#include "index/index_tests.hpp"

using namespace std::chrono_literals;

namespace {

class SkipWriterTest : public test_base {
 protected:
  void write_flush(size_t count, size_t max_levels, size_t skip) {
    std::vector<std::vector<int>> levels(max_levels);
    size_t cur_doc = std::numeric_limits<size_t>::max();

    auto write_skip = [&levels, &cur_doc](size_t level, irs::index_output& out) {
      levels[level].push_back(irs::doc_id_t(cur_doc));
      out.write_vlong(irs::doc_id_t(cur_doc));
    };

    irs::skip_writer writer(skip, skip);
    ASSERT_EQ(0, writer.max_levels());
    irs::memory_directory dir;

    // write data
    {
      writer.prepare(max_levels, count);
      ASSERT_NE(0, writer.max_levels());

      for (size_t doc = 0; doc < count; ++doc) {
        cur_doc = doc;
        // skip every "skip" document
        if (doc && 0 == doc % skip) {
          writer.skip(doc, write_skip);
        }
      }

      auto out = dir.create("docs");
      ASSERT_FALSE(!out);
      writer.flush(*out);
    }

    // check levels data
    {
      size_t step = skip;
      for (auto& level : levels) {
        int expected_doc = 0;

        for (auto doc : level) {
          expected_doc += irs::doc_id_t(step);
          ASSERT_EQ(expected_doc, doc);
        }
        step *= skip;
      }
    }

    // check data in stream
    // we write levels in reverse order into a stream (from n downto 0)
    {
      auto in = dir.open("docs", irs::IOAdvice::NORMAL);
      ASSERT_FALSE(!in);

      size_t num_levels = in->read_vint();

      // check levels from n downto 1
      for (;num_levels > 1; --num_levels) {
        // skip level size
        in->read_vlong();
        auto& level = levels[num_levels-1];
        for (auto expected_doc : level) {
          auto doc = in->read_vint();
          ASSERT_EQ(expected_doc, doc);
          // skip child pointer
          in->read_vlong();
        }
      }

      // check level 0
      if (num_levels) {
        // skip level size
        in->read_vlong();
        auto& level = levels[0];
        for (auto expected_doc : level) {
          auto doc = in->read_vint();
          ASSERT_EQ(expected_doc, doc);
        }
      }
    }
  }
};

class SkipReaderTest : public test_base { };

}

TEST_F(SkipWriterTest, Prepare) {
  // empty doc count
  {
    const size_t max_levels = 10;
    const size_t doc_count = 0;
    const size_t skip_n = 8;
    const size_t skip_0 = 16;

    irs::skip_writer writer(skip_0, skip_n);
    ASSERT_EQ(0, writer.max_levels());
    writer.prepare(max_levels, doc_count);
    ASSERT_EQ(skip_0, writer.skip_0());
    ASSERT_EQ(skip_n, writer.skip_n());
    ASSERT_EQ(0, writer.max_levels());
  }

  // at least 1 level must exists
  {
    const size_t max_levels = 0;
    const size_t doc_count = 17;
    const size_t skip_n = 8;
    const size_t skip_0 = 16;
    irs::skip_writer writer(skip_0, skip_n);
    ASSERT_EQ(0, writer.max_levels());
    writer.prepare(max_levels, doc_count);
    ASSERT_EQ(skip_0, writer.skip_0());
    ASSERT_EQ(skip_n, writer.skip_n());
    ASSERT_EQ(1, writer.max_levels());
  }

  // less than max levels
  {
    const size_t doc_count = 1923;
    const size_t skip = 8;
    const size_t max_levels = 10;
    irs::skip_writer writer(skip, skip);
    ASSERT_EQ(0, writer.max_levels());
    writer.prepare(max_levels, doc_count);
    ASSERT_EQ(skip, writer.skip_0());
    ASSERT_EQ(skip, writer.skip_n());
    ASSERT_EQ(3, writer.max_levels());
  }

  // more than max levels 
  {
    const size_t doc_count = 1923000;
    const size_t skip = 8;
    const size_t max_levels = 5;
    irs::skip_writer writer(skip, skip);
    ASSERT_EQ(0, writer.max_levels());
    writer.prepare(max_levels, doc_count);
    ASSERT_EQ(skip, writer.skip_0());
    ASSERT_EQ(skip, writer.skip_n());
    ASSERT_EQ(5, writer.max_levels());

    writer.prepare(max_levels, 7);
    ASSERT_EQ(skip, writer.skip_0());
    ASSERT_EQ(skip, writer.skip_n());
    ASSERT_EQ(0, writer.max_levels());

    writer.prepare(max_levels, 0);
    ASSERT_EQ(skip, writer.skip_0());
    ASSERT_EQ(skip, writer.skip_n());
    ASSERT_EQ(0, writer.max_levels());
  }
}

TEST_F(SkipWriterTest, WriteFlush) {
  SkipWriterTest::write_flush(1923, 5, 8);
}

TEST_F(SkipWriterTest, Reset) {
  const size_t skip = 13;
  const size_t max_levels = 10;
  const size_t count = 2873;

  std::vector<int> levels[max_levels];
  size_t cur_doc = std::numeric_limits<size_t>::max();

  auto write_skip = [&cur_doc, &levels](size_t level, irs::index_output& out) {
    levels[level].push_back(cur_doc);
    out.write_vlong(cur_doc);
  };

  //.Prepare writer
  irs::skip_writer writer(skip, skip);
  ASSERT_EQ(0, writer.max_levels());
  writer.prepare(max_levels, count);
  ASSERT_NE(max_levels, writer.max_levels());

  //.Prepare directory
  irs::memory_directory dir;

  // write initial data
  {
    const std::string file = "docs_0";

    // write data
    {
      for (size_t i = 0; i < count; ++i) {
        cur_doc = i;
        if (i && 0 == i % skip) {
          writer.skip(i, write_skip);
        }
      }

      auto out = dir.create(file);
      ASSERT_FALSE(!out);
      writer.flush(*out);
    }
  
    // check levels data
    {
      size_t step = skip;
      for (auto& level : levels) {
        int expected_doc = 0;

        for (auto doc : level) {
          expected_doc += irs::doc_id_t(step);
          ASSERT_EQ(expected_doc, doc);
        }
        step *= skip;
      }
    }

    // check data in stream
    // we write levels in reverse order into a stream (from n downto 0)
    {
      auto in = dir.open(file, irs::IOAdvice::NORMAL);
      ASSERT_FALSE(!in);

      size_t num_levels = in->read_vint();

      // check levels from n downto 1
      for (;num_levels > 1; --num_levels) {
        // skip level size
        in->read_vlong();
        auto& level = levels[num_levels-1];
        for (auto expected_doc : level) {
          auto doc = in->read_vint();
          ASSERT_EQ(expected_doc, doc);
          // skip child pointer
          in->read_vlong();
        }
      }

      // check level 0
      if (num_levels) {
        // skip level size
        in->read_vlong();
        auto& level = levels[0];
        for (auto expected_doc : level) {
          auto doc = in->read_vint();
          ASSERT_EQ(expected_doc, doc);
        }
      }
    }
  }

  // reset writer and write another docs into another stream
  {
    writer.reset();
    for (auto& level : levels) {
      level.clear();
    }

    const std::string file = "docs_1";

    // write data
    {
      for (size_t i = 0; i < count; ++i) {
        cur_doc = 2*i;
        // skip every "skip" document
        if (i && 0 == i % skip) {
          writer.skip(i, write_skip);
        }
      }

      auto out = dir.create(file);
      ASSERT_FALSE(!out);
      writer.flush(*out);
    }
    
    // check levels data
    {
      size_t step = skip;
      for (auto& level : levels) {
        int expected_doc = 0;

        for (auto doc : level) {
          expected_doc += irs::doc_id_t(2*step);
          ASSERT_EQ(expected_doc, doc);
        }
        step *= skip;
      }
    }

    // check data in stream
    // we write levels in reverse order into a stream (from n downto 0)
    {
      auto in = dir.open(file, irs::IOAdvice::NORMAL);
      ASSERT_FALSE(!in);

      size_t num_levels = in->read_vint();

      // check levels from n downto 1
      for (;num_levels > 1; --num_levels) {
        // skip level size
        in->read_vlong();
        auto& level = levels[num_levels-1];
        for (auto expected_doc : level) {
          auto doc = in->read_vint();
          ASSERT_EQ(expected_doc, doc);
          // skip child pointer
          in->read_vlong();
        }
      }

      // check level 0
      if (num_levels) {
        // skip level size
        in->read_vlong();
        auto& level = levels[0];
        for (auto expected_doc : level) {
          auto doc = in->read_vint();
          ASSERT_EQ(expected_doc, doc);
        }
      }
    }
  }
}

TEST_F(SkipReaderTest,Prepare) {
  struct NoopRead {
    void MoveDown(size_t) {
      ASSERT_FALSE(true);
    }

    irs::doc_id_t Read(size_t, size_t, irs::data_input&) {
      EXPECT_FALSE(true);
      return irs::doc_limits::eof();
    }
  };

  //.Prepare empty
  {
    size_t count = 1000;
    size_t max_levels = 5;
    size_t skip = 8;

    irs::skip_writer writer(skip, skip);
    ASSERT_EQ(0, writer.max_levels());
    writer.prepare(max_levels, count);
    ASSERT_EQ(3, writer.max_levels());
    irs::memory_directory dir;
    {
      auto out = dir.create("docs");
      ASSERT_FALSE(!out);
      writer.flush(*out);
    }

    irs::SkipReader<NoopRead> reader(skip, skip, NoopRead{});
    ASSERT_EQ(0, reader.NumLevels());
    {
      auto in = dir.open("docs", irs::IOAdvice::NORMAL);
      ASSERT_FALSE(!in);
      reader.Prepare(std::move(in));
    }
    ASSERT_EQ(0, reader.NumLevels());
    ASSERT_EQ(skip, reader.Skip0());
    ASSERT_EQ(skip, reader.SkipN());
  }
 
  //.Prepare not empty
  {
    size_t count = 1932;
    size_t max_levels = 5;
    size_t skip = 8;

    struct write_skip {
      void operator()(size_t, irs::index_output& out) {
        out.write_vint(0);
      }
    };

    irs::skip_writer writer(skip, skip);
    ASSERT_EQ(0, writer.max_levels());
    irs::memory_directory dir;

    // write data
    {
      writer.prepare(max_levels, count);
      ASSERT_EQ(3, writer.max_levels());

      for (size_t i = 0; i <= count; ++i) {
        if (i && 0 == i % skip) {
          writer.skip(i, write_skip{});
        }
      }

      auto out = dir.create("docs");
      ASSERT_FALSE(!out);
      writer.flush(*out);
    }

    irs::SkipReader<NoopRead> reader(skip, skip, NoopRead{});
    ASSERT_EQ(0, reader.NumLevels());
    auto in = dir.open("docs", irs::IOAdvice::NORMAL);
    ASSERT_FALSE(!in);
    reader.Prepare(std::move(in));
    ASSERT_EQ(writer.max_levels(), reader.NumLevels());
    ASSERT_EQ(skip, reader.Skip0());
    ASSERT_EQ(skip, reader.SkipN());
  }
}

TEST_F(SkipReaderTest, Seek) {
  {
    const size_t count = 1932;
    const size_t max_levels = 5;
    const size_t skip_0 = 8;
    const size_t skip_n = 8;
    const std::string file = "docs";

    irs::memory_directory dir;
    // write data
    {
      size_t low = irs::doc_limits::invalid();
      size_t high = irs::doc_limits::invalid();

      struct write_skip {
        size_t* low;
        size_t* high;

        void operator()(size_t level, irs::index_output& out) {
          if (!level) {
            out.write_vlong(*low);
          }
          out.write_vlong(*high); // upper
        }
      };

      irs::skip_writer writer(skip_0, skip_n);
      ASSERT_EQ(0, writer.max_levels());

      // write data

      writer.prepare(max_levels, count);
      ASSERT_EQ(3, writer.max_levels());

      size_t size = 0;
      for (size_t doc = 0; doc <= count; ++doc, ++size) {
        // skip every "skip" document
        if (size == skip_0) {
          writer.skip(doc, write_skip{&low, &high});
          size = 0;
          low = high;
        }

        high = doc;
      }

      auto out = dir.create(file);
      ASSERT_FALSE(!out);
      writer.flush(*out);
    }

    // check written data
    {
      struct ReadSkip {
        irs::doc_id_t lower = irs::doc_limits::invalid();
        irs::doc_id_t upper = irs::doc_limits::invalid();
        size_t calls_count = 0;

        void MoveDown(size_t) { }

        irs::doc_id_t Read(size_t level, size_t end, irs::data_input& in) {
          ++calls_count;

          if (in.file_pointer() >= end) {
            lower = upper;
            upper = irs::doc_limits::eof();
          } else {
            if (!level) {
              lower = in.read_vlong();
            }

            upper = in.read_vlong();
          }

          return upper;
        }
      };


      irs::SkipReader<ReadSkip> reader(skip_0, skip_n, ReadSkip{});
      auto& ctx = reader.Reader();
      auto& lower = ctx.lower;
      auto& upper = ctx.upper;
      auto& calls_count = ctx.calls_count;

      ASSERT_EQ(0, reader.NumLevels());
      auto in = dir.open(file, irs::IOAdvice::NORMAL);
      ASSERT_FALSE(!in);
      reader.Prepare(std::move(in));
      ASSERT_EQ(3, reader.NumLevels());
      ASSERT_EQ(skip_0, reader.Skip0());
      ASSERT_EQ(skip_n, reader.SkipN());

      // seek to 5
      {
        ASSERT_EQ(0, reader.Seek(5));
        ASSERT_FALSE(irs::doc_limits::valid(lower));
        ASSERT_EQ(7, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(0, reader.Seek(5));
        ASSERT_EQ(0, calls_count);
      }

      // seek to last document in a 1st block 
      {
        ASSERT_EQ(0, reader.Seek(7));
        ASSERT_FALSE(irs::doc_limits::valid(lower));
        ASSERT_EQ(7, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(0, reader.Seek(7));
        ASSERT_EQ(0, calls_count);
      }

      // seek to the first document in a 2nd block
      {
        ASSERT_EQ(8, reader.Seek(8));
        ASSERT_EQ(7, lower);
        ASSERT_EQ(15, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(8, reader.Seek(8));
        ASSERT_EQ(0, calls_count);
      }

      // seek to 63
      {
        ASSERT_EQ(56, reader.Seek(63));
        ASSERT_EQ(55, lower);
        ASSERT_EQ(63, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(56, reader.Seek(63));
        ASSERT_EQ(0, calls_count);
      }

      // seek to 64
      {
        ASSERT_EQ(64, reader.Seek(64));
        ASSERT_EQ(63, lower);
        ASSERT_EQ(71, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(64, reader.Seek(64));
        ASSERT_EQ(0, calls_count);
      }

      // seek to the 767 
      {
        ASSERT_EQ(760, reader.Seek(767));
        ASSERT_EQ(759, lower);
        ASSERT_EQ(767, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(760, reader.Seek(767));
        ASSERT_EQ(0, calls_count);
      }

      // seek to the 1023 
      {
        ASSERT_EQ(1016, reader.Seek(1023));
        ASSERT_EQ(1015, lower);
        ASSERT_EQ(1023, upper);
      }

      // seek to the 1024 
      {
        ASSERT_EQ(1024, reader.Seek(1024));
        ASSERT_EQ(1023, lower);
        ASSERT_EQ(1031, upper);
      }

      // seek to the 1512
      {
        ASSERT_EQ(1512, reader.Seek(1512));
        ASSERT_EQ(1511, lower);
        ASSERT_EQ(1519, upper);
      }

      // seek to the 1701 
      {
        ASSERT_EQ(1696, reader.Seek(1701));
        ASSERT_EQ(1695, lower);
        ASSERT_EQ(1703, upper);
      }

      // seek to 1920
      {
        ASSERT_EQ(1920, reader.Seek(1920));
        ASSERT_EQ(1919, lower);
        ASSERT_EQ(1927, upper);
      }

      // seek to last doc in a skip-list
      {
        ASSERT_EQ(1928, reader.Seek(1928));
        ASSERT_EQ(1927, lower);
        ASSERT_TRUE(irs::doc_limits::eof(upper));
      }

      // seek after the last doc in a skip-list
      {
        calls_count = 0;
        ASSERT_EQ(1928, reader.Seek(1930));
        ASSERT_EQ(1927, lower);
        ASSERT_TRUE(irs::doc_limits::eof(upper));
        ASSERT_EQ(0, calls_count);
      }

      // seek after the last doc in a skip-list
      {
        calls_count = 0;
        ASSERT_EQ(1928, reader.Seek(1935));
        ASSERT_EQ(1927, lower);
        ASSERT_TRUE(irs::doc_limits::eof(upper));
        ASSERT_EQ(0, calls_count);
      }

      // seek to NO_MORE_DOCS
      {
        calls_count = 0;
        ASSERT_EQ(1928, reader.Seek(irs::doc_limits::eof()));
        ASSERT_EQ(1927, lower);
        ASSERT_TRUE(irs::doc_limits::eof(upper));
        ASSERT_EQ(0, calls_count);
      }

      // seek to lower document
      {
        calls_count = 0;
        ASSERT_EQ(1928, reader.Seek(767));
        ASSERT_EQ(1927, lower);
        ASSERT_TRUE(irs::doc_limits::eof(upper));
        ASSERT_EQ(0, calls_count);
      }

      // reset && seek to document
      {
        reader.Reset();
        ASSERT_EQ(760, reader.Seek(767));
        ASSERT_EQ(759, lower);
        ASSERT_EQ(767, upper);
      }

      // reset && seek to type_limits<type_t::doc_id_t>::min()
      {
        reader.Reset();
        ASSERT_EQ(0, reader.Seek(irs::doc_limits::min()));
        ASSERT_FALSE(irs::doc_limits::valid(lower));
        ASSERT_EQ(7, upper);
      }

      // reset && seek to irs::INVALID_DOC
      {
        calls_count = 0;
        lower = upper = irs::doc_limits::invalid();
        reader.Reset();
        ASSERT_EQ(0, reader.Seek(irs::doc_limits::invalid()));
        ASSERT_FALSE(irs::doc_limits::valid(lower));
        ASSERT_FALSE(irs::doc_limits::valid(upper));
        ASSERT_EQ(0, calls_count);
      }

      // reset && seek to irs::NO_MORE_DOCS
      {
        reader.Reset();
        ASSERT_EQ(1928, reader.Seek(irs::doc_limits::eof()));
        ASSERT_EQ(1927, lower);
        ASSERT_TRUE(irs::doc_limits::eof(upper));
      }

      // reset && seek to 1928 
      {
        reader.Reset();
        ASSERT_EQ(1928, reader.Seek(1928));
        ASSERT_EQ(1927, lower);
        ASSERT_TRUE(irs::doc_limits::eof(upper));
      }

      // reset && seek to 1927 
      {
        reader.Reset();
        ASSERT_EQ(1920, reader.Seek(1927));
        ASSERT_EQ(1919, lower);
        ASSERT_EQ(1927, upper);
      }

      // reset && seek to 1511 
      {
        reader.Reset();
        ASSERT_EQ(1504, reader.Seek(1511));
        ASSERT_EQ(1503, lower);
        ASSERT_EQ(1511, upper);
      }

      // reset && seek to 1512 
      {
        reader.Reset();
        ASSERT_EQ(1512, reader.Seek(1512));
        ASSERT_EQ(1511, lower);
        ASSERT_EQ(1519, upper);
      }
    }
  } 

  {
    const size_t count = 7192;
    const size_t max_levels = 5;
    const size_t skip_0 = 8;
    const size_t skip_n = 16;
    const std::string file = "docs";

    size_t low = irs::doc_limits::invalid();
    size_t high = irs::doc_limits::invalid();

    struct write_skip {
      size_t* low;
      size_t* high;

      void operator()(size_t level, irs::index_output& out) {
        if (!level) {
          out.write_vlong(*low);
        }
        out.write_vlong(*high); // upper
      }
    };

    irs::memory_directory dir;
    irs::skip_writer writer(skip_0, skip_n);
    ASSERT_EQ(0, writer.max_levels());

    // write data
    {
      writer.prepare(max_levels, count);
      ASSERT_EQ(3, writer.max_levels());

      size_t size = 0;
      for (size_t doc = 0; doc <= count; ++doc, ++size) {
        // skip every "skip" document
        if (size == skip_0) {
          writer.skip(doc, write_skip{&low, &high});
          size = 0;
          low = high;
        }

        high = doc;
      }

      auto out = dir.create(file);
      ASSERT_FALSE(!out);
      writer.flush(*out);
    }

    // check written data
    {
      struct ReadSkip {
        irs::doc_id_t lower = irs::doc_limits::invalid();
        irs::doc_id_t upper = irs::doc_limits::invalid();
        size_t calls_count = 0;

        void MoveDown(size_t) { }

        irs::doc_id_t Read(size_t level, size_t end, irs::data_input& in) {
          ++calls_count;

          if (in.file_pointer() >= end) {
            lower = upper;
            upper = irs::doc_limits::eof();
          } else {
            if (!level) {
              lower = in.read_vlong();
            }
            upper = in.read_vlong();
          }

          return upper;
        }
      };

      irs::SkipReader<ReadSkip> reader(skip_0, skip_n, ReadSkip{});
      auto& ctx = reader.Reader();
      auto& lower = ctx.lower;
      auto& upper = ctx.upper;
      auto& calls_count = ctx.calls_count;

      ASSERT_EQ(0, reader.NumLevels());
      auto in = dir.open(file, irs::IOAdvice::RANDOM);
      ASSERT_FALSE(!in);
      reader.Prepare(std::move(in));
      ASSERT_EQ(writer.max_levels(), reader.NumLevels());
      ASSERT_EQ(skip_0, reader.Skip0());
      ASSERT_EQ(skip_n, reader.SkipN());

      // seek to 5
      {
        ASSERT_EQ(0, reader.Seek(5));
        ASSERT_FALSE(irs::doc_limits::valid(lower));
        ASSERT_EQ(7, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(0, reader.Seek(5));
        ASSERT_EQ(0, calls_count);
      }

      // seek to last document in a 1st block 
      {
        ASSERT_EQ(0, reader.Seek(7));
        ASSERT_FALSE(irs::doc_limits::valid(lower));
        ASSERT_EQ(7, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(0, reader.Seek(7));
        ASSERT_EQ(0, calls_count);
      }

      // seek to the first document in a 2nd block
      {
        ASSERT_EQ(8, reader.Seek(8));
        ASSERT_EQ(7, lower);
        ASSERT_EQ(15, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(8, reader.Seek(8));
        ASSERT_EQ(0, calls_count);
      }

      // seek to 63
      {
        ASSERT_EQ(56, reader.Seek(63));
        ASSERT_EQ(55, lower);
        ASSERT_EQ(63, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(56, reader.Seek(63));
        ASSERT_EQ(0, calls_count);
      }

      // seek to 64
      {
        ASSERT_EQ(64, reader.Seek(64));
        ASSERT_EQ(63, lower);
        ASSERT_EQ(71, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(64, reader.Seek(64));
        ASSERT_EQ(0, calls_count);
      }

      // seek to the 767 
      {
        ASSERT_EQ(760, reader.Seek(767));
        ASSERT_EQ(759, lower);
        ASSERT_EQ(767, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(760, reader.Seek(767));
        ASSERT_EQ(0, calls_count);
      }

      // seek to the 1023 
      {
        ASSERT_EQ(1016, reader.Seek(1023));
        ASSERT_EQ(1015, lower);
        ASSERT_EQ(1023, upper);
      }

      // seek to the 1024 
      {
        ASSERT_EQ(1024, reader.Seek(1024));
        ASSERT_EQ(1023, lower);
        ASSERT_EQ(1031, upper);
      }

      // seek to the 1512
      {
        ASSERT_EQ(1512, reader.Seek(1512));
        ASSERT_EQ(1511, lower);
        ASSERT_EQ(1519, upper);
      }

      // seek to the 1701 
      {
        ASSERT_EQ(1696, reader.Seek(1701));
        ASSERT_EQ(1695, lower);
        ASSERT_EQ(1703, upper);
      }

      // seek to 1920
      {
        ASSERT_EQ(1920, reader.Seek(1920));
        ASSERT_EQ(1919, lower);
        ASSERT_EQ(1927, upper);
      }

      // seek to one doc before the last in a skip-list
      {
        ASSERT_EQ(7184, reader.Seek(7191));
        ASSERT_EQ(7183, lower);
        ASSERT_EQ(7191, upper);
      }

      // seek to last doc in a skip-list
      {
        ASSERT_EQ(7192, reader.Seek(7192));
        ASSERT_EQ(7191, lower);
        ASSERT_TRUE(irs::doc_limits::eof(upper));
      }

      // seek to after the last doc in a skip-list
      {
        ASSERT_EQ(7192, reader.Seek(7193));
        ASSERT_EQ(7191, lower);
        ASSERT_TRUE(irs::doc_limits::eof(upper));
      }
    }
  }

  {
    const size_t count = 14721;
    const size_t max_levels = 5;
    const size_t skip_0 = 16;
    const size_t skip_n = 8;
    const std::string file = "docs";

    size_t low = irs::doc_limits::invalid();
    size_t high = irs::doc_limits::invalid();

    struct write_skip {
      size_t* high;

      void operator()(size_t, irs::index_output& out) {
        out.write_vlong(*high); // upper
      }
    };

    irs::memory_directory dir;
    irs::skip_writer writer(skip_0, skip_n);
    ASSERT_EQ(0, writer.max_levels());

    // write data
    {
      writer.prepare(max_levels, count);
      ASSERT_EQ(4, writer.max_levels());

      size_t size = 0;
      for (size_t doc = 0; doc <= count; ++doc, ++size) {
        // skip every "skip" document
        if (size == skip_0) {
          writer.skip(doc, write_skip{&high});
          size = 0;
          low = high;
        }

        high = doc;
      }

      auto out = dir.create(file);
      ASSERT_FALSE(!out);
      writer.flush(*out);
    }

    // check written data
    {
      struct ReadSkip {
        irs::doc_id_t lower = irs::doc_limits::invalid();
        irs::doc_id_t upper = irs::doc_limits::invalid();
        size_t calls_count = 0;
        size_t last_level = max_levels;

        void MoveDown(size_t) { }

        irs::doc_id_t Read(size_t level, size_t end, irs::data_input& in) {
          ++calls_count;

          if (last_level > level) {
            upper = lower;
          } else {
            lower = upper;
          }
          last_level = level;

          if (in.file_pointer() >= end) {
            upper = irs::doc_limits::eof();
          } else {
            upper = in.read_vlong();
          }

          return upper;
        }
      };

      irs::SkipReader<ReadSkip> reader(skip_0, skip_n, ReadSkip{});
      auto& ctx = reader.Reader();
      auto& lower = ctx.lower;
      auto& upper = ctx.upper;
      auto& calls_count = ctx.calls_count;

      ASSERT_EQ(0, reader.NumLevels());
      auto in = dir.open(file, irs::IOAdvice::RANDOM);
      ASSERT_FALSE(!in);
      reader.Prepare(std::move(in));
      ASSERT_EQ(writer.max_levels(), reader.NumLevels());
      ASSERT_EQ(skip_0, reader.Skip0());
      ASSERT_EQ(skip_n, reader.SkipN());

      // seek to 5
      {
        ASSERT_EQ(0, reader.Seek(5));
        ASSERT_FALSE(irs::doc_limits::valid(lower));
        ASSERT_EQ(15, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(0, reader.Seek(5));
        ASSERT_EQ(0, calls_count);
      }

      // seek to last document in a 1st block 
      {
        ASSERT_EQ(0, reader.Seek(15));
        ASSERT_FALSE(irs::doc_limits::valid(lower));
        ASSERT_EQ(15, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(0, reader.Seek(7));
        ASSERT_EQ(0, calls_count);
      }

      // seek to the first document in a 2nd block
      {
        ASSERT_EQ(16, reader.Seek(16));
        ASSERT_EQ(15, lower);
        ASSERT_EQ(31, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(16, reader.Seek(16));
        ASSERT_EQ(0, calls_count);
      }

      // seek to 127 
      {
        ASSERT_EQ(112, reader.Seek(127));
        ASSERT_EQ(111, lower);
        ASSERT_EQ(127, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(112, reader.Seek(127));
        ASSERT_EQ(0, calls_count);
      }

      // seek to 128 
      {
        ASSERT_EQ(128, reader.Seek(128));
        ASSERT_EQ(127, lower);
        ASSERT_EQ(143, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(128, reader.Seek(128));
        ASSERT_EQ(0, calls_count);
      }

      // seek to the 1767 
      {
        ASSERT_EQ(1760, reader.Seek(1767));
        ASSERT_EQ(1759, lower);
        ASSERT_EQ(1775, upper);

        // seek to same document
        calls_count = 0;
        ASSERT_EQ(1760, reader.Seek(767));
        ASSERT_EQ(0, calls_count);
      }

      // seek to the 3999 
      {
        ASSERT_EQ(3984, reader.Seek(3999));
        ASSERT_EQ(3983, lower);
        ASSERT_EQ(3999, upper);
      }

      // seek to the 4000 
      {
        ASSERT_EQ(4000, reader.Seek(4000));
        ASSERT_EQ(3999, lower);
        ASSERT_EQ(4015, upper);
      }

      // seek to 7193 
      {
        ASSERT_EQ(7184, reader.Seek(7193));
        ASSERT_EQ(7183, lower);
        ASSERT_EQ(7199, upper);
      }

      // seek to last doc in a skip-list
      {
        ASSERT_EQ(14720, reader.Seek(14721));
        ASSERT_EQ(14719, lower);
        ASSERT_TRUE(irs::doc_limits::eof(upper));
      }
    }
  }

  // case with empty 4th skip-level
  {
    const size_t count = 32768;
    const size_t max_levels = 8;
    const size_t skip_0 = 128;
    const size_t skip_n = 8;
    const std::string file = "docs";

    size_t high = irs::doc_limits::invalid();

    struct write_skip {
      size_t* high;

      void operator()(size_t, irs::index_output& out) {
          out.write_vlong(*high); // upper
      }
    };

    irs::memory_directory dir;
    irs::skip_writer writer(skip_0, skip_n);
    ASSERT_EQ(0, writer.max_levels());

    // write data
    {
      writer.prepare(max_levels, count);
      ASSERT_EQ(3 , writer.max_levels());

      irs::doc_id_t doc = irs::doc_limits::min();
      for (size_t size = 0; size <= count; doc += 2, ++size) {
        // skip every "skip" document
        if (size && 0 == size % skip_0) {
          writer.skip(size, write_skip{&high});
        }

        high = doc;
      }

      auto out = dir.create(file);
      ASSERT_FALSE(!out);
      writer.flush(*out);
    }

    // check written data
    {
      struct ReadSkip {
        irs::doc_id_t lower = irs::doc_limits::invalid();
        irs::doc_id_t upper = irs::doc_limits::invalid();
        size_t calls_count = 0;
        size_t last_level = max_levels;

        void MoveDown(size_t) { }

        irs::doc_id_t Read(size_t level, size_t end, irs::data_input& in) {
          ++calls_count;

          if (last_level > level) {
            upper = lower;
          } else {
            lower = upper;
          }
          last_level = level;

          if (in.file_pointer() >= end) {
            upper = irs::doc_limits::eof();
          } else {
            upper = in.read_vlong();
          }

          return upper;
        }
      };

      irs::SkipReader<ReadSkip> reader(skip_0, skip_n, ReadSkip{});
      auto& ctx = reader.Reader();
      auto& lower = ctx.lower;
      auto& upper = ctx.upper;

      ASSERT_EQ(0, reader.NumLevels());
      auto in = dir.open(file, irs::IOAdvice::NORMAL);
      ASSERT_FALSE(!in);
      reader.Prepare(std::move(in));

      // seek for every document
      {
        irs::doc_id_t doc = irs::doc_limits::min();
        for (size_t i = 0; i <= count; ++i, doc += 2) {
          const size_t skipped = (i/skip_0) * skip_0;
          ASSERT_EQ(skipped, reader.Seek(doc));
          ASSERT_TRUE(lower < doc);
          ASSERT_TRUE(doc <= upper);
        }
      }

      // seek backwards
      irs::doc_id_t doc = count*2 + irs::doc_limits::min();
      for (size_t i = count; i <= count; --i, doc -= 2) {
        lower = irs::doc_limits::invalid();
        upper = irs::doc_limits::invalid();
        reader.Reset();
        size_t skipped = (i/skip_0)*skip_0;
        ASSERT_EQ(skipped, reader.Seek(doc));
        ASSERT_TRUE(lower < doc);
        ASSERT_TRUE(doc <= upper);
      }
    }
  }
}
