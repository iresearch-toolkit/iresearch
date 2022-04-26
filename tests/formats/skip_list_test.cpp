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

    irs::SkipWriter writer(skip, skip);
    ASSERT_EQ(0, writer.MaxLevels());
    irs::memory_directory dir;

    // write data
    {
      writer.Prepare(max_levels, count);
      ASSERT_NE(0, writer.MaxLevels());

      for (size_t doc = 0; doc < count; ++doc) {
        cur_doc = doc;
        // skip every "skip" document
        if (doc && 0 == doc % skip) {
          writer.Skip(doc, write_skip);
        }
      }

      auto out = dir.create("docs");
      ASSERT_FALSE(!out);
      writer.Flush(*out);
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

    irs::SkipWriter writer(skip_0, skip_n);
    ASSERT_EQ(0, writer.MaxLevels());
    writer.Prepare(max_levels, doc_count);
    ASSERT_EQ(skip_0, writer.Skip0());
    ASSERT_EQ(skip_n, writer.SkipN());
    ASSERT_EQ(0, writer.MaxLevels());
  }

  // at least 1 level must exists
  {
    const size_t max_levels = 0;
    const size_t doc_count = 17;
    const size_t skip_n = 8;
    const size_t skip_0 = 16;
    irs::SkipWriter writer(skip_0, skip_n);
    ASSERT_EQ(0, writer.MaxLevels());
    writer.Prepare(max_levels, doc_count);
    ASSERT_EQ(skip_0, writer.Skip0());
    ASSERT_EQ(skip_n, writer.SkipN());
    ASSERT_EQ(1, writer.MaxLevels());
  }

  // less than max levels
  {
    const size_t doc_count = 1923;
    const size_t skip = 8;
    const size_t max_levels = 10;
    irs::SkipWriter writer(skip, skip);
    ASSERT_EQ(0, writer.MaxLevels());
    writer.Prepare(max_levels, doc_count);
    ASSERT_EQ(skip, writer.Skip0());
    ASSERT_EQ(skip, writer.SkipN());
    ASSERT_EQ(3, writer.MaxLevels());
  }

  // more than max levels 
  {
    const size_t doc_count = 1923000;
    const size_t skip = 8;
    const size_t max_levels = 5;
    irs::SkipWriter writer(skip, skip);
    ASSERT_EQ(0, writer.MaxLevels());
    writer.Prepare(max_levels, doc_count);
    ASSERT_EQ(skip, writer.Skip0());
    ASSERT_EQ(skip, writer.SkipN());
    ASSERT_EQ(5, writer.MaxLevels());

    writer.Prepare(max_levels, 7);
    ASSERT_EQ(skip, writer.Skip0());
    ASSERT_EQ(skip, writer.SkipN());
    ASSERT_EQ(0, writer.MaxLevels());

    writer.Prepare(max_levels, 0);
    ASSERT_EQ(skip, writer.Skip0());
    ASSERT_EQ(skip, writer.SkipN());
    ASSERT_EQ(0, writer.MaxLevels());
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
  irs::SkipWriter writer(skip, skip);
  ASSERT_EQ(0, writer.MaxLevels());
  writer.Prepare(max_levels, count);
  ASSERT_NE(max_levels, writer.MaxLevels());

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
          writer.Skip(i, write_skip);
        }
      }

      auto out = dir.create(file);
      ASSERT_FALSE(!out);
      writer.Flush(*out);
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
    writer.Reset();
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
          writer.Skip(i, write_skip);
        }
      }

      auto out = dir.create(file);
      ASSERT_FALSE(!out);
      writer.Flush(*out);
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
    bool IsLess(size_t, irs::doc_id_t) const {
      EXPECT_FALSE(true);
      return false;
    }

    void MoveDown(size_t) {
      ASSERT_FALSE(true);
    }

    irs::doc_id_t Read(size_t, size_t, irs::data_input&) const {
      EXPECT_FALSE(true);
      return irs::doc_limits::eof();
    }
  };

  //.Prepare empty
  {
    size_t count = 1000;
    size_t max_levels = 5;
    size_t skip = 8;

    irs::SkipWriter writer(skip, skip);
    ASSERT_EQ(0, writer.MaxLevels());
    writer.Prepare(max_levels, count);
    ASSERT_EQ(3, writer.MaxLevels());
    irs::memory_directory dir;
    {
      auto out = dir.create("docs");
      ASSERT_FALSE(!out);
      writer.Flush(*out);
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

    irs::SkipWriter writer(skip, skip);
    ASSERT_EQ(0, writer.MaxLevels());
    irs::memory_directory dir;

    // write data
    {
      writer.Prepare(max_levels, count);
      ASSERT_EQ(3, writer.MaxLevels());

      for (size_t i = 0; i <= count; ++i) {
        if (i && 0 == i % skip) {
          writer.Skip(i, write_skip{});
        }
      }

      auto out = dir.create("docs");
      ASSERT_FALSE(!out);
      writer.Flush(*out);
    }

    irs::SkipReader<NoopRead> reader(skip, skip, NoopRead{});
    ASSERT_EQ(0, reader.NumLevels());
    auto in = dir.open("docs", irs::IOAdvice::NORMAL);
    ASSERT_FALSE(!in);
    reader.Prepare(std::move(in));
    ASSERT_EQ(writer.MaxLevels(), reader.NumLevels());
    ASSERT_EQ(skip, reader.Skip0());
    ASSERT_EQ(skip, reader.SkipN());
  }
}

TEST_F(SkipReaderTest, Seek) {
   struct WriteSkip {
     size_t* low;
     size_t* high;

     void operator()(size_t level, irs::index_output& out) {
       if (low && !level) {
         out.write_vlong(*low);
       }
       out.write_vlong(*high); // upper
     }
   };


   struct ReadSkip {
     size_t read_calls_count = 0;
     size_t move_down_calls_count = 0;
     mutable size_t is_less_calls_count = 0;
     size_t count;
     irs::doc_id_t lower = irs::doc_limits::invalid();
     std::vector<irs::doc_id_t> upper_bounds{};

     explicit ReadSkip(size_t count, size_t skip_levels) noexcept
       : count{count}, upper_bounds(skip_levels) {}

     bool IsLess(size_t level, irs::doc_id_t target) const {
       EXPECT_LT(level, upper_bounds.size());
       ++is_less_calls_count;
       return upper_bounds[level] < target;
     }

     void MoveDown(size_t level) {
       ASSERT_LT(level, upper_bounds.size());
       ++move_down_calls_count;
       upper_bounds[level] = lower;
     }

     bool Read(size_t level, size_t skipped, irs::data_input& in) {
       EXPECT_LT(level, upper_bounds.size());
       ++read_calls_count;

       if (skipped >= count) {
         lower = upper_bounds[level];
         upper_bounds[level] = irs::doc_limits::eof();
         return false;
       } else {
         if (level == (upper_bounds.size() - 1)) {
           lower = in.read_vlong();
         }

         upper_bounds[level] = in.read_vlong();
         return true;
       }
     }

     void ResetCallsCount() noexcept {
       is_less_calls_count = 0;
       read_calls_count = 0;
       move_down_calls_count = 0;
     }

     void Reset() noexcept {
       lower = irs::doc_limits::invalid();
       std::fill_n(std::begin(upper_bounds), upper_bounds.size(),
                   irs::doc_limits::invalid());
     }

     void AssertCallsCount(size_t is_less, size_t move, size_t read) {
       ASSERT_EQ(is_less, is_less_calls_count);
       ASSERT_EQ(move, move_down_calls_count);
       ASSERT_EQ(read, read_calls_count);
     }
   };

  {
    static constexpr size_t kCount = 1932;
    static constexpr size_t kMaxLevels = 5;
    static constexpr size_t kSkipLevels = 3;
    static constexpr size_t kSkip0 = 8;
    static constexpr size_t kSkipN = 8;
    static constexpr std::string_view kFile = "docs";

    irs::memory_directory dir;

    // write data
    {
      size_t low = irs::doc_limits::invalid();
      size_t high = irs::doc_limits::invalid();

      irs::SkipWriter writer(kSkip0, kSkipN);
      ASSERT_EQ(0, writer.MaxLevels());

      // write data

      writer.Prepare(kMaxLevels, kCount);
      ASSERT_EQ(kSkipLevels, writer.MaxLevels());

      size_t size = 0;
      for (size_t doc = 0; doc <= kCount; ++doc, ++size) {
        // skip every "skip" document
        if (size == kSkip0) {
          writer.Skip(doc, WriteSkip{&low, &high});
          size = 0;
          low = high;
        }

        high = doc;
      }

      auto out = dir.create(kFile);
      ASSERT_FALSE(!out);
      writer.Flush(*out);
    }

    // check written data
    {
      irs::SkipReader<ReadSkip> reader(kSkip0, kSkipN, ReadSkip{kCount, kSkipLevels});
      auto& ctx = reader.Reader();
      auto& lower = ctx.lower;
      auto& upper_bounds = ctx.upper_bounds;

      ASSERT_EQ(0, reader.NumLevels());
      auto in = dir.open(kFile, irs::IOAdvice::NORMAL);
      ASSERT_FALSE(!in);
      reader.Prepare(std::move(in));
      ASSERT_EQ(3, reader.NumLevels());
      ASSERT_EQ(kSkip0, reader.Skip0());
      ASSERT_EQ(kSkipN, reader.SkipN());

      // seek to 5
      ASSERT_EQ(0, reader.Seek(5));
      ASSERT_FALSE(irs::doc_limits::valid(lower));
      ASSERT_EQ((std::vector{511U,63U,7U}), upper_bounds);
      ctx.AssertCallsCount(4, 2, 3);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(5));
      ctx.AssertCallsCount(3, 0, 0);

      // seek to last document in 1st block
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(7));
      ASSERT_FALSE(irs::doc_limits::valid(lower));
      ASSERT_EQ((std::vector{511U,63U,7U}), upper_bounds);
      ctx.AssertCallsCount(3, 0, 0);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(7));
      ctx.AssertCallsCount(3, 0, 0);

      // seek to the first document in 2nd block
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(8));
      ASSERT_EQ(7, lower);
      ASSERT_EQ((std::vector{511U,63U,15U}), upper_bounds);
      ctx.AssertCallsCount(4, 0, 1);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(8));
      ctx.AssertCallsCount(3, 0, 0);

      // seek to 63
      ctx.ResetCallsCount();
      ASSERT_EQ(40, reader.Seek(63));
      ASSERT_EQ(55, lower);
      ASSERT_EQ((std::vector{511U,63U,63U}), upper_bounds);
      ctx.AssertCallsCount(9, 0, 6);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(63));
      ctx.AssertCallsCount(3, 0, 0);

      // seek to 64
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(64));
      ASSERT_EQ(63, lower);
      ASSERT_EQ((std::vector{511U,127U,71U}), upper_bounds);
      ctx.AssertCallsCount(4, 1, 2);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(64));
      ctx.AssertCallsCount(3, 0, 0);

      // seek to the 767
      ctx.ResetCallsCount();
      ASSERT_EQ(688, reader.Seek(767));
      ASSERT_EQ(759, lower);
      ASSERT_EQ((std::vector{1023U,767U,767U}), upper_bounds);
      ctx.AssertCallsCount(14, 2, 13);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(767));
      ctx.AssertCallsCount(3, 0, 0);

      // seek to the 1023
      ctx.ResetCallsCount();
      ASSERT_EQ(248, reader.Seek(1023));
      ASSERT_EQ(1015, lower);
      ASSERT_EQ((std::vector{1023U,1023U,1023U}), upper_bounds);
      ctx.AssertCallsCount(14, 1, 12);

      // seek to the 1024
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(1024));
      ASSERT_EQ(1023, lower);
      ASSERT_EQ((std::vector{1535U,1087U,1031U}), upper_bounds);
      ctx.AssertCallsCount(4, 2, 3);

      // seek to the 1512
      ctx.ResetCallsCount();
      ASSERT_EQ(480, reader.Seek(1512));
      ASSERT_EQ(1511, lower);
      ASSERT_EQ((std::vector{1535U,1535U,1519U}), upper_bounds);
      ctx.AssertCallsCount(15, 1, 13);

      // seek to the 1701 
      ctx.ResetCallsCount();
      ASSERT_EQ(176, reader.Seek(1701));
      ASSERT_EQ(1695, lower);
      ASSERT_EQ((std::vector{irs::doc_limits::eof(),1727U,1703U}), upper_bounds);
      ctx.AssertCallsCount(10, 2, 9);

      // seek to 1920
      ctx.ResetCallsCount();
      ASSERT_EQ(216, reader.Seek(1920));
      ASSERT_EQ(1919, lower);
      ASSERT_EQ((std::vector{irs::doc_limits::eof(),irs::doc_limits::eof(),1927U}), upper_bounds);
      ctx.AssertCallsCount(7, 1, 5);

      // seek to last doc in a skip-list
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(1928));
      ASSERT_EQ(1927, lower);
      ASSERT_EQ((std::vector{irs::doc_limits::eof(),irs::doc_limits::eof(),irs::doc_limits::eof()}), upper_bounds);
      ctx.AssertCallsCount(4, 0, 1);

      // seek after the last doc in a skip-list
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(1930));
      ASSERT_EQ(1927, lower);
      ASSERT_EQ((std::vector{irs::doc_limits::eof(),irs::doc_limits::eof(),irs::doc_limits::eof()}), upper_bounds);
      ctx.AssertCallsCount(3, 0, 0);

      // seek after the last doc in a skip-list
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(1935));
      ASSERT_EQ(1927, lower);
      ASSERT_EQ((std::vector{irs::doc_limits::eof(),irs::doc_limits::eof(),irs::doc_limits::eof()}), upper_bounds);
      ctx.AssertCallsCount(3, 0, 0);

      // seek to eof
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(irs::doc_limits::eof()));
      ASSERT_EQ(1927, lower);
      ASSERT_EQ((std::vector{irs::doc_limits::eof(),irs::doc_limits::eof(),irs::doc_limits::eof()}), upper_bounds);
      ctx.AssertCallsCount(3, 0, 0);

      // seek to lower document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(767));
      ASSERT_EQ(1927, lower);
      ASSERT_EQ((std::vector{irs::doc_limits::eof(),irs::doc_limits::eof(),irs::doc_limits::eof()}), upper_bounds);
      ctx.AssertCallsCount(3, 0, 0);

      // reset && seek to document
      reader.Reset();
      ctx.Reset();
      ctx.ResetCallsCount();
      ASSERT_EQ(760, reader.Seek(767));
      ASSERT_EQ(759, lower);
      ASSERT_EQ((std::vector{1023U,767U,767U}), upper_bounds);
      ctx.AssertCallsCount(15, 2, 14);

      // reset && seek to doc_limits::min()
      reader.Reset();
      ctx.Reset();
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(irs::doc_limits::min()));
      ASSERT_FALSE(irs::doc_limits::valid(lower));
      ASSERT_EQ((std::vector{511U,63U,7U}), upper_bounds);
      ctx.AssertCallsCount(4, 2, 3);

      // reset && seek to doc_limits::invalid()
      reader.Reset();
      ctx.Reset();
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(irs::doc_limits::invalid()));
      ASSERT_FALSE(irs::doc_limits::valid(lower));
      ASSERT_EQ((std::vector{0U,0U,0U}), upper_bounds);
      ctx.AssertCallsCount(3, 0, 0);

      // reset && seek to eof
      reader.Reset();
      ctx.Reset();
      ctx.ResetCallsCount();
      ASSERT_EQ(1928, reader.Seek(irs::doc_limits::eof()));
      ASSERT_EQ(1927, lower);
      ASSERT_EQ((std::vector{irs::doc_limits::eof(),irs::doc_limits::eof(),irs::doc_limits::eof()}), upper_bounds);
      ctx.AssertCallsCount(14, 2, 13);

      // reset && seek to 1928 
      reader.Reset();
      ctx.Reset();
      ctx.ResetCallsCount();
      ASSERT_EQ(1928, reader.Seek(1928));
      ASSERT_EQ(1927, lower);
      ASSERT_EQ((std::vector{irs::doc_limits::eof(),irs::doc_limits::eof(),irs::doc_limits::eof()}), upper_bounds);
      ctx.AssertCallsCount(14, 2, 13);

      // reset && seek to 1927 
      reader.Reset();
      ctx.Reset();
      ctx.ResetCallsCount();
      ASSERT_EQ(1920, reader.Seek(1927));
      ASSERT_EQ(1919, lower);
      ASSERT_EQ((std::vector{irs::doc_limits::eof(),irs::doc_limits::eof(),1927U}), upper_bounds);
      ctx.AssertCallsCount(13, 2, 12);

      // reset && seek to 1511 
      reader.Reset();
      ctx.Reset();
      ctx.ResetCallsCount();
      ASSERT_EQ(1504, reader.Seek(1511));
      ASSERT_EQ(1503, lower);
      ASSERT_EQ((std::vector{1535U,1535U,1511U}), upper_bounds);
      ctx.AssertCallsCount(17, 2, 16);

      // reset && seek to 1512 
      reader.Reset();
      ctx.Reset();
      ctx.ResetCallsCount();
      ASSERT_EQ(1512, reader.Seek(1512));
      ASSERT_EQ(1511, lower);
      ASSERT_EQ((std::vector{1535U,1535U,1519U}), upper_bounds);
      ctx.AssertCallsCount(18, 2, 17);
    }
  } 

  {
    static constexpr size_t kCount = 7192;
    static constexpr size_t kMaxLevels = 5;
    static constexpr size_t kSkipLevels = 3;
    static constexpr size_t kSkip0 = 8;
    static constexpr size_t kSkipN = 16;
    static constexpr std::string_view kFile = "docs";

    size_t low = irs::doc_limits::invalid();
    size_t high = irs::doc_limits::invalid();

    irs::memory_directory dir;
    irs::SkipWriter writer(kSkip0, kSkipN);
    ASSERT_EQ(0, writer.MaxLevels());

    // write data
    {
      writer.Prepare(kMaxLevels, kCount);
      ASSERT_EQ(kSkipLevels, writer.MaxLevels());

      size_t size = 0;
      for (size_t doc = 0; doc <= kCount; ++doc, ++size) {
        // skip every "skip" document
        if (size == kSkip0) {
          writer.Skip(doc, WriteSkip{&low, &high});
          size = 0;
          low = high;
        }

        high = doc;
      }

      auto out = dir.create(kFile);
      ASSERT_FALSE(!out);
      writer.Flush(*out);
    }

    // check written data
    {
      irs::SkipReader<ReadSkip> reader(kSkip0, kSkipN, ReadSkip{kCount, kSkipLevels});
      auto& ctx = reader.Reader();
      auto& lower = ctx.lower;
      auto& upper_bounds = ctx.upper_bounds;

      ASSERT_EQ(0, reader.NumLevels());
      auto in = dir.open(kFile, irs::IOAdvice::RANDOM);
      ASSERT_FALSE(!in);
      reader.Prepare(std::move(in));
      ASSERT_EQ(writer.MaxLevels(), reader.NumLevels());
      ASSERT_EQ(kSkip0, reader.Skip0());
      ASSERT_EQ(kSkipN, reader.SkipN());

      // seek to 5
      ASSERT_EQ(0, reader.Seek(5));
      ASSERT_FALSE(irs::doc_limits::valid(lower));
      ASSERT_EQ((std::vector{2047U,127U,7U}), upper_bounds);
      ctx.AssertCallsCount(4, 2, 3);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(5));
      ctx.AssertCallsCount(3, 0, 0);

      // seek to last document in a 1st block 
      ASSERT_EQ(0, reader.Seek(7));
      ASSERT_FALSE(irs::doc_limits::valid(lower));
      ASSERT_EQ((std::vector{2047U,127U,7U}), upper_bounds);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(7));
      ctx.AssertCallsCount(3, 0, 0);

      // seek to the first document in a 2nd block
      ASSERT_EQ(0, reader.Seek(8));
      ASSERT_EQ(7, lower);
      ASSERT_EQ((std::vector{2047U,127U,15U}), upper_bounds);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(8));
      ctx.AssertCallsCount(3, 0, 0);

      // seek to 63
      ASSERT_EQ(40, reader.Seek(63));
      ASSERT_EQ(55, lower);
      ASSERT_EQ((std::vector{2047U,127U,63U}), upper_bounds);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(63));
      ctx.AssertCallsCount(3, 0, 0);

      // seek to 64
      ASSERT_EQ(0, reader.Seek(64));
      ASSERT_EQ(63, lower);
      ASSERT_EQ((std::vector{2047U,127U,71U}), upper_bounds);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(64));
      ctx.AssertCallsCount(3, 0, 0);

      // seek to the 767 
      ASSERT_EQ(688, reader.Seek(767));
      ASSERT_EQ(759, lower);
      ASSERT_EQ((std::vector{2047U,767U,767U}), upper_bounds);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(767));
      ctx.AssertCallsCount(3, 0, 0);

      // seek to the 1023 
      ASSERT_EQ(248, reader.Seek(1023));
      ASSERT_EQ(1015, lower);
      ASSERT_EQ((std::vector{2047U,1023U,1023U}), upper_bounds);

      // seek to the 1024 
      ASSERT_EQ(0, reader.Seek(1024));
      ASSERT_EQ(1023, lower);
      ASSERT_EQ((std::vector{2047U,1151U,1031U}), upper_bounds);

      // seek to the 1512
      ASSERT_EQ(480, reader.Seek(1512));
      ASSERT_EQ(1511, lower);
      ASSERT_EQ((std::vector{2047U,1535U,1519U}), upper_bounds);

      // seek to the 1701 
      ASSERT_EQ(176, reader.Seek(1701));
      ASSERT_EQ(1695, lower);
      ASSERT_EQ((std::vector{2047U,1791U,1703U}), upper_bounds);

      // seek to 1920
      ASSERT_EQ(216, reader.Seek(1920));
      ASSERT_EQ(1919, lower);
      ASSERT_EQ((std::vector{2047U,2047U,1927U}), upper_bounds);

      // seek to one doc before the last in a skip-list
      ctx.ResetCallsCount();
      ASSERT_EQ(5256, reader.Seek(7191));
      ctx.AssertCallsCount(16, 2, 15);
      ASSERT_EQ(7183, lower);
      ASSERT_EQ((std::vector{irs::doc_limits::eof(),irs::doc_limits::eof(),irs::doc_limits::eof()}), upper_bounds);

      // seek to last doc in a skip-list
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(7192));
      ctx.AssertCallsCount(3, 0, 0);
      ASSERT_EQ(7183, lower);
      ASSERT_EQ((std::vector{irs::doc_limits::eof(),irs::doc_limits::eof(),irs::doc_limits::eof()}), upper_bounds);

      // seek to after the last doc in a skip-list
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(7193));
      ctx.AssertCallsCount(3, 0, 0);
      ASSERT_EQ(7183, lower);
      ASSERT_EQ((std::vector{irs::doc_limits::eof(),irs::doc_limits::eof(),irs::doc_limits::eof()}), upper_bounds);
    }
  }

  {
    static constexpr size_t kCount = 14721;
    static constexpr size_t kMaxLevels = 5;
    static constexpr size_t kSkipLevels = 4;
    static constexpr size_t kSkip0 = 16;
    static constexpr size_t kSkipN = 8;
    static constexpr std::string_view kFile= "docs";

    size_t low = irs::doc_limits::invalid();
    size_t high = irs::doc_limits::invalid();

    irs::memory_directory dir;
    irs::SkipWriter writer(kSkip0, kSkipN);
    ASSERT_EQ(0, writer.MaxLevels());

    // write data
    {
      writer.Prepare(kMaxLevels, kCount);
      ASSERT_EQ(kSkipLevels, writer.MaxLevels());

      size_t size = 0;
      for (size_t doc = 0; doc <= kCount; ++doc, ++size) {
        // skip every "skip" document
        if (size == kSkip0) {
          writer.Skip(doc, WriteSkip{&low, &high});
          size = 0;
          low = high;
        }

        high = doc;
      }

      auto out = dir.create(kFile);
      ASSERT_FALSE(!out);
      writer.Flush(*out);
    }

    // check written data
    {
      irs::SkipReader<ReadSkip> reader(kSkip0, kSkipN, ReadSkip{kCount, kSkipLevels});
      auto& ctx = reader.Reader();
      auto& lower = ctx.lower;
      auto& upper_bounds = ctx.upper_bounds;

      ASSERT_EQ(0, reader.NumLevels());
      auto in = dir.open(kFile, irs::IOAdvice::RANDOM);
      ASSERT_FALSE(!in);
      reader.Prepare(std::move(in));
      ASSERT_EQ(writer.MaxLevels(), reader.NumLevels());
      ASSERT_EQ(kSkip0, reader.Skip0());
      ASSERT_EQ(kSkipN, reader.SkipN());

      // seek to 5
      ASSERT_EQ(0, reader.Seek(5));
      ASSERT_FALSE(irs::doc_limits::valid(lower));
      ASSERT_EQ((std::vector{8191U,1023U,127U,15U}), upper_bounds);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(5));
      ctx.AssertCallsCount(4, 0, 0);

      // seek to last document in the 1st block
      ASSERT_EQ(0, reader.Seek(15));
      ASSERT_FALSE(irs::doc_limits::valid(lower));
      ASSERT_EQ((std::vector{8191U,1023U,127U,15U}), upper_bounds);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(7));
      ctx.AssertCallsCount(4, 0, 0);

      // seek to the first document in a 2nd block
      ASSERT_EQ(0, reader.Seek(16));
      ASSERT_EQ(15, lower);
      ASSERT_EQ((std::vector{8191U,1023U,127U,31U}), upper_bounds);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(16));
      ctx.AssertCallsCount(4, 0, 0);

      // seek to 127 
      ASSERT_EQ(80, reader.Seek(127));
      ASSERT_EQ(111, lower);
      ASSERT_EQ((std::vector{8191U,1023U,127U,127U}), upper_bounds);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(127));
      ctx.AssertCallsCount(4, 0, 0);

      // seek to 128 
      ASSERT_EQ(0, reader.Seek(128));
      ASSERT_EQ(127, lower);
      ASSERT_EQ((std::vector{8191U,1023U,255U,143U}), upper_bounds);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(128));
      ctx.AssertCallsCount(4, 0, 0);

      // seek to the 1767 
      ASSERT_EQ(1616, reader.Seek(1767));
      ASSERT_EQ(1759, lower);
      ASSERT_EQ((std::vector{8191U,2047U,1791U,1775U}), upper_bounds);

      // seek to same document
      ctx.ResetCallsCount();
      ASSERT_EQ(0, reader.Seek(767));
      ctx.AssertCallsCount(4, 0, 0);

      // seek to the 3999 
      ASSERT_EQ(2208, reader.Seek(3999));
      ASSERT_EQ(3983, lower);
      ASSERT_EQ((std::vector{8191U,4095U,4095U,3999U}), upper_bounds);

      // seek to the 4000 
      ASSERT_EQ(0, reader.Seek(4000));
      ASSERT_EQ(3999, lower);
      ASSERT_EQ((std::vector{8191U,4095U,4095U,4015U}), upper_bounds);

      // seek to 7193 
      ASSERT_EQ(3168, reader.Seek(7193));
      ASSERT_EQ(7183, lower);
      ASSERT_EQ((std::vector{8191U,8191U,7295U,7199U}), upper_bounds);

      // seek to last doc in a skip-list
      ASSERT_EQ(7520, reader.Seek(14721));
      ASSERT_EQ(14719, lower);
      ASSERT_EQ((std::vector{irs::doc_limits::eof(),irs::doc_limits::eof(),irs::doc_limits::eof(),irs::doc_limits::eof()}), upper_bounds);
    }
  }

  // case with empty 4th skip-level
  {
    static constexpr size_t kCount = 32768;
    static constexpr size_t kMaxLevels = 8;
    static constexpr size_t kSkipLevels = 3;
    static constexpr size_t kSkip0 = 128;
    static constexpr size_t kSkipN = 8;
    static constexpr std::string_view kFile = "docs";

    size_t low = irs::doc_limits::invalid();
    size_t high = irs::doc_limits::invalid();

    irs::memory_directory dir;
    irs::SkipWriter writer(kSkip0, kSkipN);
    ASSERT_EQ(0, writer.MaxLevels());

    // write data
    {
      writer.Prepare(kMaxLevels, kCount);
      ASSERT_EQ(kSkipLevels, writer.MaxLevels());

      irs::doc_id_t doc = irs::doc_limits::min();
      for (size_t size = 0; size <= kCount; doc += 2, ++size) {
        // skip every "skip" document
        if (size && 0 == size % kSkip0) {
          writer.Skip(size, WriteSkip{&low, &high});
          low = high;
        }

        high = doc;
      }

      auto out = dir.create(kFile);
      ASSERT_FALSE(!out);
      writer.Flush(*out);
    }

    // check written data
    {
      irs::SkipReader<ReadSkip> reader(kSkip0, kSkipN, ReadSkip{kCount, kSkipLevels});
      auto& ctx = reader.Reader();
      auto& lower = ctx.lower;
      auto& upper_bounds = ctx.upper_bounds;
      auto& upper = *(std::end(upper_bounds)-1);

      ASSERT_EQ(0, reader.NumLevels());
      auto in = dir.open(kFile, irs::IOAdvice::NORMAL);
      ASSERT_FALSE(!in);
      reader.Prepare(std::move(in));

      // seek forward
      {

        size_t expected = 0;
        irs::doc_id_t doc = irs::doc_limits::min();
        for (size_t i = 0; i < kCount; ++i, doc += 2) {
          auto prev = upper;
          const auto actual = reader.Seek(doc);
          if (prev != upper) {
            expected = lower - prev;
          }
          ASSERT_EQ(expected, actual);
          ASSERT_TRUE(lower < doc);
          ASSERT_TRUE(doc <= upper);
        }

        {
          ctx.ResetCallsCount();
          ASSERT_EQ(0, reader.Seek(doc));
          ctx.AssertCallsCount(kSkipLevels, 0, 0);
          ASSERT_TRUE(lower < doc);
          ASSERT_TRUE(doc <= upper);
        }
      }

      // seek backwards
      {
        irs::doc_id_t doc = kCount*2 + irs::doc_limits::min();

        {
          ctx.ResetCallsCount();
          ASSERT_EQ(0, reader.Seek(doc));
          ctx.AssertCallsCount(kSkipLevels, 0, 0);
          ASSERT_TRUE(lower < doc);
          ASSERT_TRUE(doc <= upper);
          reader.Reset();
          doc -= 2;
        }

        for (size_t i = kCount - 1; i <= kCount; --i, doc -= 2) {
          lower = irs::doc_limits::invalid();
          upper = irs::doc_limits::invalid();
          reader.Reset();
          size_t skipped = (i/kSkip0)*kSkip0;
          ASSERT_EQ(skipped, reader.Seek(doc));
          ASSERT_TRUE(lower < doc);
          ASSERT_TRUE(doc <= upper);
        }
      }
    }
  }
}
