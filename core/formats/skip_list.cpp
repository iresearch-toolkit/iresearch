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
////////////////////////////////////////////////////////////////////////////////

#include "shared.hpp"
#include "skip_list.hpp"

#include "store/store_utils.hpp"

#include "index/iterators.hpp"

#include "utils/math_utils.hpp"
#include "utils/std.hpp"

namespace {

// returns maximum number of skip levels needed to store specified
// count of objects for skip list with
// step skip_0 for 0 level, step skip_n for other levels
constexpr size_t max_levels(size_t skip_0, size_t skip_n, size_t count) {
  return skip_0 < count
    ? 1 + irs::math::log(count/skip_0, skip_n)
    : 0;
}

constexpr size_t UNDEFINED = std::numeric_limits<size_t>::max();

} // LOCAL

namespace iresearch {

// ----------------------------------------------------------------------------
// --SECTION--                                       skip_writer implementation
// ----------------------------------------------------------------------------

skip_writer::skip_writer(size_t skip_0, size_t skip_n) noexcept
  : skip_0_(skip_0), skip_n_(skip_n) {
  assert(skip_0_);
}

void skip_writer::prepare(
    size_t max_levels, 
    size_t count,
    const skip_writer::write_f& write, /* = nop */
    const memory_allocator& alloc /* = memory_allocator::global() */) {
  max_levels = std::max(size_t(1), max_levels);
  max_levels = std::min(max_levels, ::max_levels(skip_0_, skip_n_, count));
  levels_.reserve(max_levels);
  max_levels = std::max(max_levels, levels_.capacity());

  // reset existing skip levels
  for (auto& level : levels_) {
    level.reset(alloc);
  }

  // add new skip levels
  for (auto size = levels_.size(); size < max_levels; ++size) {
    levels_.emplace_back(alloc);
  }

  write_ = write;
}

void skip_writer::skip(size_t count) {
  assert(!levels_.empty());

  if (0 != count % skip_0_) {
    return;
  }

  uint64_t child = 0;

  // write 0 level
  {
    auto& stream = levels_.front().stream;
    write_(0, stream);
    count /= skip_0_;
    child = stream.file_pointer();
  }

  // write levels from 1 to n
  size_t num = 0;
  for (auto level = levels_.begin()+1, end = levels_.end();
       0 == count % skip_n_ && level != end;
       ++level, count /= skip_n_) {
    auto& stream = level->stream;
    write_(++num, stream);

    uint64_t next_child = stream.file_pointer();
    stream.write_vlong(child);
    child = next_child;
  }
}

void skip_writer::flush(index_output& out) {
  const auto rend = levels_.rend();

  // find first filled level
  auto level = std::find_if(
    levels_.rbegin(), rend,
    [](const memory_output& level) {
      return level.stream.file_pointer();
  });

  // write number of levels
  out.write_vint(uint32_t(std::distance(level, rend)));

  // write levels from n downto 0
  std::for_each(
    level, rend,
    [&out](memory_output& level) {
      auto& stream = level.stream;
      stream.flush(); // update length of each buffer

      const uint64_t length = stream.file_pointer();
      assert(length);
      out.write_vlong(length);
      stream >> out;
  });
}

void skip_writer::reset() noexcept {
  for (auto& level : levels_) {
    level.stream.reset();
  }
}

// ----------------------------------------------------------------------------
// --SECTION--                                       skip_reader implementation
// ----------------------------------------------------------------------------

skip_reader::level::level(
    index_input::ptr&& stream,
    size_t id,
    size_t step,
    uint64_t begin, 
    uint64_t end,
    uint64_t child /*= 0*/,
    size_t skipped /*= 0*/,
    doc_id_t doc /*= doc_limits::invalid()*/) noexcept
  : stream{std::move(stream)}, // thread-safe input
    begin{begin},
    end{end},
    child{child},
    id{id},
    step{step},
    skipped{skipped},
    doc{doc} {
}

skip_reader::skip_reader(
    size_t skip_0, 
    size_t skip_n) noexcept
  : skip_0_{skip_0},
    skip_n_{skip_n} {
}

void skip_reader::read_skip(skip_reader::level& level) {
  // read_ should return NO_MORE_DOCS when stream is exhausted

  assert(size_t(std::distance(&level, &levels_.back())) == level.id);
  const auto doc = read_(level.id, level);

  // read pointer to child level if needed
  if (!doc_limits::eof(doc) && level.child != UNDEFINED) {
    level.child = level.stream->read_vlong();
  }

  level.doc = doc;
  level.skipped += level.step;
}

/* static */ void skip_reader::seek_skip(
    skip_reader::level& level, 
    uint64_t ptr,
    size_t skipped) {
  auto &stream = *level.stream;
  const auto absolute_ptr = level.begin + ptr;
  if (absolute_ptr > stream.file_pointer()) {
    stream.seek(absolute_ptr);
    level.skipped = skipped;
    if (level.child != UNDEFINED) {
      level.child = stream.read_vlong();
    }
  }
}


size_t skip_reader::seek(doc_id_t target) {
  assert(!levels_.empty());
  assert(std::is_sorted(
    levels_.rbegin(), levels_.rend(),
    [](level& lhs, level& rhs) { return lhs.doc < rhs.doc; }));

  // returns highest level with the value not less than target
  auto level = [](auto begin, auto end, doc_id_t target) noexcept {
    // we prefer linear scan over binary search because
    // it's more performant for a small number of elements (< 30)

    // FIXME consider storing only doc + pointer to level
    // data to make linear search more cache friendly
    for (; begin != end; ++begin) {
      if (target >= begin->doc) {
        return begin;
      }
    }

    return std::prev(end);
  }(std::begin(levels_), std::end(levels_), target);

  uint64_t child = 0; // pointer to child skip
  size_t skipped = 0; // number of skipped documents

  for ( ; level != std::end(levels_); ++level) {
    if (level->doc < target) {
      // seek to child
      seek_skip(*level, child, skipped);

      // seek to skip
      child = level->child;
      read_skip(*level);

      for (; level->doc < target; read_skip(*level)) {
        child = level->child;
      }

      skipped = level->skipped - level->step;
    }
  }

  skipped = levels_.back().skipped;
  return skipped ? skipped - skip_0_ : 0;
}

void skip_reader::reset() {
  for (auto& level : levels_) {
    level.stream->seek(level.begin);
    if (level.child != UNDEFINED) {
      level.child = 0;
    }
    level.skipped = 0;
    level.doc = doc_limits::invalid();
  }
}

/*static*/ void skip_reader::load_level(
    std::vector<level>& levels,
    index_input::ptr&& stream,
    size_t id,
    size_t step) {
  assert(stream);

  // read level length
  const auto length = stream->read_vlong();

  if (!length) {
    throw index_error("while loading level, error: zero length");
  }

  const auto begin = stream->file_pointer();
  const auto end = begin + length;

  levels.emplace_back(std::move(stream), id, step, begin, end); // load level
}

void skip_reader::prepare(index_input::ptr&& in, const read_f& read /* = nop */) {
  assert(in && read);

  // read number of levels in a skip-list
  size_t max_levels = in->read_vint();

  if (max_levels) {
    std::vector<level> levels;
    levels.reserve(max_levels);

    size_t step = skip_0_ * size_t(pow(skip_n_, --max_levels)); // skip step of the level

    // load levels from n down to 1
    for (; max_levels; --max_levels) {
      load_level(levels, in->dup(), max_levels, step);

      // seek to the next level
      in->seek(levels.back().end);

      step /= skip_n_;
    }

    // load 0 level
    load_level(levels, std::move(in), 0, skip_0_);
    levels.back().child = UNDEFINED;

    levels_ = std::move(levels);
  }

  read_ = read;
}

}
