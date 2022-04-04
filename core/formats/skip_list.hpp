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

#ifndef IRESEARCH_SKIP_LIST_H
#define IRESEARCH_SKIP_LIST_H

#include "store/memory_directory.hpp"
#include "utils/type_limits.hpp"

namespace iresearch {

////////////////////////////////////////////////////////////////////////////////
/// @class skip_writer
/// @brief writer for storing skip-list in a directory
/// @note Example (skip_0 = skip_n = 3):
///
///                                                        c         (skip level 2)
///                    c                 c                 c         (skip level 1)
///        x     x     x     x     x     x     x     x     x     x   (skip level 0)
///  d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d (posting list)
///        3     6     9     12    15    18    21    24    27    30  (doc_count)
///
/// d - document
/// x - skip data
/// c - skip data with child pointer
////////////////////////////////////////////////////////////////////////////////
class SkipWriter : util::noncopyable {
 public:
  //////////////////////////////////////////////////////////////////////////////
  /// @brief constructor
  /// @param skip_0 skip interval for level 0
  /// @param skip_n skip interval for levels 1..n
  //////////////////////////////////////////////////////////////////////////////
  SkipWriter(doc_id_t skip_0, doc_id_t skip_n) noexcept
    : max_levels_{0}, skip_0_{skip_0}, skip_n_{skip_n} {
    assert(skip_0_);
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @returns number of elements to skip at the 0 level
  //////////////////////////////////////////////////////////////////////////////
  doc_id_t Skip0() const noexcept { return skip_0_; }

  //////////////////////////////////////////////////////////////////////////////
  /// @returns number of elements to skip at the levels from 1 to max_levels()
  //////////////////////////////////////////////////////////////////////////////
  doc_id_t SkipN() const noexcept { return skip_n_; }

  //////////////////////////////////////////////////////////////////////////////
  /// @returns number of elements in a skip-list
  //////////////////////////////////////////////////////////////////////////////
  size_t MaxLevels() const noexcept { return max_levels_; }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief prepares skip_writer
  /// @param max_levels maximum number of levels in a skip-list
  /// @param count total number of elements to store in a skip-list
  /// @param write write function
  /// @param alloc memory file allocator
  //////////////////////////////////////////////////////////////////////////////
  void Prepare(
    size_t max_levels,
    size_t count,
    const memory_allocator& alloc = memory_allocator::global());

  //////////////////////////////////////////////////////////////////////////////
  /// @brief flushes all internal data into the specified output stream
  /// @param out output stream
  //////////////////////////////////////////////////////////////////////////////
  void Flush(index_output& out);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief resets skip writer internal state
  //////////////////////////////////////////////////////////////////////////////
  void Reset() noexcept {
    for (auto& level : levels_) {
      level.stream.reset();
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief adds skip at the specified number of elements
  /// @param count number of elements to skip
  /// @tparam Write functional object is called for every skip allowing users to
  ///         store arbitrary data for a given level in corresponding output
  ///         stream
  //////////////////////////////////////////////////////////////////////////////
  template<typename Writer>
  void Skip(doc_id_t count, Writer&& write);

 protected:
  std::vector<memory_output> levels_;
  size_t max_levels_;
  doc_id_t skip_0_; // skip interval for 0 level
  doc_id_t skip_n_; // skip interval for 1..n levels
};

template<typename Writer>
void SkipWriter::Skip(doc_id_t count, Writer&& write) {
  if (0 == (count % skip_0_)) {
    assert(!levels_.empty());

    uint64_t child = 0;

    // write 0 level
    {
      auto& stream = levels_.front().stream;
      write(0, stream);
      count /= skip_0_;
      child = stream.file_pointer();
    }

    // write levels from 1 to n
    for (size_t i = 1;
         0 == count % skip_n_ && i < max_levels_;
         ++i, count /= skip_n_) {
      auto& stream = levels_[i].stream;
      write(i, stream);

      uint64_t next_child = stream.file_pointer();
      stream.write_vlong(child);
      child = next_child;
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @class skip_reader_base
/// @brief base object for searching in skip-lists
////////////////////////////////////////////////////////////////////////////////
class SkipReaderBase : util::noncopyable {
 public:
  //////////////////////////////////////////////////////////////////////////////
  /// @returns number of elements to skip at the 0 level
  //////////////////////////////////////////////////////////////////////////////
  doc_id_t Skip0() const noexcept { return skip_0_; }

  //////////////////////////////////////////////////////////////////////////////
  /// @returns number of elements to skip at the levels from 1 to num_levels()
  //////////////////////////////////////////////////////////////////////////////
  doc_id_t SkipN() const noexcept { return skip_n_; }

  //////////////////////////////////////////////////////////////////////////////
  /// @returns number of elements in a skip-list
  //////////////////////////////////////////////////////////////////////////////
  size_t NumLevels() const noexcept { return levels_.size(); }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief prepares skip_reader
  /// @param in source data stream
  /// @param max_levels maximum number of levels in a skip-list
  /// @param count total number of elements to store in a skip-list
  /// @param read read function
  //////////////////////////////////////////////////////////////////////////////
  void Prepare(index_input::ptr&& in);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief seeks to the specified target
  /// @param target target to find
  /// @returns number of elements skipped
  //////////////////////////////////////////////////////////////////////////////
  size_t Seek(doc_id_t target);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief resets skip reader internal state
  //////////////////////////////////////////////////////////////////////////////
  void Reset();

 protected:
  static constexpr size_t kUndefined = std::numeric_limits<size_t>::max();

  struct Level final {
    Level(
      index_input::ptr&& stream,
      uint32_t id,
      doc_id_t step,
      uint64_t begin) noexcept;
    Level(Level&&) = default;
    Level& operator=(Level&&) = delete;

    index_input::ptr stream; // level data stream
    uint64_t begin; // where current level starts
    uint64_t child{}; // pointer to current child level
    const uint32_t id; // level id
    const doc_id_t step; // how many docs we jump over with a single skip
    doc_id_t skipped{}; // number of skipped documents at the level
  };

  struct LevelKey {
    Level* const data; // pointer to actual level
    doc_id_t doc; // current key
  };

  static_assert(std::is_nothrow_move_constructible_v<Level>);

  static void SeekToChild(Level& lvl, uint64_t ptr, const Level& prev) {
    assert(lvl.stream);
    auto& stream = *lvl.stream;

    if (const auto absolute_ptr = lvl.begin + ptr;
        absolute_ptr > stream.file_pointer()) {
      stream.seek(absolute_ptr);
      lvl.skipped = prev.skipped - prev.step;
      if (lvl.child != kUndefined) {
        lvl.child = stream.read_vlong();
      }
    }
  }

  SkipReaderBase(doc_id_t skip_0, doc_id_t skip_n) noexcept
    : skip_0_{skip_0},
      skip_n_{skip_n} {
  }

  std::vector<Level> levels_; // input streams for skip-list levels
  std::vector<LevelKey> keys_;
  const doc_id_t skip_0_; // skip interval for 0 level
  const doc_id_t skip_n_; // skip interval for 1..n levels
};

////////////////////////////////////////////////////////////////////////////////
/// @class skip_reader_impl
/// @brief reader for searching in skip-lists in a directory
/// @tparam Read function object is called when reading of next skip. Accepts
///   the following parameters: index of the level in a skip-list, where a data
///   stream ends, stream where level data resides and  readed key if stream is
///   not exhausted, doc_limits::eof() otherwise
////////////////////////////////////////////////////////////////////////////////
template<typename ReaderType>
class SkipReader final : public SkipReaderBase {
 public:
  //////////////////////////////////////////////////////////////////////////////
  /// @brief constructor
  /// @param skip_0 skip interval for level 0
  /// @param skip_n skip interval for levels 1..n
  //////////////////////////////////////////////////////////////////////////////
  template<typename T>
  SkipReader(doc_id_t skip_0, doc_id_t skip_n, T&& reader)
    : SkipReaderBase{skip_0, skip_n},
      reader_{std::forward<T>(reader)} {
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief seeks to the specified target
  /// @param target target to find
  /// @returns number of elements skipped
  //////////////////////////////////////////////////////////////////////////////
  doc_id_t Seek(doc_id_t target);

  template<typename Pred>
  doc_id_t SeekIf(Pred pred);

  ReaderType& Reader() noexcept {
    return reader_;
  }

 private:
  ReaderType reader_;
};

template<typename Read>
template<typename Pred>
doc_id_t SkipReader<Read>::SeekIf(Pred pred) {
  assert(!levels_.empty());
  assert(std::is_sorted(
    std::begin(keys_), std::end(keys_),
    [](const auto& lhs, const auto& rhs) { return lhs.doc > rhs.doc; }));
  const size_t back = keys_.size() - 1;

  // returns the highest level with the value not less than a target
  auto i = [back, &pred]() noexcept {
      size_t i = 0;

      for (; i <= back; ++i) {
        if (pred(back - i)) {
          return i;
        }
      }

      return back;
    }();

  assert(i != std::size(keys_));

  // FIXME
  //   * use the same order of levels in formats/skip-list -> remove level.id
  //   * remove level keys
  //   * try to use predicates for docs as well (check performance)
  //   * it seems we can use predicates for skipping on both score and doc id

  for (uint64_t child_ptr{0}; i != back; ++i) {
    if (pred(back - i)) {
      auto& level = levels_[i];
      const doc_id_t step{level.step};
      assert(back - i == level.id);
      const size_t id{level.id};
      auto& stream{*level.stream};

      do {
        child_ptr = level.child;
        const auto doc = reader_.Read(id, level.skipped += step, stream);

        if (!doc_limits::eof(doc)) {
          level.child = stream.read_vlong();
        }
      } while (pred(id));

      auto* next_level = &level + 1;
      SeekToChild(*next_level, child_ptr, level);
      reader_.MoveDown(next_level->id);
    }
  }

  assert(i == back) ;
  auto& level = levels_.back();
  const doc_id_t step{level.step};
  assert(back - i == level.id);
  const size_t id{level.id};
  auto& stream{*level.stream};

  for (; pred(id); ) {
    reader_.Read(id, level.skipped += step, stream);
  }

  const doc_id_t skipped = level.skipped;
  return skipped ? skipped - step : 0;
}

template<typename Read>
doc_id_t SkipReader<Read>::Seek(doc_id_t target) {
  assert(!levels_.empty());
  assert(std::is_sorted(
    std::begin(keys_), std::end(keys_),
    [](const auto& lhs, const auto& rhs) { return lhs.doc > rhs.doc; }));

  // returns the highest level with the value not less than a target
  auto key = [this](doc_id_t target) noexcept {
      // we prefer linear scan over binary search because
      // it's more performant for a small number of elements (< 30)
      auto begin = std::begin(keys_);

      for (; begin != std::end(keys_); ++begin) {
        if (begin->doc < target) {
          return begin;
        }
      }

      return std::prev(std::end(keys_));
    }(target);

  assert(key != std::end(keys_));
  const auto back = std::prev(std::end(keys_));

  for (uint64_t child_ptr{0}; key != back; ++key) {
    if (auto& doc = key->doc; doc < target) {
      auto& level = *key->data;
      const doc_id_t step{level.step};
      const size_t id{level.id};
      auto& stream{*level.stream};

      do {
        child_ptr = level.child;
        doc = reader_.Read(id, level.skipped += step, stream);
        if (!doc_limits::eof(doc)) {
          level.child = stream.read_vlong();
        }
      } while (doc < target);

      auto* next_level = &level + 1;
      SeekToChild(*next_level, child_ptr, level);
      reader_.MoveDown(next_level->id);
    }
  }

  assert(key == back) ;
  auto& level = *key->data;
  const doc_id_t step{level.step};
  const size_t id{level.id};
  auto& stream{*level.stream};

  for (auto& doc = key->doc; doc < target; ) {
    doc = reader_.Read(id, level.skipped += step, stream);
  }

  const doc_id_t skipped = level.skipped;
  return skipped ? skipped - step : 0;
}

} // iresearch

#endif
