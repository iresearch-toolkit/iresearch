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

#include "directory_reader.hpp"

#include <absl/container/flat_hash_map.h>

#include "index/composite_reader_impl.hpp"
#include "index/segment_reader.hpp"
#include "utils/directory_utils.hpp"
#include "utils/hash_utils.hpp"
#include "utils/singleton.hpp"
#include "utils/string_utils.hpp"
#include "utils/type_limits.hpp"

namespace iresearch {
namespace {

MSVC_ONLY(__pragma(warning(push)))
MSVC_ONLY(__pragma(warning(disable : 4457)))  // variable hides function param
index_file_refs::ref_t load_newest_index_meta(index_meta& meta,
                                              const directory& dir,
                                              const format* codec) noexcept {
  // if a specific codec was specified
  if (codec) {
    try {
      auto reader = codec->get_index_meta_reader();

      if (!reader) {
        return nullptr;
      }

      index_file_refs::ref_t ref;
      std::string filename;

      // ensure have a valid ref to a filename
      while (!ref) {
        const bool index_exists = reader->last_segments_file(dir, filename);

        if (!index_exists) {
          return nullptr;
        }

        ref = directory_utils::reference(const_cast<directory&>(dir), filename);
      }

      if (ref) {
        reader->read(dir, meta, *ref);
      }

      return ref;
    } catch (const std::exception& e) {
      IR_FRMT_ERROR(
        "Caught exception while reading index meta with codec '%s', error "
        "'%s'",
        codec->type().name().data(), e.what());
    } catch (...) {
      IR_FRMT_ERROR("Caught exception while reading index meta with codec '%s'",
                    codec->type().name().data());

      return nullptr;
    }
  }

  absl::flat_hash_set<std::string_view> codecs;
  auto visitor = [&codecs](std::string_view name) -> bool {
    codecs.insert(name);
    return true;
  };

  if (!formats::visit(visitor)) {
    return nullptr;
  }

  struct {
    std::time_t mtime;
    index_meta_reader::ptr reader;
    index_file_refs::ref_t ref;
  } newest;

  newest.mtime = (std::numeric_limits<time_t>::min)();

  try {
    for (auto& name : codecs) {
      auto codec = formats::get(name);

      if (!codec) {
        continue;  // try the next codec
      }

      auto reader = codec->get_index_meta_reader();

      if (!reader) {
        continue;  // try the next codec
      }

      index_file_refs::ref_t ref;
      std::string filename;

      // ensure have a valid ref to a filename
      while (!ref) {
        const bool index_exists = reader->last_segments_file(dir, filename);

        if (!index_exists) {
          break;  // try the next codec
        }

        ref = directory_utils::reference(const_cast<directory&>(dir), filename);
      }

      // initialize to a value that will never pass 'if' below (to make valgrind
      // happy)
      std::time_t mtime = std::numeric_limits<std::time_t>::min();

      if (ref && dir.mtime(mtime, *ref) && mtime > newest.mtime) {
        newest.mtime = std::move(mtime);
        newest.reader = std::move(reader);
        newest.ref = std::move(ref);
      }
    }

    if (!newest.reader || !newest.ref) {
      return nullptr;
    }

    newest.reader->read(dir, meta, *(newest.ref));

    return newest.ref;
  } catch (const std::exception& e) {
    IR_FRMT_ERROR(
      "Caught exception while loading the newest index meta, error '%s'",
      e.what());
  } catch (...) {
    IR_FRMT_ERROR("Caught exception while loading the newest index meta");
  }

  return nullptr;
}
MSVC_ONLY(__pragma(warning(pop)))

}  // namespace

class directory_reader_impl : public composite_reader<segment_reader> {
 public:
  // open a new directory reader
  // if codec == nullptr then use the latest file for all known codecs
  // if cached != nullptr then try to reuse its segments
  static index_reader::ptr open(const directory& dir,
                                const index_reader_options& opts,
                                const format* codec = nullptr,
                                const index_reader::ptr& cached = nullptr);

  using segment_file_refs_t = absl::flat_hash_set<index_file_refs::ref_t>;
  using reader_file_refs_t = std::vector<segment_file_refs_t>;

  directory_reader_impl(const directory& dir, const index_reader_options& opts,
                        reader_file_refs_t&& file_refs, directory_meta&& meta,
                        readers_t&& readers, uint64_t docs_count,
                        uint64_t docs_max);

  const directory& dir() const noexcept { return dir_; }

  const directory_meta& meta() const noexcept { return meta_; }

  const index_reader_options& opts() const noexcept { return opts_; }

 private:
  const directory& dir_;
  reader_file_refs_t file_refs_;
  directory_meta meta_;
  index_reader_options opts_;
};

directory_reader::directory_reader(impl_ptr&& impl) noexcept
  : impl_(std::move(impl)) {}

directory_reader::directory_reader(const directory_reader& other) noexcept {
  *this = other;
}

directory_reader& directory_reader::operator=(
  const directory_reader& other) noexcept {
  if (this != &other) {
    // make a copy
    impl_ptr impl = std::atomic_load(&other.impl_);

    std::atomic_store(&impl_, impl);
  }

  return *this;
}

const directory_meta& directory_reader::meta() const {
  auto impl = std::atomic_load(&impl_);  // make a copy

  return down_cast<directory_reader_impl>(*impl).meta();
}

/*static*/ directory_reader directory_reader::open(
  const directory& dir, format::ptr codec /*= nullptr*/,
  const index_reader_options& opts /*= directory_reader_options()*/) {
  return directory_reader_impl::open(dir, opts, codec.get());
}

directory_reader directory_reader::reopen(
  format::ptr codec /*= nullptr*/) const {
  // make a copy
  impl_ptr impl = std::atomic_load(&impl_);

  const auto& reader_impl = down_cast<directory_reader_impl>(*impl);

  return directory_reader_impl::open(reader_impl.dir(), reader_impl.opts(),
                                     codec.get(), impl);
}

directory_reader_impl::directory_reader_impl(
  const directory& dir, const index_reader_options& opts,
  reader_file_refs_t&& file_refs, directory_meta&& meta, readers_t&& readers,
  uint64_t docs_count, uint64_t docs_max)
  : composite_reader(std::move(readers), docs_count, docs_max),
    dir_(dir),
    file_refs_(std::move(file_refs)),
    meta_(std::move(meta)),
    opts_(opts) {}

/*static*/ index_reader::ptr directory_reader_impl::open(
  const directory& dir, const index_reader_options& opts,
  const format* codec /*= nullptr*/,
  const index_reader::ptr& cached /*= nullptr*/) {
  index_meta meta;
  index_file_refs::ref_t meta_file_ref =
    load_newest_index_meta(meta, dir, codec);

  if (!meta_file_ref) {
    throw index_not_found{};
  }

  auto* cached_impl = down_cast<directory_reader_impl>(cached.get());

  if (cached_impl && cached_impl->meta_.meta == meta) {
    return cached;  // no changes to refresh
  }

  constexpr size_t kInvalidCandidate{std::numeric_limits<size_t>::max()};
  const size_t count = cached_impl ? cached_impl->meta_.meta.size() : 0;

  // map by segment name to old segment id
  absl::flat_hash_map<std::string_view, size_t> reuse_candidates;
  reuse_candidates.reserve(count);

  for (size_t i = 0; i < count; ++i) {
    IRS_ASSERT(cached_impl);  // ensured by loop condition above
    auto itr =
      reuse_candidates.emplace(cached_impl->meta_.meta.segment(i).meta.name, i);

    if (!itr.second) {
      itr.first->second = kInvalidCandidate;  // treat collisions as invalid
    }
  }

  readers_t readers(meta.size());
  uint64_t docs_max = 0;    // overall number of documents (with deleted)
  uint64_t docs_count = 0;  // number of live documents

  // +1 for index_meta file refs
  reader_file_refs_t file_refs(readers.size() + 1);
  segment_file_refs_t tmp_file_refs;

  const std::function visitor =
    [&tmp_file_refs](index_file_refs::ref_t&& ref) -> bool {
    tmp_file_refs.emplace(std::move(ref));
    return true;
  };

  for (size_t i = 0, size = meta.size(); i < size; ++i) {
    auto& reader = readers[i];
    auto& segment = meta.segment(i).meta;
    auto& segment_file_refs = file_refs[i];
    auto itr = reuse_candidates.find(segment.name);

    if (itr != reuse_candidates.end() && itr->second != kInvalidCandidate &&
        segment == cached_impl->meta_.meta.segment(itr->second).meta) {
      reader = (*cached_impl)[itr->second].reopen(segment);
      reuse_candidates.erase(itr);
    } else {
      reader = segment_reader::open(dir, segment, opts);
    }

    if (!reader) {
      throw index_error(string_utils::to_string(
        "while opening reader for segment '%s', error: failed to open reader",
        segment.name.c_str()));
    }

    docs_max += reader.docs_count();
    docs_count += reader.live_docs_count();
    directory_utils::reference(dir, segment, visitor, true);
    segment_file_refs.swap(tmp_file_refs);
  }

  directory_utils::reference(dir, meta, visitor, true);
  tmp_file_refs.emplace(meta_file_ref);
  file_refs.back().swap(
    tmp_file_refs);  // use last position for storing index_meta refs

  directory_meta dir_meta;

  dir_meta.filename = *meta_file_ref;
  dir_meta.meta = std::move(meta);

  return std::make_shared<directory_reader_impl>(
    dir, opts, std::move(file_refs), std::move(dir_meta), std::move(readers),
    docs_count, docs_max);
}

}  // namespace iresearch
