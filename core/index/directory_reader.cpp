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

#include "index/composite_reader_impl.hpp"
#include "index/segment_reader.hpp"
#include "utils/directory_utils.hpp"
#include "utils/hash_utils.hpp"
#include "utils/singleton.hpp"
#include "utils/string_utils.hpp"
#include "utils/type_limits.hpp"

#include <absl/container/flat_hash_map.h>

namespace irs {
namespace {

MSVC_ONLY(__pragma(warning(push)))
MSVC_ONLY(__pragma(warning(disable : 4457)))  // variable hides function param
index_file_refs::ref_t LoadNewestIndexMeta(IndexMeta& meta,
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

  std::vector<std::string_view> codecs;
  auto visitor = [&codecs](std::string_view name) -> bool {
    codecs.emplace_back(name);
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
    for (const std::string_view name : codecs) {
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

class DirectoryReaderImpl final
  : public CompositeReaderImpl<std::vector<segment_reader>> {
 public:
  using FileRefs = std::vector<index_file_refs::ref_t>;

  // open a new directory reader
  // if codec == nullptr then use the latest file for all known codecs
  // if cached != nullptr then try to reuse its segments
  static std::shared_ptr<const DirectoryReaderImpl> Open(
    const directory& dir, const index_reader_options& opts, const format* codec,
    const std::shared_ptr<const DirectoryReaderImpl>& cached);

  DirectoryReaderImpl(const directory& dir, const index_reader_options& opts,
                      FileRefs&& file_refs, DirectoryMeta&& meta,
                      ReadersType&& readers, uint64_t docs_count,
                      uint64_t docs_max);

  const directory& Dir() const noexcept { return dir_; }

  const DirectoryMeta& Meta() const noexcept { return meta_; }

  const index_reader_options& Options() const noexcept { return opts_; }

 private:
  const directory& dir_;
  FileRefs file_refs_;
  DirectoryMeta meta_;
  index_reader_options opts_;
};

directory_reader::directory_reader(
  std::shared_ptr<const DirectoryReaderImpl> impl) noexcept
  : impl_{std::move(impl)} {}

directory_reader::directory_reader(const directory_reader& other) noexcept
  : impl_{std::atomic_load(&other.impl_)} {}

directory_reader& directory_reader::operator=(
  const directory_reader& other) noexcept {
  if (this != &other) {
    // make a copy
    auto impl = std::atomic_load(&other.impl_);

    std::atomic_store(&impl_, impl);
  }

  return *this;
}

const sub_reader& directory_reader::operator[](size_t i) const {
  return (*impl_)[i];
}

uint64_t directory_reader::docs_count() const { return impl_->docs_count(); }

uint64_t directory_reader::live_docs_count() const {
  return impl_->live_docs_count();
}

size_t directory_reader::size() const { return impl_->size(); }

directory_reader::operator index_reader::ptr() const noexcept { return impl_; }

const DirectoryMeta& directory_reader::Meta() const {
  auto impl = std::atomic_load(&impl_);  // make a copy

  return down_cast<DirectoryReaderImpl>(*impl).Meta();
}

/*static*/ directory_reader directory_reader::open(
  const directory& dir, format::ptr codec /*= nullptr*/,
  const index_reader_options& opts /*= directory_reader_options()*/) {
  return directory_reader{
    DirectoryReaderImpl::Open(dir, opts, codec.get(), nullptr)};
}

directory_reader directory_reader::reopen(
  format::ptr codec /*= nullptr*/) const {
  // make a copy
  auto impl = std::atomic_load(&impl_);

  return directory_reader{
    DirectoryReaderImpl::Open(impl->Dir(), impl->Options(), codec.get(), impl)};
}

DirectoryReaderImpl::DirectoryReaderImpl(const directory& dir,
                                         const index_reader_options& opts,
                                         FileRefs&& file_refs,
                                         DirectoryMeta&& meta,
                                         ReadersType&& readers,
                                         uint64_t docs_count, uint64_t docs_max)
  : CompositeReaderImpl{std::move(readers), docs_count, docs_max},
    dir_{dir},
    file_refs_{std::move(file_refs)},
    meta_{std::move(meta)},
    opts_{opts} {}

/*static*/ std::shared_ptr<const DirectoryReaderImpl> DirectoryReaderImpl::Open(
  const directory& dir, const index_reader_options& opts, const format* codec,
  const std::shared_ptr<const DirectoryReaderImpl>& cached) {
  IndexMeta meta;
  index_file_refs::ref_t meta_file_ref = LoadNewestIndexMeta(meta, dir, codec);

  if (!meta_file_ref) {
    throw index_not_found{};
  }

  if (cached && cached->meta_.meta == meta) {
    return cached;  // no changes to refresh
  }

  constexpr size_t kInvalidCandidate{std::numeric_limits<size_t>::max()};
  absl::flat_hash_map<std::string_view, size_t> reuse_candidates;

  if (cached) {
    const auto segments = cached->Meta().meta.segments();
    reuse_candidates.reserve(segments.size());

    for (size_t i = 0; const auto& segment : segments) {
      IRS_ASSERT(cached);  // ensured by loop condition above
      auto it = reuse_candidates.emplace(segment.meta.name, i++);

      if (IRS_UNLIKELY(!it.second)) {
        it.first->second = kInvalidCandidate;  // treat collisions as invalid
      }
    }
  }

  uint64_t docs_max = 0;    // total number of documents (incl deleted)
  uint64_t docs_count = 0;  // number of live documents

  const auto segments = meta.segments();

  ReadersType readers(segments.size());
  // +1 for index_meta file refs
  FileRefs file_refs(segments.size() + 1);

  auto reader = readers.begin();
  auto ref = file_refs.begin();
  for (const auto& [filename, meta] : segments) {
    const auto it = reuse_candidates.find(meta.name);

    if (it != reuse_candidates.end() && it->second != kInvalidCandidate &&
        meta == cached->meta_.meta.segment(it->second).meta) {
      *reader = (*cached)[it->second].reopen(meta);
      reuse_candidates.erase(it);
    } else {
      *reader = segment_reader::open(dir, meta, opts);
    }

    if (!*reader) {
      throw index_error(string_utils::to_string(
        "while opening reader for segment '%s', error: failed to open reader",
        meta.name.c_str()));
    }

    *ref = directory_utils::reference(dir, filename);

    docs_max += reader->docs_count();
    docs_count += reader->live_docs_count();

    ++reader;
    ++ref;
  }

  *ref = std::move(meta_file_ref);

  return std::make_shared<DirectoryReaderImpl>(
    dir, opts, std::move(file_refs),
    DirectoryMeta{.filename = **ref, .meta = std::move(meta)},
    std::move(readers), docs_count, docs_max);
}

}  // namespace irs
