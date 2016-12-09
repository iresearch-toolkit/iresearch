//
// IResearch search engine 
// 
// Copyright © 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#include "shared.hpp"
#include "utils/directory_utils.hpp"
#include "utils/type_limits.hpp"
#include "index_reader.hpp"
#include "segment_reader.hpp"
#include "index_meta.hpp"

NS_LOCAL

MSVC_ONLY(__pragma(warning(push)))
MSVC_ONLY(__pragma(warning(disable:4457))) // variable hides function param
iresearch::index_file_refs::ref_t load_newest_index_meta(
    iresearch::index_meta& meta,
    const iresearch::directory& dir,
    const iresearch::format::ptr& codec) NOEXCEPT {
  // if a specific codec was specified
  if (codec) {
    try {
      auto reader = codec->get_index_meta_reader();

      if (!reader) {
        return nullptr;
      }

      iresearch::index_file_refs::ref_t ref;
      std::string filename;

      // ensure have a valid ref to a filename
      while (!ref) {
        const bool index_exists = reader->last_segments_file(dir, filename);

        if (!index_exists) {
          return nullptr;
        }

        ref = std::move(iresearch::directory_utils::reference(
          const_cast<iresearch::directory&>(dir), filename
        ));
      }

      if (ref) {
        reader->read(dir, meta, *ref);
      }

      return std::move(ref);
    } catch (...) {
      return nullptr;
    }
  }

  std::unordered_set<iresearch::string_ref> codecs;
  auto visitor = [&codecs](const iresearch::string_ref& name)->bool {
    codecs.insert(name);
    return true;
  };

  if (!iresearch::formats::visit(visitor)) {
    return nullptr;
  }

  struct {
    std::time_t mtime;
    iresearch::index_meta_reader::ptr reader;
    iresearch::index_file_refs::ref_t ref;
  } newest;

  newest.mtime = (iresearch::integer_traits<time_t>::min)();

  try {
    for (auto& name: codecs) {
      auto codec = iresearch::formats::get(name);

      if (!codec) {
        continue; // try the next codec
      }

      auto reader = codec->get_index_meta_reader();

      if (!reader) {
        continue; // try the next codec
      }

      iresearch::index_file_refs::ref_t ref;
      std::string filename;

      // ensure have a valid ref to a filename
      while (!ref) {
        const bool index_exists = reader->last_segments_file(dir, filename);

        if (!index_exists) {
          break; // try the next codec
        }

        ref = std::move(iresearch::directory_utils::reference(
          const_cast<iresearch::directory&>(dir), filename
        ));
      }

      std::time_t mtime;

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

    return std::move(newest.ref);
  } catch (...) {
    // NOOP
  }

  return nullptr;
}
MSVC_ONLY(__pragma(warning(pop)))

NS_END

NS_ROOT

/* -------------------------------------------------------------------
* index_reader
* ------------------------------------------------------------------*/

index_reader::~index_reader() { }

directory_reader::directory_reader(
  const format::ptr& codec,
  const directory& dir,
  reader_file_refs_t&& file_refs,
  index_meta&& meta,
  ctxs_t&& ctxs,
  uint64_t docs_count,
  uint64_t docs_max
):
  composite_reader(std::move(meta), std::move(ctxs), docs_count, docs_max),
  codec_(codec),
  dir_(dir),
  file_refs_(std::move(file_refs)) {
}

directory_reader::ptr directory_reader::open(
  const directory& dir,
  const format::ptr& codec /*= format::ptr(nullptr)*/
) {
  index_meta meta;
  index_file_refs::ref_t meta_file_ref = load_newest_index_meta(meta, dir, codec);

  if (!meta_file_ref) {
    throw index_not_found();
  }

  ctxs_t ctxs(meta.size());
  uint64_t docs_max = 0; /* overall number of documents (with deleted)*/
  uint64_t docs_count = 0; /* number of live documents */
  reader_file_refs_t file_refs(ctxs.size() + 1); // +1 for index_meta file refs
  segment_file_refs_t tmp_file_refs;
  auto visitor = [&tmp_file_refs](index_file_refs::ref_t&& ref)->bool {
    tmp_file_refs.emplace(std::move(ref));
    return true;
  };

  for (size_t i = 0, size = meta.size(); i < size; ++i) {
    auto& ctx = ctxs[i];
    auto& segment = meta.segment(i).meta;
    auto& segment_file_refs = file_refs[i];

    ctx.reader = segment_reader::open(dir, segment);
    ctx.base = static_cast<doc_id_t>(docs_max);
    docs_max += ctx.reader->docs_max();
    docs_count += ctx.reader->docs_count();
    ctx.max = doc_id_t(type_limits<type_t::doc_id_t>::min() + docs_max - 1);
    directory_utils::reference(const_cast<directory&>(dir), segment, visitor, true);
    segment_file_refs.swap(tmp_file_refs);
  }

  directory_utils::reference(const_cast<directory&>(dir), meta, visitor, true);
  tmp_file_refs.emplace(meta_file_ref);
  file_refs.back().swap(tmp_file_refs); // use last position for storing index_meta refs

  return directory_reader::make<directory_reader>(
    codec, dir, std::move(file_refs), std::move(meta), std::move(ctxs), docs_count, docs_max
  );
}

void directory_reader::refresh() {
  index_meta meta;
  index_file_refs::ref_t meta_file_ref = load_newest_index_meta(meta, dir_, codec_);

  if (!meta_file_ref || meta_.generation() == meta.generation()) {
    return; // refresh failed, or no changes to refresh
  }

  static const auto invalid_candidate = std::numeric_limits<size_t>::max();
  std::unordered_map<string_ref, size_t> reuse_candidates; // map by segment name to old segment id

  for(size_t i = 0, count = meta_.size(); i < count; ++i) {
    auto itr = reuse_candidates.emplace(meta_.segment(i).meta.name, i);

    if (!itr.second) {
      itr.first->second = invalid_candidate; // treat collisions as invalid
    }
  }

  ctxs_t ctxs(meta.size());
  uint64_t docs_max = 0; /* overall number of documents (with deleted) */
  uint64_t docs_count = 0; /* number of live documents */
  reader_file_refs_t file_refs(ctxs.size() + 1); // +1 for index_meta file refs
  segment_file_refs_t tmp_file_refs;
  auto visitor = [&tmp_file_refs](index_file_refs::ref_t&& ref)->bool {
    tmp_file_refs.emplace(std::move(ref));
    return true;
  };

  // either move existing or load new segment readers
  for (size_t i = 0, count = meta.size(); i < count; ++i) {
    auto& segment = meta.segment(i).meta;
    auto& segment_ctx = ctxs[i];
    auto& segment_file_refs = file_refs[i];
    auto itr = reuse_candidates.find(segment.name);

    if (itr == reuse_candidates.end() || itr->second == invalid_candidate) {
      segment_ctx.reader = segment_reader::open(dir_, segment);
    } else {
      segment_ctx.reader = std::move(ctxs_[itr->second].reader);
      static_cast<segment_reader*>(segment_ctx.reader.get())->refresh(segment); // always segment_reader in open(...)
      reuse_candidates.erase(itr);
    }

    segment_ctx.base = static_cast<doc_id_t>(docs_max);
    docs_max += segment_ctx.reader->docs_max();
    docs_count += segment_ctx.reader->docs_count();
    segment_ctx.max = doc_id_t(type_limits<type_t::doc_id_t>::min() + docs_max - 1);
    directory_utils::reference(const_cast<directory&>(dir_), segment, visitor, true);
    segment_file_refs.swap(tmp_file_refs);
  }

  directory_utils::reference(const_cast<directory&>(dir_), meta, visitor, true);
  tmp_file_refs.emplace(meta_file_ref);
  file_refs.back().swap(tmp_file_refs); // use last position for storing index_meta refs

  ctxs_ = std::move(ctxs);
  docs_count_ = docs_count;
  docs_max_ = docs_max;
  file_refs_ = std::move(file_refs);
  meta_ = std::move(meta);
}

NS_END