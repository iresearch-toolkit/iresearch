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

#include "shared.hpp"
#include "file_names.hpp"
#include "merge_writer.hpp"
#include "formats/format_utils.hpp"
#include "search/exclusion.hpp"
#include "utils/bitset.hpp"
#include "utils/bitvector.hpp"
#include "utils/directory_utils.hpp"
#include "utils/index_utils.hpp"
#include "utils/timer_utils.hpp"
#include "utils/type_limits.hpp"
#include "index_writer.hpp"

#include <list>

NS_LOCAL

const size_t NON_UPDATE_RECORD = irs::integer_traits<size_t>::const_max; // non-update

bool track_ref(
    irs::directory& dir,
    const std::string& name,
    std::vector<irs::index_file_refs::ref_t>& refs,
    bool include_missing = false
) {
  const auto lock_file_ref = irs::directory_utils::reference(dir, name, include_missing);

  // file exists on fs_directory
  if (lock_file_ref) {
    refs.emplace_back(lock_file_ref);
    return true;
  }

  return false;
}

std::vector<irs::index_file_refs::ref_t> extract_refs(
    const irs::ref_tracking_directory& dir
) {
  std::vector<irs::index_file_refs::ref_t> refs;
  // FIXME reserve

  auto visitor = [&refs](const irs::index_file_refs::ref_t& ref) {
    refs.emplace_back(ref);
    return true;
  };
  dir.visit_refs(visitor);

  return refs;
}

// append file refs for files from the specified segments description
template<typename T, typename M>
void append_segments_refs(
    T& buf,
    irs::directory& dir,
    const M& meta
) {
  auto visitor = [&buf](const irs::index_file_refs::ref_t& ref)->bool {
    buf.emplace_back(ref);
    return true;
  };

  // track all files referenced in index_meta
  irs::directory_utils::reference(dir, meta, visitor, true);
}

const std::string& write_document_mask(
    iresearch::directory& dir,
    iresearch::segment_meta& meta,
    const iresearch::document_mask& docs_mask,
    bool increment_version = true
) {
  assert(docs_mask.size() <= std::numeric_limits<uint32_t>::max());

  auto mask_writer = meta.codec->get_document_mask_writer();
  if (increment_version) {
    meta.files.erase(mask_writer->filename(meta)); // current filename
    ++meta.version; // segment modified due to new document_mask
  }
  const auto& file = *meta.files.emplace(mask_writer->filename(meta)).first; // new/expected filename
  mask_writer->write(dir, meta, docs_mask);
  return file;
}

// mapping: name -> { new segment, old segment }
typedef std::map<
  irs::string_ref,
  std::pair<
    const irs::segment_meta*, // new segment
    std::pair<const irs::segment_meta*, size_t> // old segment + index within merge_writer
>> candidates_mapping_t;

/// @param candidates_mapping output mapping
/// @param candidates candidates for mapping
/// @param segments map against a specified segments
/// @returns first - has removals, second - number of mapped candidates
std::pair<bool, size_t> map_candidates(
    candidates_mapping_t& candidates_mapping,
    const std::set<const irs::segment_meta*> candidates,
    const irs::index_meta::index_segments_t& segments
) {
  size_t i = 0;
  for (auto* candidate : candidates) {
    candidates_mapping.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(candidate->name),
      std::forward_as_tuple(nullptr, std::make_pair(candidate, i++))
    );
  }

  size_t found = 0;
  bool has_removals = false;
  const auto candidate_not_found = candidates_mapping.end();

  for (const auto& segment : segments) {
    const auto& meta = segment.meta;
    const auto it = candidates_mapping.find(meta.name);

    if (candidate_not_found == it) {
      // not a candidate
      continue;
    }

    auto* new_segment = it->second.first;

    if (new_segment && new_segment->version >= meta.version) {
      // mapping already has a newer segment version
      continue;
    }

    ++found;

    assert(it->second.second.first);
    it->second.first = &meta;

    has_removals |= (meta.version != it->second.second.first->version);
  }

  return std::make_pair(has_removals, found);
}

void map_removals(
    const candidates_mapping_t& candidates_mapping,
    const irs::merge_writer& merger,
    irs::directory& dir,
    irs::readers_cache& readers,
    irs::document_mask& docs_mask
) {
  assert(merger);

  for (auto& mapping : candidates_mapping) {
    const auto& segment_mapping = mapping.second;

    if (segment_mapping.first->version != segment_mapping.second.first->version) {
      auto& merge_ctx = merger[segment_mapping.second.second];

      auto reader = readers.emplace(dir, *segment_mapping.first);

      irs::exclusion deleted_docs(
        merge_ctx.reader->docs_iterator(),
        reader->docs_iterator()
      );

      while (deleted_docs.next()) {
        docs_mask.insert(merge_ctx.doc_map(deleted_docs.value()));
      }
    }
  }
}

NS_END // NS_LOCAL

NS_ROOT

segment_reader readers_cache::emplace(
    directory& dir, const segment_meta& meta
) {
  REGISTER_TIMER_DETAILED();

  segment_reader cached_reader;

  // FIXME consider moving open/reopen out of the scope of the lock
  SCOPED_LOCK(lock_);
  auto& reader = cache_[meta.name];

  cached_reader = std::move(reader); // clear existing reader

  // update cache, in case of failure reader stays empty
  reader = cached_reader
    ? cached_reader.reopen(meta)
    : segment_reader::open(dir, meta);

  return reader;
}

void readers_cache::clear() NOEXCEPT {
  SCOPED_LOCK(lock_);
  cache_.clear();
}

size_t readers_cache::purge(const std::unordered_set<string_ref>& segments) NOEXCEPT {
  if (segments.empty()) {
    return 0;
  }

  size_t erased = 0;

  SCOPED_LOCK(lock_);

  for (auto it = cache_.begin(); it != cache_.end(); ) {
    if (segments.end() != segments.find(it->first)) {
      it = cache_.erase(it);
      ++erased;
    } else {
      ++it;
    }
  }

  return erased;
}

// ----------------------------------------------------------------------------
// --SECTION--                                      index_writer implementation 
// ----------------------------------------------------------------------------

const std::string index_writer::WRITE_LOCK_NAME = "write.lock";

index_writer::flush_context::flush_context():
  generation_(0),
  writers_pool_(THREAD_COUNT) {
}

void index_writer::flush_context::reset() {
  generation_.store(0);
  dir_->clear_refs();
  modification_queries_.clear();
  pending_segments_.clear();
  segment_mask_.clear();
  writers_pool_.visit([](segment_writer& writer)->bool {
    writer.reset();
    return true;
  });
}

index_writer::index_writer( 
    index_lock::ptr&& lock,
    directory& dir,
    format::ptr codec,
    index_meta&& meta,
    committed_state_t&& committed_state
) NOEXCEPT:
    codec_(codec),
    committed_state_(std::move(committed_state)),
    dir_(dir),
    flush_context_pool_(2), // 2 because just swap them due to common commit lock
    meta_(std::move(meta)),
    writer_(codec->get_index_meta_writer()),
    write_lock_(std::move(lock)) {
  assert(codec);
  flush_context_.store(&flush_context_pool_[0]);

  // setup round-robin chain
  for (size_t i = 0, count = flush_context_pool_.size() - 1; i < count; ++i) {
    flush_context_pool_[i].dir_ = memory::make_unique<ref_tracking_directory>(dir);
    flush_context_pool_[i].next_context_ = &flush_context_pool_[i + 1];
  }

  // setup round-robin chain
  flush_context_pool_[flush_context_pool_.size() - 1].dir_ = memory::make_unique<ref_tracking_directory>(dir);
  flush_context_pool_[flush_context_pool_.size() - 1].next_context_ = &flush_context_pool_[0];
}

void index_writer::clear() {
  SCOPED_LOCK(commit_lock_);

  if (!pending_state_
      && meta_.empty()
      && type_limits<type_t::index_gen_t>::valid(meta_.last_gen_)) {
    return; // already empty
  }

  auto ctx = get_flush_context(false);
  SCOPED_LOCK(ctx->mutex_); // ensure there are no active struct update operations

  auto pending_commit = memory::make_shared<committed_state_t::element_type>(
    std::piecewise_construct,
    std::forward_as_tuple(memory::make_shared<index_meta>()),
    std::forward_as_tuple()
  );

  auto& dir = *ctx->dir_;
  auto& pending_meta = *pending_commit->first;
  auto& pending_refs = pending_commit->second;

  // setup new meta
  pending_meta.update_generation(meta_); // clone index metadata generation
  pending_meta.seg_counter_.store(meta_.counter()); // ensure counter() >= max(seg#)

  // write 1st phase of index_meta transaction
  if (!writer_->prepare(dir, pending_meta)) {
    throw illegal_state();
  }

  // track references
  if (write_lock_) {
    // track write lock if exists
    track_ref(dir, WRITE_LOCK_NAME, pending_refs);
  }
  track_ref(dir, writer_->filename(pending_meta), pending_refs, true);

  // 1st phase of the transaction successfully finished here
  meta_.update_generation(pending_meta); // ensure new generation reflected in 'meta_'
  pending_state_.ctx = std::move(ctx); // retain flush context reference
  pending_state_.commit = std::move(pending_commit);

  finish();

  // ...........................................................................
  // all functions below are noexcept
  // ...........................................................................

  meta_.segments_.clear(); // noexcept op (clear after finish(), to match reset of pending_state_ inside finish(), allows recovery on clear() failure)
  cached_readers_.clear(); // original readers no longer required

  // clear consolidating segments
  SCOPED_LOCK(consolidation_lock_);
  consolidating_segments_.clear();
}

index_writer::ptr index_writer::make(
    directory& dir,
    format::ptr codec,
    OpenMode mode,
    size_t memory_pool_size /*= 0*/
) {
  std::vector<index_file_refs::ref_t> file_refs;
  index_lock::ptr lock;

  if (0 == (OM_NOLOCK & mode)) {
    // lock the directory
    lock = dir.make_lock(WRITE_LOCK_NAME);

    if (!lock || !lock->try_lock()) {
      throw lock_obtain_failed(WRITE_LOCK_NAME);
    }

    track_ref(dir, WRITE_LOCK_NAME, file_refs);
  }

  // read from directory or create index metadata
  index_meta meta;
  {
    auto reader = codec->get_index_meta_reader();

    std::string segments_file;
    const bool index_exists = reader->last_segments_file(dir, segments_file);

    mode &= OM_CREATE | OM_APPEND;

    if (OM_CREATE == mode || ((OM_CREATE | OM_APPEND) == mode && !index_exists)) {
      // Try to read. It allows us to
      // create writer against an index that's
      // currently opened for searching

      try {
        // for OM_CREATE meta must be fully recreated, meta read only to get last version
        if (index_exists) {
          reader->read(dir, meta, segments_file);
          meta.clear();
          meta.last_gen_ = type_limits<type_t::index_gen_t>::invalid(); // this meta is for a totaly new index
        }
      } catch (const error_base&) {
        meta = index_meta();
      }
    } else if (!index_exists) {
      throw file_not_found(); // no segments file found
    } else {
      reader->read(dir, meta, segments_file);
      append_segments_refs(file_refs, dir, meta);
      file_refs.emplace_back(iresearch::directory_utils::reference(dir, segments_file));
    }
  }

  auto comitted_state = memory::make_shared<committed_state_t::element_type>(
    memory::make_shared<index_meta>(meta),
    std::move(file_refs)
  );

  PTR_NAMED(
    index_writer,
    writer,
    std::move(lock), 
    dir, codec,
    std::move(meta),
    std::move(comitted_state)
  );

  directory_utils::ensure_allocator(dir, memory_pool_size); // ensure memory_allocator set in directory
  directory_utils::remove_all_unreferenced(dir); // remove non-index files from directory

  return writer;
}

index_writer::~index_writer() {
  close();
}

void index_writer::close() {
  cached_readers_.clear(); // cached_readers_ read/modified during flush()
  write_lock_.reset();
}

uint64_t index_writer::buffered_docs() const { 
  uint64_t docs_in_ram = 0;

  auto visitor = [&docs_in_ram](const segment_writer& writer) {
    docs_in_ram += writer.docs_cached();
    return true;
  };

  auto ctx = const_cast<index_writer*>(this)->get_flush_context();

  ctx->writers_pool_.visit(visitor);

  return docs_in_ram;
}

bool index_writer::add_document_mask_modified_records(
    modification_requests_t& modification_queries,
    document_mask& docs_mask,
    segment_meta& meta,
    size_t min_doc_id_generation /*= 0*/
) {
  if (modification_queries.empty()) {
    return false; // nothing new to flush
  }

  bool modified = false;
  auto rdr = cached_readers_.emplace(dir_, meta);

  if (!rdr) {
    throw index_error(); // failed to open segment
  }

  for (auto& mod : modification_queries) {
    auto prepared = mod.filter->prepare(rdr);

    for (auto docItr = prepared->execute(rdr); docItr->next();) {
      auto doc = docItr->value();

      // if indexed doc_id was not add()ed after the request for modification
      // and doc_id not already masked then mark query as seen and segment as modified
      if (mod.generation >= min_doc_id_generation &&
          docs_mask.insert(doc).second) {
        assert(meta.live_docs_count);
        --meta.live_docs_count; // decrement count of live docs
        mod.seen = true;
        modified = true;
      }
    }
  }

  return modified;
}

bool index_writer::add_document_mask_modified_records(
  modification_requests_t& modification_queries,
  segment_writer& writer,
  segment_meta& meta
) {
  if (modification_queries.empty()) {
    return false; // nothing new to flush
  }

  auto& doc_id_generation = writer.docs_context();
  bool modified = false;
  auto rdr = cached_readers_.emplace(dir_, meta);

  if (!rdr) {
    throw index_error(); // failed to open segment
  }

  for (auto& mod : modification_queries) {
    if (!mod.filter) {
      continue; // skip invalid modification queries
    }

    auto prepared = mod.filter->prepare(rdr);

    for (auto docItr = prepared->execute(rdr); docItr->next();) {
      const auto doc = docItr->value() - (type_limits<type_t::doc_id_t>::min)();

      if (doc >= doc_id_generation.size()) {
        continue;
      }

      const auto& doc_ctx = doc_id_generation[doc];

      // if indexed doc_id was add()ed after the request for modification then it should be skipped
      if (mod.generation < doc_ctx.generation) {
        continue; // the current modification query does not match any records
      }

      // if not already masked
      if (writer.remove(doc)) {
        // if not an update modification (i.e. a remove modification) or
        // if non-update-value record or update-value record whose query was seen
        // for every update request a replacement 'update-value' is optimistically inserted
        if (!mod.update ||
            doc_ctx.update_id == NON_UPDATE_RECORD ||
            modification_queries[doc_ctx.update_id].seen) {
          assert(meta.live_docs_count);
          --meta.live_docs_count; // decrement count of live docs
          mod.seen = true;
          modified = true;
        }
      }
    }
  }

  return modified;
}

/* static */ bool index_writer::add_document_mask_unused_updates(
    modification_requests_t& modification_queries,
    segment_writer& writer,
    segment_meta& meta
) {
  UNUSED(meta);

  if (modification_queries.empty()) {
    return false; // nothing new to add
  }

  auto& doc_id_generation = writer.docs_context();
  bool modified = false;

  // the implementation generates doc_ids sequentially
//  for (doc_id_t doc = 0, count = meta.docs_count; doc < count; ++doc) {
//    if (doc >= doc_id_generation.size()) {
//      continue;
//    }
  doc_id_t doc = 0;
  for (const auto& doc_ctx : doc_id_generation) {

//    const auto& doc_ctx = doc_id_generation[doc];

    // if it's an update record placeholder who's query did not match any records
    if (doc_ctx.update_id != NON_UPDATE_RECORD
        && !modification_queries[doc_ctx.update_id].seen
        && writer.remove(doc)) {
      assert(meta.live_docs_count);
      --meta.live_docs_count; // decrement count of live docs
      modified  = true;
    }

    ++doc;
  }

  return modified;
}

bool index_writer::consolidate(
    const consolidation_policy_t& policy, format::ptr codec /*= nullptr*/
) {
  REGISTER_TIMER_DETAILED();

  if (!codec) {
    // use default codec if not specified
    codec = codec_;
  }

  std::set<const segment_meta*> candidates;

  // hold reference to the last committed state
  // to prevent files to be deleted by a cleaner
  // during upcoming consolidation
  const auto committed_state = committed_state_;
  assert(committed_state);
  auto committed_meta = committed_state->first;
  assert(committed_meta);

  // collect a list of consolidation candidates
  {
    SCOPED_LOCK(consolidation_lock_);
    policy(candidates, dir_, *committed_meta, consolidating_segments_);
  }

  switch (candidates.size()) {
    case 0:
      // nothing to consolidate
      return true;
    case 1: {
      const auto* segment = *candidates.begin();

      if (!segment) {
        // invalid candidate
        return false;
      }

      if (segment->live_docs_count == segment->docs_count) {
        // no deletes, nothing to consolidate
        return true;
      }
    }
  }

  // validate candidates
  {
    size_t found = 0;

    const auto candidate_not_found = candidates.end();

    for (const auto& segment : *committed_meta) {
      found += size_t(candidate_not_found != candidates.find(&segment.meta));
    }

    if (found != candidates.size()) {
      // not all candidates are valid
      IR_FRMT_WARN(
        "Failed to start consolidation for index generation '" IR_SIZE_T_SPECIFIER "', found only '" IR_SIZE_T_SPECIFIER "' out of '" IR_SIZE_T_SPECIFIER "' candidates",
        committed_meta->generation(),
        found,
        candidates.size()
      );
      return false;
    }
  }

  // register segments for consolidation
  {
    SCOPED_LOCK(consolidation_lock_);

    // check that candidates are not involved in ongoing merges
    for (const auto* candidate : candidates) {
      if (consolidating_segments_.end() != consolidating_segments_.find(candidate)) {
        // segment has been already chosen for consolidation, give up
        return false;
      }
    }

    // register for consolidation
    consolidating_segments_.insert(candidates.begin(), candidates.end());
  }

  // unregisterer for all registered candidates
  auto unregister_segments = irs::make_finally([&candidates, this]() NOEXCEPT {
    if (candidates.empty()) {
      return;
    }

    SCOPED_LOCK(consolidation_lock_);
    for (const auto* candidate : candidates) {
      consolidating_segments_.erase(candidate);
    }
  });

  // do merge (without locking)

  index_meta::index_segment_t consolidation_segment;
  consolidation_segment.meta.codec = codec_; // should use new codec
  consolidation_segment.meta.version = 0; // reset version for new segment
  consolidation_segment.meta.name = file_name(meta_.increment()); // increment active meta, not fn arg

  ref_tracking_directory dir(dir_); // track references for new segment
  merge_writer merger(dir, consolidation_segment.meta.name);
  merger.reserve(candidates.size());

  // add consolidated segments to the merge_writer
  for (const auto* segment : candidates) {
    // already checked validity
    assert(segment);

    auto reader = cached_readers_.emplace(dir_, *segment);

    if (reader) {
      // merge_writer holds a reference to reader
      merger.add(static_cast<irs::sub_reader::ptr>(reader));
    }
  }

  // we do not persist segment meta since some removals may come later
  if (!merger.flush(consolidation_segment.filename, consolidation_segment.meta, false)) {
    return false; // nothing to consolidate or consolidation failure
  }

  // commit merge
  {
    SCOPED_LOCK_NAMED(commit_lock_, lock); // ensure committed_state_ segments are not modified by concurrent consolidate()/commit()
    const auto current_committed_meta = committed_state_->first;
    assert(current_committed_meta);

    if (pending_state_) {
      // transaction has been started, we're somewhere in the middle
      auto ctx = get_flush_context(); // can modify ctx->segment_mask_ without lock since have commit_lock_

      // register consolidation for the next transaction
      ctx->pending_segments_.emplace_back(
        std::move(consolidation_segment),
        integer_traits<size_t>::max(), // skip deletes, will accumulate deletes from existing candidates
        extract_refs(dir), // do not forget to track refs
        std::move(candidates), // consolidation context candidates
        std::move(committed_meta), // consolidation context meta
        std::move(merger) // merge context
      );

    } else if (committed_meta == current_committed_meta) {
      // before new transaction was started:
      // no commits happened in since consolidation was started

      auto ctx = get_flush_context();
      SCOPED_LOCK(ctx->mutex_); // lock due to context modification

      lock.unlock(); // can release commit lock, we guarded against commit by locked flush context

      // persist segment meta
      consolidation_segment.filename = index_utils::write_segment_meta(
        dir, consolidation_segment.meta
      );

      ctx->segment_mask_.reserve(
        ctx->segment_mask_.size() + candidates.size()
      );

      ctx->pending_segments_.emplace_back(
        std::move(consolidation_segment),
        0, // deletes must be applied to the consolidated segment
        extract_refs(dir), // do not forget to track refs
        std::move(candidates), // consolidation context candidates
        std::move(committed_meta) // consolidation context meta
      );

      // filter out merged segments for the next commit
      const auto& consolidation_ctx = ctx->pending_segments_.back().consolidation_ctx;

      for (const auto* segment : consolidation_ctx.candidates) {
        ctx->segment_mask_.emplace(segment->name);
      }
    } else {
      // before new transaction was started:
      // there was a commit(s) since consolidation was started,

      auto ctx = get_flush_context();
      SCOPED_LOCK(ctx->mutex_); // lock due to context modification

      lock.unlock(); // can release commit lock, we guarded against commit by locked flush context

      candidates_mapping_t mappings;
      const auto res = map_candidates(mappings, candidates, current_committed_meta->segments());

      if (res.second != candidates.size()) {
        // at least one candidate is missing
        // can't finish consolidation
        IR_FRMT_WARN(
          "Failed to finish merge for segment '%s', found only '" IR_SIZE_T_SPECIFIER "' out of '" IR_SIZE_T_SPECIFIER "' candidates",
          consolidation_segment.meta.name.c_str(),
          res.second,
          candidates.size()
        );

        return false;
      }

      // handle deletes if something changed
      if (res.first) {
        irs::document_mask docs_mask;
        map_removals(mappings, merger, dir, cached_readers_, docs_mask);

        if (!docs_mask.empty()) {
          consolidation_segment.meta.live_docs_count -= docs_mask.size();
          write_document_mask(dir, consolidation_segment.meta, docs_mask, false);
        }
      }

      // persist segment meta
      consolidation_segment.filename = index_utils::write_segment_meta(
        dir, consolidation_segment.meta
      );

      ctx->segment_mask_.reserve(
        ctx->segment_mask_.size() + candidates.size()
      );

      ctx->pending_segments_.emplace_back(
        std::move(consolidation_segment),
        0, // deletes must be applied to the consolidated segment
        extract_refs(dir), // do not forget to track refs
        std::move(candidates), // consolidation context candidates
        std::move(committed_meta) // consolidation context meta
      );

      // filter out merged segments for the next commit
      const auto& consolidation_ctx = ctx->pending_segments_.back().consolidation_ctx;

      for (const auto* segment : consolidation_ctx.candidates) {
        ctx->segment_mask_.emplace(segment->name);
      }
    }
  }

  return true;
}

bool index_writer::import(const index_reader& reader, format::ptr codec /*= nullptr*/) {
  if (!reader.live_docs_count()) {
    return true; // skip empty readers since no documents to import
  }

  if (!codec) {
    codec = codec_;
  }

  ref_tracking_directory dir(dir_); // track references

  index_meta::index_segment_t segment;
  segment.meta.name = file_name(meta_.increment());
  segment.meta.codec = codec;

  merge_writer merger(dir, segment.meta.name);
  merger.reserve(reader.size());

  for (auto& segment : reader) {
    merger.add(segment);
  }

  if (!merger.flush(segment.filename, segment.meta)) {
    return false; // import failure (no files created, nothing to clean up)
  }

  auto refs = extract_refs(dir);

  auto ctx = get_flush_context();
  SCOPED_LOCK(ctx->mutex_); // lock due to context modification

  ctx->pending_segments_.emplace_back(
    std::move(segment),
    ctx->generation_.load(), // current modification generation
    std::move(refs) // do not forget to track refs
  );

  return true;
}

index_writer::flush_context::ptr index_writer::get_flush_context(bool shared /*= true*/) {
  auto* ctx = flush_context_.load(); // get current ctx

  if (!shared) {
    for(;;) {
      async_utils::read_write_mutex::write_mutex mutex(ctx->flush_mutex_);
      SCOPED_LOCK_NAMED(mutex, lock); // lock ctx exchange (write-lock)

      // aquire the current flush_context and its lock
      if (!flush_context_.compare_exchange_strong(ctx, ctx->next_context_)) {
        ctx = flush_context_.load(); // it might have changed
        continue;
      }

      lock.release();

      return flush_context::ptr(ctx, false);
    }
  }

  for(;;) {
    async_utils::read_write_mutex::read_mutex mutex(ctx->flush_mutex_);
    TRY_SCOPED_LOCK_NAMED(mutex, lock); // lock current ctx (read-lock)

    if (!lock) {
      std::this_thread::yield(); // allow flushing thread to finish exchange
      ctx = flush_context_.load(); // it might have changed
      continue;
    }

    // at this point flush_context_ might have already changed
    // get active ctx, since initial_ctx is locked it will never be swapped with current until unlocked
    auto* flush_ctx = flush_context_.load();

    // primary_flush_context_ has changed
    if (ctx != flush_ctx) {
      ctx = flush_ctx;
      continue;
    }

    lock.release();

    return flush_context::ptr(ctx, true);
  }
}

index_writer::flush_context::segment_writers_t::ptr index_writer::get_segment_context(
    flush_context& ctx) {
  auto writer = ctx.writers_pool_.emplace(*(ctx.dir_));

  if (!writer->initialized()) {
    writer->reset(segment_meta(file_name(meta_.increment()), codec_));
  }

  return writer;
}

void index_writer::remove(const filter& filter) {
  auto ctx = get_flush_context();
  SCOPED_LOCK(ctx->mutex_); // lock due to context modification
  ctx->modification_queries_.emplace_back(filter, ctx->generation_++, false);
}

void index_writer::remove(const std::shared_ptr<filter>& filter) {
  if (!filter) {
    return; // skip empty filters
  }

  auto ctx = get_flush_context();
  SCOPED_LOCK(ctx->mutex_); // lock due to context modification
  ctx->modification_queries_.emplace_back(filter, ctx->generation_++, false);
}

void index_writer::remove(filter::ptr&& filter) {
  if (!filter) {
    return; // skip empty filters
  }

  auto ctx = get_flush_context();
  SCOPED_LOCK(ctx->mutex_); // lock due to context modification
  ctx->modification_queries_.emplace_back(std::move(filter), ctx->generation_++, false);
}

index_writer::pending_context_t index_writer::flush_all() {
  REGISTER_TIMER_DETAILED();
  bool modified = !type_limits<type_t::index_gen_t>::valid(meta_.last_gen_);
  sync_context to_sync;
  document_mask docs_mask;

  auto pending_meta = memory::make_unique<index_meta>();
  auto& segments = pending_meta->segments_;

  auto ctx = get_flush_context(false);
  auto& dir = *(ctx->dir_);
  SCOPED_LOCK(ctx->mutex_); // ensure there are no active struct update operations

  /////////////////////////////////////////////////////////////////////////////
  /// Stage 1
  /// update document_mask for existing (i.e. sealed) segments
  /////////////////////////////////////////////////////////////////////////////

  for (auto& existing_segment: meta_) {
    // skip already masked segments
    if (ctx->segment_mask_.end() != ctx->segment_mask_.find(existing_segment.meta.name)) {
      continue;
    }

    const auto segment_id = segments.size();
    segments.emplace_back(existing_segment);

    auto& segment = segments.back();

    docs_mask.clear();
    index_utils::read_document_mask(docs_mask, dir, segment.meta);

    // write docs_mask if masks added, if all docs are masked then mask segment
    if (add_document_mask_modified_records(ctx->modification_queries_, docs_mask, segment.meta)) {
      // mask empty segments
      if (!segment.meta.live_docs_count) {
        ctx->segment_mask_.emplace(existing_segment.meta.name); // mask segment to clear reader cache
        segments.pop_back(); // remove empty segment
        modified = true; // removal of one fo the existing segments
        continue;
      }

      to_sync.register_partial_sync(segment_id, write_document_mask(dir, segment.meta, docs_mask));
      segment.filename = index_utils::write_segment_meta(dir, segment.meta); // write with new mask
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  /// Stage 2
  /// add pending complete segments registered by import or consolidation
  /////////////////////////////////////////////////////////////////////////////

  // number of candidates that have been registered for
  // pending consolidation
  size_t pending_candidates_count = 0;

  for (auto& pending_segment : ctx->pending_segments_) {
    // pending consolidation
    auto& candidates = pending_segment.consolidation_ctx.candidates;

    // unregisterer for all registered candidates
    auto unregister_segments = irs::make_finally([&candidates, this]() NOEXCEPT {
      if (candidates.empty()) {
        return;
      }

      SCOPED_LOCK(consolidation_lock_);
      for (const auto* candidate : candidates) {
        consolidating_segments_.erase(candidate);
      }
    });

    docs_mask.clear();

    bool pending_consolidation = pending_segment.consolidation_ctx.merger;

    if (pending_consolidation) {
      // pending consolidation request
      candidates_mapping_t mappings;
      const auto res = map_candidates(mappings, candidates, segments);

      if (res.second != candidates.size()) {
        // at least one candidate is missing
        // in pending meta can't finish consolidation
        IR_FRMT_WARN(
          "Failed to finish merge for segment '%s', found only '" IR_SIZE_T_SPECIFIER "' out of '" IR_SIZE_T_SPECIFIER "' candidates",
          pending_segment.segment.meta.name.c_str(),
          res.second,
          candidates.size()
        );

        continue; // skip this particular consolidation
      }

      // mask mapped candidates
      for (auto& mapping : mappings) {
        ctx->segment_mask_.emplace(mapping.first);
      }

      // have some changes, apply deletes
      if (res.first) {
        map_removals(
          mappings,
          pending_segment.consolidation_ctx.merger,
          dir_,
          cached_readers_,
          docs_mask
        );
      }

      // we're done with removals for pending consolidation
      // they have been already applied to candidates above
      // and succesfully remapped to consolidated segment
      pending_segment.segment.meta.live_docs_count -= docs_mask.size();

      // we've seen at least 1 successfully applied
      // pending consolidation request
      pending_candidates_count += candidates.size();
    } else {
      // pending already imported/consolidated segment, apply deletes
      add_document_mask_modified_records(
        ctx->modification_queries_,
        docs_mask,
        pending_segment.segment.meta,
        pending_segment.generation
      );
    }

    // skip empty segments
    if (!pending_segment.segment.meta.live_docs_count) {
      ctx->segment_mask_.emplace(pending_segment.segment.meta.name);
      continue;
    }

    // write non-empty document mask
    if (!docs_mask.empty()) {
      write_document_mask(dir, pending_segment.segment.meta, docs_mask, !pending_consolidation);
      pending_consolidation = true; // force write new segment meta
    }

    // persist segment meta
    if (pending_consolidation) {
      pending_segment.segment.filename = index_utils::write_segment_meta(
        dir, pending_segment.segment.meta
      );
    }

    // register full segment sync
    to_sync.register_full_sync(segments.size());
    segments.emplace_back(std::move(pending_segment.segment));
  }

  if (pending_candidates_count) {
    // for pending consolidation we need to filter out
    // consolidation candidates after applying them
    index_meta::index_segments_t tmp;
    tmp.reserve(segments.size() - pending_candidates_count);

    auto begin = to_sync.segments.begin();
    auto end = to_sync.segments.end();

    for (size_t i = 0, size = segments.size(); i < size; ++i) {
      auto& segment = segments[i];

      // valid segment
      const bool valid = ctx->segment_mask_.end() == ctx->segment_mask_.find(segment.meta.name);

      if (begin != end && i == begin->first) {
        begin->first = valid ? tmp.size() : integer_traits<size_t>::const_max; // mark invalid
        ++begin;
      }

      if (valid) {
        tmp.emplace_back(std::move(segment));
      }
    }

    segments = std::move(tmp);
  }

  /////////////////////////////////////////////////////////////////////////////
  /// Stage 3
  /// create new segments
  /////////////////////////////////////////////////////////////////////////////

  {
    struct flush_context : util::noncopyable {
      flush_context(
          const string_ref& name,
          format::ptr codec,
          segment_writer& writer)
        : segment(segment_meta(name, codec)),
          writer(writer) {
      }

      flush_context(flush_context&& rhs) NOEXCEPT
        : segment(std::move(rhs.segment)),
          writer(rhs.writer) {
      }

      flush_context& operator=(flush_context&&) = delete; // because of reference

      index_meta::index_segment_t segment;
      segment_writer& writer;
    };

    std::vector<flush_context> segment_ctxs;

    auto flush = [this, &ctx, &segment_ctxs](segment_writer& writer) {
      if (!writer.initialized()) {
        return true;
      }

      segment_ctxs.emplace_back(writer.name(), codec_, writer);

      auto& segment = segment_ctxs.back().segment;

      if (!writer.flush(segment.filename, segment.meta)) {
        return false;
      }

      // flush document_mask after regular flush() so remove_query can traverse
      add_document_mask_modified_records(
        ctx->modification_queries_, writer, segment.meta
      );

      return true;
    };

    if (!ctx->writers_pool_.visit(flush)) {
      return pending_context_t();
    }

    // write docs_mask if !empty(), if all docs are masked then remove segment altogether
    for (auto& segment_ctx: segment_ctxs) {
      auto& segment = segment_ctx.segment;
      auto& writer = segment_ctx.writer;

      // if have a writer with potential update-replacement records then check if they were seen
      add_document_mask_unused_updates(
        ctx->modification_queries_, writer, segment.meta
      );

      auto& docs_mask = writer.docs_mask();

      // mask empty segments
      if (!segment.meta.live_docs_count) {
        ctx->segment_mask_.emplace(writer.name()); // ref to writer name will not change
        continue;
      }

      // write non-empty document mask
      if (!docs_mask.empty()) {
        write_document_mask(dir, segment.meta, docs_mask);
        segment.filename = index_utils::write_segment_meta(dir, segment.meta); // write with new mask
      }

      // register full segment sync
      to_sync.register_full_sync(segments.size());
      segments.emplace_back(std::move(segment));
    }
  }

  pending_meta->update_generation(meta_); // clone index metadata generation
  cached_readers_.purge(ctx->segment_mask_); // release cached readers

  modified |= !to_sync.empty(); // new files added

  // only flush a new index version upon a new index or a metadata change
  if (!modified) {
    return pending_context_t();
  }

  pending_meta->seg_counter_.store(meta_.counter()); // ensure counter() >= max(seg#)

  pending_context_t pending_context;
  pending_context.ctx = std::move(ctx); // retain flush context reference
  pending_context.meta = std::move(pending_meta); // retain meta pending flush
  pending_context.to_sync = std::move(to_sync);
  meta_.segments_ = pending_context.meta->segments_; // create copy

  return pending_context;
}

/*static*/ segment_writer::update_context index_writer::make_update_context(
    flush_context& ctx) {
  return segment_writer::update_context {
    ctx.generation_.load(), // current modification generation
    NON_UPDATE_RECORD
  };
}

segment_writer::update_context index_writer::make_update_context(
    flush_context& ctx, const filter& filter) {
  auto generation = ++ctx.generation_;
  SCOPED_LOCK(ctx.mutex_); // lock due to context modification
  size_t update_id = ctx.modification_queries_.size();

  ctx.modification_queries_.emplace_back(filter, generation - 1, true); // -1 for previous generation

  return segment_writer::update_context {
    generation, // current modification generation
    update_id // entry in modification_queries_
  };
}

segment_writer::update_context index_writer::make_update_context(
    flush_context& ctx, const std::shared_ptr<filter>& filter) {
  auto generation = ++ctx.generation_;
  SCOPED_LOCK(ctx.mutex_); // lock due to context modification
  size_t update_id = ctx.modification_queries_.size();

  ctx.modification_queries_.emplace_back(filter, generation - 1, true); // -1 for previous generation

  return segment_writer::update_context {
    generation, // current modification generation
    update_id // entry in modification_queries_
  };
}

segment_writer::update_context index_writer::make_update_context(
    flush_context& ctx, filter::ptr&& filter) {
  assert(filter);
  auto generation = ++ctx.generation_;
  SCOPED_LOCK(ctx.mutex_); // lock due to context modification
  size_t update_id = ctx.modification_queries_.size();

  ctx.modification_queries_.emplace_back(std::move(filter), generation - 1, true); // -1 for previous generation

  return segment_writer::update_context {
    generation, // current modification generation
    update_id // entry in modification_queries_
  };
}

bool index_writer::begin() {
  SCOPED_LOCK(commit_lock_);
  return start();
}

bool index_writer::start() {
  REGISTER_TIMER_DETAILED();

  if (pending_state_) {
    // begin has been already called 
    // without corresponding call to commit
    return false;
  }

  auto to_commit = flush_all();

  if (!to_commit) {
    // nothing to commit, no transaction started
    return false;
  }

  auto& dir = *to_commit.ctx->dir_;
  auto& pending_meta = *to_commit.meta;

  // write 1st phase of index_meta transaction
  if (!writer_->prepare(dir, pending_meta)) {
    throw illegal_state();
  }

  // sync all pending files
  try {
    auto update_generation = make_finally([this, &pending_meta] {
      meta_.update_generation(pending_meta);
    });

    auto sync = [&dir](const std::string& file) {
      if (!dir.sync(file)) {
        throw detailed_io_error("Failed to sync file, path: ") << file;
      }

      return true;
    };

    // sync files
    to_commit.to_sync.visit(sync, pending_meta);
  } catch (...) {
    // in case of syncing error, just clear pending meta & peform rollback
    // next commit will create another meta & sync all pending files
    writer_->rollback();
    pending_state_.reset(); // flush is rolled back
    throw;
  }

  // track all refs
  file_refs_t pending_refs;
  if (write_lock_) {
    // track write lock if exists
    track_ref(dir, WRITE_LOCK_NAME, pending_refs);
  }
  track_ref(dir, writer_->filename(pending_meta), pending_refs, true);
  append_segments_refs(pending_refs, dir, pending_meta);

  // 1st phase of the transaction successfully finished here,
  // set to_commit as active flush context containing pending meta
  pending_state_.commit = memory::make_shared<committed_state_t::element_type>(
    std::piecewise_construct,
    std::forward_as_tuple(std::move(to_commit.meta)),
    std::forward_as_tuple(std::move(pending_refs))
  );
  pending_state_.ctx = std::move(to_commit.ctx);

  return true;
}

void index_writer::finish() {
  REGISTER_TIMER_DETAILED();

  if (!pending_state_) {
    return;
  }

  // ...........................................................................
  // lightweight 2nd phase of the transaction
  // ...........................................................................

  writer_->commit();

  // ...........................................................................
  // after here transaction successfull (only noexcept operations below)
  // ...........................................................................

  committed_state_ = std::move(pending_state_.commit);
  meta_.last_gen_ = committed_state_->first->gen_; // update 'last_gen_' to last commited/valid generation
  pending_state_.reset(); // flush is complete, release reference to flush_context
}

void index_writer::commit() {
  SCOPED_LOCK(commit_lock_);

  start();
  finish();
}

void index_writer::rollback() {
  SCOPED_LOCK(commit_lock_);

  if (!pending_state_) {
    // there is no open transaction
    return;
  }

  // ...........................................................................
  // all functions below are noexcept 
  // ...........................................................................

  // guarded by commit_lock_
  writer_->rollback();
  pending_state_.reset();

  // reset actual meta, note that here we don't change 
  // segment counters since it can be changed from insert function
  meta_.reset(*(committed_state_->first));
}

NS_END

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
