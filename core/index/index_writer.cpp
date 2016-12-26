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
#include "file_names.hpp"
#include "merge_writer.hpp"
#include "formats/format_utils.hpp"
#include "utils/directory_utils.hpp"
#include "utils/timer_utils.hpp"
#include "utils/type_limits.hpp"
#include "index_writer.hpp"

#include <list>

NS_LOCAL

const size_t NON_UPDATE_RECORD = iresearch::integer_traits<size_t>::const_max; // non-update

// append file refs for files from the specified segments description
template<typename T, typename M>
void append_segments_refs(
  T& buf,
  iresearch::directory& dir,
  const M& meta
) {
  auto visitor = [&buf](const iresearch::index_file_refs::ref_t& ref)->bool {
    buf.emplace_back(ref);
    return true;
  };

  // track all files referenced in index_meta
  iresearch::directory_utils::reference(dir, meta, visitor, true);
}

void read_document_mask(
  iresearch::document_mask& docs_mask,
  const iresearch::directory& dir,
  const iresearch::segment_meta& meta
) {
  auto reader = meta.codec->get_document_mask_reader();
  auto visitor = [&docs_mask](iresearch::doc_id_t value)->bool {
    docs_mask.insert(value);
    return true;
  };

  // there will not be a document_mask list for new segments without deletes
  if (reader->prepare(dir, meta)) {
    iresearch::read_all<iresearch::doc_id_t>(visitor, *reader, reader->begin());
    reader->end();
  }
}

const std::string& write_document_mask(
  iresearch::directory& dir,
  iresearch::segment_meta& meta,
  const iresearch::document_mask& docs_mask
) {
  assert(docs_mask.size() <= std::numeric_limits<uint32_t>::max());

  auto mask_writer = meta.codec->get_document_mask_writer();
  meta.files.erase(mask_writer->filename(meta)); // current filename
  ++meta.version; // segment modified due to new document_mask
  const auto& file = *meta.files.emplace(mask_writer->filename(meta)).first; // new/expected filename
  mask_writer->prepare(dir, meta);
  mask_writer->begin((uint32_t)docs_mask.size());
  write_all(*mask_writer, docs_mask.begin(), docs_mask.end());
  mask_writer->end();
  return file;
}

std::string write_segment_meta(
  iresearch::directory& dir,
  iresearch::segment_meta& meta) {
  auto writer = meta.codec->get_segment_meta_writer();
  writer->write(dir, meta);

  return writer->filename(meta);
}

NS_END // NS_LOCAL

NS_ROOT

// ----------------------------------------------------------------------------
// --SECTION--                                      index_writer implementation 
// ----------------------------------------------------------------------------

const std::string index_writer::WRITE_LOCK_NAME = "write.lock";

index_writer::flush_context::flush_context():
  generation_(0),
  writers_pool_(THREAD_COUNT) {
}

void index_writer::flush_context::reset() {
  consolidation_policies_.clear();
  generation_.store(0);
  dir_->clear_refs();
  meta_.clear();
  modification_queries_.clear();
  pending_segments_.clear();
  segment_mask_.clear();
  to_sync_.clear();
  writers_pool_.visit([this](segment_writer& writer)->bool {
    writer.reset();
    return true;
  });
}

index_writer::index_writer( 
    index_lock::ptr&& lock,
    directory& dir,
    format::ptr codec,
    index_meta&& meta,
    index_meta::ptr&& commited_meta
) NOEXCEPT:
    flush_context_pool_(2), // 2 because just swap them due to common commit lock
    meta_(std::move(meta)),
    commited_meta_(std::move(commited_meta)),
    write_lock_(std::move(lock)),
    codec_(codec),
    dir_(dir),
    writer_(codec->get_index_meta_writer()) {
  assert(codec);
  flush_context_.store(&flush_context_pool_[0]);

  // setup round-robin chain
  for (size_t i = 0, count = flush_context_pool_.size() - 1; i < count; ++i) {
    flush_context_pool_[i].dir_.reset(new ref_tracking_directory(dir));
    flush_context_pool_[i].next_context_ = &flush_context_pool_[i + 1];
  }

  // setup round-robin chain
  flush_context_pool_[flush_context_pool_.size() - 1].dir_.reset(new ref_tracking_directory(dir));
  flush_context_pool_[flush_context_pool_.size() - 1].next_context_ = &flush_context_pool_[0];
}

index_writer::ptr index_writer::make(directory& dir, format::ptr codec, OPEN_MODE mode) {
  // lock the directory
  auto lock = dir.make_lock(WRITE_LOCK_NAME);

  if (!lock || !lock->try_lock()) {
    throw lock_obtain_failed() << lock_obtain_failed::lock_name(WRITE_LOCK_NAME);
  }

  /* read from directory
   * or create index metadata */
  index_meta meta;
  std::vector<index_file_refs::ref_t> file_refs;
  {
    auto reader = codec->get_index_meta_reader();

    std::string segments_file;
    const bool index_exists = reader->last_segments_file(dir, segments_file);

    if (OM_CREATE == mode || (OM_CREATE_APPEND == mode && !index_exists)) {
      // Try to read. It allows us to
      // create against an index that's
      // currently open for searching

      try {
        // for OM_CREATE meta must be fully recreated, meta read only to get last version
        if (index_exists) {
          reader->read(dir, meta, segments_file);
          meta.clear();
        }
      } catch (const error_base&) {
        meta = index_meta();
      }
    } else {
      if (!index_exists) {
        throw file_not_found();
      }

      reader->read(dir, meta, segments_file);
      append_segments_refs(file_refs, dir, meta);
      file_refs.emplace_back(iresearch::directory_utils::reference(dir, segments_file));
    }

    auto lock_file_ref = iresearch::directory_utils::reference(dir, WRITE_LOCK_NAME);

    // file exists on fs_directory
    if (lock_file_ref) {
      file_refs.emplace_back(lock_file_ref);
    }
  }

  auto commited_meta = memory::make_unique<index_meta>(meta);
  index_writer::ptr writer(new index_writer( 
    std::move(lock), 
    dir, codec,
    std::move(meta),
    std::move(commited_meta)
  ));

  writer->file_refs_ = std::move(file_refs);
  directory_utils::remove_all_unreferenced(dir); // remove non-index files from directory

  return writer;
}

index_writer::~index_writer() {
  close();
}

void index_writer::close() {
  {
    SCOPED_LOCK(commit_lock_); // cached_segment_readers_ read/modified during flush()
    cached_segment_readers_.clear();
  }
  write_lock_.reset();
}

uint64_t index_writer::buffered_docs() const { 
  uint64_t docs_in_ram = 0;

  auto visitor = [&docs_in_ram](const segment_writer& writer) {
    docs_in_ram += writer.docs_cached();
    return true;
  };

  auto ctx = const_cast<index_writer*>(this)->get_flush_context();

  ctx->writers_pool_.visit(visitor, true);

  return docs_in_ram;
}

segment_reader::ptr index_writer::get_segment_reader(
  const segment_meta& meta
) {
  auto it = cached_segment_readers_.find(meta.name);

  if (it == cached_segment_readers_.end()) {
    it = cached_segment_readers_.emplace(
      meta.name, segment_reader::open(dir_, meta)
    ).first;

    if (!it->second) {
      cached_segment_readers_.erase(it);

      return nullptr; // reader open failure
    }
  }

  it->second->refresh(meta);

  return it->second;
}

bool index_writer::add_document_mask_modified_records(
    modification_requests_t& modification_queries,
    document_mask& docs_mask,
    const segment_meta& meta,
    size_t min_doc_id_generation /*= 0*/) {
  if (modification_queries.empty()) {
    return false; // nothing new to flush
  }

  bool modified = false;
  auto rdr = get_segment_reader(meta);

  if (!rdr) {
    throw index_error(); // failed to open segment
  }

  for (auto& mod : modification_queries) {
    auto prepared = mod.filter->prepare(*rdr);

    for (auto docItr = prepared->execute(*rdr); docItr->next();) {
      auto doc = docItr->value();

      // if indexed doc_id was not add()ed after the request for modification
      // and doc_id not already masked then mark query as seen and segment as modified
      if (mod.generation >= min_doc_id_generation &&
          docs_mask.insert(doc).second) {
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
  const segment_meta& meta
) {
  if (modification_queries.empty()) {
    return false; // nothing new to flush
  }

  auto& doc_id_generation = writer.docs_context();
  bool modified = false;
  auto rdr = get_segment_reader(meta);

  if (!rdr) {
    throw index_error(); // failed to open segment
  }

  for (auto& mod : modification_queries) {
    if (!mod.filter) {
      continue; // skip invalid modification queries
    }

    auto prepared = mod.filter->prepare(*rdr);

    for (auto docItr = prepared->execute(*rdr); docItr->next();) {
      auto doc = docItr->value();
      auto generationItr = doc_id_generation.find(doc);

      // if indexed doc_id was add()ed after the request for modification then it should be skipped
      if (generationItr == doc_id_generation.end() ||
          mod.generation < generationItr->second.generation) {
        continue; // the current modification query does not match any records
      }

      // if not already masked
      if (writer.remove(doc)) {
        auto& doc_ctx = generationItr->second;

        // if not an update modification (i.e. a remove modification) or
        // if non-update-value record or update-value record whose query was seen
        // for every update request a replacement 'update-value' is optimistically inserted
        if (!mod.update ||
            doc_ctx.update_id == NON_UPDATE_RECORD ||
            modification_queries[doc_ctx.update_id].seen) {
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
  const segment_meta& meta
) {
  if (modification_queries.empty()) {
    return false; // nothing new to add
  }

  auto& doc_id_generation = writer.docs_context();
  bool modified = false;

  // the implementation generates doc_ids sequentially
  for (size_t i = 0, count = meta.docs_count; i < count; ++i) {
    auto doc = doc_id_t(type_limits<type_t::doc_id_t>::min() + i); // can't have more docs then highest doc_id
    auto generationItr = doc_id_generation.find(doc);

    // if it's an update record placeholder who's query did not match any records
    if (generationItr != doc_id_generation.end() &&
        generationItr->second.update_id != NON_UPDATE_RECORD &&
        !modification_queries[generationItr->second.update_id].seen) {
      modified |= writer.remove(doc);
    }
  }

  return modified;
}

bool index_writer::add_segment_mask_consolidated_records(
  index_meta::index_segment_t& segment,
  const index_meta& meta,
  flush_context& ctx,
  index_writer::consolidation_policy_t& policy
) {
  auto& dir = *(ctx.dir_);
  auto merge = policy(dir, meta);
  std::vector<segment_reader::ptr> merge_candidates;
  const index_meta::index_segment_t* merge_candindate_default = nullptr;
  flush_context::segment_mask_t segment_mask;

  // find merge candidates
  for (auto& segment: meta) {
    if (ctx.segment_mask_.end() != ctx.segment_mask_.find(segment.meta.name)) {
      continue; // skip already removed segment
    }

    if (!merge(segment.meta)) {
      merge_candindate_default = &segment; // pick the last non-merged segment as default
      continue; // fill min threshold not reached
    }

    auto merge_candidate = get_segment_reader(segment.meta);

    if (!merge_candidate) {
      continue; // skip empty readers
    }

    merge_candidates.emplace_back(std::move(merge_candidate));
    segment_mask.insert(segment.meta.name);
  }

  if (merge_candidates.empty()) {
    return false; // nothing to merge
  }

  // if only one merge candidate and another segment available then merge with other
  if (merge_candidates.size() < 2 && merge_candindate_default) {
    auto merge_candidate = get_segment_reader(merge_candindate_default->meta);

    if (!merge_candidate) {
      return false; // failed to open segment and nothing else to merge with
    }

    merge_candidates.emplace_back(std::move(merge_candidate));
    segment_mask.insert(merge_candindate_default->meta.name);
  }

  auto merge_segment_name = file_name(meta_.increment()); // increment active meta, not fn arg
  merge_writer merge_writer(dir, codec_, merge_segment_name);

  for (auto& merge_candidate: merge_candidates) {
    merge_writer.add(*merge_candidate);
  }

  if (!merge_writer.flush(segment.filename, segment.meta)) {
    return false; // import failure (no files created, nothing to clean up)
  }

  ctx.segment_mask_.insert(segment_mask.begin(), segment_mask.end());

  return true;
}

void index_writer::consolidate(
  consolidation_policy_t&& policy, bool immediate /*= true*/
) {
  if (!immediate) {
    auto ctx = get_flush_context();
    SCOPED_LOCK(ctx->mutex_); // lock due to context modification

    ctx->consolidation_policies_.emplace_back(std::move(policy));

    return;
  }

  index_meta::index_segment_t segment;
  SCOPED_LOCK(commit_lock_); // ensure meta_ segments are not modified
  auto ctx = get_flush_context();

  if (add_segment_mask_consolidated_records(segment, meta_, *ctx, policy)) {
    SCOPED_LOCK(ctx->mutex_); // lock due to context modification
    // 0 == merged segments existed before start of tx (all removes apply)
    ctx->pending_segments_.emplace_back(std::move(segment), 0);
  }
}

bool index_writer::import(const index_reader& reader) {
  auto ctx = get_flush_context();
  auto merge_segment_name = file_name(meta_.increment());
  merge_writer merge_writer(*(ctx->dir_), codec_, merge_segment_name);

  for (auto itr = reader.begin(), end = reader.end(); itr != end; ++itr) {
    merge_writer.add(*itr);
  }

  index_meta::index_segment_t segment;

  if (!merge_writer.flush(segment.filename, segment.meta)) {
    return false; // import failure (no files created, nothing to clean up)
  }

  SCOPED_LOCK(ctx->mutex_); // lock due to context modification

  ctx->pending_segments_.emplace_back(
    std::move(segment),
    ctx->generation_.load() // current modification generation
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

      return flush_context::ptr(ctx, [](flush_context* ctx)->void {
        async_utils::read_write_mutex::write_mutex mutex(ctx->flush_mutex_);
        ADOPT_SCOPED_LOCK_NAMED(mutex, lock);

        ctx->reset(); // reset context and make ready for reuse
      });
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

    return flush_context::ptr(ctx, [](flush_context* ctx)->void {
      async_utils::read_write_mutex::read_mutex mutex(ctx->flush_mutex_);
      ADOPT_SCOPED_LOCK_NAMED(mutex, lock);
    });
  }
}

index_writer::flush_context::segment_writers_t::ptr index_writer::get_segment_context(
  flush_context& ctx
) {
  auto writer = ctx.writers_pool_.emplace(*(ctx.dir_), codec_);

  if (!writer->initialized()) {
    writer->reset(file_name(meta_.increment()));
  }

  return writer;
}

void index_writer::remove(const filter& filter) {
  auto ctx = get_flush_context();
  SCOPED_LOCK(ctx->mutex_); // lock due to context modification
  ctx->modification_queries_.emplace_back(filter, ctx->generation_++, false);
}

void index_writer::remove(const std::shared_ptr<filter>& filter) {
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

index_writer::flush_context::ptr index_writer::flush_all() {
  REGISTER_TIMER_DETAILED();
  auto ctx = get_flush_context(false);
  auto& dir = *(ctx->dir_);
  bool modified = !type_limits<type_t::index_gen_t>::valid(meta_.last_gen_);
  std::unordered_set<string_ref> to_sync;
  SCOPED_LOCK(ctx->mutex_); // ensure there are no active struct update operations

  ctx->meta_ = std::move(index_meta(meta_)); // clone index metadata

  // update document_mask for existing (i.e. sealed) segments
  for(size_t i = 0, count = ctx->meta_.segments_.size(); i < count; ++i) {
    auto& segment = ctx->meta_.segments_[i];

    if (ctx->segment_mask_.end() != ctx->segment_mask_.find(segment.meta.name)) {
      continue;
    }

    document_mask docs_mask;

    read_document_mask(docs_mask, dir, segment.meta);

    // write docs_mask if masks added, if all docs are masked then mask segment
    if (add_document_mask_modified_records(ctx->modification_queries_, docs_mask, segment.meta)) {
      // mask empty segments
      if (docs_mask.size() == segment.meta.docs_count) {
        ctx->segment_mask_.emplace(meta_.segments_[i].meta.name); // ref to segment name in original meta_ will not change
        modified = true; // removal of one fo the existing segments
        continue;
      }

      to_sync.emplace(write_document_mask(dir, segment.meta, docs_mask));
      segment.filename = write_segment_meta(dir, segment.meta); // write with new mask
    }
  }

  // add pending complete segments
  for (auto& pending_segment: ctx->pending_segments_) {
    ctx->meta_.segments_.emplace_back(std::move(pending_segment.segment));

    auto& segment = ctx->meta_.segments_.back();
    document_mask docs_mask;

    // flush document_mask after regular flush() so remove_query can traverse
    add_document_mask_modified_records(
      ctx->modification_queries_, docs_mask, segment.meta, pending_segment.generation
    );

    // remove empty segments
    if (docs_mask.size() == segment.meta.docs_count) {
      ctx->meta_.segments_.pop_back();
      continue;
    }

    // write non-empty document mask
    if (!docs_mask.empty()) {
      write_document_mask(dir, segment.meta, docs_mask);
      segment.filename = write_segment_meta(dir, segment.meta); // write with new mask
    }

    // add files from segment to list of files to sync
    to_sync.insert(segment.meta.files.begin(), segment.meta.files.end());
  }

  {
    struct flush_context {
      size_t segment_offset;
      segment_writer& writer;
      flush_context(
        size_t v_segment_offset, segment_writer& v_writer
      ): segment_offset(v_segment_offset), writer(v_writer) {}
    };

    std::vector<flush_context> segment_ctxs;
    auto flush = [this, &ctx, &segment_ctxs](segment_writer& writer) {
      if (!writer.initialized()) {
        return true;
      }

      segment_ctxs.emplace_back(ctx->meta_.segments_.size(), writer);
      ctx->meta_.segments_.emplace_back(segment_meta(writer.name(), writer.codec()));

      auto& segment = ctx->meta_.segments_.back();

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
      return nullptr;
    }

    // write docs_mask if !empty(), if all docs are masked then remove segment altogether
    for (auto& segment_ctx: segment_ctxs) {
      auto& segment = ctx->meta_.segments_[segment_ctx.segment_offset];
      auto& writer = segment_ctx.writer;

      // if have a writer with potential update-replacement records then check if they were seen
      add_document_mask_unused_updates(
        ctx->modification_queries_, writer, segment.meta
      );

      auto& docs_mask = writer.docs_mask();

      // mask empty segments
      if (docs_mask.size() == segment.meta.docs_count) {
        ctx->segment_mask_.emplace(writer.name()); // ref to writer name will not change
        continue;
      }

      // write non-empty document mask
      if (!docs_mask.empty()) {
        write_document_mask(dir, segment.meta, docs_mask);
        segment.filename = write_segment_meta(dir, segment.meta); // write with new mask
      }

      // add files from segment to list of files to sync
      to_sync.insert(segment.meta.files.begin(), segment.meta.files.end());
    }
  }

  // add empty segment name for use with add_segment_mask_defragmented_records(...)
  ctx->segment_mask_.emplace("");

  // add segments generated by deferred merge policies to meta
  for (auto& policy: ctx->consolidation_policies_) {
    ctx->meta_.segments_.emplace_back();

    auto& segment = ctx->meta_.segments_.back();

    // remove empty segments
    if (!add_segment_mask_consolidated_records(segment, ctx->meta_, *ctx, policy) ||
        !segment.meta.docs_count) {
      ctx->meta_.segments_.pop_back();
      continue;
    }

    // add files from segment to list of files to sync
    to_sync.insert(segment.meta.files.begin(), segment.meta.files.end());
    to_sync.emplace(segment.filename);
  }

  index_meta::index_segments_t segments;
  flush_context::segment_mask_t segment_names;

  // create list of non-masked segments
  for (auto& segment: ctx->meta_) {
    if (ctx->segment_mask_.end() == ctx->segment_mask_.find(segment.meta.name)) {
      segments.emplace_back(std::move(segment));
    }
  }

  ctx->meta_.segments_ = segments; // retain list of only non-masked segments

  // create list of segment names and files requiring FS sync
  // for refs to be valid this must be done only after all changes ctx->meta_.segments_
  for (auto& segment: ctx->meta_) {
    bool sync_segment = false;
    segment_names.emplace(segment.meta.name);

    for (auto& file: segment.meta.files) {
      if (to_sync.erase(file)) {
        ctx->to_sync_.emplace_back(file); // add modified files requiring FS sync
        sync_segment = true; // at least one file in the segment was modified
      }
    }

    // must sync segment.filename if at least one file in the segment was modified
    // since segment moved above all its segment.filename references were invalidated
    if (sync_segment) {
      ctx->to_sync_.emplace_back(segment.filename); // add modified files requiring FS sync
    }
  }

  modified |= !ctx->to_sync_.empty(); // new files added

  // remove stale readers from cache
  for (auto itr = cached_segment_readers_.begin(); itr != cached_segment_readers_.end();) {
    if (segment_names.find(itr->first) == segment_names.end()) {
      itr = cached_segment_readers_.erase(itr);
    } else {
      ++itr;
    }
  }

  // only flush a new index version upon a new index or a metadata change
  if (!modified) {
    return nullptr;
  }

  meta_.segments_.swap(segments); // noexcept op
  ctx->meta_.seg_counter_.store(meta_.counter()); // ensure counter() >= max(seg#)

  return ctx;
}

segment_writer::update_context index_writer::make_update_context(
  flush_context& ctx
) {
  return segment_writer::update_context {
    ctx.generation_.load(), // current modification generation
    NON_UPDATE_RECORD
  };
}

segment_writer::update_context index_writer::make_update_context(
  flush_context& ctx, const filter& filter
) {
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
  flush_context& ctx, const std::shared_ptr<filter>& filter
) {
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
  flush_context& ctx, filter::ptr&& filter
) {
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
  assert(write_lock_);

  if (active_flush_context_) {
    // begin has been already called 
    // without corresponding call to commit
    return false;
  }

  // !!! remember that to_sync_ stores string_ref's to index_writer::meta !!!
  // it's valid since there is no small buffer optimization at the moment
  auto to_commit = flush_all(); // index metadata to commit

  if (!to_commit) {
    return true; // nothing to commit
  }

  // write 1st phase of index_meta transaction
  if (!writer_->prepare(*(to_commit->dir_), to_commit->meta_)) {
    throw illegal_state();
  }

  // 1st phase of the transaction successfully finished here,
  // set to_commit as active flush context containing pending meta
  active_flush_context_ = to_commit;

  // sync all pending files
  try {
    auto update_generation = make_finally([this] {
      meta_.update_generation(active_flush_context_->meta_);
    });

    // sync files
    for (auto& file : active_flush_context_->to_sync_) {
      if (!to_commit->dir_->sync(file)) {
        std::stringstream ss;

        ss << "Failed to sync file, path: " << file.get();

        throw detailed_io_error(ss.str());
      }
    }
  } catch (...) {
    // in case of syncing error, just clear pending meta & peform rollback
    // next commit will create another meta & sync all pending files
    writer_->rollback();
    active_flush_context_.reset(); // flush is rolled back
    throw;
  }

  return true;
}

void index_writer::finish() {
  REGISTER_TIMER_DETAILED();

  if (!active_flush_context_) {
    return;
  }

  auto ctx = active_flush_context_;
  auto& dir = *(ctx->dir_);
  std::vector<index_file_refs::ref_t> file_refs;
  auto lock_file_ref = iresearch::directory_utils::reference(dir, WRITE_LOCK_NAME);

  if (lock_file_ref) {
    // file exists on fs_directory
    file_refs.emplace_back(lock_file_ref);
  }

  file_refs.emplace_back(iresearch::directory_utils::reference(dir, writer_->filename(ctx->meta_), true));
  append_segments_refs(file_refs, dir, ctx->meta_);
  writer_->commit();
  commited_meta_ = memory::make_unique<index_meta>(ctx->meta_);

  // ...........................................................................
  // after here transaction successfull
  // ...........................................................................

  file_refs_ = std::move(file_refs);
  active_flush_context_.reset(); // flush is complete
}

void index_writer::commit() {
  assert(write_lock_);
  SCOPED_LOCK(commit_lock_);

  start();
  finish();
}

void index_writer::rollback() {
  assert(write_lock_);
  SCOPED_LOCK(commit_lock_);

  if (!active_flush_context_) {
    // there is no open transaction
    return;
  }

  // ...........................................................................
  // all functions below are noexcept 
  // ...........................................................................

  // guarded by commit_lock_
  writer_->rollback();
  active_flush_context_.reset();
  
  // reset actual meta, note that here we don't change 
  // segment counters since it can be changed from insert function
  meta_.reset(*commited_meta_);
}

NS_END