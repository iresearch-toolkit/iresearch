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

#include <deque>

NS_LOCAL

const size_t NON_UPDATE_RECORD = std::numeric_limits<size_t>::max(); // non-update

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

const std::string index_writer::WRITE_LOCK_NAME = "write.lock";

index_writer::flush_context::flush_context():
  generation_(0),
  writers_pool_(THREAD_COUNT) {
}

void index_writer::flush_context::reset() {
  generation_.store(0);
  meta_.clear();
  modification_queries_.clear();
  pending_segments_.clear();
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
    flush_context_pool_[i].next_context_ = &flush_context_pool_[i + 1];
  }

  // setup round-robin chain
  flush_context_pool_[flush_context_pool_.size() - 1].next_context_ = &flush_context_pool_[0];
}

index_writer::ptr index_writer::make(directory& dir, format::ptr codec, OPEN_MODE mode) {
  // lock the directory
  auto lock = dir.make_lock(WRITE_LOCK_NAME);

  if (!index_lock::lock(*lock)) {
    throw lock_obtain_failed() << lock_obtain_failed::lock_name(WRITE_LOCK_NAME);
  }

  /* read from directory
   * or create index metadata */
  index_meta meta;
  std::vector<index_file_refs::ref_t> file_refs;
  {
    auto reader = codec->get_index_meta_reader();
    directory::files files;
    dir.list( files );

    if ( OM_CREATE == mode
        || (OM_CREATE_APPEND == mode && !reader->index_exists(files))) {

      /* Try to read. It allows us to
       * create against an index that's
       * currently open for searching */
      try {
        auto* segments_file = reader->last_segments_file(files);

        // for OM_CREATE meta must be fully recreated, meta read only to get last version
        if (segments_file) {
          reader->read(dir, meta, *segments_file);
          meta.clear();
        }
      } catch ( const error_base& ) {
        meta = index_meta();
      }
    } else {
      auto* segments_file = reader->last_segments_file(files);

      if (!segments_file) {
        throw file_not_found();
      }

      reader->read(dir, meta, *segments_file);
      append_segments_refs(file_refs, dir, meta);
      file_refs.emplace_back(iresearch::directory_utils::reference(dir, *segments_file));
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

segment_reader& index_writer::get_segment_reader(
    const segment_meta& meta) {
  auto it = cached_segment_readers_.find(meta.name);

  if (it == cached_segment_readers_.end()) {
    it = cached_segment_readers_.emplace(
      meta.name, segment_reader::open(*dir_, meta)
    ).first;
  }

  it->second->refresh(meta);

  return *it->second;
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
  auto& rdr = get_segment_reader(meta);

  for (auto& mod : modification_queries) {
    auto prepared = mod.filter->prepare(rdr);

    for (auto docItr = prepared->execute(rdr); docItr->next();) {
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
    document_mask& docs_mask,
    const segment_meta& meta,
    const segment_writer::update_contexts& doc_id_generation) {
  if (modification_queries.empty()) {
    return false; // nothing new to flush
  }

  bool modified = false;
  auto& rdr = get_segment_reader(meta);

  for (auto& mod : modification_queries) {
    auto prepared = mod.filter->prepare(rdr);

    for (auto docItr = prepared->execute(rdr); docItr->next();) {
      auto doc = docItr->value();
      auto generationItr = doc_id_generation.find(doc);

      // if indexed doc_id was add()ed after the request for modification then it should be skipped
      if (generationItr == doc_id_generation.end() ||
          mod.generation < generationItr->second.generation) {
        continue; // the current modification query does not match any records
      }

      // if not already masked
      if (docs_mask.insert(doc).second) {
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
    document_mask& docs_mask,
    const segment_meta& meta,
    const segment_writer::update_contexts& doc_id_generation) {
  if (modification_queries.empty()) {
    return false; // nothing new to add
  }

  bool modified = false;

  // the implementation generates doc_ids sequentially
  for (size_t i = 0, count = meta.docs_count; i < count; ++i) {
    auto doc = doc_id_t(type_limits<type_t::doc_id_t>::min() + i); // can't have more docs then highest doc_id
    auto generationItr = doc_id_generation.find(doc);

    // if it's an update record placeholder who's query did not match any records
    if (generationItr != doc_id_generation.end() &&
        generationItr->second.update_id != NON_UPDATE_RECORD &&
        !modification_queries[generationItr->second.update_id].seen) {
      modified |= docs_mask.insert(doc).second;
    }
  }

  return modified;
}

void index_writer::defragment(const defragment_policy_t& policy) {
  assert(write_lock_);

  std::unordered_set<index_meta::index_segment_t*> discarded_segments;
  std::vector<segment_reader::ptr> merge_candidates;
  index_meta::index_segment_t* merge_candindate_default = nullptr;

  SCOPED_LOCK(meta_lock_); // ensure meta is not modified during defragment
  auto merge = policy(*dir_, meta_);

  // find merge candidates
  for (auto& segment : meta_) {
    if (!merge(segment.meta)) {
      merge_candindate_default = &segment; // pick the last non-merged segment as default
      continue; // fill min threshold not reached
    }

    merge_candidates.emplace_back(iresearch::segment_reader::open(*dir_, segment.meta));
    discarded_segments.insert(&segment);
  }

  if (discarded_segments.empty()) {
    return; // done
  }

  // if only one merge candidate and another segment available then merge with other
  if (merge_candidates.size() < 2 && meta_.size() > 1) {
    merge_candidates.emplace_back(
      iresearch::segment_reader::open(*dir_, merge_candindate_default->meta)
    );
    discarded_segments.insert(merge_candindate_default);
  }

  auto merge_segment_name = file_name(meta_.increment());
  ref_tracking_directory dir(*dir_); // refs held until end of scope (until commit())
  merge_writer merge_writer(dir, codec_, merge_segment_name);

  for (auto& merge_candidate: merge_candidates) {
    if (merge_candidate) {
      merge_writer.add(*merge_candidate); // skip empty readers
    }
  }

  index_meta::index_segments_t updated_segments;

  updated_segments.reserve(meta_.size() - merge_candidates.size() + 1); // +1 for merged segment
  updated_segments.emplace_back();

  auto& merge_segment = updated_segments.back();

  if (!merge_writer.flush(merge_segment.filename, merge_segment.meta)) {
    return; // merge failure (no files created, nothing to clean up)
  }

  for (auto& segment : meta_) {
    if (discarded_segments.find(&segment) == discarded_segments.end()) {
      updated_segments.emplace_back(std::move(segment));
    }
  }

  meta_.segments_.swap(updated_segments);
  meta_.gen_dirty_ = true;
}

bool index_writer::import(const index_reader& reader) {
  assert(write_lock_);

  auto merge_segment_name = file_name(meta_.increment());
  ref_tracking_directory dir(*dir_); // refs held until end of scope (until commit())
  merge_writer merge_writer(dir, codec_, merge_segment_name);

  for (auto itr = reader.begin(), end = reader.end(); itr != end; ++itr) {
    merge_writer.add(*itr);
  }

  index_meta::index_segment_t segment;

  if (!merge_writer.flush(segment.filename, segment.meta)) {
    return false; // import failure (no files created, nothing to clean up)
  }

  std::vector<index_file_refs::ref_t> file_refs;

  file_refs.emplace_back(iresearch::directory_utils::reference(*dir_, segment.filename, true));
  append_segments_refs(file_refs, *dir_, segment.meta);

  auto ctx = get_flush_context();
  SCOPED_LOCK(ctx->mutex_); // lock due to context modification

  ctx->pending_segments_.emplace_back(
    std::move(segment),
    ctx->generation_.load(), // current modification generation
    std::move(file_refs)
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
        ctx->reset(); // reset context and make ready for reuse
        ctx->flush_mutex_.unlock();
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

    return flush_context::ptr(ctx, [](flush_context* ctx)->void{ctx->flush_mutex_.unlock();});
  }
}

index_writer::flush_context::segment_writers_t::ptr index_writer::get_segment_context(
  flush_context& ctx
) {
  auto writer = ctx.writers_pool_.emplace(dir_, codec_);

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
  SCOPED_LOCK(ctx->mutex_); // ensure there are no active struct update operations

  index_meta::index_segments_t flushed;
  size_t files_flushed = 0; // number of flushed files
  {
    struct flush_context {
      flush_context(): writer(nullptr) {}
      flush_context(segment_writer& writer): writer(&writer) { }

      document_mask docs_mask;
      segment_writer* writer;
    }; // flush_context
    
    std::deque<flush_context> segment_ctxs;
    auto& modification_queries = ctx->modification_queries_;
    auto flush = [this, &modification_queries, &files_flushed, &flushed, &segment_ctxs](segment_writer& writer) {
      if (!writer.initialized()) {
        return true;
      }

      segment_ctxs.emplace_back(writer);

      flushed.emplace_back(segment_meta(writer.name(), writer.codec()));

      auto& flush_ctx = segment_ctxs.back();
      auto& flushed_segment = flushed.back();

      writer.flush(flushed_segment.filename, flushed_segment.meta);
      files_flushed += flushed_segment.meta.files.size();

      // flush document_mask after regular flush() so remove_query can traverse
      add_document_mask_modified_records(
        modification_queries,
        flush_ctx.docs_mask,
        flushed_segment.meta,
        writer.docs_context()
      );

      return true;
    };
    ctx->writers_pool_.visit(flush);

    // add pending complete segments
    for (auto& pending_segment: ctx->pending_segments_) {
      flushed.emplace_back(std::move(pending_segment.segment));
      segment_ctxs.emplace_back();

      auto begin = pending_segment.file_refs.begin();
      auto end = pending_segment.file_refs.end();
      auto files = [&begin, &end]() -> const std::string* {
        if (begin == end) {
          return nullptr;
        }

        return (begin++)->get();
      };
      auto visitor = [](const index_file_refs::ref_t&)->bool { return true; };
      auto& flush_ctx = segment_ctxs.back();
      auto& flushed_segment = flushed.back();

      directory_utils::reference(*dir_, files, visitor); // add refs
      add_document_mask_modified_records(
        modification_queries,
        flush_ctx.docs_mask,
        flushed_segment.meta,
        pending_segment.generation
      ); // flush document_mask after regular flush() so remove_query can traverse
    }

    // update document_mask for existing (i.e. sealed) segments
    {
      SCOPED_LOCK(meta_lock_);

      for (auto metaItr = meta_.begin(); metaItr != meta_.end();) {
        auto& seg_meta = metaItr->meta;
        document_mask docs_mask;

        read_document_mask(docs_mask, *dir_, seg_meta);

        // write docs_mask if masks added, if all docs are masked then remove segment altogether
        if (add_document_mask_modified_records(modification_queries, docs_mask, seg_meta)) {
          meta_.gen_dirty_ = true;

          if (docs_mask.size() == seg_meta.docs_count) { // remove empty segments
            metaItr = meta_.segments_.erase(metaItr);
            continue;
          }

          ctx->to_sync_.emplace_back(write_document_mask(*dir_, seg_meta, docs_mask));
          ctx->to_sync_.emplace_back(metaItr->filename = write_segment_meta(*dir_, seg_meta)); // write with new mask
        }

        ++metaItr;
      }
    }

    // 'flushed' and 'writers' are filled in parallel above, differing only in scope
    assert(flushed.size() == segment_ctxs.size());
    auto metaItr = flushed.begin();

    // write docs_mask if !empty(), if all docs are masked then remove segment altogether
    for (auto ctxItr = segment_ctxs.begin(); ctxItr != segment_ctxs.end(); ++ctxItr) {
      auto& seg_meta = metaItr->meta;
      auto& flush_ctx = *ctxItr;
      auto& docs_mask = flush_ctx.docs_mask;
      auto* seg_writer = flush_ctx.writer;

      // if have a writer with potential update-replacement records then check if they were seen
      if (seg_writer) {
        add_document_mask_unused_updates(
          modification_queries,
          docs_mask, 
          seg_meta, 
          seg_writer->docs_context()
        );
      }

      if (docs_mask.size() == seg_meta.docs_count) { // remove empty segments
        metaItr = flushed.erase(metaItr);
      } else {
        if (!docs_mask.empty()) { // write non-empty document mask
          ctx->to_sync_.emplace_back(write_document_mask(*dir_, seg_meta, docs_mask));
          ctx->to_sync_.emplace_back(metaItr->filename = write_segment_meta(*dir_, seg_meta)); // write with new mask
        }

        ++metaItr;
      }
    }
  }

  {
    SCOPED_LOCK(meta_lock_);
    // reserve memory
    ctx->to_sync_.reserve(ctx->to_sync_.size() + files_flushed);
    meta_.segments_.reserve(meta_.size() + flushed.size());
    // noexcept block begin
    meta_.gen_dirty_ |= (flushed.size() > 0);
    std::for_each(
      flushed.begin(), flushed.end(),
      [this, &ctx] (index_meta::index_segment_t& segment) {
        meta_.segments_.emplace_back(std::move(segment));
        for (const auto& name : meta_.segments_.back().meta.files) {
          ctx->to_sync_.emplace_back(name);
        }
    });
    // noexcept block end 

    // remove stale readers from cache
    if (!cached_segment_readers_.empty()) {
      std::unordered_set<string_ref> segment_names;

      for (auto itr = meta_.begin(); itr != meta_.end(); ++itr) {
        segment_names.emplace(itr->meta.name);
      }

      for (auto itr = cached_segment_readers_.begin(); itr != cached_segment_readers_.end();) {
        if (segment_names.find(itr->first) == segment_names.end()) {
          itr = cached_segment_readers_.erase(itr);
        } else {
          ++itr;
        }
      }
    }

    // clone index metadata
    if (!meta_.gen_dirty_ && meta_.last_gen_ != index_meta::INVALID_GEN) {
      return nullptr;
    }

    ctx->meta_ = index_meta(meta_);

    return ctx;
  }
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
  if (!writer_->prepare(dir_, to_commit->meta_)) {
    throw illegal_state();
  }

  // 1st phase of the transaction successfully finished here,
  // set to_commit as active flush context containing pending meta
  active_flush_context_ = to_commit;

  // sync all pending files
  try {
    auto update_generation = make_finally([this] {
      SCOPED_LOCK(meta_lock_);
      meta_.update_generation(active_flush_context_->meta_);
    });

    // sync files
    for (auto& file : active_flush_context_->to_sync_) {
      (*dir_).sync(file);
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
  std::vector<index_file_refs::ref_t> file_refs;
  auto lock_file_ref = iresearch::directory_utils::reference(*dir_, WRITE_LOCK_NAME);

  if (lock_file_ref) {
    // file exists on fs_directory
    file_refs.emplace_back(lock_file_ref);
  }

  file_refs.emplace_back(iresearch::directory_utils::reference(*dir_, writer_->filename(ctx->meta_), true));
  append_segments_refs(file_refs, *dir_, ctx->meta_);
  writer_->commit();
  commited_meta_ = memory::make_unique<index_meta>(ctx->meta_);

  // ...........................................................................
  // after here transaction successfull
  // ...........................................................................

  file_refs_ = std::move(file_refs);
  dir_.clear_refs(); 
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

  SCOPED_LOCK(meta_lock_); // ensure meta is not modified during rollback 

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