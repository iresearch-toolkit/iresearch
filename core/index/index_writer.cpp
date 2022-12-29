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

#include "index_writer.hpp"

#include "formats/format_utils.hpp"
#include "index/comparer.hpp"
#include "index/composite_reader_impl.hpp"
#include "index/file_names.hpp"
#include "index/index_meta.hpp"
#include "index/merge_writer.hpp"
#include "shared.hpp"
#include "utils/bitvector.hpp"
#include "utils/compression.hpp"
#include "utils/directory_utils.hpp"
#include "utils/index_utils.hpp"
#include "utils/timer_utils.hpp"
#include "utils/type_limits.hpp"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/str_cat.h>

namespace irs {
namespace {

constexpr size_t kNonUpdateRecord = std::numeric_limits<size_t>::max();

// do-nothing progress reporter, used as fallback if no other progress
// reporter is used
const index_writer::progress_report_callback kNoProgress =
  [](std::string_view /*phase*/, size_t /*current*/, size_t /*total*/) {
    // intentionally do nothing
  };

const column_info_provider_t kDefaultColumnInfo = [](std::string_view) {
  // no compression, no encryption
  return column_info{irs::type<compression::none>::get(), {}, false};
};

const feature_info_provider_t kDefaultFeatureInfo =
  [](irs::type_info::type_id) {
    // no compression, no encryption
    return std::make_pair(
      column_info{irs::type<compression::none>::get(), {}, false},
      feature_writer_factory_t{});
  };

struct FlushSegmentContext {
  // starting doc_id to consider in 'segment.meta' (inclusive)
  const size_t doc_id_begin_;
  // ending doc_id to consider in 'segment.meta' (exclusive)
  const size_t doc_id_end_;
  // doc_ids masked in SegmentMeta
  document_mask docs_mask_;
  // copy so that it can be moved into 'index_writer::pending_state_'
  IndexSegment segment_;
  // modification contexts referenced by 'update_contexts_'
  std::span<const index_writer::modification_context> modification_contexts_;
  // update contexts for documents in SegmentMeta
  std::span<const segment_writer::update_context> update_contexts_;
  SegmentReader reader_;

  FlushSegmentContext(
    SegmentReader&& reader, const IndexSegment& segment, size_t doc_id_begin,
    size_t doc_id_end,
    std::span<const segment_writer::update_context> update_contexts,
    std::span<const index_writer::modification_context> modification_contexts)
    : doc_id_begin_(doc_id_begin),
      doc_id_end_(doc_id_end),
      segment_(segment),
      modification_contexts_{modification_contexts},
      update_contexts_{update_contexts},
      reader_{std::move(reader)} {
    IRS_ASSERT(doc_id_begin_ <= doc_id_end_);
    IRS_ASSERT(doc_id_end_ - doc_limits::min() <= segment_.meta.docs_count);
    IRS_ASSERT(update_contexts.size() == segment_.meta.docs_count);
  }
};

// Apply any document removals based on filters in the segment.
// modifications where to get document update_contexts from
// docs_mask where to apply document removals to
// readers readers by segment name
// meta key used to get reader for the segment to evaluate
// min_modification_generation smallest consider modification generation
// Return if any new records were added (modification_queries_ modified).
bool add_document_mask_modified_records(
  std::span<index_writer::modification_context> modifications,
  const SegmentReader& reader, size_t min_modification_generation = 0) {
  if (modifications.empty()) {
    return false;  // nothing new to flush
  }

  // FIXME(gnusi): modify doc_mask in-place
  IRS_ASSERT(reader.docs_mask());
  auto& docs_mask = const_cast<document_mask&>(*reader.docs_mask());
  auto& meta = const_cast<SegmentInfo&>(reader->meta());

  bool modified = false;

  for (auto& modification : modifications) {
    if (!modification.filter) {
      continue;  // skip invalid or uncommitted modification queries
    }

    auto prepared = modification.filter->prepare(reader);

    if (!prepared) {
      continue;  // skip invalid prepared filters
    }

    auto itr = prepared->execute(reader);

    if (!itr) {
      continue;  // skip invalid iterators
    }

    while (itr->next()) {
      const auto doc_id = itr->value();

      // if the indexed doc_id was insert()ed after the request for modification
      // or the indexed doc_id was already masked then it should be skipped
      if (modification.generation < min_modification_generation ||
          !docs_mask.insert(doc_id).second) {
        continue;  // the current modification query does not match any records
      }

      IRS_ASSERT(meta.live_docs_count);
      --meta.live_docs_count;  // decrement count of live docs
      modification.seen = true;
      modified = true;
    }
  }

  return modified;
}

// Apply any document removals based on filters in the segment.
// modifications where to get document update_contexts from
// segment where to apply document removals to
// min_doc_id staring doc_id that should be considered
// readers readers by segment name
// Return if any new records were added (modification_queries_ modified).
bool add_document_mask_modified_records(
  std::span<index_writer::modification_context> modifications,
  FlushSegmentContext& ctx, const SegmentReader& reader) {
  if (modifications.empty()) {
    return false;  // nothing new to flush
  }

  // FIXME(gnusi): modify doc_mask in-place
  IRS_ASSERT(reader.docs_mask());
  auto& docs_mask = const_cast<document_mask&>(*reader.docs_mask());
  auto& meta = const_cast<SegmentInfo&>(reader->meta());

  IRS_ASSERT(doc_limits::valid(ctx.doc_id_begin_));
  IRS_ASSERT(ctx.doc_id_begin_ <= ctx.doc_id_end_);
  IRS_ASSERT(ctx.doc_id_end_ <=
             ctx.update_contexts_.size() + doc_limits::min());
  bool modified = false;

  for (auto& modification : modifications) {
    if (!modification.filter) {
      continue;  // skip invalid or uncommitted modification queries
    }

    auto prepared = modification.filter->prepare(reader);

    if (!prepared) {
      continue;  // skip invalid prepared filters
    }

    auto itr = prepared->execute(reader);

    if (!itr) {
      continue;  // skip invalid iterators
    }

    while (itr->next()) {
      const auto doc_id = itr->value();

      if (doc_id < ctx.doc_id_begin_ || doc_id >= ctx.doc_id_end_) {
        continue;  // doc_id is not part of the current flush_context
      }

      // valid because of asserts above
      const auto& doc_ctx = ctx.update_contexts_[doc_id - doc_limits::min()];

      // if the indexed doc_id was insert()ed after the request for modification
      // or the indexed doc_id was already masked then it should be skipped
      if (modification.generation < doc_ctx.generation ||
          !docs_mask.insert(doc_id).second) {
        continue;  // the current modification query does not match any records
      }

      // if an update modification and update-value record whose query was not
      // seen (i.e. replacement value whose filter did not match any documents)
      // for every update request a replacement 'update-value' is optimistically
      // inserted
      if (modification.update && doc_ctx.update_id != kNonUpdateRecord &&
          !ctx.modification_contexts_[doc_ctx.update_id].seen) {
        continue;  // the current modification matched a replacement document
                   // which in turn did not match any records
      }

      modification.seen = true;
      modified = true;
    }
  }

  // decrement count of live docs
  IRS_ASSERT(docs_mask.size() <= meta.docs_count);
  meta.live_docs_count = meta.docs_count - docs_mask.size();

  return modified;
}

// Mask documents created by updates which did not have any matches.
// Return if any new records were added (modification_contexts_ modified).
bool add_document_mask_unused_updates(FlushSegmentContext& ctx) {
  if (ctx.modification_contexts_.empty()) {
    return false;  // nothing new to add
  }
  IRS_ASSERT(doc_limits::valid(ctx.doc_id_begin_));
  IRS_ASSERT(ctx.doc_id_begin_ <= ctx.doc_id_end_);
  IRS_ASSERT(ctx.doc_id_end_ <=
             ctx.update_contexts_.size() + doc_limits::min());
  bool modified = false;

  auto& docs_mask = const_cast<document_mask&>(*ctx.reader_->docs_mask());
  auto& meta = const_cast<SegmentInfo&>(ctx.reader_->meta());

  for (auto doc_id = ctx.doc_id_begin_; doc_id < ctx.doc_id_end_; ++doc_id) {
    // valid because of asserts above
    const auto& doc_ctx = ctx.update_contexts_[doc_id - doc_limits::min()];

    if (doc_ctx.update_id == kNonUpdateRecord) {
      continue;  // not an update operation
    }

    IRS_ASSERT(ctx.modification_contexts_.size() > doc_ctx.update_id);

    // if it's an update record placeholder who's query already match some
    // record
    if (ctx.modification_contexts_[doc_ctx.update_id].seen ||
        !docs_mask.insert(doc_id).second) {
      continue;  // the current placeholder record is in-use and valid
    }

    IRS_ASSERT(meta.live_docs_count);
    --meta.live_docs_count;  // decrement count of live docs
    modified = true;
  }

  return modified;
}

// append file refs for files from the specified segments description
template<typename T, typename M>
void append_segments_refs(T& buf, directory& dir, const M& meta) {
  auto visitor = [&buf](const index_file_refs::ref_t& ref) -> bool {
    buf.emplace_back(ref);
    return true;
  };

  // track all files referenced in index_meta
  directory_utils::reference(dir, meta, visitor, true);
}

std::string_view write_document_mask(directory& dir, SegmentMeta& meta,
                                     const document_mask& docs_mask,
                                     bool increment_version = true) {
  IRS_ASSERT(docs_mask.size() <= std::numeric_limits<uint32_t>::max());

  auto mask_writer = meta.codec->get_document_mask_writer();

  if (increment_version) {
    meta.files.erase(mask_writer->filename(meta));  // current filename
    ++meta.version;  // segment modified due to new document_mask

    // a second time +1 to avoid overlap with version increment due to commit of
    // uncommitted segment tail which must mask committed segment head
    // NOTE0: +1 extra is enough since a segment can reside in at most 2
    //        flush_contexts, there fore no more than 1 tail
    // NOTE1: flush_all() Stage3 increments version by _only_ 1 to avoid overlap
    //        with here, i.e. segment tail version will always be odd due to the
    //        aforementioned and because there is at most 1 tail
    ++meta.version;
  }

  const auto [file, _] =
    meta.files.emplace(mask_writer->filename(meta));  // new/expected filename

  mask_writer->write(dir, meta, docs_mask);

  // reset no longer valid size, to be recomputed on
  // index_utils::write_index_segment(...)
  meta.size_in_bytes = 0;

  return *file;
}

// mapping: name -> { new segment, old segment }
using CandidatesMapping = absl::flat_hash_map<
  std::string_view,
  std::pair<const SubReader*,                       // new segment
            std::pair<const SubReader*, size_t>>>;  // old segment + index
                                                    // within merge_writer

// candidates_mapping output mapping
// candidates candidates for mapping
// segments map against a specified segments
// Returns first - has removals, second - number of mapped candidates.
std::pair<bool, size_t> map_candidates(CandidatesMapping& candidates_mapping,
                                       ConsolidationView candidates,
                                       const IndexReader& index) {
  size_t i = 0;
  for (const auto* candidate : candidates) {
    candidates_mapping.emplace(
      std::piecewise_construct, std::forward_as_tuple(candidate->meta().name),
      std::forward_as_tuple(nullptr, std::make_pair(candidate, i++)));
  }

  size_t found = 0;
  bool has_removals = false;
  const auto candidate_not_found = candidates_mapping.end();

  for (const auto& segment : index) {
    const auto& meta = segment.meta();
    const auto it = candidates_mapping.find(meta.name);

    if (candidate_not_found == it) {
      // not a candidate
      continue;
    }

    auto& mapping = it->second;
    const auto* new_segment = mapping.first;

    if (new_segment && new_segment->meta().version >= meta.version) {
      // mapping already has a newer segment version
      continue;
    }

    ++found;

    IRS_ASSERT(mapping.second.first);
    mapping.first = &segment;

    // FIXME(gnusi): can't we just check pointers?
    has_removals |= (meta.version != it->second.second.first->meta().version);
  }

  return std::make_pair(has_removals, found);
}

bool map_removals(const CandidatesMapping& candidates_mapping,
                  const merge_writer& merger, document_mask& docs_mask) {
  IRS_ASSERT(merger);

  for (auto& mapping : candidates_mapping) {
    const auto& segment_mapping = mapping.second;
    const auto* new_segment = segment_mapping.first;
    const auto& new_meta = new_segment->meta();
    const auto& old_meta = segment_mapping.second.first->meta();

    if (new_meta.version != old_meta.version) {
      const auto& merge_ctx = merger[segment_mapping.second.second];
      auto merged_itr = merge_ctx.reader->docs_iterator();
      auto current_itr = new_segment->docs_iterator();

      // this only masks documents of a single segment
      // this works due to the current architectural approach of segments,
      // either removals are new and will be applied during flush_all()
      // or removals are in the docs_mask and still be applied by the reader
      // passed to the merge_writer

      // no more docs in merged reader
      if (!merged_itr->next()) {
        if (current_itr->next()) {
          IR_FRMT_WARN(
            "Failed to map removals for consolidated segment '%s' version "
            "'" IR_UINT64_T_SPECIFIER
            "' from current segment '%s' version '" IR_UINT64_T_SPECIFIER
            "', current segment has doc_id '" IR_UINT32_T_SPECIFIER
            "' not present in the consolidated segment",
            old_meta.name.c_str(), old_meta.version, new_meta.name.c_str(),
            new_meta.version, current_itr->value());

          return false;  // current reader has unmerged docs
        }

        continue;  // continue wih next mapping
      }

      // mask all remaining doc_ids
      if (!current_itr->next()) {
        do {
          IRS_ASSERT(doc_limits::valid(merge_ctx.doc_map(
            merged_itr->value())));  // doc_id must have a valid mapping
          docs_mask.insert(merge_ctx.doc_map(merged_itr->value()));
        } while (merged_itr->next());

        continue;  // continue wih next mapping
      }

      // validate that all docs in the current reader were merged, and add any
      // removed docs to the merged mask
      for (;;) {
        while (merged_itr->value() < current_itr->value()) {
          // doc_id must have a valid mapping
          IRS_ASSERT(doc_limits::valid(merge_ctx.doc_map(merged_itr->value())));
          docs_mask.insert(merge_ctx.doc_map(merged_itr->value()));

          if (!merged_itr->next()) {
            IR_FRMT_WARN(
              "Failed to map removals for consolidated segment '%s' version "
              "'" IR_UINT64_T_SPECIFIER
              "' from current segment '%s' version '" IR_UINT64_T_SPECIFIER
              "', current segment has doc_id '" IR_UINT32_T_SPECIFIER
              "' not present in the consolidated segment",
              old_meta.name.c_str(), old_meta.version, new_meta.name.c_str(),
              new_meta.version, current_itr->value());

            return false;  // current reader has unmerged docs
          }
        }

        if (merged_itr->value() > current_itr->value()) {
          IR_FRMT_WARN(
            "Failed to map removals for consolidated segment '%s' version "
            "'" IR_UINT64_T_SPECIFIER
            "' from current segment '%s' version '" IR_UINT64_T_SPECIFIER
            "', current segment has doc_id '" IR_UINT32_T_SPECIFIER
            "' not present in the consolidated segment",
            old_meta.name.c_str(), old_meta.version, new_meta.name.c_str(),
            new_meta.version, current_itr->value());

          return false;  // current reader has unmerged docs
        }

        // no more docs in merged reader
        if (!merged_itr->next()) {
          if (current_itr->next()) {
            IR_FRMT_WARN(
              "Failed to map removals for consolidated segment '%s' version "
              "'" IR_UINT64_T_SPECIFIER
              "' from current segment '%s' version '" IR_UINT64_T_SPECIFIER
              "', current segment has doc_id '" IR_UINT32_T_SPECIFIER
              "' not present in the consolidated segment",
              old_meta.name.c_str(), old_meta.version, new_meta.name.c_str(),
              new_meta.version, current_itr->value());

            return false;  // current reader has unmerged docs
          }

          break;  // continue wih next mapping
        }

        // mask all remaining doc_ids
        if (!current_itr->next()) {
          do {
            // doc_id must have a valid mapping
            IRS_ASSERT(
              doc_limits::valid(merge_ctx.doc_map(merged_itr->value())));
            docs_mask.insert(merge_ctx.doc_map(merged_itr->value()));
          } while (merged_itr->next());

          break;  // continue wih next mapping
        }
      }
    }
  }

  return true;
}

std::string to_string(ConsolidationView consolidation) {
  std::string str;

  size_t total_size = 0;
  size_t total_docs_count = 0;
  size_t total_live_docs_count = 0;

  for (const auto* segment : consolidation) {
    auto& meta = segment->meta();

    absl::StrAppend(&str, "Name='", meta.name,
                    "', docs_count=", meta.docs_count,
                    ", live_docs_count=", meta.live_docs_count,
                    ", size=", meta.size_in_bytes, "\n");

    total_docs_count += meta.docs_count;
    total_live_docs_count += meta.live_docs_count;
    total_size += meta.size_in_bytes;
  }

  absl::StrAppend(&str, "Total: segments=", consolidation.size(),
                  ", docs_count=", total_docs_count,
                  ", live_docs_count=", total_live_docs_count,
                  " size=", total_size, "");

  return str;
}

}  // namespace

using namespace std::chrono_literals;

index_writer::active_segment_context::active_segment_context(
  std::shared_ptr<segment_context> ctx, std::atomic<size_t>& segments_active,
  flush_context* flush_ctx, size_t pending_segment_context_offset) noexcept
  : ctx_{std::move(ctx)},
    flush_ctx_{flush_ctx},
    pending_segment_context_offset_{pending_segment_context_offset},
    segments_active_{&segments_active} {
#ifdef IRESEARCH_DEBUG
  if (flush_ctx) {
    // ensure there are no active struct update operations
    // (only needed for IRS_ASSERT)
    // cppcheck-suppress unreadVariable
    std::lock_guard lock{flush_ctx->mutex_};

    // IRS_ASSERT that flush_ctx and ctx are compatible
    IRS_ASSERT(
      flush_ctx->pending_segment_contexts_[pending_segment_context_offset_]
        .segment_ == ctx_);
  }
#endif

  if (ctx_) {
    // track here since guaranteed to have 1 ref per active
    // segment
    segments_active_->fetch_add(1);
  }
}

index_writer::active_segment_context::~active_segment_context() {
  if (ctx_) {
    // track here since guaranteed to have 1 ref per active
    // segment
    segments_active_->fetch_sub(1);
  }

  if (flush_ctx_) {
    ctx_.reset();

    try {
      std::lock_guard lock{flush_ctx_->mutex_};
      flush_ctx_->pending_segment_context_cond_.notify_all();
    } catch (...) {
      // lock may throw
    }
  }  // FIXME TODO remove once col_writer tail is fixed to flush() multiple
     // times without overwrite (since then the tail will be in a different
     // context)
}

index_writer::active_segment_context&
index_writer::active_segment_context::operator=(
  active_segment_context&& other) noexcept {
  if (this != &other) {
    if (ctx_) {
      // track here since guaranteed to have 1 ref per active segment
      segments_active_->fetch_sub(1);
    }

    ctx_ = std::move(other.ctx_);
    flush_ctx_ = other.flush_ctx_;
    pending_segment_context_offset_ = other.pending_segment_context_offset_;
    segments_active_->store(other.segments_active_->load());
  }

  return *this;
}

index_writer::Document::Document(flush_context& ctx,
                                 std::shared_ptr<segment_context> segment,
                                 const segment_writer::update_context& update)
  : segment_{std::move(segment)},
    writer_{*segment_->writer_},
    ctx_{ctx},
    update_id_{update.update_id} {
  IRS_ASSERT(segment_);
  IRS_ASSERT(segment_->writer_);
  const auto uncomitted_doc_id_begin =
    segment_->uncomitted_doc_id_begin_ >
        segment_->flushed_update_contexts_.size()
      // uncomitted start in 'writer_'
      ? (segment_->uncomitted_doc_id_begin_ -
         segment_->flushed_update_contexts_.size())
      // uncommitted start in 'flushed_'
      : doc_limits::min();
  IRS_ASSERT(uncomitted_doc_id_begin <=
             writer_.docs_cached() + doc_limits::min());
  // ensure reset() will be noexcept
  ++segment_->active_count_;
  const auto rollback_extra =
    writer_.docs_cached() + doc_limits::min() - uncomitted_doc_id_begin;
  writer_.begin(update, rollback_extra);  // ensure reset() will be noexcept
  segment_->buffered_docs_.store(writer_.docs_cached());
}

index_writer::Document::~Document() noexcept {
  if (!segment_) {
    return;  // another instance will call commit()
  }

  IRS_ASSERT(segment_->writer_);
  IRS_ASSERT(&writer_ == segment_->writer_.get());

  try {
    writer_.commit();
  } catch (...) {
    writer_.rollback();
  }

  if (!*this && update_id_ != kNonUpdateRecord) {
    // mark invalid
    segment_->modification_queries_[update_id_].filter = nullptr;
  }

  // optimization to notify any ongoing flush_all() operations so they wake up
  // earlier
  if (1 == segment_->active_count_.fetch_sub(1)) {
    // lock due to context modification and notification, note:
    // std::mutex::try_lock() does not throw exceptions as per documentation
    // @see https://en.cppreference.com/w/cpp/named_req/Mutex
    std::unique_lock lock{ctx_.mutex_, std::try_to_lock};

    if (lock.owns_lock()) {
      // ignore if lock failed because it implies that
      // flush_all() is not waiting for a notification
      ctx_.pending_segment_context_cond_.notify_all();
    }
  }
}

void index_writer::Transaction::ForceFlush() {
  if (const auto& ctx = segment_.ctx(); !ctx) {
    return;  // nothing to do
  }

  if (writer_.get_flush_context()->AddToPending(segment_)) {
#ifdef IRESEARCH_DEBUG
    ++segment_use_count_;
#endif
  }
}

bool index_writer::Transaction::Commit() noexcept {
  const auto& ctx = segment_.ctx();

  // failure may indicate a dangling 'document' instance
#ifdef IRESEARCH_DEBUG
  IRS_ASSERT(ctx.use_count() == segment_use_count_);
#endif

  if (!ctx) {
    return true;  // nothing to do
  }

  if (auto& writer = *ctx->writer_; writer.tick() < last_operation_tick_) {
    writer.tick(last_operation_tick_);
  }

  try {
    // FIXME move emplace into active_segment_context destructor commit segment
    writer_.get_flush_context()->emplace(std::move(segment_),
                                         first_operation_tick_);
    return true;
  } catch (...) {
    Reset();  // abort segment
    return false;
  }
}

void index_writer::Transaction::Reset() noexcept {
  last_operation_tick_ = 0;  // reset tick

  const auto& ctx = segment_.ctx();

  if (!ctx) {
    return;  // nothing to reset
  }

  // rollback modification queries
  std::for_each(std::begin(ctx->modification_queries_) +
                  ctx->uncomitted_modification_queries_,
                std::end(ctx->modification_queries_),
                [](modification_context& ctx) noexcept {
                  // mark invalid
                  ctx.filter = nullptr;
                });

  auto& flushed_update_contexts = ctx->flushed_update_contexts_;

  // find and mask/truncate uncomitted tail
  for (size_t i = 0, count = ctx->flushed_.size(), flushed_docs_count = 0;
       i < count; ++i) {
    auto& segment = ctx->flushed_[i];
    auto flushed_docs_start = flushed_docs_count;

    // sum of all previous SegmentMeta::docs_count
    // including this meta
    flushed_docs_count += segment.meta.docs_count;

    if (flushed_docs_count <=
        ctx->uncomitted_doc_id_begin_ - doc_limits::min()) {
      continue;  // all documents in this this index_meta have been committed
    }

    auto docs_mask_tail_doc_id =
      ctx->uncomitted_doc_id_begin_ - flushed_docs_start;

    IRS_ASSERT(docs_mask_tail_doc_id <= segment.meta.live_docs_count);
    IRS_ASSERT(docs_mask_tail_doc_id <= std::numeric_limits<doc_id_t>::max());
    segment.docs_mask_tail_doc_id = doc_id_t(docs_mask_tail_doc_id);

    if (docs_mask_tail_doc_id - doc_limits::min() >= segment.meta.docs_count) {
      ctx->flushed_.resize(i);  // truncate including current empty meta
    } else {
      ctx->flushed_.resize(i + 1);  // truncate starting from subsequent meta
    }

    IRS_ASSERT(flushed_update_contexts.size() >= flushed_docs_count);
    // truncate 'flushed_update_contexts_'
    flushed_update_contexts.resize(flushed_docs_count);
    // reset to start of 'writer_'
    ctx->uncomitted_doc_id_begin_ =
      flushed_update_contexts.size() + doc_limits::min();

    break;
  }

  if (!ctx->writer_) {
    IRS_ASSERT(ctx->uncomitted_doc_id_begin_ - doc_limits::min() ==
               flushed_update_contexts.size());
    ctx->buffered_docs_.store(flushed_update_contexts.size());

    return;  // nothing to reset
  }

  auto& writer = *(ctx->writer_);
  auto writer_docs = writer.initialized() ? writer.docs_cached() : 0;

  IRS_ASSERT(std::numeric_limits<doc_id_t>::max() >= writer.docs_cached());
  // update_contexts located inside th writer
  IRS_ASSERT(ctx->uncomitted_doc_id_begin_ - doc_limits::min() >=
             flushed_update_contexts.size());
  IRS_ASSERT(ctx->uncomitted_doc_id_begin_ - doc_limits::min() <=
             flushed_update_contexts.size() + writer_docs);
  ctx->buffered_docs_.store(flushed_update_contexts.size() + writer_docs);

  // rollback document insertions
  // cannot segment_writer::reset(...) since documents_context::reset() noexcept
  for (auto doc_id =
              ctx->uncomitted_doc_id_begin_ - flushed_update_contexts.size(),
            doc_id_end = writer_docs + doc_limits::min();
       doc_id < doc_id_end; ++doc_id) {
    IRS_ASSERT(doc_id <= std::numeric_limits<doc_id_t>::max());
    writer.remove(doc_id_t(doc_id));
  }
}

bool index_writer::FlushRequired(const segment_writer& segment) const noexcept {
  const auto& limits = segment_limits_;
  const auto docs_max = limits.segment_docs_max.load();
  const auto memory_max = limits.segment_memory_max.load();

  const auto docs = segment.docs_cached();
  const auto memory = segment.memory_active();

  return (docs_max != 0 && docs_max > docs) ||         // too many docs
         doc_limits::eof(docs)                         // segment is full
         || (memory_max != 0 && memory_max > memory);  // too much memory
}

index_writer::flush_context_ptr index_writer::Transaction::UpdateSegment(
  bool disable_flush) {
  auto ctx = writer_.get_flush_context();

  // refresh segment if required (guarded by flush_context::flush_mutex_)

  while (!segment_.ctx()) {  // no segment (lazy initialized)
    segment_ = writer_.get_segment_context(*ctx);
#ifdef IRESEARCH_DEBUG
    segment_use_count_ = segment_.ctx().use_count();
#endif

    // must unlock/relock flush_context before retrying to get a new segment so
    // as to avoid a deadlock due to a read-write-read situation for
    // flush_context::flush_mutex_ with threads trying to lock
    // flush_context::flush_mutex_ to return their segment_context
    if (!segment_.ctx()) {
      ctx.reset();  // reset before reacquiring
      ctx = writer_.get_flush_context();
    }
  }

  IRS_ASSERT(segment_.ctx());
  IRS_ASSERT(segment_.ctx()->writer_);
  auto& segment = *(segment_.ctx());
  auto& writer = *segment.writer_;

  if (IRS_UNLIKELY(!writer.initialized())) {
    segment.prepare();
    assert(segment.writer_->initialized());
  } else if (!disable_flush && writer_.FlushRequired(writer)) {
    // force a flush of a full segment
    IR_FRMT_TRACE(
      "Flushing segment '%s', docs=" IR_SIZE_T_SPECIFIER
      ", memory=" IR_SIZE_T_SPECIFIER ", docs limit=" IR_SIZE_T_SPECIFIER
      ", memory limit=" IR_SIZE_T_SPECIFIER "",
      writer.name().c_str(), writer.docs_cached(), writer.memory_active(),
      writer_.segment_limits_.segment_docs_max.load(),
      writer_.segment_limits_.segment_memory_max.load());

    try {
      std::unique_lock segment_flush_lock{segment.flush_mutex_};
      segment.flush();
    } catch (...) {
      IR_FRMT_ERROR(
        "while flushing segment '%s', error: failed to flush segment",
        segment.writer_meta_.meta.name.c_str());

      segment.reset(true);

      throw;
    }
  }

  return ctx;
}

void index_writer::flush_context::emplace(active_segment_context&& segment,
                                          uint64_t generation_base) {
  if (!segment.ctx_) {
    return;  // nothing to do
  }

  auto& flush_ctx = segment.flush_ctx_;

  // failure may indicate a dangling 'document' instance
  IRS_ASSERT(
    // +1 for 'active_segment_context::ctx_'
    (!flush_ctx && segment.ctx_.use_count() == 1) ||
    // +1 for 'active_segment_context::ctx_' (flush_context switching made a
    // full-circle)
    (this == flush_ctx && segment.ctx_->dirty_.load() &&
     segment.ctx_.use_count() == 1) ||
    // +1 for 'active_segment_context::ctx_',
    // +1 for 'pending_segment_context::segment_'
    (this == flush_ctx && !segment.ctx_->dirty_.load() &&
     segment.ctx_.use_count() == 2) ||
    // +1 for 'active_segment_context::ctx_',
    // +1 for 'pending_segment_context::segment_'
    (this != flush_ctx && flush_ctx && segment.ctx_.use_count() == 2) ||
    // +1 for 'active_segment_context::ctx_',
    // +0 for 'pending_segment_context::segment_' that was already cleared
    (this != flush_ctx && flush_ctx && segment.ctx_.use_count() == 1));

  freelist_t::node_type* freelist_node = nullptr;
  size_t modification_count{};
  auto& ctx = *(segment.ctx_);

  // prevent concurrent flush related modifications,
  // i.e. if segment is also owned by another flush_context
  std::unique_lock flush_lock{ctx.flush_mutex_, std::defer_lock};

  {
    // pending_segment_contexts_ may be asynchronously read
    std::lock_guard lock{mutex_};

    // update pending_segment_context
    // this segment_context has not yet been seen by this flush_context
    // or was marked dirty implies flush_context switching making a full-circle
    if (this != flush_ctx || ctx.dirty_) {
      freelist_node = &pending_segment_contexts_.emplace_back(
        segment.ctx_, pending_segment_contexts_.size());

      // mark segment as non-reusable if it was peviously registered with a
      // different flush_context NOTE: 'ctx.dirty_' implies flush_context
      // switching making a full-circle
      //       and this emplace(...) call being the first and only call for this
      //       segment (not given out again via free-list) so no 'dirty_' check
      if (flush_ctx && this != flush_ctx) {
        ctx.dirty_ = true;
        // 'segment.flush_ctx_' may be asynchronously flushed
        flush_lock.lock();
        // thread-safe because pending_segment_contexts_ is a deque
        IRS_ASSERT(
          flush_ctx
            ->pending_segment_contexts_[segment.pending_segment_context_offset_]
            .segment_ == segment.ctx_);
        // ^^^ FIXME TODO remove last line
        /* FIXME TODO uncomment once col_writer tail is written correctly (need
        to track tail in new segment
        // if this segment is still referenced by the previous flush_context
        then
        // store 'pending_segment_contexts_' and
        'uncomitted_modification_queries_'
        // in the previous flush_context because they will be modified lower
        down if (segment.ctx_.use_count() != 2) {
          IRS_ASSERT(segment.flush_ctx_->pending_segment_contexts_.size() >
        segment.pending_segment_context_offset_);
          IRS_ASSERT(segment.flush_ctx_->pending_segment_contexts_[segment.pending_segment_context_offset_].segment_
        == segment.ctx_); // thread-safe because pending_segment_contexts_ is a
        deque
          IRS_ASSERT(segment.flush_ctx_->pending_segment_contexts_[segment.pending_segment_context_offset_].segment_.use_count()
        == 3); // +1 for the reference in 'pending_segment_contexts_', +1 for
        the reference in other flush_context 'pending_segment_contexts_', +1 for
        the reference in 'active_segment_context'
          segment.flush_ctx_->pending_segment_contexts_[segment.pending_segment_context_offset_].doc_id_end_
        = ctx.uncomitted_doc_id_begin_;
          segment.flush_ctx_->pending_segment_contexts_[segment.pending_segment_context_offset_].modification_offset_end_
        = ctx.uncomitted_modification_queries_;
        }
        */
      }

      if (flush_ctx && this != flush_ctx) {
        pending_segment_contexts_.pop_back();
        freelist_node = nullptr;
      }  // FIXME TODO remove this condition once col_writer tail is writteng
         // correctly
    } else {
      // the segment is present in this flush_context
      // 'pending_segment_contexts_'
      IRS_ASSERT(pending_segment_contexts_.size() >
                 segment.pending_segment_context_offset_);
      IRS_ASSERT(
        pending_segment_contexts_[segment.pending_segment_context_offset_]
          .segment_ == segment.ctx_);
      // +1 for the reference in 'pending_segment_contexts_',
      // +1 for the reference in 'active_segment_context'
      IRS_ASSERT(
        pending_segment_contexts_[segment.pending_segment_context_offset_]
          .segment_.use_count() == 2);
      freelist_node =
        &(pending_segment_contexts_[segment.pending_segment_context_offset_]);
    }

    // NOTE: if the first uncommitted operation is a removal operation then it
    //       is fully valid for its 'committed' generation value to equal the
    //       generation of the last 'committed' insert operation since removals
    //       are applied to documents with generation <= removal
    IRS_ASSERT(ctx.uncomitted_modification_queries_ <=
               ctx.modification_queries_.size());
    modification_count =
      ctx.modification_queries_.size() - ctx.uncomitted_modification_queries_;
    if (!generation_base) {
      if (flush_ctx && this != flush_ctx) {
        generation_base = flush_ctx->generation_ += modification_count;
      } else {
        // FIXME remove this condition once col_writer tail is writteng
        // correctly

        // atomic increment to end of unique generation range
        generation_base = generation_ += modification_count;
      }
      generation_base -= modification_count;  // start of generation range
    }
  }

  // noexcept state update operations below here
  // no need for segment lock since flush_all() operates on values < '*_end_'
  // update generation of segment operation

  // update generations of modification_queries_
  std::for_each(
    std::begin(ctx.modification_queries_) +
      ctx.uncomitted_modification_queries_,
    std::end(ctx.modification_queries_),
    [generation_base, modification_count](modification_context& v) noexcept {
      // must be < modification_count since inserts come after
      // modification
      IRS_ASSERT(v.generation < modification_count);
      IRS_IGNORE(modification_count);

      // update to flush_context generation
      const_cast<size_t&>(v.generation) += generation_base;
    });

  auto update_generation = [uncomitted_doc_id_begin =
                              ctx.uncomitted_doc_id_begin_ - doc_limits::min(),
                            modification_count, generation_base](
                             std::span<segment_writer::update_context> ctxs) {
    // update generations of segment_context::flushed_update_contexts_
    for (size_t i = uncomitted_doc_id_begin, end = ctxs.size(); i < end; ++i) {
      // can == modification_count if inserts come  after
      // modification
      IRS_ASSERT(ctxs[i].generation <= modification_count);
      IRS_IGNORE(modification_count);
      // update to flush_context generation
      ctxs[i].generation += generation_base;
    }
  };

  auto& flushed_update_contexts = ctx.flushed_update_contexts_;

  // update generations of segment_context::flushed_update_contexts_
  update_generation(flushed_update_contexts);

  IRS_ASSERT(ctx.writer_);
  IRS_ASSERT(ctx.writer_->docs_cached() <= doc_limits::eof());
  auto& writer = *(ctx.writer_);
  const auto writer_docs = writer.initialized() ? writer.docs_cached() : 0;

  // update generations of segment_writer::doc_contexts
  if (writer_docs) {
    update_generation(writer.docs_context());
  }

  // reset counters for segment reuse

  ctx.uncomitted_generation_offset_ = 0;
  ctx.uncomitted_doc_id_begin_ =
    flushed_update_contexts.size() + writer_docs + doc_limits::min();
  ctx.uncomitted_modification_queries_ = ctx.modification_queries_.size();

  if (!freelist_node) {
    // FIXME remove this condition once col_writer tail is writteng correctly
    return;
  }

  // do not reuse segments that are present in another flush_context
  if (!ctx.dirty_) {
    IRS_ASSERT(freelist_node);
    // +1 for 'active_segment_context::ctx_', +1 for
    IRS_ASSERT(segment.ctx_.use_count() == 2);
    // 'pending_segment_context::segment_'
    auto& segments_active = *(segment.segments_active_);
    // release hold (delcare before aquisition since operator++() is noexcept)
    Finally segments_active_decrement = [&segments_active]() noexcept {
      segments_active.fetch_sub(1);
    };
    // increment counter to hold reservation while segment_context is being
    // released and added to the freelist
    segments_active.fetch_add(1);
    // reset before adding to freelist to garantee proper use_count() in
    // get_segment_context(...)
    segment = active_segment_context();
    // add segment_context to free-list
    pending_segment_contexts_freelist_.push(*freelist_node);
  }
}

bool index_writer::flush_context::AddToPending(
  active_segment_context& segment) {
  if (segment.flush_ctx_ != nullptr) {
    // re-used active_segment_context
    return false;
  }
  std::lock_guard lock{mutex_};
  auto const size_before = pending_segment_contexts_.size();
  pending_segment_contexts_.emplace_back(segment.ctx_, size_before);
  segment.flush_ctx_ = this;
  segment.pending_segment_context_offset_ = size_before;
  return true;
}

void index_writer::flush_context::reset() noexcept {
  // reset before returning to pool
  for (auto& entry : pending_segment_contexts_) {
    if (auto& segment = entry.segment_; segment.use_count() == 1) {
      // reset only if segment not tracked anywhere else
      segment->reset();
    }
  }

  // clear() before pending_segment_contexts_
  while (pending_segment_contexts_freelist_.pop())
    ;

  generation_.store(0);
  dir_->clear_refs();
  pending_segments_.clear();
  pending_segment_contexts_.clear();
  segment_mask_.clear();
}

index_writer::segment_context::segment_context(
  directory& dir, segment_meta_generator_t&& meta_generator,
  const column_info_provider_t& column_info,
  const feature_info_provider_t& feature_info, const Comparer* comparator)
  : active_count_(0),
    buffered_docs_(0),
    dirty_(false),
    dir_(dir),
    meta_generator_(std::move(meta_generator)),
    uncomitted_doc_id_begin_(doc_limits::min()),
    uncomitted_generation_offset_(0),
    uncomitted_modification_queries_(0),
    writer_(segment_writer::make(dir_, column_info, feature_info, comparator)) {
  IRS_ASSERT(meta_generator_);
}

uint64_t index_writer::segment_context::flush() {
  // must be already locked to prevent concurrent flush related modifications
  IRS_ASSERT(!flush_mutex_.try_lock());

  if (!writer_ || !writer_->initialized() || !writer_->docs_cached()) {
    return 0;  // skip flushing an empty writer
  }

  IRS_ASSERT(writer_->docs_cached() <= doc_limits::eof());

  auto& segment = flushed_.emplace_back(std::move(writer_meta_.meta));

  try {
    writer_->flush(segment);

    const std::span ctxs{writer_->docs_context()};
    flushed_update_contexts_.insert(flushed_update_contexts_.end(),
                                    ctxs.begin(), ctxs.end());
  } catch (...) {
    // failed to flush segment
    flushed_.pop_back();

    throw;
  }

  auto const tick = writer_->tick();
  writer_->reset();  // mark segment as already flushed
  return tick;
}

index_writer::segment_context::ptr index_writer::segment_context::make(
  directory& dir, segment_meta_generator_t&& meta_generator,
  const column_info_provider_t& column_info,
  const feature_info_provider_t& feature_info, const Comparer* comparator) {
  return std::make_unique<segment_context>(
    dir, std::move(meta_generator), column_info, feature_info, comparator);
}

segment_writer::update_context
index_writer::segment_context::make_update_context(const filter& filter) {
  // increment generation due to removal
  auto generation = ++uncomitted_generation_offset_;
  auto update_id = modification_queries_.size();

  // -1 for previous generation
  modification_queries_.emplace_back(filter, generation - 1, true);

  return {generation, update_id};
}

segment_writer::update_context
index_writer::segment_context::make_update_context(
  std::shared_ptr<const filter> filter) {
  IRS_ASSERT(filter);
  // increment generation due to removal
  auto generation = ++uncomitted_generation_offset_;
  auto update_id = modification_queries_.size();

  // -1 for previous generation
  modification_queries_.emplace_back(std::move(filter), generation - 1, true);

  return {generation, update_id};
}

segment_writer::update_context
index_writer::segment_context::make_update_context(filter::ptr&& filter) {
  IRS_ASSERT(filter);
  // increment generation due to removal
  auto generation = ++uncomitted_generation_offset_;
  auto update_id = modification_queries_.size();

  // -1 for previous generation
  modification_queries_.emplace_back(std::move(filter), generation - 1, true);

  return {generation, update_id};
}

void index_writer::segment_context::prepare() {
  IRS_ASSERT(writer_);

  if (!writer_->initialized()) {
    writer_meta_ = meta_generator_();
    writer_->reset(writer_meta_.meta);
  }
}

void index_writer::segment_context::remove(const filter& filter) {
  modification_queries_.emplace_back(filter, uncomitted_generation_offset_++,
                                     false);
}

void index_writer::segment_context::remove(
  std::shared_ptr<const filter> filter) {
  if (!filter) {
    return;  // skip empty filters
  }

  modification_queries_.emplace_back(std::move(filter),
                                     uncomitted_generation_offset_++, false);
}

void index_writer::segment_context::remove(filter::ptr&& filter) {
  if (!filter) {
    return;  // skip empty filters
  }

  modification_queries_.emplace_back(std::move(filter),
                                     uncomitted_generation_offset_++, false);
}

void index_writer::segment_context::reset(bool store_flushed) noexcept {
  active_count_.store(0);
  buffered_docs_.store(0);
  dirty_.store(false);
  // in some cases we need to store flushed segments for further commits
  if (!store_flushed) {
    flushed_.clear();
    flushed_update_contexts_.clear();
  }
  modification_queries_.clear();
  uncomitted_doc_id_begin_ = doc_limits::min();
  uncomitted_generation_offset_ = 0;
  uncomitted_modification_queries_ = 0;

  if (writer_->initialized()) {
    writer_->reset();  // try to reduce number of files flushed below
  }

  // release refs only after clearing writer state to ensure
  // 'writer_' does not hold any files
  dir_.clear_refs();
}

index_writer::index_writer(
  ConstructToken, index_lock::ptr&& lock,
  index_file_refs::ref_t&& lock_file_ref, directory& dir, format::ptr codec,
  size_t segment_pool_size, const segment_options& segment_limits,
  const Comparer* comparator, const column_info_provider_t& column_info,
  const feature_info_provider_t& feature_info,
  const payload_provider_t& meta_payload_provider, IndexMeta&& meta,
  std::shared_ptr<CommittedState>&& committed_state)
  : feature_info_{feature_info},
    column_info_{column_info},
    meta_payload_provider_{meta_payload_provider},
    comparator_{comparator},
    codec_{std::move(codec)},
    committed_state_{std::move(committed_state)},
    dir_{dir},
    // 2 because just swap them due to common commit lock
    flush_context_pool_{2},
    meta_{std::move(meta)},
    segment_limits_{segment_limits},
    segment_writer_pool_{segment_pool_size},
    segments_active_{0},
    writer_{codec_->get_index_meta_writer()},
    write_lock_{std::move(lock)},
    write_lock_file_ref_{std::move(lock_file_ref)} {
  IRS_ASSERT(column_info);   // ensured by 'make'
  IRS_ASSERT(feature_info);  // ensured by 'make'
  IRS_ASSERT(codec);
  flush_context_.store(flush_context_pool_.data());

  // setup round-robin chain
  for (size_t i = 0, count = flush_context_pool_.size() - 1; i < count; ++i) {
    auto* ctx = flush_context_pool_.data() + i;
    ctx->dir_ = std::make_unique<ref_tracking_directory>(dir);
    ctx->next_context_ = ctx + 1;
  }

  // setup round-robin chain
  auto& ctx = flush_context_pool_[flush_context_pool_.size() - 1];
  ctx.dir_ = std::make_unique<ref_tracking_directory>(dir);
  ctx.next_context_ = flush_context_pool_.data();
}

void index_writer::clear(uint64_t tick) {
  // cppcheck-suppress unreadVariable
  std::lock_guard commit_lock{commit_lock_};

  if (!pending_state_ && meta_.empty() &&
      index_gen_limits::valid(meta_.last_gen_)) {
    return;  // already empty
  }

  auto ctx = get_flush_context(false);
  // cppcheck-suppress unreadVariable
  // ensure there are no active struct update operations
  std::lock_guard ctx_lock{ctx->mutex_};

  auto pending_commit = std::make_shared<CommittedState>();

  auto& dir = *ctx->dir_;
  auto& pending_meta = *pending_commit->meta;

  // setup new meta
  pending_meta.update_generation(meta_);  // clone index metadata generation
  if (meta_payload_provider_) {
    if (pending_meta.payload_.has_value()) {
      pending_meta.payload_->clear();
    } else {
      pending_meta.payload_.emplace(bstring{});
    }
    IRS_ASSERT(pending_meta.payload_.has_value());
    if (!meta_payload_provider_(tick, *pending_meta.payload_)) {
      pending_meta.payload_.reset();
    }
  }

  // ensure counter() >= max(seg#)
  pending_meta.seg_counter_.store(meta_.counter());

  // rollback already opened transaction if any
  writer_->rollback();

  // write 1st phase of index_meta transaction
  if (!writer_->prepare(dir, pending_meta)) {
    throw illegal_state{"Failed to write index metadata."};
  }

  auto ref =
    directory_utils::reference(dir, writer_->filename(pending_meta), true);
  if (ref) {
    auto& pending_refs = pending_commit->refs;
    pending_refs.emplace_back(std::move(ref));
  }

  // 1st phase of the transaction successfully finished here
  // ensure new generation reflected in 'meta_'
  meta_.update_generation(pending_meta);
  pending_state_.ctx = std::move(ctx);  // retain flush context reference
  pending_state_.commit = std::move(pending_commit);

  finish();

  // all functions below are noexcept

  meta_.segments_.clear();  // noexcept op (clear after finish(), to match reset
                            // of pending_state_ inside finish(), allows
                            // recovery on clear() failure)

  // clear consolidating segments
  // cppcheck-suppress unreadVariable
  std::lock_guard lock{consolidation_lock_};
  consolidating_segments_.clear();
}

index_writer::ptr index_writer::make(
  directory& dir, format::ptr codec, OpenMode mode,
  const init_options& opts /*= init_options()*/) {
  std::vector<index_file_refs::ref_t> file_refs;
  index_lock::ptr lock;
  index_file_refs::ref_t lockfile_ref;

  if (opts.lock_repository) {
    // lock the directory
    lock = dir.make_lock(kWriteLockName);
    // will be created by try_lock
    lockfile_ref = directory_utils::reference(dir, kWriteLockName, true);

    if (!lock || !lock->try_lock()) {
      throw lock_obtain_failed(kWriteLockName);
    }
  }

  // read from directory or create index metadata
  IndexMeta meta;
  {
    auto reader = codec->get_index_meta_reader();
    std::string segments_file;
    const bool index_exists = reader->last_segments_file(dir, segments_file);

    if (OM_CREATE == mode ||
        ((OM_CREATE | OM_APPEND) == mode && !index_exists)) {
      // Try to read. It allows us to
      // create writer against an index that's
      // currently opened for searching

      try {
        // for OM_CREATE meta must be fully recreated, meta read only to get
        // last version
        if (index_exists) {
          reader->read(dir, meta, segments_file);
          meta.clear();
          // this meta is for a totally new index
          meta.last_gen_ = index_gen_limits::invalid();
        }
      } catch (const error_base&) {
        meta = IndexMeta();
      }
    } else if (!index_exists) {
      throw file_not_found();  // no segments file found
    } else {
      reader->read(dir, meta, segments_file);
      append_segments_refs(file_refs, dir, meta);
      auto ref = directory_utils::reference(dir, segments_file);
      if (ref) {
        file_refs.emplace_back(std::move(ref));
      }
    }
  }

  auto comitted_state = std::make_shared<CommittedState>(
    std::make_shared<IndexMeta>(meta), std::move(file_refs));

  auto writer = std::make_shared<index_writer>(
    ConstructToken{}, std::move(lock), std::move(lockfile_ref), dir,
    std::move(codec), opts.segment_pool_size, segment_options(opts),
    opts.comparator, opts.column_info ? opts.column_info : kDefaultColumnInfo,
    opts.features ? opts.features : kDefaultFeatureInfo,
    opts.meta_payload_provider, std::move(meta), std::move(comitted_state));

  // remove non-index files from directory
  directory_utils::remove_all_unreferenced(dir);

  return writer;
}

index_writer::~index_writer() noexcept {
  // failure may indicate a dangling 'document' instance
  IRS_ASSERT(!segments_active_.load());
  write_lock_.reset();  // reset write lock if any
  // reset pending state (if any) before destroying flush contexts
  pending_state_.reset();
  flush_context_.store(nullptr);
  // ensue all tracked segment_contexts are released before
  // segment_writer_pool_ is deallocated
  flush_context_pool_.clear();
}

uint64_t index_writer::buffered_docs() const {
  uint64_t docs_in_ram = 0;
  auto ctx = const_cast<index_writer*>(this)->get_flush_context();
  // 'pending_used_segment_contexts_'/'pending_free_segment_contexts_'
  // may be modified
  // cppcheck-suppress unreadVariable
  std::lock_guard lock{ctx->mutex_};

  for (auto& entry : ctx->pending_segment_contexts_) {
    // reading segment_writer::docs_count() is not thread safe
    // cppcheck-suppress useStlAlgorithm
    docs_in_ram += entry.segment_->buffered_docs_.load();
  }

  return docs_in_ram;
}

ConsolidationResult index_writer::consolidate(
  const ConsolidationPolicy& policy, format::ptr codec /*= nullptr*/,
  const merge_writer::flush_progress_t& progress /*= {}*/) {
  REGISTER_TIMER_DETAILED();
  if (!codec) {
    // use default codec if not specified
    codec = codec_;
  }

  Consolidation candidates;
  const auto run_id = reinterpret_cast<size_t>(&candidates);

  // hold a reference to the last committed state to prevent files from being
  // deleted by a cleaner during the upcoming consolidation
  // use atomic_load(...) since finish() may modify the pointer
  auto committed_state = std::atomic_load(&committed_state_);
  IRS_ASSERT(committed_state);
  if (IRS_UNLIKELY(!committed_state)) {
    return {0, ConsolidationError::FAIL};
  }

  auto committed_reader =
    *committed_state_->reader;  // FIXME(gnusi): need a copy?

  if (committed_reader->size() == 0) {
    // nothing to consolidate
    return {0, ConsolidationError::OK};
  }

  auto committed_meta = committed_state->meta;  // FIXME(gnusi): need a copy?
  IRS_ASSERT(committed_meta);
  if (IRS_UNLIKELY(!committed_meta)) {
    return {0, ConsolidationError::FAIL};
  }

  // collect a list of consolidation candidates
  {
    std::lock_guard lock{consolidation_lock_};
    // FIXME TODO remove from 'consolidating_segments_' any segments in
    // 'committed_state_' or 'pending_state_' to avoid data duplication
    policy(candidates, *committed_reader, consolidating_segments_);

    switch (candidates.size()) {
      case 0:
        // nothing to consolidate
        return {0, ConsolidationError::OK};
      case 1: {
        const auto* segment = *candidates.begin();

        if (!segment) {
          // invalid candidate
          return {0, ConsolidationError::FAIL};
        }

        if (!HasRemovals(segment->meta())) {
          // no deletes, nothing to consolidate
          return {0, ConsolidationError::OK};
        }
      }
    }

    // check that candidates are not involved in ongoing merges
    for (const auto* candidate : candidates) {
      // segment is already chosen for consolidation (or at least was
      // chosen), give up
      if (!candidate || consolidating_segments_.contains(candidate)) {
        return {0, ConsolidationError::FAIL};
      }
    }

    try {
      // register for consolidation
      consolidating_segments_.insert(candidates.begin(), candidates.end());
    } catch (...) {
      // rollback in case of insertion fails (finalizer below won`t handle
      // partial insert as concurrent consolidation is free to select same
      // candidate before finalizer reacquires the consolidation_lock)
      for (const auto* candidate : candidates) {
        consolidating_segments_.erase(candidate);
      }
      throw;
    }
  }

  // unregisterer for all registered candidates
  Finally unregister_segments = [&candidates, this]() noexcept {
    if (candidates.empty()) {
      return;
    }
    std::lock_guard lock{consolidation_lock_};
    for (const auto* candidate : candidates) {
      consolidating_segments_.erase(candidate);
    }
  };

  // sort candidates
  std::sort(candidates.begin(), candidates.end());

  // remove duplicates
  candidates.erase(std::unique(candidates.begin(), candidates.end()),
                   candidates.end());

  // validate candidates
  {
    size_t found = 0;

    for (const auto& segment : *committed_reader) {
      const auto it =
        std::find(std::begin(candidates), std::end(candidates), &segment);
      found +=
        static_cast<size_t>(it != std::end(candidates) && *it == &segment);
    }

    if (found != candidates.size()) {
      // not all candidates are valid
      IR_FRMT_DEBUG(
        "Failed to start consolidation for index generation "
        "'" IR_UINT64_T_SPECIFIER
        "', "
        "found only '" IR_SIZE_T_SPECIFIER "' out of '" IR_SIZE_T_SPECIFIER
        "' candidates",
        committed_meta->generation(), found, candidates.size());
      return {0, ConsolidationError::FAIL};
    }
  }

  IR_FRMT_TRACE("Starting consolidation id='" IR_SIZE_T_SPECIFIER "':\n%s",
                run_id, to_string(candidates).c_str());

  // do lock-free merge

  ConsolidationResult result{candidates.size(), ConsolidationError::FAIL};

  IndexSegment consolidation_segment;
  consolidation_segment.meta.codec = codec;  // should use new codec
  consolidation_segment.meta.version = 0;    // reset version for new segment
  // increment active meta, not fn arg
  consolidation_segment.meta.name = file_name(meta_.increment());

  ref_tracking_directory dir{dir_};  // track references for new segment
  merge_writer merger{dir, column_info_, feature_info_, comparator_};
  merger.reserve(result.size);

  // add consolidated segments to the merge_writer
  for (const auto* segment : candidates) {
    // already checked validity
    IRS_ASSERT(segment);
    // merge_writer holds a reference to reader
    merger.add(*segment);
  }

  // we do not persist segment meta since some removals may come later
  if (!merger.flush(consolidation_segment, progress)) {
    // nothing to consolidate or consolidation failure
    return result;
  }

  // commit merge
  {
    // ensure committed_state_ segments are not modified by concurrent
    // consolidate()/commit()
    std::unique_lock lock{commit_lock_};
    const auto current_committed_reader = committed_state_->reader;
    IRS_ASSERT(current_committed_reader);
    if (IRS_UNLIKELY(!current_committed_reader)) {
      return {0, ConsolidationError::FAIL};
    }
    const auto current_committed_meta = committed_state_->meta;
    IRS_ASSERT(current_committed_meta);
    if (IRS_UNLIKELY(!current_committed_meta)) {
      return {0, ConsolidationError::FAIL};
    }

    if (pending_state_) {
      // check that we haven't added to reader cache already absent readers
      // only if we have different index meta
      if (committed_reader != current_committed_reader) {
        auto begin = current_committed_reader->begin();
        auto end = current_committed_reader->end();

        // pointers are different so check by name
        for (const auto* candidate : candidates) {
          if (end == std::find_if(begin, end, [&candidate](const SubReader& s) {
                // FIXME(gnusi): compare pointers?
                return candidate->meta().name == s.meta().name;
              })) {
            // not all candidates are valid
            IR_FRMT_DEBUG(
              "Failed to start consolidation for index generation "
              "'" IR_UINT64_T_SPECIFIER
              "', not found segment %s in committed state",
              committed_meta->generation(), candidate->meta().name.c_str());
            return result;
          }
        }
      }

      result.error = ConsolidationError::PENDING;

      // transaction has been started, we're somewhere in the middle

      // can modify ctx->segment_mask_ without
      // lock since have commit_lock_
      auto ctx = get_flush_context();

      // register consolidation for the next transaction
      ctx->pending_segments_.emplace_back(
        std::move(consolidation_segment),
        std::numeric_limits<size_t>::max(),  // skip deletes, will accumulate
                                             // deletes from existing candidates
        dir.GetRefs(),                       // do not forget to track refs
        std::move(candidates),               // consolidation context candidates
        std::move(committed_meta),           // consolidation context meta
        std::move(merger));                  // merge context

      IR_FRMT_TRACE("Consolidation id='" IR_SIZE_T_SPECIFIER
                    "' successfully finished: pending",
                    run_id);
    } else if (committed_reader == current_committed_reader) {
      // before new transaction was started:
      // no commits happened in since consolidation was started

      auto ctx = get_flush_context();
      // lock due to context modification
      std::lock_guard ctx_lock{ctx->mutex_};

      // can release commit lock, we guarded against commit by
      // locked flush context
      lock.unlock();

      auto& segment_mask = ctx->segment_mask_;

      // persist segment meta
      index_utils::flush_index_segment(dir, consolidation_segment);
      segment_mask.reserve(segment_mask.size() + candidates.size());
      ctx->pending_segments_.emplace_back(
        std::move(consolidation_segment),
        0,              // deletes must be applied to the consolidated segment
        dir.GetRefs(),  // do not forget to track refs
        std::move(candidates),       // consolidation context candidates
        std::move(committed_meta));  // consolidation context meta

      // filter out merged segments for the next commit
      const auto& pending_segment = ctx->pending_segments_.back();
      const auto& consolidation_ctx = pending_segment.consolidation_ctx;
      const auto& consolidation_meta = pending_segment.segment.meta;

      // mask mapped candidates
      // segments from the to-be added new segment
      for (const auto* segment : consolidation_ctx.candidates) {
        segment_mask.emplace(segment);
      }

      IR_FRMT_TRACE(
        "Consolidation id='" IR_SIZE_T_SPECIFIER
        "' successfully finished: "
        "Name='%s', docs_count=" IR_UINT64_T_SPECIFIER
        ", "
        "live_docs_count=" IR_UINT64_T_SPECIFIER
        ", "
        "size=" IR_SIZE_T_SPECIFIER "",
        run_id, consolidation_meta.name.c_str(), consolidation_meta.docs_count,
        consolidation_meta.live_docs_count, consolidation_meta.size_in_bytes);
    } else {
      // before new transaction was started:
      // there was a commit(s) since consolidation was started,

      auto ctx = get_flush_context();
      // lock due to context modification
      std::lock_guard ctx_lock{ctx->mutex_};

      // can release commit lock, we guarded against commit by
      // locked flush context
      lock.unlock();

      auto& segment_mask = ctx->segment_mask_;

      CandidatesMapping mappings;
      const auto [has_removals, count] =
        map_candidates(mappings, candidates, *current_committed_reader);

      if (count != candidates.size()) {
        // at least one candidate is missing
        // can't finish consolidation
        IR_FRMT_DEBUG("Failed to finish consolidation id='" IR_SIZE_T_SPECIFIER
                      "' for segment '%s', "
                      "found only '" IR_SIZE_T_SPECIFIER
                      "' out of '" IR_SIZE_T_SPECIFIER "' candidates",
                      run_id, consolidation_segment.meta.name.c_str(), count,
                      candidates.size());

        return result;
      }

      // handle deletes if something changed
      if (has_removals) {
        document_mask docs_mask;

        if (!map_removals(mappings, merger, docs_mask)) {
          // consolidated segment has docs missing from
          // current_committed_meta->segments()
          IR_FRMT_DEBUG(
            "Failed to finish consolidation id='" IR_SIZE_T_SPECIFIER
            "' for segment '%s', "
            "due removed documents still present the consolidation candidates",
            run_id, consolidation_segment.meta.name.c_str());

          return result;
        }

        if (!docs_mask.empty()) {
          consolidation_segment.meta.live_docs_count -= docs_mask.size();
          write_document_mask(dir, consolidation_segment.meta, docs_mask,
                              false);
        }
      }

      // persist segment meta
      index_utils::flush_index_segment(dir, consolidation_segment);
      segment_mask.reserve(segment_mask.size() + candidates.size());
      ctx->pending_segments_.emplace_back(
        std::move(consolidation_segment),
        0,              // deletes must be applied to the consolidated segment
        dir.GetRefs(),  // do not forget to track refs
        std::move(candidates),       // consolidation context candidates
        std::move(committed_meta));  // consolidation context meta

      // filter out merged segments for the next commit
      const auto& pending_segment = ctx->pending_segments_.back();
      const auto& consolidation_ctx = pending_segment.consolidation_ctx;
      const auto& consolidation_meta = pending_segment.segment.meta;

      // mask mapped candidates
      // segments from the to-be added new segment
      for (const auto* segment : consolidation_ctx.candidates) {
        segment_mask.emplace(segment);
      }

      // mask mapped (matched) segments
      // segments from the already finished commit
      for (const auto& segment : current_committed_reader) {
        if (mappings.contains(segment.meta().name)) {
          segment_mask.emplace(&segment);
        }
      }

      IR_FRMT_TRACE(
        "Consolidation id='" IR_SIZE_T_SPECIFIER
        "' successfully finished:\nName='%s', "
        "docs_count=" IR_UINT64_T_SPECIFIER
        ", "
        "live_docs_count=" IR_UINT64_T_SPECIFIER
        ", "
        "size=" IR_SIZE_T_SPECIFIER "",
        run_id, consolidation_meta.name.c_str(), consolidation_meta.docs_count,
        consolidation_meta.live_docs_count, consolidation_meta.size_in_bytes);
    }
  }

  result.error = ConsolidationError::OK;
  return result;
}

bool index_writer::import(
  const IndexReader& reader, format::ptr codec /*= nullptr*/,
  const merge_writer::flush_progress_t& progress /*= {}*/) {
  if (!reader.live_docs_count()) {
    return true;  // skip empty readers since no documents to import
  }

  if (!codec) {
    codec = codec_;
  }

  ref_tracking_directory dir(dir_);  // track references

  IndexSegment segment;
  segment.meta.name = file_name(meta_.increment());
  segment.meta.codec = codec;

  merge_writer merger(dir, column_info_, feature_info_, comparator_);
  merger.reserve(reader.size());

  for (const auto& curr_segment : reader) {
    merger.add(curr_segment);
  }

  if (!merger.flush(segment, progress)) {
    return false;  // import failure (no files created, nothing to clean up)
  }

  index_utils::flush_index_segment(dir, segment);

  auto refs = dir.GetRefs();

  auto ctx = get_flush_context();
  // lock due to context modification
  // cppcheck-suppress unreadVariable
  std::lock_guard lock{ctx->mutex_};

  ctx->pending_segments_.emplace_back(
    std::move(segment),
    ctx->generation_.load(),  // current modification generation
    std::move(refs));         // do not forget to track refs

  return true;
}

index_writer::flush_context_ptr index_writer::get_flush_context(
  bool shared /*= true*/) {
  auto* ctx = flush_context_.load();  // get current ctx

  if (!shared) {
    for (;;) {
      // lock ctx exchange (write-lock)
      std::unique_lock lock{ctx->flush_mutex_};

      // acquire the current flush_context and its lock
      if (!flush_context_.compare_exchange_strong(ctx, ctx->next_context_)) {
        ctx = flush_context_.load();  // it might have changed
        continue;
      }

      lock.release();

      return {ctx, [](flush_context* ctx) noexcept -> void {
                std::unique_lock lock{ctx->flush_mutex_, std::adopt_lock};
                // reset context and make ready for reuse
                ctx->reset();
              }};
    }
  }

  for (;;) {
    // lock current ctx (read-lock)
    std::shared_lock lock{ctx->flush_mutex_, std::try_to_lock};

    if (!lock) {
      std::this_thread::yield();    // allow flushing thread to finish exchange
      ctx = flush_context_.load();  // it might have changed
      continue;
    }

    // at this point flush_context_ might have already changed
    // get active ctx, since initial_ctx is locked it will never be swapped with
    // current until unlocked
    auto* flush_ctx = flush_context_.load();

    // primary_flush_context_ has changed
    if (ctx != flush_ctx) {
      ctx = flush_ctx;
      continue;
    }

    lock.release();

    return {ctx, [](flush_context* ctx) noexcept -> void {
              std::shared_lock lock{ctx->flush_mutex_, std::adopt_lock};
            }};
  }
}

index_writer::active_segment_context index_writer::get_segment_context(
  flush_context& ctx) {
  // release reservation
  Finally segments_active_decrement = [this]() noexcept {
    segments_active_.fetch_sub(1);
  };
  // increment counter to aquire reservation, if another thread
  // tries to reserve last context then it'll be over limit
  const auto segments_active = segments_active_.fetch_add(1) + 1;

  // no free segment_context available and maximum number of segments reached
  // must return to caller so as to unlock/relock flush_context before retrying
  // to get a new segment so as to avoid a deadlock due to a read-write-read
  // situation for flush_context::flush_mutex_ with threads trying to lock
  // flush_context::flush_mutex_ to return their segment_context
  if (const auto segment_count_max = segment_limits_.segment_count_max.load();
      segment_count_max &&
      // '<' to account for +1 reservation
      segment_count_max < segments_active) {
    return {};
  }

  // only nodes of type 'pending_segment_context' are added to
  // 'pending_segment_contexts_freelist_'
  if (auto* freelist_node = ctx.pending_segment_contexts_freelist_.pop();
      freelist_node) {
    const auto& segment =
      static_cast<flush_context::pending_segment_context*>(freelist_node)
        ->segment_;

    // +1 for the reference in 'pending_segment_contexts_'
    IRS_ASSERT(segment.use_count() == 1);
    IRS_ASSERT(!segment->dirty_);
    return {segment, segments_active_, &ctx, freelist_node->value};
  }

  // should allocate a new segment_context from the pool
  std::shared_ptr<segment_context> segment_ctx{segment_writer_pool_.emplace(
    dir_,
    [this]() {
      return SegmentMeta{file_name(meta_.increment()), codec_};
    },
    column_info_, feature_info_, comparator_)};

  // recreate writer if it reserved more memory than allowed by current limits
  if (auto segment_memory_max = segment_limits_.segment_memory_max.load();
      segment_memory_max &&
      segment_memory_max < segment_ctx->writer_->memory_reserved()) {
    segment_ctx->writer_ = segment_writer::make(segment_ctx->dir_, column_info_,
                                                feature_info_, comparator_);
  }

  return {segment_ctx, segments_active_};
}

std::pair<std::vector<std::unique_lock<std::mutex>>, uint64_t>
index_writer::flush_pending(flush_context& ctx,
                            std::unique_lock<std::mutex>& ctx_lock) {
  uint64_t max_tick = 0;
  std::vector<std::unique_lock<std::mutex>> segment_flush_locks;
  segment_flush_locks.reserve(ctx.pending_segment_contexts_.size());

  for (auto& entry : ctx.pending_segment_contexts_) {
    auto& segment = entry.segment_;

    // mark the 'segment_context' as dirty so that it will not be reused if this
    // 'flush_context' once again becomes the active context while the
    // 'segment_context' handle is still held by documents()
    segment->dirty_.store(true);

    // wait for the segment to no longer be active
    // i.e. wait for all ongoing document operations to finish (insert/replace)
    // the segment will not be given out again by the active 'flush_context'
    // because it was started by a different 'flush_context', i.e. by 'ctx'

    // FIXME remove this condition once col_writer tail is written correctly
    while (segment->active_count_.load() || segment.use_count() != 1) {
      // arbitrary sleep interval
      ctx.pending_segment_context_cond_.wait_for(ctx_lock, 50ms);
    }

    // prevent concurrent modification of segment_context properties during
    // flush_context::emplace(...)
    // FIXME flush_all() blocks flush_context::emplace(...) and
    // insert()/remove()/replace()
    segment_flush_locks.emplace_back(segment->flush_mutex_);

    // force a flush of the underlying segment_writer
    max_tick = std::max(segment->flush(), max_tick);

    // may be std::numeric_limits<size_t>::max() if SegmentMeta only in this
    // flush_context
    entry.doc_id_end_ =
      std::min(segment->uncomitted_doc_id_begin_, entry.doc_id_end_);
    entry.modification_offset_end_ =
      std::min(segment->uncomitted_modification_queries_,
               entry.modification_offset_end_);
  }

  return {std::move(segment_flush_locks), max_tick};
}

using CompositeReaderView = CompositeReaderImpl<std::span<SegmentReader>>;

index_writer::pending_context_t index_writer::flush_all(
  progress_report_callback const& progress_callback) {
  REGISTER_TIMER_DETAILED();

  auto const& progress =
    (progress_callback != nullptr ? progress_callback : kNoProgress);

  bool modified = !index_gen_limits::valid(meta_.last_gen_);
  sync_context to_sync;

  auto pending_meta = std::make_unique<IndexMeta>();
  auto& segments = pending_meta->segments_;

  auto ctx = get_flush_context(false);
  auto& dir = *(ctx->dir_);
  // ensure there are no active struct update operations
  std::unique_lock lock{ctx->mutex_};

  // register consolidating segments cleanup.
  // we need raw ptr as ctx may be moved
  Finally unregister_segments = [ctx_raw = ctx.get(), this]() noexcept {
    // FIXME make me noexcept as I'm begin called from within ~finally()
    IRS_ASSERT(ctx_raw);
    if (ctx_raw->pending_segments_.empty()) {
      return;
    }
    std::lock_guard lock{consolidation_lock_};

    for (auto& pending_segment : ctx_raw->pending_segments_) {
      auto& candidates = pending_segment.consolidation_ctx.candidates;
      for (const auto* candidate : candidates) {
        consolidating_segments_.erase(candidate);
      }
    }
  };

  // Stage 0
  // wait for any outstanding segments to settle to ensure that any rollbacks
  // are properly tracked in 'modification_queries_'

  const auto [segment_flush_locks, max_tick] = flush_pending(*ctx, lock);

  // Stage 1
  // update document_mask for existing (i.e. sealed) segments

  auto& segment_mask = ctx->segment_mask_;

  // only used for progress reporting
  size_t current_segment_index = 0;

  // FIXME(gnusi): check committed state
  auto committed_reader = committed_state_->reader;

  std::vector<std::string> files_to_sync;

  std::vector<SegmentReader> readers;
  readers.reserve(committed_reader->size());

  for (auto& existing_segment : committed_reader) {
    // report progress
    progress("Stage 1: Apply removals to the existing segments",
             current_segment_index, meta_.size());
    ++current_segment_index;

    // skip already masked segments
    if (ctx->segment_mask_.contains(&existing_segment)) {
      continue;
    }

    const auto segment_id = segments.size();
    auto& segment = segments.emplace_back(existing_segment);
    auto& reader = readers.emplace_back(existing_segment);
    auto mask_modified = false;

    // mask documents matching filters from segment_contexts (i.e. from new
    // operations)
    for (auto& modifications : ctx->pending_segment_contexts_) {
      // modification_queries_ range
      // [flush_segment_context::modification_offset_begin_,
      // segment_context::uncomitted_modification_queries_)
      const auto begin = modifications.modification_offset_begin_;
      const auto end = modifications.modification_offset_end_;

      IRS_ASSERT(begin <= end);
      IRS_ASSERT(end <= modifications.segment_->modification_queries_.size());
      const std::span modification_queries{
        modifications.segment_->modification_queries_.data() + begin,
        end - begin};

      mask_modified |=
        add_document_mask_modified_records(modification_queries, reader);
    }

    // write docs_mask if masks added, if all docs are masked then mask segment
    if (mask_modified) {
      // mask empty segments
      if (!reader->meta().live_docs_count) {
        segment_mask.emplace(&existing_segment);
        // remove empty segment
        segments.pop_back();
        readers.pop_back();
        // removal of one of the existing segments
        modified = true;
        continue;
      }

      // FIXME(gnusi): modify doc_mask in-place
      IRS_ASSERT(existing_segment.docs_mask());
      auto& docs_mask =
        const_cast<document_mask&>(*existing_segment.docs_mask());

      to_sync.register_partial_sync(
        segment_id, write_document_mask(dir, segment.meta, docs_mask));
      index_utils::flush_index_segment(dir, segment);  // write with new mask
    }
  }

  // Stage 2
  // add pending complete segments registered by import or consolidation

  // number of candidates that have been registered for
  // pending consolidation
  size_t current_pending_segments_index = 0;
  size_t pending_candidates_count = 0;

  const CompositeReaderView current_reader{readers, 0, 0};
  for (auto& pending_segment : ctx->pending_segments_) {
    auto& pending_meta = pending_segment.segment.meta;

    SegmentReader pending_reader;

    auto open_pending_reader = [&]() -> SegmentReader& {
      // FIXME(gnusi): reader options? warmup?
      pending_reader = SegmentReader::open(dir, pending_meta, {});

      if (!pending_reader) {
        throw index_error{
          absl::StrCat("While adding document mask modified records "
                       "to flush_segment_context of "
                       "segment '",
                       pending_meta.name, "', error: failed to open segment")};
      }

      return pending_reader;
    };

    // report progress
    progress("Stage 2: Handling consolidated/imported segments",
             current_pending_segments_index, ctx->pending_segments_.size());
    ++current_pending_segments_index;

    bool docs_mask_modified = false;

    // pending consolidation
    const ConsolidationView candidates{
      pending_segment.consolidation_ctx.candidates};
    bool pending_consolidation = pending_segment.consolidation_ctx.merger;
    if (pending_consolidation) {
      // pending consolidation request
      CandidatesMapping mappings;
      const auto [has_removals, count] =
        map_candidates(mappings, candidates, current_reader);

      if (count != candidates.size()) {
        // at least one candidate is missing
        // in pending meta can't finish consolidation
        IR_FRMT_DEBUG(
          "Failed to finish merge for segment '%s', found only "
          "'" IR_SIZE_T_SPECIFIER "' out of '" IR_SIZE_T_SPECIFIER
          "' candidates",
          pending_meta.name.c_str(), count, candidates.size());

        continue;  // skip this particular consolidation
      }

      // mask mapped candidates
      // segments from the to-be added new segment
      for (auto& mapping : mappings) {
        ctx->segment_mask_.emplace(*(mapping.second.second.first));
      }

      // mask mapped (matched) segments
      // segments from the currently ongoing commit
      for (auto& segment : readers) {
        if (mappings.contains(segment.meta().name)) {
          ctx->segment_mask_.emplace(&segment);
        }
      }

      auto& reader = open_pending_reader();

      // have some changes, apply deletes
      if (has_removals) {
        auto& docs_mask = const_cast<document_mask&>(*reader.docs_mask());

        // FIXME(gnusi): reopen committed_reader because of removals??
        const auto success = map_removals(
          mappings, pending_segment.consolidation_ctx.merger, docs_mask);

        if (!success) {
          // consolidated segment has docs missing from 'segments'
          IR_FRMT_WARN(
            "Failed to finish merge for segment '%s', due removed documents "
            "still present the consolidation candidates",
            pending_meta.name.c_str());

          continue;  // skip this particular consolidation
        }

        // we're done with removals for pending consolidation
        // they have been already applied to candidates above
        // and successfully remapped to consolidated segment
        pending_meta.live_docs_count -= docs_mask.size();
        docs_mask_modified |= true;
      }

      // we've seen at least 1 successfully applied
      // pending consolidation request
      pending_candidates_count += candidates.size();
    } else {
      // during consolidation doc_mask could be already populated even for just
      // merged segment
      open_pending_reader();

      // pending already imported/consolidated segment, apply deletes
      // mask documents matching filters from segment_contexts (i.e. from new
      // operations)
      for (auto& modifications : ctx->pending_segment_contexts_) {
        // modification_queries_ range
        // [flush_segment_context::modification_offset_begin_,
        // segment_context::uncomitted_modification_queries_)
        auto modifications_begin = modifications.modification_offset_begin_;
        auto modifications_end = modifications.modification_offset_end_;

        IRS_ASSERT(modifications_begin <= modifications_end);
        IRS_ASSERT(modifications_end <=
                   modifications.segment_->modification_queries_.size());
        const std::span modification_queries{
          modifications.segment_->modification_queries_.data() +
            modifications_begin,
          modifications_end - modifications_begin};

        docs_mask_modified |= add_document_mask_modified_records(
          modification_queries, *pending_reader, pending_segment.generation);
      }
    }

    // skip empty segments
    if (!pending_reader->live_docs_count()) {
      modified = true;
      continue;
    }

    // write non-empty document mask
    if (auto* mask = docs_mask_modified ? pending_reader.docs_mask() : nullptr;
        mask && !mask->empty()) {
      write_document_mask(dir, pending_meta, *mask, !pending_consolidation);
      pending_consolidation = true;  // force write new segment meta
    }

    // persist segment meta
    if (pending_consolidation) {
      index_utils::flush_index_segment(dir, pending_segment.segment);
    }

    // register full segment sync
    to_sync.register_full_sync(segments.size());
    segments.emplace_back(std::move(pending_segment.segment));
    readers.emplace_back(std::move(pending_reader));
  }

  if (pending_candidates_count) {
    // for pending consolidation we need to filter out
    // consolidation candidates after applying them
    std::vector<IndexSegment> tmp;
    decltype(sync_context::segments) tmp_sync;

    tmp.reserve(segments.size() - pending_candidates_count);
    tmp_sync.reserve(to_sync.segments.size());

    auto begin = to_sync.segments.begin();
    auto end = to_sync.segments.end();

    for (size_t i = 0, size = readers.size(); i < size; ++i) {
      auto& segment = readers[i];

      // valid segment
      const bool valid = !ctx->segment_mask_.contains(&segment);

      if (begin != end && i == begin->first) {
        if (valid) {
          tmp_sync.emplace_back(tmp.size(), begin->second);
        }

        ++begin;
      }

      if (valid) {
        tmp.emplace_back(std::move(segments[i]));
      }
    }

    segments = std::move(tmp);
    to_sync.segments = std::move(tmp_sync);
  }

  // Stage 3
  // create new segments

  {
    // count total number of segments once
    size_t total_pending_segment_context_segments = 0;
    for (const auto& pending_segment_context : ctx->pending_segment_contexts_) {
      if (const auto& segment = pending_segment_context.segment_; segment) {
        total_pending_segment_context_segments += segment->flushed_.size();
      }
    }

    std::vector<FlushSegmentContext> segment_ctxs;
    size_t current_pending_segment_context_segments = 0;

    // proces all segments that have been seen by the current flush_context
    for (auto& pending_segment_context : ctx->pending_segment_contexts_) {
      const auto& segment = pending_segment_context.segment_;

      if (!segment) {
        continue;  // skip empty segments
      }

      size_t flushed_docs_count = 0;
      // was updated after flush
      auto flushed_doc_id_end = pending_segment_context.doc_id_end_;
      IRS_ASSERT(pending_segment_context.doc_id_begin_ <= flushed_doc_id_end);
      IRS_ASSERT(flushed_doc_id_end - doc_limits::min() <=
                 segment->flushed_update_contexts_.size());

      // process individually each flushed SegmentMeta from the segment_context
      for (auto& flushed : segment->flushed_) {
        // report progress
        progress("Stage 3: Creating new segments",
                 current_pending_segment_context_segments,
                 total_pending_segment_context_segments);
        ++current_pending_segment_context_segments;

        const auto flushed_docs_start = flushed_docs_count;

        // sum of all previous SegmentMeta::docs_count including this meta
        flushed_docs_count += flushed.meta.docs_count;

        if (!flushed.meta.live_docs_count /* empty SegmentMeta */
            // SegmentMeta fully before the start of this flush_context
            || flushed_doc_id_end - doc_limits::min() <= flushed_docs_start
            // SegmentMeta fully after the start of this flush_context
            || pending_segment_context.doc_id_begin_ - doc_limits::min() >=
                 flushed_docs_count) {
          continue;
        }

        // 0-based
        auto update_contexts_begin =
          std::max(pending_segment_context.doc_id_begin_ - doc_limits::min(),
                   flushed_docs_start);
        // 0-based
        auto update_contexts_end =
          std::min(flushed_doc_id_end - doc_limits::min(), flushed_docs_count);
        IRS_ASSERT(update_contexts_begin <= update_contexts_end);
        // beginning doc_id in this SegmentMeta
        auto valid_doc_id_begin =
          update_contexts_begin - flushed_docs_start + doc_limits::min();
        auto valid_doc_id_end =
          std::min(update_contexts_end - flushed_docs_start + doc_limits::min(),
                   static_cast<size_t>(flushed.docs_mask_tail_doc_id));
        IRS_ASSERT(valid_doc_id_begin <= valid_doc_id_end);

        if (valid_doc_id_begin == valid_doc_id_end) {
          continue;  // empty segment since head+tail == 'docs_count'
        }

        const std::span flush_update_contexts{
          segment->flushed_update_contexts_.data() + flushed_docs_start,
          flushed.meta.docs_count};

        // FIXME(gnusi): reader options? warmup?
        auto reader =
          SegmentReader::open(dir, flushed.meta, IndexReaderOptions{});

        if (!reader) {
          throw index_error{absl::StrCat(
            "while adding document mask modified records "
            "to flush_segment_context of "
            "segment '",
            flushed.meta.name, "', error: failed to open segment")};
        }

        auto& flush_segment_ctx = segment_ctxs.emplace_back(
          std::move(reader), flushed, valid_doc_id_begin, valid_doc_id_end,
          flush_update_contexts, segment->modification_queries_);

        // read document_mask as was originally flushed
        // could be due to truncated records due to rollback of uncommitted data
        auto& docs_mask =
          const_cast<document_mask&>(*flush_segment_ctx.reader_->docs_mask());
        auto& flushed_meta =
          const_cast<SegmentInfo&>(flush_segment_ctx.reader_->meta());

        // increment version for next run due to documents masked
        // from this run, similar to write_document_mask(...)
        ++flushed_meta.version;

        // add doc_ids before start of this flush_context to document_mask
        for (size_t doc_id = doc_limits::min(); doc_id < valid_doc_id_begin;
             ++doc_id) {
          IRS_ASSERT(std::numeric_limits<doc_id_t>::max() >= doc_id);
          if (docs_mask.emplace(static_cast<doc_id_t>(doc_id)).second) {
            // decrement count of live docs
            --flushed_meta.live_docs_count;
          }
        }

        // add tail doc_ids not part of this flush_context to documents_mask
        // (including truncated)
        for (size_t doc_id = valid_doc_id_end,
                    doc_id_end = flushed.meta.docs_count + doc_limits::min();
             doc_id < doc_id_end; ++doc_id) {
          IRS_ASSERT(std::numeric_limits<doc_id_t>::max() >= doc_id);
          if (docs_mask.emplace(static_cast<doc_id_t>(doc_id)).second) {
            // decrement count of live docs
            --flushed_meta.live_docs_count;
          }
        }

        // mask documents matching filters from all flushed segment_contexts
        // (i.e. from new operations)
        for (auto& modifications : ctx->pending_segment_contexts_) {
          auto modifications_begin = modifications.modification_offset_begin_;
          auto modifications_end = modifications.modification_offset_end_;

          IRS_ASSERT(modifications_begin <= modifications_end);
          IRS_ASSERT(modifications_end <=
                     modifications.segment_->modification_queries_.size());
          const std::span modification_queries(
            modifications.segment_->modification_queries_.data() +
              modifications_begin,
            modifications_end - modifications_begin);

          add_document_mask_modified_records(modification_queries,
                                             flush_segment_ctx, *reader);
        }
      }
    }

    // write docs_mask if !empty(), if all docs are masked then remove segment
    // altogether
    size_t current_segment_ctxs = 0;
    for (auto& segment_ctx : segment_ctxs) {
      // report progress - note: from the code, we are still a part of stage 3,
      // but we need to report something different here, i.e. "stage 4"
      progress("Stage 4: Applying removals for new segments",
               current_segment_ctxs, segment_ctxs.size());
      ++current_segment_ctxs;

      // if have a writer with potential update-replacement records then check
      // if they were seen
      add_document_mask_unused_updates(segment_ctx);

      auto& meta = const_cast<SegmentInfo&>(segment_ctx.reader_->meta());

      // after mismatched replaces here could be also empty segment
      // so masking is needed
      if (!meta.live_docs_count) {
        continue;
      }

      const auto& docs_mask = *segment_ctx.reader_->docs_mask();

      // write non-empty document mask
      if (!docs_mask.empty()) {
        write_document_mask(dir, meta, docs_mask);
        // write with new mask
        index_utils::flush_index_segment(dir, segment_ctx.segment_);
      }

      // register full segment sync
      to_sync.register_full_sync(segments.size());
      segments.emplace_back(std::move(segment_ctx.segment_));
    }
  }

  pending_meta->update_generation(meta_);  // clone index metadata generation

  modified |= !to_sync.empty();

  // only flush a new index version upon a new index or a metadata change
  if (!modified) {
    return {};
  }

  if (meta_payload_provider_) {
    if (pending_meta->payload_.has_value()) {
      pending_meta->payload_->clear();
    } else {
      pending_meta->payload_.emplace(bstring{});
    }
    IRS_ASSERT(pending_meta->payload_.has_value());
    if (!meta_payload_provider_(max_tick, *pending_meta->payload_)) {
      pending_meta->payload_.reset();
    }
  }

  // ensure counter() >= max(seg#)
  pending_meta->SetCounter(meta_.counter());

  pending_context_t pending_context;
  pending_context.ctx = std::move(ctx);  // retain flush context reference
  pending_context.meta = std::move(pending_meta);  // retain meta pending flush
  pending_context.to_sync = std::move(to_sync);

  return pending_context;
}

bool index_writer::start(progress_report_callback const& progress) {
  IRS_ASSERT(!commit_lock_.try_lock());  // already locked

  REGISTER_TIMER_DETAILED();

  if (pending_state_) {
    // begin has been already called
    // without corresponding call to commit
    return false;
  }

  auto to_commit = flush_all(progress);

  if (!to_commit) {
    // nothing to commit, no transaction started
    return false;
  }

  auto& dir = *to_commit.ctx->dir_;
  auto& pending_meta = *to_commit.meta;

  // write 1st phase of index_meta transaction
  if (!writer_->prepare(dir, pending_meta)) {
    throw illegal_state{"Failed to write index metadata."};
  }

  Finally update_generation = [this, &pending_meta]() noexcept {
    meta_.update_generation(pending_meta);
  };

  files_to_sync_.clear();
  auto sync = [this](std::string_view file) {
    files_to_sync_.emplace_back(file);
    return true;
  };

  try {
    // sync all pending files
    to_commit.to_sync.visit(sync, pending_meta);

    if (!dir.sync(files_to_sync_)) {
      throw io_error("Failed to sync files.");
    }

    // track all refs
    FileRefs pending_refs;
    append_segments_refs(pending_refs, dir, pending_meta);
    auto ref =
      directory_utils::reference(dir, writer_->filename(pending_meta), true);
    if (ref) {
      pending_refs.emplace_back(std::move(ref));
    }

    meta_.segments_ = to_commit.meta->segments_;  // create copy

    // 1st phase of the transaction successfully finished here,
    // set to_commit as active flush context containing pending meta
    pending_state_.commit = std::make_shared<CommittedState>(
      std::move(to_commit.meta), std::move(pending_refs));
  } catch (...) {
    writer_->rollback();  // rollback started transaction

    throw;
  }

  // only noexcept operations below

  // release cached readers
  pending_state_.ctx = std::move(to_commit.ctx);

  return true;
}

void index_writer::finish() {
  IRS_ASSERT(!commit_lock_.try_lock());  // already locked

  REGISTER_TIMER_DETAILED();

  if (!pending_state_) {
    return;
  }

  Finally reset_state = [this]() noexcept {
    // release reference to flush_context
    pending_state_.reset();
  };

  // lightweight 2nd phase of the transaction

  try {
    if (!writer_->commit()) {
      throw illegal_state{"Failed to commit index metadata."};
    }
#ifndef __APPLE__
    // atomic_store may throw
    static_assert(!noexcept(
      std::atomic_store(&committed_state_, std::move(pending_state_.commit))));
#endif
    std::atomic_store(&committed_state_, std::move(pending_state_.commit));
  } catch (...) {
    abort();  // rollback transaction

    throw;
  }

  // after this line transaction is successfull (only noexcept operations below)

  // update 'last_gen_' to last committed/valid generation
  meta_.last_gen_ = committed_state_->meta->gen_;
}

void index_writer::abort() {
  IRS_ASSERT(!commit_lock_.try_lock());  // already locked

  if (!pending_state_) {
    // there is no open transaction
    return;
  }

  // all functions below are noexcept

  // guarded by commit_lock_
  writer_->rollback();
  pending_state_.reset();

  // reset actual meta, note that here we don't change
  // segment counters since it can be changed from insert function
  meta_.reset(*(committed_state_->meta));
}

}  // namespace irs
