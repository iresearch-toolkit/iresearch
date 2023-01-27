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
#include "index/directory_reader_impl.hpp"
#include "index/file_names.hpp"
#include "index/index_meta.hpp"
#include "index/merge_writer.hpp"
#include "index/segment_reader_impl.hpp"
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
const ProgressReportCallback kNoProgress =
  [](std::string_view /*phase*/, size_t /*current*/, size_t /*total*/) {
    // intentionally do nothing
  };

const ColumnInfoProvider kDefaultColumnInfo = [](std::string_view) {
  // no compression, no encryption
  return ColumnInfo{irs::type<compression::none>::get(), {}, false};
};

const FeatureInfoProvider kDefaultFeatureInfo = [](irs::type_info::type_id) {
  // no compression, no encryption
  return std::pair{ColumnInfo{irs::type<compression::none>::get(), {}, false},
                   FeatureWriterFactory{}};
};

struct FlushSegmentContext {
  // starting doc_id to consider in 'segment.meta' (inclusive)
  const size_t doc_id_begin_;
  // ending doc_id to consider in 'segment.meta' (exclusive)
  const size_t doc_id_end_;
  // copy so that it can be moved into 'index_writer::pending_state_'
  IndexSegment segment_;
  // doc_ids masked in SegmentMeta
  document_mask docs_mask_;
  // modification contexts referenced by 'update_contexts_'
  std::span<const IndexWriter::ModificationContext> modification_contexts_;
  // update contexts for documents in SegmentMeta
  std::span<const segment_writer::update_context> update_contexts_;
  std::shared_ptr<const SegmentReaderImpl> reader_;

  FlushSegmentContext(
    std::shared_ptr<const SegmentReaderImpl>&& reader, IndexSegment&& segment,
    document_mask&& docs_mask, size_t doc_id_begin, size_t doc_id_end,
    std::span<const segment_writer::update_context> update_contexts,
    std::span<const IndexWriter::ModificationContext> modification_contexts)
    : doc_id_begin_{doc_id_begin},
      doc_id_end_{doc_id_end},
      segment_{std::move(segment)},
      docs_mask_{std::move(docs_mask)},
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
// Return if any new records were added (modification_queries_ modified).
void RemoveFromExistingSegment(
  std::vector<doc_id_t>& deleted_docs,
  std::span<IndexWriter::ModificationContext> modifications,
  const SubReader& reader) {
  auto& docs_mask = *reader.docs_mask();

  for (auto& modification : modifications) {
    if (IRS_UNLIKELY(!modification.filter)) {
      continue;  // skip invalid or uncommitted modification queries
    }

    auto prepared = modification.filter->prepare(reader);

    if (IRS_UNLIKELY(!prepared)) {
      continue;  // skip invalid prepared filters
    }

    auto itr = prepared->execute(reader);

    if (IRS_UNLIKELY(!itr)) {
      continue;  // skip invalid iterators
    }

    while (itr->next()) {
      const auto doc_id = itr->value();

      // if the indexed doc_id was already masked then it should be skipped
      if (docs_mask.contains(doc_id)) {
        continue;  // the current modification query does not match any records
      }

      deleted_docs.emplace_back(doc_id);
      modification.seen = true;
    }
  }
}

bool RemoveFromImportedSegment(
  std::span<IndexWriter::ModificationContext> modifications,
  const SubReader& reader, document_mask& docs_mask,
  size_t min_modification_generation) {
  IRS_ASSERT(!modifications.empty());
  bool modified = false;

  for (auto& modification : modifications) {
    // if the indexed doc_id was insert()ed after the request for modification
    if (modification.generation < min_modification_generation) {
      continue;  // the current modification query does not need to be applied
    }

    if (IRS_UNLIKELY(!modification.filter)) {
      continue;  // skip invalid or uncommitted modification queries
    }

    auto prepared = modification.filter->prepare(reader);

    if (IRS_UNLIKELY(!prepared)) {
      continue;  // skip invalid prepared filters
    }

    auto itr = prepared->execute(reader);

    if (IRS_UNLIKELY(!itr)) {
      continue;  // skip invalid iterators
    }

    while (itr->next()) {
      const auto doc_id = itr->value();

      // if the indexed doc_id was already masked then it should be skipped
      if (!docs_mask.insert(doc_id).second) {
        continue;  // the current modification query does not match any records
      }

      modified = true;
      modification.seen = true;
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
void RemoveFromNewSegment(
  std::span<IndexWriter::ModificationContext> modifications,
  FlushSegmentContext& ctx) {
  IRS_ASSERT(!modifications.empty());
  IRS_ASSERT(ctx.reader_);
  IRS_ASSERT(ctx.doc_id_begin_ <= ctx.doc_id_end_);
  IRS_ASSERT(ctx.doc_id_end_ <=
             ctx.update_contexts_.size() + doc_limits::min());
  IRS_ASSERT(doc_limits::valid(ctx.doc_id_begin_));

  auto& reader = *ctx.reader_;
  auto& docs_mask = ctx.docs_mask_;

  for (auto& modification : modifications) {
    if (IRS_UNLIKELY(!modification.filter)) {
      continue;  // Skip invalid or uncommitted modification queries
    }

    auto prepared = modification.filter->prepare(reader);

    if (IRS_UNLIKELY(!prepared)) {
      continue;  // Skip invalid prepared filters
    }

    auto itr = prepared->execute(reader);

    if (IRS_UNLIKELY(!itr)) {
      continue;  // Skip invalid iterators
    }

    while (itr->next()) {
      const auto doc_id = itr->value();

      if (doc_id < ctx.doc_id_begin_ || doc_id >= ctx.doc_id_end_) {
        continue;  // doc_id is not part of the current flush_context
      }

      // Valid because of asserts above
      const auto& doc_ctx = ctx.update_contexts_[doc_id - doc_limits::min()];

      // If the indexed doc_id was insert()ed after the request for modification
      // or the indexed doc_id was already masked then it should be skipped
      if (modification.generation < doc_ctx.generation ||
          !docs_mask.insert(doc_id).second) {
        continue;  // the current modification query does not match any records
      }

      // If an update modification and update-value record whose query was not
      // seen (i.e. replacement value whose filter did not match any documents)
      // for every update request a replacement 'update-value' is optimistically
      // inserted
      if (modification.update && doc_ctx.update_id != kNonUpdateRecord &&
          !ctx.modification_contexts_[doc_ctx.update_id].seen) {
        // The current modification matched a replacement document
        // which in turn did not match any records
        continue;
      }

      modification.seen = true;
    }
  }
}

// Mask documents created by updates which did not have any matches.
// Return if any new records were added (modification_contexts_ modified).
void MaskUnusedUpdatesInNewSegment(FlushSegmentContext& ctx) {
  if (ctx.modification_contexts_.empty()) {
    return;  // nothing new to add
  }
  IRS_ASSERT(doc_limits::valid(ctx.doc_id_begin_));
  IRS_ASSERT(ctx.doc_id_begin_ <= ctx.doc_id_end_);
  IRS_ASSERT(ctx.doc_id_end_ <=
             ctx.update_contexts_.size() + doc_limits::min());

  auto& docs_mask = ctx.docs_mask_;

  for (auto doc_id = ctx.doc_id_begin_; doc_id < ctx.doc_id_end_; ++doc_id) {
    // valid because of asserts above
    const auto& doc_ctx = ctx.update_contexts_[doc_id - doc_limits::min()];

    if (doc_ctx.update_id == kNonUpdateRecord) {
      continue;  // not an update operation
    }

    IRS_ASSERT(ctx.modification_contexts_.size() > doc_ctx.update_id);

    // If it's an update record placeholder who's query already match some
    // record
    if (!ctx.modification_contexts_[doc_ctx.update_id].seen) {
      docs_mask.insert(doc_id);
    }
  }
}

// Write the specified document mask and adjust version and
// live documents count of the specifed meta.
// Return index of the mask file withing segment file list
size_t WriteDocumentMask(directory& dir, SegmentMeta& meta,
                         const document_mask& docs_mask,
                         bool increment_version = true) {
  IRS_ASSERT(!docs_mask.empty());
  IRS_ASSERT(docs_mask.size() <= std::numeric_limits<uint32_t>::max());

  // Update live docs count
  IRS_ASSERT(docs_mask.size() < meta.docs_count);
  meta.live_docs_count =
    meta.docs_count - static_cast<doc_id_t>(docs_mask.size());

  auto mask_writer = meta.codec->get_document_mask_writer();

  auto it = meta.files.end();
  if (increment_version) {
    // Current filename
    it = std::find(meta.files.begin(), meta.files.end(),
                   mask_writer->filename(meta));

    ++meta.version;  // Segment modified due to new document_mask

    // FIXME(gnusi): cosider removing this
    // a second time +1 to avoid overlap with version increment due to commit of
    // uncommitted segment tail which must mask committed segment head
    // NOTE0: +1 extra is enough since a segment can reside in at most 2
    //        flush_contexts, there fore no more than 1 tail
    // NOTE1: flush_all() Stage3 increments version by _only_ 1 to avoid overlap
    //        with here, i.e. segment tail version will always be odd due to the
    //        aforementioned and because there is at most 1 tail
    ++meta.version;
  }

  // FIXME(gnusi): Consider returning mask file name from `write` to avoding
  // calling `filename`.
  // Write docs mask file after version is incremented
  meta.byte_size += mask_writer->write(dir, meta, docs_mask);

  if (it != meta.files.end()) {
    // FIXME(gnusi): We can avoid calling `length` in case if size of
    // the previous mask file would be known.
    auto get_file_size = [&dir](std::string_view file) -> uint64_t {
      uint64_t size;
      if (!dir.length(size, file)) {
        throw io_error{
          absl::StrCat("Failed to get length of the file '", file, "'")};
      }
      return size;
    };

    meta.byte_size -= get_file_size(*it);

    // Replace existing mask file with the new one
    *it = mask_writer->filename(meta);
  } else {
    // Add mask file to the list of files
    meta.files.emplace_back(mask_writer->filename(meta));
    it = std::prev(meta.files.end());
  }

  IRS_ASSERT(meta.files.begin() <= it);
  return static_cast<size_t>(it - meta.files.begin());
}

struct CandidateMapping {
  const SubReader* new_segment{};
  struct Old {
    const SubReader* segment{};
    size_t index{};  // within merge_writer
  } old;
};

// mapping: name -> { new segment, old segment }
using CandidatesMapping =
  absl::flat_hash_map<std::string_view, CandidateMapping>;

struct MapCandidatesResult {
  // Number of mapped candidates.
  size_t count{0};
  bool has_removals{false};
};

// candidates_mapping output mapping
// candidates candidates for mapping
// segments map against a specified segments
MapCandidatesResult MapCandidates(CandidatesMapping& candidates_mapping,
                                  ConsolidationView candidates,
                                  const auto& index) {
  size_t num_candidates = 0;
  for (const auto* candidate : candidates) {
    candidates_mapping.emplace(
      std::piecewise_construct, std::forward_as_tuple(candidate->Meta().name),
      std::forward_as_tuple(
        CandidateMapping{.old = {candidate, num_candidates++}}));
  }

  size_t found = 0;
  bool has_removals = false;
  const auto candidate_not_found = candidates_mapping.end();

  for (const auto& segment : index) {
    const auto& meta = segment.Meta();
    const auto it = candidates_mapping.find(meta.name);

    if (candidate_not_found == it) {
      // not a candidate
      continue;
    }

    auto& mapping = it->second;
    const auto* new_segment = mapping.new_segment;

    if (new_segment && new_segment->Meta().version >= meta.version) {
      // mapping already has a newer segment version
      continue;
    }

    IRS_ASSERT(mapping.old.segment);
    if constexpr (std::is_same_v<SegmentReader,
                                 std::decay_t<decltype(segment)>>) {
      mapping.new_segment = segment.GetImpl().get();
    } else {
      mapping.new_segment = &segment;
    }

    // FIXME(gnusi): can't we just check pointers?
    IRS_ASSERT(mapping.old.segment);
    has_removals |= (meta.version != mapping.old.segment->Meta().version);

    if (++found == num_candidates) {
      break;
    }
  }

  return {found, has_removals};
}

bool MapRemovals(const CandidatesMapping& candidates_mapping,
                 const MergeWriter& merger, document_mask& docs_mask) {
  IRS_ASSERT(merger);

  for (auto& mapping : candidates_mapping) {
    const auto& segment_mapping = mapping.second;
    const auto* new_segment = segment_mapping.new_segment;
    IRS_ASSERT(new_segment);
    const auto& new_meta = new_segment->Meta();
    IRS_ASSERT(segment_mapping.old.segment);
    const auto& old_meta = segment_mapping.old.segment->Meta();

    if (new_meta.version != old_meta.version) {
      const auto& merge_ctx = merger[segment_mapping.old.index];
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

std::string ToString(ConsolidationView consolidation) {
  std::string str;

  size_t total_size = 0;
  size_t total_docs_count = 0;
  size_t total_live_docs_count = 0;

  for (const auto* segment : consolidation) {
    auto& meta = segment->Meta();

    absl::StrAppend(&str, "Name='", meta.name,
                    "', docs_count=", meta.docs_count,
                    ", live_docs_count=", meta.live_docs_count,
                    ", size=", meta.byte_size, "\n");

    total_docs_count += meta.docs_count;
    total_live_docs_count += meta.live_docs_count;
    total_size += meta.byte_size;
  }

  absl::StrAppend(&str, "Total: segments=", consolidation.size(),
                  ", docs_count=", total_docs_count,
                  ", live_docs_count=", total_live_docs_count,
                  " size=", total_size, "");

  return str;
}

bool IsInitialCommit(const DirectoryMeta& meta) noexcept {
  // Initial commit is always for required for empty directory
  return meta.filename.empty();
}

struct PartialSync {
  PartialSync(size_t segment_index, size_t file_index) noexcept
    : segment_index{segment_index}, file_index{file_index} {}

  size_t segment_index;  // Index of the segment within index meta
  size_t file_index;     // Index of the file in segment file list
};

std::vector<std::string_view> GetFilesToSync(
  std::span<const IndexSegment> segments,
  std::span<const PartialSync> partial_sync, size_t partial_sync_threshold) {
  // FIXME(gnusi): make format dependent?
  static constexpr size_t kFilesPerSegment = 9;

  IRS_ASSERT(partial_sync_threshold <= segments.size());
  const size_t full_sync_count = segments.size() - partial_sync_threshold;

  std::vector<std::string_view> files_to_sync;
  // +1 for index meta
  files_to_sync.reserve(1 + partial_sync.size() * 2 +
                        full_sync_count * kFilesPerSegment);

  for (auto sync : partial_sync) {
    IRS_ASSERT(sync.segment_index < partial_sync_threshold);
    const auto& segment = segments[sync.segment_index];
    files_to_sync.emplace_back(segment.filename);
    files_to_sync.emplace_back(segment.meta.files[sync.file_index]);
  }

  std::for_each(segments.begin() + partial_sync_threshold, segments.end(),
                [&files_to_sync](const IndexSegment& segment) {
                  files_to_sync.emplace_back(segment.filename);
                  const auto& files = segment.meta.files;
                  IRS_ASSERT(files.size() <= kFilesPerSegment);
                  files_to_sync.insert(files_to_sync.end(), files.begin(),
                                       files.end());
                });

  return files_to_sync;
}

}  // namespace

using namespace std::chrono_literals;

IndexWriter::ActiveSegmentContext::ActiveSegmentContext(
  std::shared_ptr<SegmentContext> ctx, std::atomic_size_t& segments_active,
  FlushContext* flush_ctx, size_t pending_segment_context_offset) noexcept
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

IndexWriter::ActiveSegmentContext::~ActiveSegmentContext() {
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

IndexWriter::ActiveSegmentContext& IndexWriter::ActiveSegmentContext::operator=(
  ActiveSegmentContext&& other) noexcept {
  if (this != &other) {
    if (ctx_) {
      // track here since guaranteed to have 1 ref per active segment
      segments_active_->fetch_sub(1);
    }

    ctx_ = std::move(other.ctx_);
    flush_ctx_ = other.flush_ctx_;
    pending_segment_context_offset_ = other.pending_segment_context_offset_;
    segments_active_ = other.segments_active_;
  }

  return *this;
}

IndexWriter::Document::Document(FlushContext& ctx,
                                std::shared_ptr<SegmentContext> segment,
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
  segment_->buffered_docs_.store(writer_.docs_cached(),
                                 std::memory_order_relaxed);
}

IndexWriter::Document::~Document() noexcept {
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

void IndexWriter::Transaction::ForceFlush() {
  if (const auto& ctx = segment_.ctx(); !ctx) {
    return;  // nothing to do
  }

  if (writer_.GetFlushContext()->AddToPending(segment_)) {
#ifdef IRESEARCH_DEBUG
    ++segment_use_count_;
#endif
  }
}

bool IndexWriter::Transaction::Commit() noexcept {
  const auto& ctx = segment_.ctx();

  // failure may indicate a dangling 'document' instance
  // TODO(MBkkt) Disable while we cannot understand why it's failed
  // Maybe issue in reset() I see it in last chaos run
  // IRS_ASSERT(ctx.use_count() == segment_use_count_);

  if (!ctx) {
    return true;  // nothing to do
  }

  if (auto& writer = *ctx->writer_; writer.tick() < last_operation_tick_) {
    writer.tick(last_operation_tick_);
  }

  try {
    // FIXME move emplace into active_segment_context destructor commit segment
    const auto& flush_ctx = writer_.GetFlushContext();
    IRS_ASSERT(flush_ctx);
    flush_ctx->Emplace(std::move(segment_), first_operation_tick_);
    return true;
  } catch (...) {
    Reset();  // abort segment
    return false;
  }
}

void IndexWriter::Transaction::Reset() noexcept {
  last_operation_tick_ = 0;  // reset tick

  const auto& ctx = segment_.ctx();

  if (!ctx) {
    return;  // nothing to reset
  }

  // rollback modification queries
  std::for_each(std::begin(ctx->modification_queries_) +
                  ctx->uncomitted_modification_queries_,
                std::end(ctx->modification_queries_),
                [](ModificationContext& ctx) noexcept {
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
    ctx->buffered_docs_.store(flushed_update_contexts.size(),
                              std::memory_order_relaxed);

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
  ctx->buffered_docs_.store(flushed_update_contexts.size() + writer_docs,
                            std::memory_order_relaxed);

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

bool IndexWriter::FlushRequired(const segment_writer& segment) const noexcept {
  const auto& limits = segment_limits_;
  const auto docs_max = limits.segment_docs_max.load();
  const auto memory_max = limits.segment_memory_max.load();

  const auto docs = segment.docs_cached();
  const auto memory = segment.memory_active();

  return (docs_max != 0 && docs_max <= docs) ||        // too many docs
         (memory_max != 0 && memory_max <= memory) ||  // too much memory
         doc_limits::eof(docs);                        // segment is full
}

IndexWriter::FlushContextPtr IndexWriter::Transaction::UpdateSegment(
  bool disable_flush) {
  auto ctx = writer_.GetFlushContext();

  // refresh segment if required (guarded by flush_context::flush_mutex_)

  while (!segment_.ctx()) {  // no segment (lazy initialized)
    segment_ = writer_.GetSegmentContext(*ctx);
#ifdef IRESEARCH_DEBUG
    segment_use_count_ = segment_.ctx().use_count();
#endif

    // must unlock/relock flush_context before retrying to get a new segment so
    // as to avoid a deadlock due to a read-write-read situation for
    // flush_context::flush_mutex_ with threads trying to lock
    // flush_context::flush_mutex_ to return their segment_context
    if (!segment_.ctx()) {
      ctx.reset();  // reset before reacquiring
      ctx = writer_.GetFlushContext();
    }
  }

  IRS_ASSERT(segment_.ctx());
  IRS_ASSERT(segment_.ctx()->writer_);
  auto& segment = *(segment_.ctx());
  auto& writer = *segment.writer_;

  if (IRS_LIKELY(writer.initialized())) {
    if (disable_flush || !writer_.FlushRequired(writer)) {
      return ctx;
    }

    // Force flush of a full segment
    IR_FRMT_TRACE(
      "Flushing segment '%s', docs=" IR_SIZE_T_SPECIFIER
      ", memory=" IR_SIZE_T_SPECIFIER ", docs limit=" IR_SIZE_T_SPECIFIER
      ", memory limit=" IR_SIZE_T_SPECIFIER "",
      writer.name().c_str(), writer.docs_cached(), writer.memory_active(),
      writer_.segment_limits_.segment_docs_max.load(),
      writer_.segment_limits_.segment_memory_max.load());

    try {
      std::unique_lock segment_flush_lock{segment.flush_mutex_};
      segment.Flush();
    } catch (...) {
      IR_FRMT_ERROR(
        "while flushing segment '%s', error: failed to flush segment",
        segment.writer_meta_.meta.name.c_str());

      segment.Reset(true);

      throw;
    }
  }

  segment.Prepare();
  IRS_ASSERT(segment.writer_->initialized());

  return ctx;
}

void IndexWriter::FlushContext::Emplace(ActiveSegmentContext&& segment,
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

  Freelist::node_type* freelist_node = nullptr;
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
      // different flush_context
      // NOTE: 'ctx.dirty_' implies flush_context switching making a full-circle
      // and this emplace(...) call being the first and only call for this
      // segment (not given out again via free-list) so no 'dirty_' check
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
      }  // FIXME TODO remove this condition once col_writer tail is written
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
        // FIXME remove this condition once col_writer tail is written
        // correctly

        // atomic increment to end of unique generation range
        generation_base = generation_ += modification_count;
      }
      generation_base -= modification_count;  // start of generation range
    }
  }

  // noexcept state update operations below here
  // no need for segment lock since flush_all() operates on values < '*_end_'

  // Update generation of segment operation
  ctx.UpdateGeneration(generation_base);

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
    segment = ActiveSegmentContext{};
    // add segment_context to free-list
    pending_segment_contexts_freelist_.push(*freelist_node);
  }
}

bool IndexWriter::FlushContext::AddToPending(ActiveSegmentContext& segment) {
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

void IndexWriter::FlushContext::Reset() noexcept {
  // reset before returning to pool
  for (auto& entry : pending_segment_contexts_) {
    if (auto& segment = entry.segment_; segment.use_count() == 1) {
      // reset only if segment not tracked anywhere else
      segment->Reset();
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

IndexWriter::SegmentContext::SegmentContext(
  directory& dir, segment_meta_generator_t&& meta_generator,
  const ColumnInfoProvider& column_info,
  const FeatureInfoProvider& feature_info, const Comparer* comparator)
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

void IndexWriter::SegmentContext::UpdateGeneration(uint64_t base) noexcept {
  IRS_ASSERT(writer_);
  IRS_ASSERT(writer_->docs_cached() <= doc_limits::eof());

  // Update generations of modification queries
  std::for_each(
    std::begin(modification_queries_) + uncomitted_modification_queries_,
    std::end(modification_queries_), [&](ModificationContext& v) noexcept {
      // Must be < than 'count' since inserts come after modification
      IRS_ASSERT(v.generation < modification_queries_.size() -
                                  uncomitted_modification_queries_);

      // Update to flush_context generation
      const_cast<size_t&>(v.generation) += base;
    });

  // Update generations for insertions
  auto update_generation =
    [&](std::span<segment_writer::update_context> ctxs) noexcept {
      for (auto begin =
                  ctxs.begin() + uncomitted_doc_id_begin_ - doc_limits::min(),
                end = ctxs.end();
           begin < end; ++begin) {
        // Can be == to 'count' if inserts come after modification
        IRS_ASSERT(begin->generation <= modification_queries_.size() -
                                          uncomitted_modification_queries_);
        begin->generation += base;
      }
    };

  update_generation(flushed_update_contexts_);

  IRS_ASSERT(writer_);
  if (writer_->initialized() && writer_->docs_cached()) {
    // update generations of segment_writer::doc_contexts
    update_generation(writer_->docs_context());
  }

  // Reset counters for reuse
  uncomitted_generation_offset_ = 0;
  uncomitted_doc_id_begin_ = flushed_update_contexts_.size() +
                             writer_->docs_cached() + doc_limits::min();
  uncomitted_modification_queries_ = modification_queries_.size();
}

uint64_t IndexWriter::SegmentContext::Flush() {
  // Must be already locked to prevent concurrent flush related modifications
  IRS_ASSERT(!flush_mutex_.try_lock());

  if (!writer_ || !writer_->initialized() || writer_->docs_cached() == 0) {
    return 0;  // Skip flushing an empty writer
  }

  IRS_ASSERT(writer_->docs_cached() <= doc_limits::eof());

  // Ensure writer is reset
  Finally reset_writer = [&]() noexcept { writer_->reset(); };

  document_mask docs_mask;
  const std::span ctxs = writer_->flush(writer_meta_, docs_mask);

  if (ctxs.empty() || writer_meta_.meta.live_docs_count == 0) {
    return 0;  // Skip flushing an empty writer
  }

  IRS_ASSERT(ctxs.size() == writer_meta_.meta.docs_count);

  const auto docs_start =
    static_cast<doc_id_t>(flushed_update_contexts_.size());

  flushed_.emplace_back(std::move(writer_meta_), std::move(docs_mask),
                        docs_start);

  try {
    flushed_update_contexts_.insert(flushed_update_contexts_.end(),
                                    ctxs.begin(), ctxs.end());
  } catch (...) {
    // Failed to flush segment
    flushed_.pop_back();

    throw;
  }

  return writer_->tick();
}

IndexWriter::SegmentContext::ptr IndexWriter::SegmentContext::make(
  directory& dir, segment_meta_generator_t&& meta_generator,
  const ColumnInfoProvider& column_info,
  const FeatureInfoProvider& feature_info, const Comparer* comparator) {
  return std::make_unique<SegmentContext>(
    dir, std::move(meta_generator), column_info, feature_info, comparator);
}

segment_writer::update_context IndexWriter::SegmentContext::MakeUpdateContext(
  const filter& filter) {
  // increment generation due to removal
  auto generation = ++uncomitted_generation_offset_;
  auto update_id = modification_queries_.size();

  // -1 for previous generation
  modification_queries_.emplace_back(filter, generation - 1, true);

  return {generation, update_id};
}

segment_writer::update_context IndexWriter::SegmentContext::MakeUpdateContext(
  std::shared_ptr<const filter> filter) {
  IRS_ASSERT(filter);
  // increment generation due to removal
  auto generation = ++uncomitted_generation_offset_;
  auto update_id = modification_queries_.size();

  // -1 for previous generation
  modification_queries_.emplace_back(std::move(filter), generation - 1, true);

  return {generation, update_id};
}

segment_writer::update_context IndexWriter::SegmentContext::MakeUpdateContext(
  filter::ptr&& filter) {
  IRS_ASSERT(filter);
  // increment generation due to removal
  auto generation = ++uncomitted_generation_offset_;
  auto update_id = modification_queries_.size();

  // -1 for previous generation
  modification_queries_.emplace_back(std::move(filter), generation - 1, true);

  return {generation, update_id};
}

void IndexWriter::SegmentContext::Prepare() {
  IRS_ASSERT(writer_);

  if (!writer_->initialized()) {
    writer_meta_.filename.clear();
    writer_meta_.meta = meta_generator_();
    writer_->reset(writer_meta_.meta);
  }
}

void IndexWriter::SegmentContext::Remove(const filter& filter) {
  modification_queries_.emplace_back(filter, uncomitted_generation_offset_++,
                                     false);
}

void IndexWriter::SegmentContext::Remove(std::shared_ptr<const filter> filter) {
  if (!filter) {
    return;  // skip empty filters
  }

  modification_queries_.emplace_back(std::move(filter),
                                     uncomitted_generation_offset_++, false);
}

void IndexWriter::SegmentContext::Remove(filter::ptr&& filter) {
  if (!filter) {
    return;  // skip empty filters
  }

  modification_queries_.emplace_back(std::move(filter),
                                     uncomitted_generation_offset_++, false);
}

void IndexWriter::SegmentContext::Reset(bool store_flushed) noexcept {
  active_count_.store(0);
  buffered_docs_.store(0, std::memory_order_relaxed);
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

IndexWriter::IndexWriter(
  ConstructToken, index_lock::ptr&& lock,
  index_file_refs::ref_t&& lock_file_ref, directory& dir, format::ptr codec,
  size_t segment_pool_size, const SegmentOptions& segment_limits,
  const Comparer* comparator, const ColumnInfoProvider& column_info,
  const FeatureInfoProvider& feature_info,
  const PayloadProvider& meta_payload_provider,
  std::shared_ptr<const DirectoryReaderImpl>&& committed_reader)
  : feature_info_{feature_info},
    column_info_{column_info},
    meta_payload_provider_{meta_payload_provider},
    comparator_{comparator},
    codec_{std::move(codec)},
    dir_{dir},
    // 2 because just swap them due to common commit lock
    flush_context_pool_{2},
    committed_reader_{std::move(committed_reader)},
    segment_limits_{segment_limits},
    segment_writer_pool_{segment_pool_size},
    segments_active_{0},
    seg_counter_{committed_reader_->Meta().index_meta.seg_counter},
    last_gen_{committed_reader_->Meta().index_meta.gen},
    writer_{codec_->get_index_meta_writer()},
    write_lock_{std::move(lock)},
    write_lock_file_ref_{std::move(lock_file_ref)} {
  IRS_ASSERT(column_info);   // ensured by 'make'
  IRS_ASSERT(feature_info);  // ensured by 'make'
  IRS_ASSERT(codec_);
  flush_context_.store(flush_context_pool_.data());

  // setup round-robin chain
  auto* ctx = flush_context_pool_.data();
  for (auto* last = ctx + flush_context_pool_.size() - 1; ctx != last; ++ctx) {
    ctx->dir_ = std::make_unique<RefTrackingDirectory>(dir);
    ctx->next_context_ = ctx + 1;
  }
  ctx->dir_ = std::make_unique<RefTrackingDirectory>(dir);
  ctx->next_context_ = flush_context_pool_.data();
}

void IndexWriter::InitMeta(IndexMeta& meta, uint64_t tick) const {
  if (meta_payload_provider_) {
    IRS_ASSERT(!meta.payload.has_value());
    auto& payload = meta.payload.emplace(bstring{});
    if (IRS_UNLIKELY(!meta_payload_provider_(tick, payload))) {
      meta.payload.reset();
    }
  }
  meta.seg_counter = CurrentSegmentId();  // Ensure counter() >= max(seg#)
  meta.gen = last_gen_;                   // Clone index metadata generation
}

void IndexWriter::Clear(uint64_t tick) {
  // cppcheck-suppress unreadVariable
  std::lock_guard commit_lock{commit_lock_};

  IRS_ASSERT(committed_reader_);
  if (auto& committed_meta = committed_reader_->Meta();
      !pending_state_.Valid() && committed_meta.index_meta.segments.empty() &&
      !IsInitialCommit(committed_meta)) {
    return;  // Already empty
  }

  DirectoryMeta pending_commit;
  InitMeta(pending_commit.index_meta, tick);

  auto ctx = GetFlushContext(false);
  // Ensure there are no active struct update operations
  // cppcheck-suppress unreadVariable
  std::lock_guard ctx_lock{ctx->mutex_};

  Abort();  // Abort any already opened transaction
  StartImpl(std::move(ctx), std::move(pending_commit), {}, {});
  Finish();

  // Clear consolidating segments
  // cppcheck-suppress unreadVariable
  std::lock_guard lock{consolidation_lock_};
  consolidating_segments_.clear();
}

IndexWriter::ptr IndexWriter::Make(
  directory& dir, format::ptr codec, OpenMode mode,
  const IndexWriterOptions& opts /*= init_options()*/) {
  index_lock::ptr lock;
  index_file_refs::ref_t lock_ref;

  if (opts.lock_repository) {
    // lock the directory
    lock = dir.make_lock(kWriteLockName);
    // will be created by try_lock
    lock_ref = dir.attributes().refs().add(kWriteLockName);

    if (!lock || !lock->try_lock()) {
      throw lock_obtain_failed(kWriteLockName);
    }
  }

  // read from directory or create index metadata
  DirectoryMeta meta;

  {
    auto reader = codec->get_index_meta_reader();
    const bool index_exists = reader->last_segments_file(dir, meta.filename);

    if (OM_CREATE == mode ||
        ((OM_CREATE | OM_APPEND) == mode && !index_exists)) {
      // for OM_CREATE meta must be fully recreated, meta read only to get
      // last version
      if (index_exists) {
        // Try to read. It allows us to create writer against an index that's
        // currently opened for searching
        reader->read(dir, meta.index_meta, meta.filename);

        meta.filename.clear();  // Empty index meta -> new index
        auto& index_meta = meta.index_meta;
        index_meta.payload.reset();
        index_meta.segments.clear();
      }
    } else if (!index_exists) {
      throw file_not_found{meta.filename};  // no segments file found
    } else {
      reader->read(dir, meta.index_meta, meta.filename);
    }
  }

  auto reader = [](directory& dir, format::ptr codec, DirectoryMeta&& meta,
                   const IndexReaderOptions& opts) {
    const auto& segments = meta.index_meta.segments;

    std::vector<SegmentReader> readers;
    readers.reserve(segments.size());

    for (auto& segment : segments) {
      // Segment reader holds refs to all segment files
      readers.emplace_back(dir, segment.meta, opts);
      IRS_ASSERT(readers.back());
    }

    return std::make_shared<const DirectoryReaderImpl>(
      dir, std::move(codec), opts, std::move(meta), std::move(readers));
  }(dir, codec, std::move(meta), opts.reader_options);

  auto writer = std::make_shared<IndexWriter>(
    ConstructToken{}, std::move(lock), std::move(lock_ref), dir,
    std::move(codec), opts.segment_pool_size, SegmentOptions{opts},
    opts.comparator, opts.column_info ? opts.column_info : kDefaultColumnInfo,
    opts.features ? opts.features : kDefaultFeatureInfo,
    opts.meta_payload_provider, std::move(reader));

  // Remove non-index files from directory
  directory_utils::RemoveAllUnreferenced(dir);

  return writer;
}

IndexWriter::~IndexWriter() noexcept {
  // Failure may indicate a dangling 'document' instance
  IRS_ASSERT(!segments_active_.load());
  write_lock_.reset();  // Reset write lock if any
  // Reset pending state (if any) before destroying flush contexts
  pending_state_.Reset();
  flush_context_.store(nullptr);
  // Ensure all tracked segment_contexts are released before
  // segment_writer_pool_ is deallocated
  flush_context_pool_.clear();
}

uint64_t IndexWriter::BufferedDocs() const {
  uint64_t docs_in_ram = 0;
  auto ctx = const_cast<IndexWriter*>(this)->GetFlushContext();
  // 'pending_used_segment_contexts_'/'pending_free_segment_contexts_'
  // may be modified
  // cppcheck-suppress unreadVariable
  std::lock_guard lock{ctx->mutex_};

  for (auto& entry : ctx->pending_segment_contexts_) {
    // reading segment_writer::docs_count() is not thread safe
    // cppcheck-suppress useStlAlgorithm
    docs_in_ram +=
      entry.segment_->buffered_docs_.load(std::memory_order_relaxed);
  }

  return docs_in_ram;
}

uint64_t IndexWriter::NextSegmentId() noexcept {
  return seg_counter_.fetch_add(1, std::memory_order_relaxed) + 1;
}

uint64_t IndexWriter::CurrentSegmentId() const noexcept {
  return seg_counter_.load(std::memory_order_relaxed);
}

ConsolidationResult IndexWriter::Consolidate(
  const ConsolidationPolicy& policy, format::ptr codec /*= nullptr*/,
  const MergeWriter::FlushProgress& progress /*= {}*/) {
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
  auto committed_reader =
    std::atomic_load_explicit(&committed_reader_, std::memory_order_acquire);
  IRS_ASSERT(committed_reader);
  if (IRS_UNLIKELY(!committed_reader)) {
    return {0, ConsolidationError::FAIL};
  }

  if (committed_reader->size() == 0) {
    // nothing to consolidate
    return {0, ConsolidationError::OK};
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

        if (!HasRemovals(segment->Meta())) {
          // no removals, nothing to consolidate
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
        committed_reader->Meta().index_meta.gen, found, candidates.size());
      return {0, ConsolidationError::FAIL};
    }
  }

  IR_FRMT_TRACE("Starting consolidation id='" IR_SIZE_T_SPECIFIER "':\n%s",
                run_id, ToString(candidates).c_str());

  // do lock-free merge

  ConsolidationResult result{candidates.size(), ConsolidationError::FAIL};

  IndexSegment consolidation_segment;
  consolidation_segment.meta.codec = codec;  // Should use new codec
  consolidation_segment.meta.version = 0;    // Reset version for new segment
  // Increment active meta
  consolidation_segment.meta.name = file_name(NextSegmentId());

  RefTrackingDirectory dir{dir_};  // Track references for new segment

  MergeWriter merger{dir, column_info_, feature_info_, comparator_};
  merger.Reset(candidates.begin(), candidates.end());

  // We do not persist segment meta since some removals may come later
  if (!merger.Flush(consolidation_segment.meta, progress)) {
    // Nothing to consolidate or consolidation failure
    return result;
  }

  auto pending_reader = SegmentReaderImpl::Open(
    dir_, consolidation_segment.meta, committed_reader->Options());

  if (!pending_reader) {
    throw index_error{
      absl::StrCat("Failed to open reader for consolidated segment '",
                   consolidation_segment.meta.name, "'")};
  }

  // Commit merge
  {
    // ensure committed_state_ segments are not modified by concurrent
    // consolidate()/commit()
    std::unique_lock lock{commit_lock_};
    const auto current_committed_reader = committed_reader_;
    IRS_ASSERT(current_committed_reader);
    if (IRS_UNLIKELY(!current_committed_reader)) {
      return {0, ConsolidationError::FAIL};
    }

    if (pending_state_.Valid()) {
      // check that we haven't added to reader cache already absent readers
      // only if we have different index meta
      if (committed_reader != current_committed_reader) {
        auto begin = current_committed_reader->begin();
        auto end = current_committed_reader->end();

        // pointers are different so check by name
        for (const auto* candidate : candidates) {
          if (end == std::find_if(
                       begin, end,
                       [candidate = std::string_view{candidate->Meta().name}](
                         const SubReader& s) {
                         // FIXME(gnusi): compare pointers?
                         return candidate == s.Meta().name;
                       })) {
            // not all candidates are valid
            IR_FRMT_DEBUG(
              "Failed to start consolidation for index generation "
              "'" IR_UINT64_T_SPECIFIER
              "', not found segment %s in committed state",
              committed_reader->Meta().index_meta.gen,
              candidate->Meta().name.c_str());
            return result;
          }
        }
      }

      result.error = ConsolidationError::PENDING;

      // transaction has been started, we're somewhere in the middle

      // can modify ctx->segment_mask_ without
      // lock since have commit_lock_
      auto ctx = GetFlushContext();

      // register consolidation for the next transaction
      ctx->pending_segments_.emplace_back(
        std::move(consolidation_segment),
        std::numeric_limits<size_t>::max(),  // skip removals, will accumulate
                                             // removals from existing
                                             // candidates
        dir.GetRefs(),                       // do not forget to track refs
        std::move(candidates),               // consolidation context candidates
        std::move(pending_reader),           // consolidated reader
        std::move(committed_reader),         // consolidation context meta
        std::move(merger));                  // merge context

      IR_FRMT_TRACE("Consolidation id='" IR_SIZE_T_SPECIFIER
                    "' successfully finished: pending",
                    run_id);
    } else if (committed_reader == current_committed_reader) {
      // before new transaction was started:
      // no commits happened in since consolidation was started

      auto ctx = GetFlushContext();
      // lock due to context modification
      std::lock_guard ctx_lock{ctx->mutex_};

      // can release commit lock, we guarded against commit by
      // locked flush context
      lock.unlock();

      auto& segment_mask = ctx->segment_mask_;

      // persist segment meta
      index_utils::FlushIndexSegment(dir, consolidation_segment);
      segment_mask.reserve(segment_mask.size() + candidates.size());
      const auto& pending_segment = ctx->pending_segments_.emplace_back(
        std::move(consolidation_segment),
        0,              // removals must be applied to the consolidated segment
        dir.GetRefs(),  // do not forget to track refs
        std::move(candidates),         // consolidation context candidates
        std::move(pending_reader),     // consolidated reader
        std::move(committed_reader));  // consolidation context meta

      // filter out merged segments for the next commit
      const auto& consolidation_ctx = pending_segment.consolidation_ctx;
      const auto& consolidation_meta = pending_segment.segment.meta;

      // mask mapped candidates
      // segments from the to-be added new segment
      segment_mask.insert(consolidation_ctx.candidates.begin(),
                          consolidation_ctx.candidates.end());

      IR_FRMT_TRACE(
        "Consolidation id='" IR_SIZE_T_SPECIFIER
        "' successfully finished: Name='%s', docs_count=" IR_UINT64_T_SPECIFIER
        ", live_docs_count=" IR_UINT64_T_SPECIFIER ", size=" IR_SIZE_T_SPECIFIER
        "",
        run_id, consolidation_meta.name.c_str(), consolidation_meta.docs_count,
        consolidation_meta.live_docs_count, consolidation_meta.byte_size);
    } else {
      // before new transaction was started:
      // there was a commit(s) since consolidation was started,

      auto ctx = GetFlushContext();
      // lock due to context modification
      std::lock_guard ctx_lock{ctx->mutex_};

      // can release commit lock, we guarded against commit by
      // locked flush context
      lock.unlock();

      auto& segment_mask = ctx->segment_mask_;

      CandidatesMapping mappings;
      const auto [count, has_removals] =
        MapCandidates(mappings, candidates, *current_committed_reader);

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

      // handle removals if something changed
      if (has_removals) {
        document_mask docs_mask;

        if (!MapRemovals(mappings, merger, docs_mask)) {
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
          WriteDocumentMask(dir, consolidation_segment.meta, docs_mask, false);

          // Reopen modified reader
          pending_reader = std::make_shared<SegmentReaderImpl>(
            *pending_reader, consolidation_segment.meta, std::move(docs_mask));
        }
      }

      // persist segment meta
      index_utils::FlushIndexSegment(dir, consolidation_segment);
      segment_mask.reserve(segment_mask.size() + candidates.size());
      const auto& pending_segment = ctx->pending_segments_.emplace_back(
        std::move(consolidation_segment),
        0,              // removals must be applied to the consolidated segment
        dir.GetRefs(),  // do not forget to track refs
        std::move(candidates),         // consolidation context candidates
        std::move(pending_reader),     // consolidated reader
        std::move(committed_reader));  // consolidation context meta

      // filter out merged segments for the next commit
      const auto& consolidation_ctx = pending_segment.consolidation_ctx;
      const auto& consolidation_meta = pending_segment.segment.meta;

      // mask mapped candidates
      // segments from the to-be added new segment
      segment_mask.insert(consolidation_ctx.candidates.begin(),
                          consolidation_ctx.candidates.end());

      // mask mapped (matched) segments
      // segments from the already finished commit
      for (const auto& segment : *current_committed_reader) {
        if (mappings.contains(segment.Meta().name)) {
          segment_mask.emplace(&segment);
        }
      }

      IR_FRMT_TRACE(
        "Consolidation id='" IR_SIZE_T_SPECIFIER
        "' successfully finished:\nName='%s', docs_count=" IR_UINT64_T_SPECIFIER
        ", live_docs_count=" IR_UINT64_T_SPECIFIER ", size=" IR_SIZE_T_SPECIFIER
        "",
        run_id, consolidation_meta.name.c_str(), consolidation_meta.docs_count,
        consolidation_meta.live_docs_count, consolidation_meta.byte_size);
    }
  }

  result.error = ConsolidationError::OK;
  return result;
}

bool IndexWriter::Import(const IndexReader& reader,
                         format::ptr codec /*= nullptr*/,
                         const MergeWriter::FlushProgress& progress /*= {}*/) {
  if (!reader.live_docs_count()) {
    return true;  // Skip empty readers since no documents to import
  }

  if (!codec) {
    codec = codec_;
  }

  auto options = [&]() -> std::optional<IndexReaderOptions> {
    auto committed_reader =
      std::atomic_load_explicit(&committed_reader_, std::memory_order_acquire);
    IRS_ASSERT(committed_reader);
    if (IRS_UNLIKELY(!committed_reader)) {
      return std::nullopt;
    }

    return committed_reader->Options();
  }();

  if (IRS_UNLIKELY(!options.has_value())) {
    return false;
  }

  RefTrackingDirectory dir{dir_};  // Track references

  IndexSegment segment;
  segment.meta.name = file_name(NextSegmentId());
  segment.meta.codec = codec;

  MergeWriter merger(dir, column_info_, feature_info_, comparator_);
  merger.Reset(reader.begin(), reader.end());

  if (!merger.Flush(segment.meta, progress)) {
    return false;  // Import failure (no files created, nothing to clean up)
  }

  auto imported_reader = SegmentReaderImpl::Open(dir_, segment.meta, *options);

  if (!imported_reader) {
    throw index_error{absl::StrCat(
      "Failed to open reader for imported segment '", segment.meta.name, "'")};
  }

  index_utils::FlushIndexSegment(dir, segment);

  auto refs = dir.GetRefs();

  auto ctx = GetFlushContext();
  // lock due to context modification
  // cppcheck-suppress unreadVariable
  std::lock_guard lock{ctx->mutex_};

  ctx->pending_segments_.emplace_back(
    std::move(segment),
    ctx->generation_.load(),  // current modification generation
    std::move(refs),
    std::move(imported_reader));  // do not forget to track refs

  return true;
}

IndexWriter::FlushContextPtr IndexWriter::GetFlushContext(
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

      return {ctx, [](FlushContext* ctx) noexcept -> void {
                std::unique_lock lock{ctx->flush_mutex_, std::adopt_lock};
                // reset context and make ready for reuse
                ctx->Reset();
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

    return {ctx, [](FlushContext* ctx) noexcept -> void {
              std::shared_lock lock{ctx->flush_mutex_, std::adopt_lock};
            }};
  }
}

IndexWriter::ActiveSegmentContext IndexWriter::GetSegmentContext(
  FlushContext& ctx) {
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
      static_cast<PendindSegmentContext*>(freelist_node)->segment_;

    // +1 for the reference in 'pending_segment_contexts_'
    IRS_ASSERT(segment.use_count() == 1);
    IRS_ASSERT(!segment->dirty_);
    return {segment, segments_active_, &ctx, freelist_node->value};
  }

  // should allocate a new segment_context from the pool
  std::shared_ptr<SegmentContext> segment_ctx{segment_writer_pool_.emplace(
    dir_,
    [this]() {
      SegmentMeta meta{.codec = codec_};
      meta.name = file_name(NextSegmentId());

      return meta;
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
IndexWriter::FlushPending(FlushContext& ctx,
                          std::unique_lock<std::mutex>& ctx_lock) {
  uint64_t max_tick = 0;
  std::vector<std::unique_lock<std::mutex>> segment_flush_locks;
  segment_flush_locks.reserve(ctx.pending_segment_contexts_.size());

  for (auto& entry : ctx.pending_segment_contexts_) {
    auto& segment = entry.segment_;

    // mark the 'segment_context' as dirty so that it will not be reused if this
    // 'flush_context' once again becomes the active context while the
    // 'segment_context' handle is still held by GetBatch()
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
    max_tick = std::max(segment->Flush(), max_tick);
  }

  return {std::move(segment_flush_locks), max_tick};
}

IndexWriter::PendingContext IndexWriter::FlushAll(
  uint64_t tick, ProgressReportCallback const& progress_callback) {
  REGISTER_TIMER_DETAILED();

  auto const& progress =
    (progress_callback != nullptr ? progress_callback : kNoProgress);

  IndexMeta pending_meta;
  std::vector<PartialSync> partial_sync;
  std::vector<SegmentReader> readers;

  auto ctx = GetFlushContext(false);
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

  const auto& committed_reader = *committed_reader_;
  const auto& committed_meta = committed_reader.Meta();
  const auto& reader_options = committed_reader.Options();

  // If there is no index we shall initialize it
  bool modified = IsInitialCommit(committed_meta);

  // Stage 0
  // wait for any outstanding segments to settle to ensure that any rollbacks
  // are properly tracked in 'modification_queries_'

  const auto [segment_flush_locks, max_tick] = FlushPending(*ctx, lock);

  // Stage 1
  // update document_mask for existing (i.e. sealed) segments

  auto& segment_mask = ctx->segment_mask_;

  // only used for progress reporting
  size_t current_segment_index = 0;
  const size_t commited_reader_size = committed_reader.size();

  readers.reserve(commited_reader_size);
  pending_meta.segments.reserve(commited_reader_size);

  for (std::vector<doc_id_t> deleted_docs;
       const auto& existing_segment : committed_reader.GetReaders()) {
    // report progress
    progress("Stage 1: Apply removals to the existing segments",
             current_segment_index, commited_reader_size);

    Finally increment = [&]() noexcept { ++current_segment_index; };

    // skip already masked segments
    if (segment_mask.contains(&existing_segment)) {
      continue;
    }

    IRS_ASSERT(deleted_docs.empty());

    // mask documents matching filters from segment_contexts (i.e. from new
    // operations)
    for (auto& modifications : ctx->pending_segment_contexts_) {
      const std::span modification_queries{
        modifications.segment_->modification_queries_};

      if (modification_queries.empty()) {
        continue;
      }

      // FIXME(gnusi): optimize PK queries
      RemoveFromExistingSegment(deleted_docs, modification_queries,
                                existing_segment);
    }

    // Write docs_mask if masks added
    if (const size_t num_removals = deleted_docs.size(); num_removals) {
      // Ensure we clear accumulated removals
      Finally cleanup = [&]() noexcept { deleted_docs.clear(); };

      // If all docs are masked then mask segment
      if (existing_segment.live_docs_count() == num_removals) {
        // It's important to mask empty segment to rollback
        // the affected consolidations

        segment_mask.emplace(&existing_segment);
        modified = true;
        continue;
      }

      // Append removals
      IRS_ASSERT(existing_segment.docs_mask());
      auto docs_mask = *existing_segment.docs_mask();
      docs_mask.insert(deleted_docs.begin(), deleted_docs.end());

      IndexSegment segment{
        .meta = committed_meta.index_meta.segments[current_segment_index].meta};

      const auto mask_file_index =
        WriteDocumentMask(dir, segment.meta, docs_mask);
      index_utils::FlushIndexSegment(dir, segment);  // Write with new mask
      partial_sync.emplace_back(readers.size(), mask_file_index);

      auto new_segment = std::make_shared<SegmentReaderImpl>(
        *existing_segment.GetImpl(), segment.meta, std::move(docs_mask));
      readers.emplace_back(std::move(new_segment));
      pending_meta.segments.emplace_back(std::move(segment));
    } else {
      readers.emplace_back(existing_segment.GetImpl());
      pending_meta.segments.emplace_back(
        committed_meta.index_meta.segments[current_segment_index]);
    }
  }

  // Stage 2
  // Add pending complete segments registered by import or consolidation

  // Number of candidates that have been registered for pending consolidation
  size_t current_pending_segments_index = 0;
  size_t pending_candidates_count = 0;
  size_t partial_sync_threshold = readers.size();

  for (auto& pending_segment : ctx->pending_segments_) {
    IRS_ASSERT(pending_segment.reader);  // Ensured by Consolidation/Import
    auto& meta = pending_segment.segment.meta;
    auto& pending_reader = pending_segment.reader;
    auto pending_docs_mask = *pending_reader->docs_mask();  // Intenional copy

    // Report progress
    progress("Stage 2: Handling consolidated/imported segments",
             current_pending_segments_index, ctx->pending_segments_.size());

    Finally increment = [&]() noexcept { ++current_pending_segments_index; };

    bool docs_mask_modified = false;

    const ConsolidationView candidates{
      pending_segment.consolidation_ctx.candidates};

    const auto pending_consolidation =
      static_cast<bool>(pending_segment.consolidation_ctx.merger);

    if (pending_consolidation) {
      // Pending consolidation request
      CandidatesMapping mappings;
      const auto [count, has_removals] =
        MapCandidates(mappings, candidates, readers);

      if (count != candidates.size()) {
        // At least one candidate is missing in pending meta can't finish
        // consolidation
        IR_FRMT_DEBUG(
          "Failed to finish merge for segment '%s', found only "
          "'" IR_SIZE_T_SPECIFIER "' out of '" IR_SIZE_T_SPECIFIER
          "' candidates",
          meta.name.c_str(), count, candidates.size());

        continue;  // Skip this particular consolidation
      }

      // Mask mapped candidates segments from the to-be added new segment
      for (const auto& mapping : mappings) {
        const auto* reader = mapping.second.old.segment;
        IRS_ASSERT(reader);
        segment_mask.emplace(reader);
      }

      // Mask mapped (matched) segments from the currently ongoing commit
      for (const auto& segment : readers) {
        if (mappings.contains(segment.Meta().name)) {
          // Important to store the address of implementation
          segment_mask.emplace(segment.GetImpl().get());
        }
      }

      // Have some changes, apply removals
      if (has_removals) {
        const auto success =
          MapRemovals(mappings, pending_segment.consolidation_ctx.merger,
                      pending_docs_mask);

        if (!success) {
          // Consolidated segment has docs missing from 'segments'
          IR_FRMT_WARN(
            "Failed to finish merge for segment '%s', due to removed documents "
            "still present the consolidation candidates",
            meta.name.c_str());

          continue;  // Skip this particular consolidation
        }

        // We're done with removals for pending consolidation
        // they have been already applied to candidates above
        // and successfully remapped to consolidated segment
        docs_mask_modified |= true;
      }

      // We've seen at least 1 successfully applied
      // pending consolidation request
      pending_candidates_count += candidates.size();
    } else {
      // During consolidation doc_mask could be already populated even for just
      // merged segment
      // pending already imported/consolidated segment, apply removals
      // mask documents matching filters from segment_contexts (i.e. from new
      // operations)
      for (auto& modifications : ctx->pending_segment_contexts_) {
        const std::span modification_queries{
          modifications.segment_->modification_queries_};

        if (modification_queries.empty()) {
          continue;  // Nothing to do
        }

        // FIXME(gnusi): optimize PK queries
        docs_mask_modified |= RemoveFromImportedSegment(
          modification_queries, *pending_reader, pending_docs_mask,
          pending_segment.generation);
      }
    }

    // Skip empty segments
    if (meta.docs_count <= pending_docs_mask.size()) {
      IRS_ASSERT(meta.docs_count == pending_docs_mask.size());
      modified = true;  // FIXME(gnusi): looks strange
      continue;
    }

    // Write non-empty document mask
    if (docs_mask_modified) {
      WriteDocumentMask(dir, meta, pending_docs_mask, !pending_consolidation);

      // Reopen modified reader
      pending_reader = std::make_shared<SegmentReaderImpl>(
        *pending_reader, meta, std::move(pending_docs_mask));
    }

    // Persist segment meta
    if (docs_mask_modified || pending_consolidation) {
      index_utils::FlushIndexSegment(dir, pending_segment.segment);
    }

    readers.emplace_back(std::move(pending_reader));
    pending_meta.segments.emplace_back(std::move(pending_segment.segment));
  }

  // For pending consolidation we need to filter out consolidation
  // candidates after applying them
  if (pending_candidates_count) {
    IRS_ASSERT(pending_candidates_count <= readers.size());
    const size_t count = readers.size() - pending_candidates_count;
    std::vector<SegmentReader> tmp_readers;
    tmp_readers.reserve(count);
    IndexMeta tmp_meta;
    tmp_meta.segments.reserve(count);
    std::vector<PartialSync> tmp_partial_sync;

    auto partial_sync_begin = partial_sync.begin();
    for (size_t i = 0; i < partial_sync_threshold; ++i) {
      if (const auto& segment = readers[i];
          !segment_mask.contains(segment.GetImpl().get())) {
        partial_sync_begin =
          std::find_if(partial_sync_begin, partial_sync.end(),
                       [i](const auto& v) { return i == v.segment_index; });
        if (partial_sync_begin != partial_sync.end()) {
          tmp_partial_sync.emplace_back(tmp_readers.size(),
                                        partial_sync_begin->file_index);
        }
        tmp_readers.emplace_back(std::move(segment));
        tmp_meta.segments.emplace_back(std::move(pending_meta.segments[i]));
      }
    }
    const auto tmp_partial_sync_threshold = tmp_readers.size();

    tmp_readers.insert(
      tmp_readers.end(),
      std::make_move_iterator(readers.begin() + partial_sync_threshold),
      std::make_move_iterator(readers.end()));
    tmp_meta.segments.insert(
      tmp_meta.segments.end(),
      std::make_move_iterator(pending_meta.segments.begin() +
                              partial_sync_threshold),
      std::make_move_iterator(pending_meta.segments.end()));

    partial_sync_threshold = tmp_partial_sync_threshold;
    partial_sync = std::move(tmp_partial_sync);
    readers = std::move(tmp_readers);
    pending_meta = std::move(tmp_meta);
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
      const auto flushed_doc_id_end = segment->uncomitted_doc_id_begin_;
      IRS_ASSERT(flushed_doc_id_end - doc_limits::min() <=
                 segment->flushed_update_contexts_.size());

      // process individually each flushed SegmentMeta from the segment_context
      for (auto& flushed : segment->flushed_) {
        IRS_ASSERT(flushed.meta.live_docs_count != 0);

        // report progress
        progress("Stage 3: Creating new segments",
                 current_pending_segment_context_segments,
                 total_pending_segment_context_segments);
        ++current_pending_segment_context_segments;

        const auto flushed_docs_start = flushed_docs_count;

        // sum of all previous SegmentMeta::docs_count including this meta
        flushed_docs_count += flushed.meta.docs_count;

        // SegmentMeta fully before the start of this flush_context
        if (flushed_doc_id_end - doc_limits::min() <= flushed_docs_start) {
          continue;
        }

        auto update_contexts_begin = flushed_docs_start;
        // 0-based
        auto update_contexts_end =
          std::min(flushed_doc_id_end - doc_limits::min(), flushed_docs_count);
        IRS_ASSERT(update_contexts_begin <= update_contexts_end);
        // beginning doc_id in this SegmentMeta
        const auto valid_doc_id_begin =
          update_contexts_begin - flushed_docs_start + doc_limits::min();
        const auto valid_doc_id_end =
          std::min(update_contexts_end - flushed_docs_start + doc_limits::min(),
                   static_cast<size_t>(flushed.docs_mask_tail_doc_id));
        IRS_ASSERT(valid_doc_id_begin <= valid_doc_id_end);

        if (valid_doc_id_begin == valid_doc_id_end) {
          continue;  // empty segment since head+tail == 'docs_count'
        }

        auto reader =
          SegmentReaderImpl::Open(dir, flushed.meta, reader_options);

        if (!reader) {
          throw index_error{absl::StrCat(
            "while adding document mask modified records "
            "to flush_segment_context of "
            "segment '",
            flushed.meta.name, "', error: failed to open segment")};
        }

        const std::span flush_update_contexts{
          segment->flushed_update_contexts_.data() + flushed_docs_start,
          flushed.meta.docs_count};

        auto& flush_segment_ctx = segment_ctxs.emplace_back(
          std::move(reader), std::move(flushed), std::move(flushed.docs_mask),
          valid_doc_id_begin, valid_doc_id_end, flush_update_contexts,
          segment->modification_queries_);

        // read document_mask as was originally flushed
        // could be due to truncated records due to rollback of uncommitted data
        auto& docs_mask = flush_segment_ctx.docs_mask_;

        // add doc_ids before start of this flush_context to document_mask
        for (size_t doc_id = doc_limits::min(); doc_id < valid_doc_id_begin;
             ++doc_id) {
          IRS_ASSERT(std::numeric_limits<doc_id_t>::max() >= doc_id);
          docs_mask.emplace(static_cast<doc_id_t>(doc_id));
        }

        // add tail doc_ids not part of this flush_context to documents_mask
        // (including truncated)
        for (size_t doc_id = valid_doc_id_end,
                    doc_id_end = flushed.meta.docs_count + doc_limits::min();
             doc_id < doc_id_end; ++doc_id) {
          IRS_ASSERT(std::numeric_limits<doc_id_t>::max() >= doc_id);
          docs_mask.emplace(static_cast<doc_id_t>(doc_id));
        }

        // mask documents matching filters from all flushed segment_contexts
        // (i.e. from new operations)
        for (auto& modifications : ctx->pending_segment_contexts_) {
          const std::span modification_queries(
            modifications.segment_->modification_queries_);

          if (modification_queries.empty()) {
            continue;  // Nothing to do
          }

          // FIXME(gnusi): optimize PK queries
          RemoveFromNewSegment(modification_queries, flush_segment_ctx);
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
      MaskUnusedUpdatesInNewSegment(segment_ctx);

      auto& docs_mask = segment_ctx.docs_mask_;
      auto& meta = segment_ctx.segment_.meta;

      // After mismatched replaces here could be also empty segment
      // so masking is needed
      if (meta.docs_count <= docs_mask.size()) {
        IRS_ASSERT(meta.docs_count == docs_mask.size());
        continue;
      }

      if (!docs_mask.empty()) {  // Write non-empty document mask
        WriteDocumentMask(dir, meta, docs_mask);
        // Write updated segment metadata
        index_utils::FlushIndexSegment(dir, segment_ctx.segment_);
        // Refresh reader
        segment_ctx.reader_ = std::make_shared<SegmentReaderImpl>(
          *segment_ctx.reader_, meta, std::move(docs_mask));
      }

      readers.emplace_back(std::move(segment_ctx.reader_));
      pending_meta.segments.emplace_back(std::move(segment_ctx.segment_));
    }
  }

  auto files_to_sync =
    GetFilesToSync(pending_meta.segments, partial_sync, partial_sync_threshold);

  modified |= !files_to_sync.empty();

  // only flush a new index version upon a new index or a metadata change
  if (!modified) {
    return {};
  }

  InitMeta(pending_meta, max_tick);

  return {.ctx = std::move(ctx),            // Retain flush context reference
          .meta = std::move(pending_meta),  // Retain meta pending flush
          .readers = std::move(readers),
          .files_to_sync = std::move(files_to_sync)};
}

void IndexWriter::StartImpl(FlushContextPtr&& ctx, DirectoryMeta&& to_commit,
                            std::vector<SegmentReader>&& readers,
                            std::vector<std::string_view>&& files_to_sync) {
  IRS_ASSERT(ctx);
  IRS_ASSERT(ctx->dir_);
  IRS_ASSERT(!pending_state_.Valid());

  RefTrackingDirectory& dir = *ctx->dir_;

  std::string index_meta_file;

  // Execute 1st phase of index meta transaction
  if (!writer_->prepare(dir, to_commit.index_meta, to_commit.filename,
                        index_meta_file)) {
    throw illegal_state{absl::StrCat(
      "Failed to write index metadata for segment '", index_meta_file, "'.")};
  }

  // The 1st phase of the transaction successfully finished here,
  // ensure we rollback changes if something goes wrong afterwards
  Finally update_generation = [this,
                               new_gen = to_commit.index_meta.gen]() noexcept {
    if (IRS_UNLIKELY(!pending_state_.Valid())) {
      writer_->rollback();  // Rollback failed transaction
    }

    // Ensure writer's generation is updated
    last_gen_ = new_gen;
  };

  files_to_sync.emplace_back(to_commit.filename);

  if (!dir.sync(files_to_sync)) {
    throw io_error{absl::StrCat("Failed to sync files for segment '",
                                index_meta_file, "'.")};
  }

  // Update file name so that directory reader holds a reference
  to_commit.filename = std::move(index_meta_file);
  // Assemble directory reader
  pending_state_.commit = std::make_shared<const DirectoryReaderImpl>(
    dir, codec_, committed_reader_->Options(), std::move(to_commit),
    std::move(readers));
  // Retain flush context reference
  pending_state_.ctx = std::move(ctx);
  IRS_ASSERT(pending_state_.Valid());
}

bool IndexWriter::Start(uint64_t tick, ProgressReportCallback const& progress) {
  IRS_ASSERT(!commit_lock_.try_lock());  // already locked

  REGISTER_TIMER_DETAILED();

  if (pending_state_.Valid()) {
    // Begin has been already called without corresponding call to commit
    return false;
  }

  auto to_commit = FlushAll(tick, progress);

  if (to_commit.Empty()) {
    // Nothing to commit, no transaction started
    return false;
  }

  StartImpl(std::move(to_commit.ctx), {.index_meta = std::move(to_commit.meta)},
            std::move(to_commit.readers), std::move(to_commit.files_to_sync));

  return true;
}

void IndexWriter::Finish() {
  IRS_ASSERT(!commit_lock_.try_lock());  // Already locked

  REGISTER_TIMER_DETAILED();

  if (!pending_state_.Valid()) {
    return;
  }

  bool res{false};

  Finally reset_state = [this, &res]() noexcept {
    if (IRS_UNLIKELY(!res)) {
      Abort();  // Rollback failed transaction
    }

    // Release reference to flush_context
    pending_state_.Reset();
  };

  // Lightweight 2nd phase of the transaction

  res = writer_->commit();

  if (IRS_UNLIKELY(!res)) {
    throw illegal_state{"Failed to commit index metadata."};
  }

  // after this line transaction is successfull (only noexcept operations below)
  std::atomic_store_explicit(&committed_reader_,
                             std::move(pending_state_.commit),
                             std::memory_order_release);
}

void IndexWriter::Abort() noexcept {
  IRS_ASSERT(!commit_lock_.try_lock());  // already locked

  if (!pending_state_.Valid()) {
    // There is no open transaction
    return;
  }

  writer_->rollback();
  pending_state_.Reset();
}

}  // namespace irs
