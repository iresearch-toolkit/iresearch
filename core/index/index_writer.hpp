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

#pragma once

#include <atomic>
#include <functional>
#include <string_view>

#include "formats/formats.hpp"
#include "index/column_info.hpp"
#include "index/directory_reader.hpp"
#include "index/field_meta.hpp"
#include "index/index_features.hpp"
#include "index/index_meta.hpp"
#include "index/index_reader_options.hpp"
#include "index/merge_writer.hpp"
#include "index/segment_reader.hpp"
#include "index/segment_writer.hpp"
#include "search/filter.hpp"
#include "utils/async_utils.hpp"
#include "utils/bitvector.hpp"
#include "utils/noncopyable.hpp"
#include "utils/object_pool.hpp"
#include "utils/string.hpp"
#include "utils/thread_utils.hpp"

#include <absl/container/flat_hash_map.h>

namespace irs {

class Comparer;
struct directory;

// Defines how index writer should be opened
enum OpenMode {
  // Creates new index repository. In case if repository already
  // exists, all contents will be cleared.
  OM_CREATE = 1,

  // Opens existing index repository. In case if repository does not
  // exists, error will be generated.
  OM_APPEND = 2,
};

ENABLE_BITMASK_ENUM(OpenMode);

// A set of candidates denoting an instance of consolidation
using Consolidation = std::vector<const SubReader*>;
using ConsolidationView = std::span<const SubReader* const>;

struct CandidateHash {
  size_t operator()(const SubReader* segment) const noexcept {
    return absl::HashOf(segment->Meta().name);
  }
};

struct CandidateEqual {
  bool operator()(const SubReader* lhs, const SubReader* rhs) const noexcept {
    return lhs->Meta().name == rhs->Meta().name;
  }
};

// segments that are under consolidation
using ConsolidatingSegments =
  absl::flat_hash_set<const SubReader*, CandidateHash, CandidateEqual>;

// Mark consolidation candidate segments matching the current policy
// candidates the segments that should be consolidated
// in: segment candidates that may be considered by this policy
// out: actual segments selected by the current policy
// dir the segment directory
// meta the index meta containing segments to be considered
// Consolidating_segments segments that are currently in progress
// of consolidation
// Final candidates are all segments selected by at least some policy
using ConsolidationPolicy =
  std::function<void(Consolidation& candidates, const IndexReader& index,
                     const ConsolidatingSegments& consolidating_segments)>;

enum class ConsolidationError : uint32_t {
  // Consolidation failed
  FAIL = 0,

  // Consolidation successfully finished
  OK,

  // Consolidation was scheduled for the upcoming commit
  PENDING
};

// Represents result of a consolidation
struct ConsolidationResult {
  // Number of candidates
  size_t size;

  // Error code
  ConsolidationError error;

  // intentionally implicit
  operator bool() const noexcept { return error != ConsolidationError::FAIL; }
};

// Options the the writer should use for segments
struct SegmentOptions {
  // Segment acquisition requests will block and wait for free segments
  // after this many segments have been acquired e.g. via GetBatch()
  // 0 == unlimited
  size_t segment_count_max{0};

  // Flush the segment to the repository after its total document
  // count (live + masked) grows beyond this byte limit, in-flight
  // documents will still be written to the segment before flush
  // 0 == unlimited
  size_t segment_docs_max{0};

  // Flush the segment to the repository after its in-memory size
  // grows beyond this byte limit, in-flight documents will still be
  // written to the segment before flush
  // 0 == unlimited
  size_t segment_memory_max{0};
};

// Progress report callback types for commits.
using ProgressReportCallback =
  std::function<void(std::string_view phase, size_t current, size_t total)>;

// Functor for creating payload. Operation tick is provided for
// payload generation.
using PayloadProvider = std::function<bool(uint64_t, bstring&)>;

// Options the the writer should use after creation
struct IndexWriterOptions : public SegmentOptions {
  // Options for snapshot management
  IndexReaderOptions reader_options;

  // Returns column info for a feature the writer should use for
  // columnstore
  FeatureInfoProvider features;

  // Returns column info the writer should use for columnstore
  ColumnInfoProvider column_info;

  // Provides payload for index_meta created by writer
  PayloadProvider meta_payload_provider;

  // Comparator defines physical order of documents in each segment
  // produced by an index_writer.
  // empty == use default system sorting order
  const Comparer* comparator{nullptr};

  // Number of free segments cached in the segment pool for reuse
  // 0 == do not cache any segments, i.e. always create new segments
  size_t segment_pool_size{128};  // arbitrary size

  // Acquire an exclusive lock on the repository to guard against index
  // corruption from multiple index_writers
  bool lock_repository{true};

  IndexWriterOptions() {}  // compiler requires non-default definition
};

// The object is using for indexing data. Only one writer can write to
// the same directory simultaneously.
// Thread safe.
class IndexWriter : private util::noncopyable {
 private:
  struct FlushContext;
  struct SegmentContext;

  // unique pointer required since need pointer declaration before class
  // declaration e.g. for 'documents_context'
  //
  // sizeof(std::function<void(flush_context*)>) >
  // sizeof(void(*)(flush_context*))
  using FlushContextPtr =
    std::unique_ptr<FlushContext, void (*)(FlushContext*)>;

  // Disallow using public constructor
  struct ConstructToken {
    explicit ConstructToken() = default;
  };

  // Segment references given out by flush_context to allow tracking
  // and updating flush_context::pending_segment_context
  //
  // non-copyable to ensure only one copy for get/put
  class ActiveSegmentContext : private util::noncopyable {
   public:
    ActiveSegmentContext() = default;
    ActiveSegmentContext(
      std::shared_ptr<SegmentContext> ctx, std::atomic_size_t& segments_active,
      // the flush_context the segment_context is currently registered with
      FlushContext* flush_ctx = nullptr,
      // the segment offset in flush_ctx_->pending_segments_
      size_t pending_segment_context_offset =
        std::numeric_limits<size_t>::max()) noexcept;
    ActiveSegmentContext(ActiveSegmentContext&&) = default;
    ActiveSegmentContext& operator=(ActiveSegmentContext&& other) noexcept;

    ~ActiveSegmentContext();

    const std::shared_ptr<SegmentContext>& ctx() const noexcept { return ctx_; }

   private:
    friend struct FlushContext;  // for flush_context::emplace(...)

    std::shared_ptr<SegmentContext> ctx_;
    // nullptr will not match any flush_context
    FlushContext* flush_ctx_{nullptr};
    // segment offset in flush_ctx_->pending_segment_contexts_
    size_t pending_segment_context_offset_{};
    // reference to index_writer::segments_active_
    std::atomic_size_t* segments_active_{};
  };

  static_assert(std::is_nothrow_move_constructible_v<ActiveSegmentContext>);

 public:
  // A context allowing index modification operations.
  // The object is non-thread-safe, each thread should use its own
  // separate instance.
  class Document : private util::noncopyable {
   public:
    Document(FlushContext& ctx, std::shared_ptr<SegmentContext> segment,
             const segment_writer::update_context& update);
    Document(Document&&) = default;
    ~Document() noexcept;

    Document& operator=(Document&&) = delete;

    // Return current state of the object
    // Note that if the object is in an invalid state all further operations
    // will not take any effect
    explicit operator bool() const noexcept { return writer_.valid(); }

    // Inserts the specified field into the document according to the
    // specified ACTION
    // Note that 'Field' type type must satisfy the Field concept
    // field attribute to be inserted
    // Return true, if field was successfully inserted
    template<Action action, typename Field>
    bool Insert(Field&& field) const {
      return writer_.insert<action>(std::forward<Field>(field));
    }

    // Inserts the specified field (denoted by the pointer) into the
    //        document according to the specified ACTION
    // Note that 'Field' type type must satisfy the Field concept
    // Note that pointer must not be nullptr
    // field attribute to be inserted
    // Return true, if field was successfully inserted
    template<Action action, typename Field>
    bool Insert(Field* field) const {
      return writer_.insert<action>(*field);
    }

    // Inserts the specified range of fields, denoted by the [begin;end)
    // into the document according to the specified ACTION
    // Note that 'Iterator' underline value type must satisfy the Field concept
    // begin the beginning of the fields range
    // end the end of the fields range
    // Return true, if the range was successfully inserted
    template<Action action, typename Iterator>
    bool Insert(Iterator begin, Iterator end) const {
      for (; writer_.valid() && begin != end; ++begin) {
        Insert<action>(*begin);
      }

      return writer_.valid();
    }

   private:
    // hold reference to segment to prevent if from going back into the pool
    std::shared_ptr<SegmentContext> segment_;
    segment_writer& writer_;  // cached *segment->writer_ to avoid dereference
    FlushContext& ctx_;       // for rollback operations
    size_t update_id_;
  };

  class Transaction : private util::noncopyable {
   public:
    // cppcheck-suppress constParameter
    explicit Transaction(IndexWriter& writer) noexcept : writer_{writer} {}

    Transaction(Transaction&& other) noexcept
      : segment_{std::move(other.segment_)},
        last_operation_tick_{std::exchange(other.last_operation_tick_, 0)},
        first_operation_tick_(std::exchange(other.first_operation_tick_, 0)),
#ifdef IRESEARCH_DEBUG
        segment_use_count_{std::exchange(other.segment_use_count_, 0)},
#endif
        writer_{other.writer_} {
    }

    ~Transaction() {
      // FIXME(gnusi): consider calling reset in future
      Commit();
    }

    // Create a document to filled by the caller
    // for insertion into the index index
    // applied upon return value deallocation
    // `disable_flush` don't trigger segment flush
    //
    // The changes are not visible until commit()
    Document Insert(bool disable_flush = false) {
      // thread-safe to use ctx_/segment_ while have lock since active
      // flush_context will not change

      // updates 'segment_' and 'ctx_'
      auto ctx = UpdateSegment(disable_flush);
      IRS_ASSERT(segment_.ctx());

      return {*ctx, segment_.ctx(), segment_.ctx()->MakeUpdateContext()};
    }

    // Marks all documents matching the filter for removal.
    // Filter the filter selecting which documents should be removed.
    // Note that changes are not visible until commit().
    // Note that filter must be valid until commit().
    template<typename Filter>
    void Remove(Filter&& filter) {
      // thread-safe to use ctx_/segment_ while have lock since active
      // flush_context will not change cppcheck-suppress unreadVariable
      auto ctx = UpdateSegment(false);  // updates 'segment_' and 'ctx_'
      IRS_ASSERT(segment_.ctx());

      // guarded by flush_context::flush_mutex_
      segment_.ctx()->Remove(std::forward<Filter>(filter));
    }

    // Create a document to filled by the caller
    // for replacement of existing documents already in the index
    // matching filter with the filled document
    // applied upon return value deallocation
    // filter the filter selecting which documents should be replaced
    // Note the changes are not visible until commit()
    // Note that filter must be valid until commit()
    template<typename Filter>
    Document Replace(Filter&& filter) {
      // thread-safe to use ctx_/segment_ while have lock since active
      // flush_context will not change
      auto ctx = UpdateSegment(false);  // updates 'segment_' and 'ctx_'
      IRS_ASSERT(segment_.ctx());

      return {*ctx, segment_.ctx(),
              segment_.ctx()->MakeUpdateContext(std::forward<Filter>(filter))};
    }

    // Commit all accumulated modifications and release resources
    bool Commit() noexcept;

    // Revert all pending document modifications and release resources
    // noexcept because all insertions reserve enough space for rollback
    void Reset() noexcept;

    void SetLastTick(uint64_t tick) noexcept { last_operation_tick_ = tick; }
    uint64_t GetLastTick() const noexcept { return last_operation_tick_; }

    void SetFirstTick(uint64_t tick) noexcept { first_operation_tick_ = tick; }
    uint64_t GetFirstTick() const noexcept { return first_operation_tick_; }

    // Register underlying segment to be flushed with the upcoming index commit
    void ForceFlush();

   private:
    // refresh segment if required (guarded by flush_context::flush_mutex_)
    // is is thread-safe to use ctx_/segment_ while holding 'flush_context_ptr'
    // since active 'flush_context' will not change and hence no reload required
    FlushContextPtr UpdateSegment(bool disable_flush);

    // the segment_context used for storing changes (lazy-initialized)
    ActiveSegmentContext segment_;
    uint64_t last_operation_tick_{0};   // transaction commit tick
    uint64_t first_operation_tick_{0};  // transaction tick
#ifdef IRESEARCH_DEBUG
    // segment_.ctx().use_count() at constructor/destructor time must equal
    long segment_use_count_{0};
#endif
    IndexWriter& writer_;
  };

  // Additional information required for removal/update requests
  struct ModificationContext {
    ModificationContext(std::shared_ptr<const irs::filter> match_filter,
                        size_t gen, bool is_update)
      : filter(std::move(match_filter)), generation(gen), update(is_update) {}
    ModificationContext(const irs::filter& match_filter, size_t gen,
                        bool is_update)
      : ModificationContext{
          {std::shared_ptr<const irs::filter>{}, &match_filter},
          gen,
          is_update} {}

    ModificationContext(irs::filter::ptr&& match_filter, size_t gen,
                        bool is_update)
      : ModificationContext{std::shared_ptr{std::move(match_filter)}, gen,
                            is_update} {}

    // keep a handle to the filter for the case when this object has ownership
    std::shared_ptr<const irs::filter> filter;
    size_t generation;
    // this is an update modification (as opposed to remove)
    bool update;
    bool seen{false};
  };

  static_assert(std::is_nothrow_move_constructible_v<ModificationContext>);

  using ptr = std::shared_ptr<IndexWriter>;

  // Name of the lock for index repository
  static constexpr std::string_view kWriteLockName = "write.lock";

  ~IndexWriter() noexcept;

  // Returns current index snapshot
  DirectoryReader GetSnapshot() const noexcept {
    return DirectoryReader{
      std::atomic_load_explicit(&committed_reader_, std::memory_order_acquire)};
  }

  // Returns overall number of buffered documents in a writer
  uint64_t BufferedDocs() const;

  // Clears the existing index repository by staring an empty index.
  // Previously opened readers still remain valid.
  // truncate transaction tick
  // Call will rollback any opened transaction.
  void Clear(uint64_t tick = 0);

  // Merges segments accepted by the specified defragment policy into
  // a new segment. For all accepted segments frees the space occupied
  // by the documents marked as deleted and deduplicate terms.
  // Policy the specified defragmentation policy
  // Codec desired format that will be used for segment creation,
  // nullptr == use index_writer's codec
  // Progress callback triggered for consolidation steps, if the
  // callback returns false then consolidation is aborted
  // For deferred policies during the commit stage each policy will be
  // given the exact same index_meta containing all segments in the
  // commit, however, the resulting acceptor will only be segments not
  // yet marked for consolidation by other policies in the same commit
  ConsolidationResult Consolidate(
    const ConsolidationPolicy& policy, format::ptr codec = nullptr,
    const MergeWriter::FlushProgress& progress = {});

  // Returns a context allowing index modification operations
  // All document insertions will be applied to the same segment on a
  // best effort basis, e.g. a flush_all() will cause a segment switch
  Transaction GetBatch() noexcept { return Transaction{*this}; }

  // Imports index from the specified index reader into new segment
  // Reader the index reader to import.
  // Desired format that will be used for segment creation,
  // nullptr == use index_writer's codec.
  // Progress callback triggered for consolidation steps, if the
  // callback returns false then consolidation is aborted.
  // Returns true on success.
  bool Import(const IndexReader& reader, format::ptr codec = nullptr,
              const MergeWriter::FlushProgress& progress = {});

  // Opens new index writer.
  // dir directory where index will be should reside
  // codec format that will be used for creating new index segments
  // mode specifies how to open a writer
  // options the configuration parameters for the writer
  static IndexWriter::ptr Make(directory& dir, format::ptr codec, OpenMode mode,
                               const IndexWriterOptions& opts = {});

  // Modify the runtime segment options as per the specified values
  // options will apply no later than after the next commit()
  void Options(const SegmentOptions& opts) noexcept { segment_limits_ = opts; }

  // Returns comparator using for sorting documents by a primary key
  // nullptr == default sort order
  const Comparer* Comparator() const noexcept { return comparator_; }

  // Begins the two-phase transaction.
  // payload arbitrary user supplied data to store in the index
  // Returns true if transaction has been successflully started.
  bool Begin(uint64_t tick = std::numeric_limits<uint64_t>::max(),
             ProgressReportCallback const& progress = {}) {
    // cppcheck-suppress unreadVariable
    std::lock_guard lock{commit_lock_};

    return Start(tick, progress);
  }

  // Rollbacks the two-phase transaction
  void Rollback() {
    // cppcheck-suppress unreadVariable
    std::lock_guard lock{commit_lock_};

    Abort();
  }

  // Make all buffered changes visible for readers.
  // payload arbitrary user supplied data to store in the index
  // Return whether any changes were committed.
  //
  // Note that if begin() has been already called commit() is
  // relatively lightweight operation.
  // FIXME(gnusi): Commit() should return committed index snapshot
  bool Commit(uint64_t tick = std::numeric_limits<uint64_t>::max(),
              ProgressReportCallback const& progress = {}) {
    // cppcheck-suppress unreadVariable
    std::lock_guard lock{commit_lock_};

    const bool modified = Start(tick, progress);
    Finish();
    return modified;
  }

  // Returns field features.
  const FeatureInfoProvider& FeatureInfo() const noexcept {
    return feature_info_;
  }

  bool FlushRequired(const segment_writer& writer) const noexcept;

  // public because we want to use std::make_shared
  IndexWriter(ConstructToken, index_lock::ptr&& lock,
              index_file_refs::ref_t&& lock_file_ref, directory& dir,
              format::ptr codec, size_t segment_pool_size,
              const SegmentOptions& segment_limits, const Comparer* comparator,
              const ColumnInfoProvider& column_info,
              const FeatureInfoProvider& feature_info,
              const PayloadProvider& meta_payload_provider,
              std::shared_ptr<const DirectoryReaderImpl>&& committed_reader);

 private:
  using FileRefs = std::vector<index_file_refs::ref_t>;

  static constexpr size_t kNonUpdateRecord = std::numeric_limits<size_t>::max();

  struct ConsolidationContext : util::noncopyable {
    std::shared_ptr<const DirectoryReaderImpl> consolidation_reader;
    Consolidation candidates;
    MergeWriter merger;
  };

  static_assert(std::is_nothrow_move_constructible_v<ConsolidationContext>);

  struct ImportContext {
    ImportContext(
      IndexSegment&& segment, size_t generation, FileRefs&& refs,
      Consolidation&& consolidation_candidates,
      std::shared_ptr<const SegmentReaderImpl>&& reader,
      std::shared_ptr<const DirectoryReaderImpl>&& consolidation_reader,
      MergeWriter&& merger) noexcept
      : generation(generation),
        segment(std::move(segment)),
        refs(std::move(refs)),
        reader{std::move(reader)},
        consolidation_ctx{
          .consolidation_reader = std::move(consolidation_reader),
          .candidates = std::move(consolidation_candidates),
          .merger = std::move(merger)} {}

    ImportContext(IndexSegment&& segment, size_t generation, FileRefs&& refs,
                  Consolidation&& consolidation_candidates,
                  std::shared_ptr<const SegmentReaderImpl>&& reader,
                  std::shared_ptr<const DirectoryReaderImpl>&&
                    consolidation_reader) noexcept
      : generation{generation},
        segment{std::move(segment)},
        refs{std::move(refs)},
        reader{std::move(reader)},
        consolidation_ctx{
          .consolidation_reader = std::move(consolidation_reader),
          .candidates = std::move(consolidation_candidates)} {}

    ImportContext(IndexSegment&& segment, size_t generation, FileRefs&& refs,
                  Consolidation&& consolidation_candidates,
                  std::shared_ptr<const SegmentReaderImpl>&& reader) noexcept
      : generation{generation},
        segment{std::move(segment)},
        refs{std::move(refs)},
        reader{std::move(reader)},
        consolidation_ctx{.candidates = std::move(consolidation_candidates)} {}

    ImportContext(IndexSegment&& segment, size_t generation, FileRefs&& refs,
                  std::shared_ptr<const SegmentReaderImpl>&& reader) noexcept
      : generation{generation},
        segment{std::move(segment)},
        refs{std::move(refs)},
        reader{std::move(reader)} {}

    ImportContext(IndexSegment&& segment, size_t generation,
                  std::shared_ptr<const SegmentReaderImpl>&& reader) noexcept
      : generation{generation},
        segment{std::move(segment)},
        reader{std::move(reader)} {}

    ImportContext(ImportContext&&) = default;

    ImportContext& operator=(const ImportContext&) = delete;
    ImportContext& operator=(ImportContext&&) = delete;

    const size_t generation;
    IndexSegment segment;
    FileRefs refs;
    std::shared_ptr<const SegmentReaderImpl> reader;
    ConsolidationContext consolidation_ctx;
  };

  static_assert(std::is_nothrow_move_constructible_v<ImportContext>);

  struct FlushedSegment : public IndexSegment {
    FlushedSegment() = default;
    explicit FlushedSegment(SegmentMeta&& meta, doc_id_t docs_begin,
                            doc_id_t docs_end) noexcept
      : IndexSegment{.meta = std::move(meta)},
        docs_begin{docs_begin},
        docs_end{docs_end} {}

    // Range of the flushed docs in SegmentContext::flushed_update_contexts_
    doc_id_t docs_begin{};
    doc_id_t docs_end{};
    // Flushed segment removals
    document_mask docs_mask;
    // starting doc_id that should be added to docs_mask
    doc_id_t docs_mask_tail_doc_id{doc_limits::eof()};
  };

  // The segment writer and its associated ref tracking directory
  // for use with an unbounded_object_pool
  struct SegmentContext {
    using segment_meta_generator_t = std::function<SegmentMeta()>;
    using ptr = std::unique_ptr<SegmentContext>;

    // number of active in-progress operations (insert/replace) (e.g.
    // document instances or replace(...))
    std::atomic_size_t active_count_;
    // for use with index_writer::buffered_docs(), asynchronous call
    std::atomic_size_t buffered_docs_;
    // the codec to used for flushing a segment writer
    format::ptr codec_;
    // true if flush_all() started processing this segment (this
    // segment should not be used for any new operations), guarded by
    // the flush_context::flush_mutex_
    std::atomic_bool dirty_;
    // ref tracking for segment_writer to allow for easy ref removal on
    // segment_writer reset
    RefTrackingDirectory dir_;
    // guard 'flushed_', 'uncomitted_*' and 'writer_' from concurrent flush
    std::mutex flush_mutex_;
    // all of the previously flushed versions of this segment, guarded by the
    // flush_context::flush_mutex_
    std::vector<FlushedSegment> flushed_;
    // update_contexts to use with 'flushed_'
    // sequentially increasing through all offsets
    // (sequential doc_id in 'flushed_' == offset + doc_limits::min(), size()
    // == sum of all 'flushed_'.'docs_count')
    std::vector<segment_writer::update_context> flushed_update_contexts_;
    // function to get new SegmentMeta from
    segment_meta_generator_t meta_generator_;
    // sequential list of pending modification
    std::vector<ModificationContext> modification_queries_;
    // requests (remove/update)
    // starting doc_id that is not part of the current flush_context (doc_id
    // sequentially increasing through all 'flushed_' offsets and into
    // 'segment_writer::doc_contexts' hence value may be greater than
    // doc_id_t::max)
    size_t uncomitted_doc_id_begin_;
    // current modification/update
    // generation offset for asignment to uncommited operations (same as
    // modification_queries_.size() - uncomitted_modification_queries_)
    // FIXME TODO consider removing
    size_t uncomitted_generation_offset_;
    // staring offset in
    // 'modification_queries_' that is not part of the current flush_context
    size_t uncomitted_modification_queries_;
    std::unique_ptr<segment_writer> writer_;
    // the SegmentMeta this writer was initialized with
    SegmentMeta writer_meta_;

    static std::unique_ptr<SegmentContext> make(
      directory& dir, segment_meta_generator_t&& meta_generator,
      const ColumnInfoProvider& column_info,
      const FeatureInfoProvider& feature_info, const Comparer* comparator);

    SegmentContext(directory& dir, segment_meta_generator_t&& meta_generator,
                   const ColumnInfoProvider& column_info,
                   const FeatureInfoProvider& feature_info,
                   const Comparer* comparator);

    // Flush current writer state into a materialized segment.
    // Return tick of last committed transaction.
    uint64_t Flush();

    // Returns context for "insert" operation.
    segment_writer::update_context MakeUpdateContext() const noexcept {
      return {uncomitted_generation_offset_, kNonUpdateRecord};
    }

    // Returns context for "update" operation.
    segment_writer::update_context MakeUpdateContext(const filter& filter);
    segment_writer::update_context MakeUpdateContext(
      std::shared_ptr<const filter> filter);
    segment_writer::update_context MakeUpdateContext(filter::ptr&& filter);

    // Ensure writer is ready to receive documents
    void Prepare();

    // Modifies context for "remove" operation
    void Remove(const filter& filter);
    void Remove(std::shared_ptr<const filter> filter);
    void Remove(filter::ptr&& filter);

    // Reset segment state to the initial state
    // store_flushed should store info about flushed segments?
    // Note should be true if something went wrong during segment flush
    void Reset(bool store_flushed = false) noexcept;
  };

  struct SegmentLimits {
    // see segment_options::max_segment_count
    std::atomic_size_t segment_count_max;
    // see segment_options::max_segment_docs
    std::atomic_size_t segment_docs_max;
    // see segment_options::max_segment_memory
    std::atomic_size_t segment_memory_max;

    explicit SegmentLimits(const SegmentOptions& opts) noexcept
      : segment_count_max(opts.segment_count_max),
        segment_docs_max(opts.segment_docs_max),
        segment_memory_max(opts.segment_memory_max) {}

    SegmentLimits& operator=(const SegmentOptions& opts) noexcept {
      segment_count_max.store(opts.segment_count_max);
      segment_docs_max.store(opts.segment_docs_max);
      segment_memory_max.store(opts.segment_memory_max);
      return *this;
    }
  };

  using SegmentPool = unbounded_object_pool<SegmentContext>;
  // 'value' == node offset into 'pending_segment_context_'
  using Freelist = concurrent_stack<size_t>;

  struct PendindSegmentContext : public Freelist::node_type {
    std::shared_ptr<SegmentContext> segment_;

    PendindSegmentContext(std::shared_ptr<SegmentContext> segment,
                          size_t pending_segment_context_offset)
      : Freelist::node_type{.value = pending_segment_context_offset},
        segment_(std::move(segment)) {
      IRS_ASSERT(segment_);
    }
  };

  // The context containing data collected for the next commit() call
  // Note a 'segment_context' is tracked by at most 1 'flush_context', it is
  // the job of the 'documents_context' to guarantee that the
  // 'segment_context' is not used once the tracker 'flush_context' is no
  // longer active.
  struct FlushContext {
    using SegmentMask = absl::flat_hash_set<const SubReader*>;

    // current modification/update generation
    std::atomic_size_t generation_{0};
    // ref tracking directory used by this context (tracks all/only
    // refs for this context)
    RefTrackingDirectory::ptr dir_;
    // guard for the current context during flush (write)
    // operations vs update (read)
    std::shared_mutex flush_mutex_;
    // guard for the current context during struct update operations,
    // e.g. pending_segments_, pending_segment_contexts_
    std::mutex mutex_;
    // the next context to switch to
    FlushContext* next_context_;
    // complete segments to be added during next commit (import)
    std::vector<ImportContext> pending_segments_;
    // notified when a segment has been freed (guarded by mutex_)
    std::condition_variable pending_segment_context_cond_;
    // segment writers with data pending for next commit
    // (all segments that have been used by this flush_context)
    // must be std::deque to garantee that element memory location does
    // not change for use with 'pending_segment_contexts_freelist_'
    std::deque<PendindSegmentContext> pending_segment_contexts_;
    // entries from 'pending_segment_contexts_' that are available for reuse
    Freelist pending_segment_contexts_freelist_;
    // set of segments to be removed from the index upon commit
    SegmentMask segment_mask_;

    FlushContext() = default;

    ~FlushContext() noexcept { Reset(); }

    // add the segment to this flush_context
    void Emplace(ActiveSegmentContext&& segment, uint64_t first_operation_tick);

    // add the segment to this flush_context pending segments
    // but not to freelist. So this segment would be waited upon flushing
    bool AddToPending(ActiveSegmentContext& segment);

    void Reset() noexcept;
  };

  struct PendingContext {
    // Reference to flush context held until end of commit
    FlushContextPtr ctx{nullptr, nullptr};
    // Index meta of the next commit
    IndexMeta meta;
    // Segment readers of the next commit
    std::vector<SegmentReader> readers;
    // Files to sync
    std::vector<std::string_view> files_to_sync;

    bool Empty() const noexcept { return !ctx; }
  };

  static_assert(std::is_nothrow_move_constructible_v<PendingContext>);
  static_assert(std::is_nothrow_move_assignable_v<PendingContext>);

  struct PendingState {
    // reference to flush context held until end of commit
    FlushContextPtr ctx{nullptr, nullptr};
    // meta + references of next commit
    std::shared_ptr<const DirectoryReaderImpl> commit;

    void Reset() noexcept {
      ctx.reset();
      commit.reset();
    }

    bool Valid() const noexcept { return ctx && commit; }
  };

  static_assert(std::is_nothrow_move_constructible_v<PendingState>);
  static_assert(std::is_nothrow_move_assignable_v<PendingState>);

  std::pair<std::vector<std::unique_lock<std::mutex>>, uint64_t> FlushPending(
    FlushContext& ctx, std::unique_lock<std::mutex>& ctx_lock);

  PendingContext FlushAll(uint64_t tick,
                          ProgressReportCallback const& progress_callback);

  FlushContextPtr GetFlushContext(bool shared = true);

  // return a usable segment or a nullptr segment if
  // retry is required (e.g. no free segments available)
  ActiveSegmentContext GetSegmentContext(FlushContext& ctx);

  // Return next segment identifier
  uint64_t NextSegmentId() noexcept;
  // Return current segment identifier
  uint64_t CurrentSegmentId() const noexcept;
  // Initialize new index meta
  void InitMeta(IndexMeta& meta, uint64_t tick) const;

  // Start transaction
  bool Start(uint64_t tick, ProgressReportCallback const& progress);
  void StartImpl(FlushContextPtr&& ctx, DirectoryMeta&& to_commit,
                 std::vector<SegmentReader>&& readers,
                 std::vector<std::string_view>&& files_to_sync);
  // Finish transaction
  void Finish();
  // Abort transaction
  void Abort() noexcept;

  FeatureInfoProvider feature_info_;
  ColumnInfoProvider column_info_;
  // provides payload for new segments
  PayloadProvider meta_payload_provider_;
  const Comparer* comparator_;
  format::ptr codec_;
  // guard for cached_segment_readers_, commit_pool_, meta_
  // (modification during commit()/defragment()), payload_buf_
  std::mutex commit_lock_;
  std::recursive_mutex consolidation_lock_;
  // segments that are under consolidation
  ConsolidatingSegments consolidating_segments_;
  // directory used for initialization of readers
  directory& dir_;
  // collection of contexts that collect data to be
  // flushed, 2 because just swap them
  std::vector<FlushContext> flush_context_pool_;
  // currently active context accumulating data to be
  // processed during the next flush
  std::atomic<FlushContext*> flush_context_;
  // latest/active index snapshot
  std::shared_ptr<const DirectoryReaderImpl> committed_reader_;
  // current state awaiting commit completion
  PendingState pending_state_;
  // limits for use with respect to segments
  SegmentLimits segment_limits_;
  // a cache of segments available for reuse
  SegmentPool segment_writer_pool_;
  // number of segments currently in use by the writer
  std::atomic_size_t segments_active_;
  std::atomic_uint64_t seg_counter_;  // segment counter
  uint64_t last_gen_;                 // last committed index meta generation
  index_meta_writer::ptr writer_;
  index_lock::ptr write_lock_;  // exclusive write lock for directory
  index_file_refs::ref_t write_lock_file_ref_;  // file ref for lock file
};

}  // namespace irs
