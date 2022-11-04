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

#ifndef IRESEARCH_INDEX_WRITER_H
#define IRESEARCH_INDEX_WRITER_H

#include <absl/container/flat_hash_map.h>

#include <atomic>
#include <functional>
#include <string_view>

#include "formats/formats.hpp"
#include "index/column_info.hpp"
#include "index/field_meta.hpp"
#include "index/index_features.hpp"
#include "index/index_meta.hpp"
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

namespace iresearch {

class comparer;
class bitvector;
struct directory;
class directory_reader;

class readers_cache final : util::noncopyable {
 public:
  struct key_t {
    // cppcheck-suppress noExplicitConstructor
    key_t(const segment_meta& meta);  // implicit constructor

    bool operator==(const key_t& other) const noexcept {
      return name == other.name && version == other.version;
    }

    std::string name;
    uint64_t version;
  };

  struct key_hash_t {
    size_t operator()(const key_t& key) const noexcept {
      return hash_utils::hash(key.name);
    }
  };

  // cppcheck-suppress constParameter
  explicit readers_cache(directory& dir) noexcept : dir_(dir) {}

  void clear() noexcept;
  segment_reader emplace(const segment_meta& meta);
  size_t purge(const absl::flat_hash_set<key_t, key_hash_t>& segments) noexcept;

 private:
  std::mutex lock_;
  absl::flat_hash_map<key_t, segment_reader, key_hash_t> cache_;
  directory& dir_;
};  // readers_cache

// Defines how index writer should be opened
enum OpenMode {
  // Creates new index repository. In case if repository already
  // exists, all contents will be cleared.
  OM_CREATE = 1,

  // Opens existsing index repository. In case if repository does not
  // exists, error will be generated.
  OM_APPEND = 2,
};

ENABLE_BITMASK_ENUM(OpenMode);

// The object is using for indexing data. Only one writer can write to
// the same directory simultaneously.
// Thread safe.
class index_writer : private util::noncopyable {
 private:
  struct flush_context;
  struct segment_context;

  // unique pointer required since need ponter declaration before class
  // declaration e.g. for 'documents_context'
  //
  // sizeof(std::function<void(flush_context*)>) >
  // sizeof(void(*)(flush_context*))
  using flush_context_ptr =
    std::unique_ptr<flush_context, void (*)(flush_context*)>;

  // declaration from segment_context::ptr below
  using segment_context_ptr = std::shared_ptr<segment_context>;

  // Segment references given out by flush_context to allow tracking
  // and updating flush_context::pending_segment_context
  //
  // non-copyable to ensure only one copy for get/put
  class active_segment_context : private util::noncopyable {
   public:
    active_segment_context() = default;
    active_segment_context(
      segment_context_ptr ctx, std::atomic<size_t>& segments_active,
      // the flush_context the segment_context is currently registered with
      flush_context* flush_ctx = nullptr,
      // the segment offset in flush_ctx_->pending_segments_
      size_t pending_segment_context_offset =
        std::numeric_limits<size_t>::max()) noexcept;
    active_segment_context(active_segment_context&&) = default;
    ~active_segment_context();
    active_segment_context& operator=(active_segment_context&& other) noexcept;

    const segment_context_ptr& ctx() const noexcept { return ctx_; }

   private:
    friend struct flush_context;  // for flush_context::emplace(...)
    segment_context_ptr ctx_{nullptr};
    // nullptr will not match any flush_context
    flush_context* flush_ctx_{nullptr};
    // segment offset in flush_ctx_->pending_segment_contexts_
    size_t pending_segment_context_offset_;
    // reference to index_writer::segments_active_
    std::atomic<size_t>* segments_active_;
  };

  static_assert(std::is_nothrow_move_constructible_v<active_segment_context>);

 public:
  // A context allowing index modification operations
  // The object is non-thread-safe, each thread should use its own
  // separate instance
  //
  // Noncopyable because of segments_
  class documents_context : private util::noncopyable {
   public:
    // A wrapper around a segment_writer::document with commit/rollback
    class document : public segment_writer::document {
     public:
      document(flush_context_ptr&& ctx, const segment_context_ptr& segment,
               const segment_writer::update_context& update);
      document(document&& other) noexcept;
      ~document() noexcept;

     private:
      // reference to flush_context for rollback operations
      flush_context& ctx_;
      // hold reference to segment to prevent if from going back into the pool
      segment_context_ptr segment_;
      size_t update_id_;
    };

    // cppcheck-suppress constParameter
    explicit documents_context(index_writer& writer) noexcept
      : writer_(writer) {}

    documents_context(documents_context&& other) noexcept
      : segment_(std::move(other.segment_)),
        segment_use_count_(std::move(other.segment_use_count_)),
        last_operation_tick_(other.last_operation_tick_),
        first_operation_tick_(other.first_operation_tick_),
        writer_(other.writer_) {
      other.last_operation_tick_ = 0;
      other.first_operation_tick_ = 0;
      other.segment_use_count_ = 0;
    }

    ~documents_context() noexcept;

    // Create a document to filled by the caller
    // for insertion into the index index
    // applied upon return value deallocation
    // `disable_flush` don't trigger segment flush
    //
    // The changes are not visible until commit()
    document insert(bool disable_flush = false) {
      // thread-safe to use ctx_/segment_ while have lock since active
      // flush_context will not change

      // updates 'segment_' and 'ctx_'
      auto ctx = update_segment(disable_flush);
      assert(segment_.ctx());

      return document(std::move(ctx), segment_.ctx(),
                      segment_.ctx()->make_update_context());
    }

    // Marks all documents matching the filter for removal.
    // Filter the filter selecting which documents should be removed.
    // Note that changes are not visible until commit().
    // Note that filter must be valid until commit().
    template<typename Filter>
    void remove(Filter&& filter) {
      // thread-safe to use ctx_/segment_ while have lock since active
      // flush_context will not change cppcheck-suppress unreadVariable
      auto ctx = update_segment(false);  // updates 'segment_' and 'ctx_'
      assert(segment_.ctx());

      // guarded by flush_context::flush_mutex_
      segment_.ctx()->remove(std::forward<Filter>(filter));
    }

    // Create a document to filled by the caller
    // for replacement of existing documents already in the index
    // matching filter with the filled document
    // applied upon return value deallocation
    // filter the filter selecting which documents should be replaced
    // Note the changes are not visible until commit()
    // Note that filter must be valid until commit()
    template<typename Filter>
    document replace(Filter&& filter) {
      // thread-safe to use ctx_/segment_ while have lock since active
      // flush_context will not change
      auto ctx = update_segment(false);  // updates 'segment_' and 'ctx_'
      assert(segment_.ctx());

      return document(
        std::move(ctx), segment_.ctx(),
        segment_.ctx()->make_update_context(std::forward<Filter>(filter)));
    }

    // Replace existing documents already in the index matching filter
    // with the documents filled by the specified functor
    // filter the filter selecting which documents should be replaced
    // func the insertion logic, similar in signature to e.g.:
    // std::function<bool(segment_writer::document&)>
    // Note the changes are not visible until commit()
    // Note that filter must be valid until commit()
    // Return all fields/attributes successfully insterted
    //         if false && valid() then it is safe to retry the operation
    //         e.g. if the segment is full and a new one must be started
    template<typename Filter, typename Func>
    bool replace(Filter&& filter, Func func) {
      flush_context* ctx;
      segment_context_ptr segment;

      {
        // thread-safe to use ctx_/segment_ while have lock since active
        // flush_context will not change
        auto ctx_ptr = update_segment(false);  // updates 'segment_' and 'ctx_'

        assert(ctx_ptr);
        assert(segment_.ctx());
        assert(segment_.ctx()->writer_);
        ctx = ctx_ptr.get();
        segment = segment_.ctx();
        ++segment->active_count_;
      }

      auto clear_busy = make_finally([ctx, segment]() noexcept {
        // FIXME make me noexcept as I'm begin called from within ~finally()
        if (!--segment->active_count_) {
          // lock due to context modification and notification
          std::lock_guard lock{ctx->mutex_};
          // in case ctx is in flush_all()
          ctx->pending_segment_context_cond_.notify_all();
        }
      });
      auto& writer = *(segment->writer_);
      segment_writer::document doc(writer);
      std::exception_ptr exception;
      // 0-based offsets to rollback on failure for this specific replace(..)
      // operation

      // cppcheck-suppress shadowFunction
      bitvector rollback;
      auto uncomitted_doc_id_begin =
        segment->uncomitted_doc_id_begin_ >
            segment->flushed_update_contexts_.size()
          // uncomitted start in 'writer_'
          ? (segment->uncomitted_doc_id_begin_ -
             segment->flushed_update_contexts_.size())
          // uncommited start in 'flushed_'
          : doc_limits::min();
      auto update = segment->make_update_context(std::forward<Filter>(filter));

      try {
        for (;;) {
          assert(uncomitted_doc_id_begin <=
                 writer.docs_cached() + doc_limits::min());
          // ensure reset() will be noexcept
          auto rollback_extra =
            writer.docs_cached() + doc_limits::min() - uncomitted_doc_id_begin;
          // reserve space for rollback
          rollback.reserve(writer.docs_cached() + 1);

          if (std::numeric_limits<doc_id_t>::max() <=
                writer.docs_cached() + doc_limits::min() ||
              doc_limits::eof(writer.begin(update, rollback_extra))) {
            break;  // the segment cannot fit any more docs, must roll back
          }

          assert(writer.docs_cached());
          rollback.set(writer.docs_cached() - 1);  // 0-based
          segment->buffered_docs_.store(writer.docs_cached());

          auto done = !func(doc);

          if (writer.valid()) {
            writer.commit();

            if (done) {
              return true;
            }
          }
        }
      } catch (...) {
        exception = std::current_exception();  // track exception
      }

      // perform rollback
      // implicitly noexcept since memory reserved in the call to begin(...)

      writer.rollback();  // mark as failed

      for (auto i = rollback.size(); i && rollback.any();) {
        if (rollback.test(--i)) {
          rollback.unset(
            i);  // if new doc_ids at end this allows to terminate 'for' earlier
          assert(std::numeric_limits<doc_id_t>::max() >= i + doc_limits::min());
          writer.remove(doc_id_t(i + doc_limits::min()));  // convert to doc_id
        }
      }

      segment->modification_queries_[update.update_id].filter =
        nullptr;  // mark invalid

      if (exception) {
        std::rethrow_exception(exception);
      }

      return false;
    }

    // Revert all pending document modifications and release resources
    // noexcept because all insertions reserve enough space for rollback
    void reset() noexcept;

    void SetLastTick(uint64_t tick) noexcept { last_operation_tick_ = tick; }
    uint64_t GetLastTick() const noexcept { return last_operation_tick_; }

    void SetFirstTick(uint64_t tick) noexcept { first_operation_tick_ = tick; }
    uint64_t GetFirstTick() const noexcept { return first_operation_tick_; }

    void AddToFlush();

   private:
    // the segment_context used for storing changes (lazy-initialized)
    active_segment_context segment_;
    // segment_.ctx().use_count() at constructor/destructor time must equal
    uint64_t segment_use_count_{0};
    uint64_t last_operation_tick_{0};   // transaction commit tick
    uint64_t first_operation_tick_{0};  // transaction tick
    index_writer& writer_;

    // refresh segment if required (guarded by flush_context::flush_mutex_)
    // is is thread-safe to use ctx_/segment_ while holding 'flush_context_ptr'
    // since active 'flush_context' will not change and hence no reload required
    flush_context_ptr update_segment(bool disable_flush);
  };

  // progress report callback types for commits.
  using progress_report_callback =
    std::function<void(std::string_view phase, size_t current, size_t total)>;

  // Additional information required for removal/update requests
  struct modification_context {
    using filter_ptr = std::shared_ptr<const irs::filter>;

    modification_context(const irs::filter& match_filter, size_t gen,
                         bool isUpdate)
      : filter(filter_ptr(), &match_filter),
        generation(gen),
        update(isUpdate),
        seen(false) {}
    modification_context(const filter_ptr& match_filter, size_t gen,
                         bool isUpdate)
      : filter(match_filter), generation(gen), update(isUpdate), seen(false) {}
    modification_context(irs::filter::ptr&& match_filter, size_t gen,
                         bool isUpdate)
      : filter(std::move(match_filter)),
        generation(gen),
        update(isUpdate),
        seen(false) {}
    modification_context(modification_context&&) = default;
    modification_context& operator=(const modification_context&) = delete;
    modification_context& operator=(modification_context&&) = delete;

    // keep a handle to the filter for the case when this object has ownership
    filter_ptr filter;
    const size_t generation;
    // this is an update modification (as opposed to remove)
    const bool update;
    bool seen;
  };

  static_assert(std::is_nothrow_move_constructible_v<modification_context>);

  // Options the the writer should use for segments
  struct segment_options {
    // Segment aquisition requests will block and wait for free segments
    // after this many segments have been aquired e.g. via documents()
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

  // Functor for creating payload. Operation tick is provided for
  // payload generation.
  using payload_provider_t = std::function<bool(uint64_t, bstring&)>;

  // Options the the writer should use after creation
  struct init_options : public segment_options {
    // Returns column info for a feature the writer should use for
    // columnstore
    feature_info_provider_t features;

    // Returns column info the writer should use for columnstore
    column_info_provider_t column_info;

    // Provides payload for index_meta created by writer
    payload_provider_t meta_payload_provider;

    // Comparator defines physical order of documents in each segment
    // produced by an index_writer.
    // empty == use default system sorting order
    const comparer* comparator{nullptr};

    // Number of free segments cached in the segment pool for reuse
    // 0 == do not cache any segments, i.e. always create new segments
    size_t segment_pool_size{128};  // arbitrary size

    // Aquire an exclusive lock on the repository to guard against index
    // corruption from multiple index_writers
    bool lock_repository{true};

    init_options() {}  // GCC5 requires non-default definition
  };

  struct segment_hash {
    size_t operator()(const segment_meta* segment) const noexcept {
      return hash_utils::hash(segment->name);
    }
  };

  struct segment_equal {
    size_t operator()(const segment_meta* lhs,
                      const segment_meta* rhs) const noexcept {
      return lhs->name == rhs->name;
    }
  };

  // segments that are under consolidation
  using consolidating_segments_t =
    absl::flat_hash_set<const segment_meta*, segment_hash, segment_equal>;

  enum class ConsolidationError : uint32_t {
    // Consolidation failed
    FAIL = 0,

    // Consolidation succesfully finished
    OK,

    // Consolidation was scheduled for the upcoming commit
    PENDING
  };

  // Represents result of a consolidation
  struct consolidation_result {
    // Number of candidates
    size_t size;

    // Error code
    ConsolidationError error;

    // intentionally implicit
    operator bool() const noexcept { return error != ConsolidationError::FAIL; }
  };

  using ptr = std::shared_ptr<index_writer>;

  // A set of candidates denoting an instance of consolidation
  using consolidation_t = std::vector<const segment_meta*>;

  // Mark consolidation candidate segments matching the current policy
  // candidates the segments that should be consolidated
  // in: segment candidates that may be considered by this policy
  // out: actual segments selected by the current policy
  // dir the segment directory
  // meta the index meta containing segments to be considered
  // Consolidating_segments segments that are currently in progress
  // of consolidation
  // Final candidates are all segments selected by at least some policy
  using consolidation_policy_t =
    std::function<void(consolidation_t& candidates, const index_meta& meta,
                       const consolidating_segments_t& consolidating_segments)>;

  // Name of the lock for index repository
  static constexpr std::string_view kWriteLockName = "write.lock";

  ~index_writer() noexcept;

  // Returns overall number of buffered documents in a writer
  uint64_t buffered_docs() const;

  // Clears the existing index repository by staring an empty index.
  // Previously opened readers still remain valid.
  // truncate transaction tick
  // Call will rollback any opened transaction.
  void clear(uint64_t tick = 0);

  // Merges segments accepted by the specified defragment policty into
  // a new segment. For all accepted segments frees the space occupied
  // by the doucments marked as deleted and deduplicate terms.
  // Policy the speicified defragmentation policy
  // Codec desired format that will be used for segment creation,
  // nullptr == use index_writer's codec
  // Progress callback triggered for consolidation steps, if the
  // callback returns false then consolidation is aborted
  // For deffered policies during the commit stage each policy will be
  // given the exact same index_meta containing all segments in the
  // commit, however, the resulting acceptor will only be segments not
  // yet marked for consolidation by other policies in the same commit
  consolidation_result consolidate(
    const consolidation_policy_t& policy, format::ptr codec = nullptr,
    const merge_writer::flush_progress_t& progress = {});

  // Returns a context allowing index modification operations
  // All document insertions will be applied to the same segment on a
  // best effort basis, e.g. a flush_all() will cause a segment switch
  documents_context documents() noexcept { return documents_context(*this); }

  // Imports index from the specified index reader into new segment
  // Reader the index reader to import.
  // Desired format that will be used for segment creation,
  // nullptr == use index_writer's codec.
  // Progress callback triggered for consolidation steps, if the
  // callback returns false then consolidation is aborted.
  // Returns true on success.
  bool import(const index_reader& reader, format::ptr codec = nullptr,
              const merge_writer::flush_progress_t& progress = {});

  // Opens new index writer.
  // dir directory where index will be should reside
  // codec format that will be used for creating new index segments
  // mode specifies how to open a writer
  // options the configuration parameters for the writer
  static index_writer::ptr make(directory& dir, format::ptr codec,
                                OpenMode mode,
                                const init_options& opts = init_options());

  // Modify the runtime segment options as per the specified values
  // options will apply no later than after the next commit()
  void options(const segment_options& opts) noexcept { segment_limits_ = opts; }

  // Returns comparator using for sorting documents by a primary key
  // nullptr == default sort order
  const comparer* comparator() const noexcept { return comparator_; }

  // Begins the two-phase transaction.
  // payload arbitrary user supplied data to store in the index
  // Returns true if transaction has been sucessflully started.
  bool begin() {
    // cppcheck-suppress unreadVariable
    std::lock_guard lock{commit_lock_};

    return start();
  }

  // Rollbacks the two-phase transaction
  void rollback() {
    // cppcheck-suppress unreadVariable
    std::lock_guard lock{commit_lock_};

    abort();
  }

  // Make all buffered changes visible for readers.
  // payload arbitrary user supplied data to store in the index
  // Return whether any changes were committed.
  //
  // Note that if begin() has been already called commit() is
  // relatively lightweight operation.
  bool commit(progress_report_callback const& progress = nullptr) {
    // cppcheck-suppress unreadVariable
    std::lock_guard lock{commit_lock_};

    const bool modified = start(progress);
    finish();
    return modified;
  }

  // Clears index writer's reader cache.
  void purge_cached_readers() noexcept { cached_readers_.clear(); }

  // Returns field features.
  const feature_info_provider_t& feature_info() const noexcept {
    return feature_info_;
  }

 private:
  using file_refs_t = std::vector<index_file_refs::ref_t>;

  static constexpr size_t kNonUpdateRecord = std::numeric_limits<size_t>::max();

  struct consolidation_context_t : util::noncopyable {
    consolidation_context_t() = default;

    consolidation_context_t(consolidation_context_t&&) = default;
    consolidation_context_t& operator=(consolidation_context_t&&) = delete;

    consolidation_context_t(std::shared_ptr<index_meta>&& consolidaton_meta,
                            consolidation_t&& candidates,
                            merge_writer&& merger) noexcept
      : consolidaton_meta(std::move(consolidaton_meta)),
        candidates(std::move(candidates)),
        merger(std::move(merger)) {}

    consolidation_context_t(std::shared_ptr<index_meta>&& consolidaton_meta,
                            consolidation_t&& candidates) noexcept
      : consolidaton_meta(std::move(consolidaton_meta)),
        candidates(std::move(candidates)) {}

    std::shared_ptr<index_meta> consolidaton_meta;
    consolidation_t candidates;
    merge_writer merger;
  };

  static_assert(std::is_nothrow_move_constructible_v<consolidation_context_t>);

  struct import_context {
    import_context(index_meta::index_segment_t&& segment, size_t generation,
                   file_refs_t&& refs,
                   consolidation_t&& consolidation_candidates,
                   std::shared_ptr<index_meta>&& consolidation_meta,
                   merge_writer&& merger) noexcept
      : generation(generation),
        segment(std::move(segment)),
        refs(std::move(refs)),
        consolidation_ctx(std::move(consolidation_meta),
                          std::move(consolidation_candidates),
                          std::move(merger)) {}

    import_context(index_meta::index_segment_t&& segment, size_t generation,
                   file_refs_t&& refs,
                   consolidation_t&& consolidation_candidates,
                   std::shared_ptr<index_meta>&& consolidation_meta) noexcept
      : generation(generation),
        segment(std::move(segment)),
        refs(std::move(refs)),
        consolidation_ctx(std::move(consolidation_meta),
                          std::move(consolidation_candidates)) {}

    import_context(index_meta::index_segment_t&& segment, size_t generation,
                   file_refs_t&& refs,
                   consolidation_t&& consolidation_candidates) noexcept
      : generation(generation),
        segment(std::move(segment)),
        refs(std::move(refs)),
        consolidation_ctx(nullptr, std::move(consolidation_candidates)) {}

    import_context(index_meta::index_segment_t&& segment, size_t generation,
                   file_refs_t&& refs) noexcept
      : generation(generation),
        segment(std::move(segment)),
        refs(std::move(refs)) {}

    import_context(index_meta::index_segment_t&& segment,
                   size_t generation) noexcept
      : generation(generation), segment(std::move(segment)) {}

    import_context(import_context&&) = default;

    import_context& operator=(const import_context&) = delete;
    import_context& operator=(import_context&&) = delete;

    const size_t generation;
    index_meta::index_segment_t segment;
    file_refs_t refs;
    consolidation_context_t consolidation_ctx;
  };  // import_context

  static_assert(std::is_nothrow_move_constructible_v<import_context>);

  // The segment writer and its associated ref tracing directory
  // for use with an unbounded_object_pool
  //
  // Note the segment flows through following stages
  //  1a) taken from pool (!busy_, !dirty) {Thread A}
  //  2a) requested by documents() (!busy_, !dirty)
  //  3a) documents() validates that active context is the same &&
  //  !dirty_
  //  4a) documents() sets 'busy_', guarded by flush_context::flush_mutex_
  //  5a) documents() starts operation
  //  6a) documents() finishes operation
  //  7a) documents() unsets 'busy_', guarded by flush_context::mutex_
  //    (different mutex for cond notify)
  //  8a) documents() notifies flush_context::pending_segment_context_cond_
  //    ... after some time ...
  // 10a) documents() validates that active context is the same && !dirty_
  // 11a) documents() sets 'busy_', guarded by flush_context::flush_mutex_
  // 12a) documents() starts operation
  // 13b) flush_all() switches active context {Thread B}
  // 14b) flush_all() sets 'dirty_', guarded by flush_context::mutex_
  // 15b) flush_all() checks 'busy_' and waits on flush_context::mutex_
  //    (different mutex for cond notify)
  // 16a) documents() finishes operation {Thread A}
  // 17a) documents() unsets 'busy_', guarded by flush_context::mutex_
  //    (different mutex for cond notify)
  // 18a) documents() notifies flush_context::pending_segment_context_cond_
  // 19b) flush_all() checks 'busy_' and continues flush {Thread B}
  //    (different mutex for cond notify)  {scenario 1} ... after some time
  //    reuse of same documents() {Thread A}
  // 20a) documents() validates that active context is not the same
  // 21a) documents() re-requests a new segment, i.e. continues
  //    to (1a) {scenario 2} ...  after some time reuse of same
  //    documents() {Thread A}
  // 20a) documents() validates that active context is the same && dirty_
  // 21a) documents() re-requests a new segment, i.e. continues to (1a)
  // Note segment_writer::doc_contexts[...uncomitted_document_contexts_):
  //   generation == flush_context::generation
  // Note segment_writer::doc_contexts[uncomitted_document_contexts_...]:
  //   generation == local generation (updated when segment_context
  //   registered once again with flush_context)
  struct segment_context {
    struct flushed_t : public index_meta::index_segment_t {
      // starting doc_id that should be added to docs_mask
      doc_id_t docs_mask_tail_doc_id{doc_limits::eof()};

      flushed_t() = default;
      explicit flushed_t(segment_meta&& meta) noexcept
        : index_meta::index_segment_t{std::move(meta)} {}
    };

    using segment_meta_generator_t = std::function<segment_meta()>;
    using ptr = std::unique_ptr<segment_context>;

    // number of active in-progress
    // operations (insert/replace) (e.g.
    // document instances or replace(...))
    std::atomic<size_t> active_count_;
    // for use with index_writer::buffered_docs()
    // asynchronous call
    std::atomic<size_t> buffered_docs_;
    // the codec to used for flushing a segment writer
    format::ptr codec_;
    // true if flush_all() started processing this segment (this
    // segment should not be used for any new operations), guarded by
    // the flush_context::flush_mutex_
    std::atomic<bool> dirty_;
    // ref tracking for segment_writer to allow for easy ref removal on
    // segment_writer reset
    ref_tracking_directory dir_;
    // guard 'flushed_', 'uncomitted_*' and 'writer_'
    // from concurrent flush
    std::mutex flush_mutex_;
    // all of the previously flushed versions of this segment,
    // guarded by the flush_context::flush_mutex_
    std::vector<flushed_t> flushed_;
    // update_contexts to use with 'flushed_'
    // sequentially increasing through all offsets
    // (sequential doc_id in 'flushed_' == offset +
    // type_limits<type_t::doc_id_t>::min(), size()
    // == sum of all 'flushed_'.'docs_count')
    std::vector<segment_writer::update_context> flushed_update_contexts_;
    // function to get new segment_meta from
    segment_meta_generator_t meta_generator_;
    // sequential list of pending modification
    std::vector<modification_context> modification_queries_;
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
    segment_writer::ptr writer_;
    // the segment_meta this writer was initialized with
    index_meta::index_segment_t writer_meta_;

    static segment_context::ptr make(
      directory& dir, segment_meta_generator_t&& meta_generator,
      const column_info_provider_t& column_info,
      const feature_info_provider_t& feature_info, const comparer* comparator);

    segment_context(directory& dir, segment_meta_generator_t&& meta_generator,
                    const column_info_provider_t& column_info,
                    const feature_info_provider_t& feature_info,
                    const comparer* comparator);

    // Flush current writer state into a materialized segment.
    // Return tick of last committed transaction.
    uint64_t flush();

    // Returns context for "insert" operation.
    segment_writer::update_context make_update_context() const noexcept {
      return {uncomitted_generation_offset_, kNonUpdateRecord};
    }

    // Returns context for "update" operation.
    segment_writer::update_context make_update_context(const filter& filter);
    segment_writer::update_context make_update_context(
      const std::shared_ptr<filter>& filter);
    segment_writer::update_context make_update_context(filter::ptr&& filter);

    // Ensure writer is ready to recieve documents
    void prepare();

    // Modifies context for "remove" operation
    void remove(const filter& filter);
    void remove(const std::shared_ptr<filter>& filter);
    void remove(filter::ptr&& filter);

    // Reset segment state to the initial state
    // store_flushed should store info about flushed segments?
    // Note should be true if something went wrong during segment flush
    void reset(bool store_flushed = false) noexcept;
  };

  struct segment_limits {
    // see segment_options::max_segment_count
    std::atomic<size_t> segment_count_max;
    // see segment_options::max_segment_docs
    std::atomic<size_t> segment_docs_max;
    // see segment_options::max_segment_memory
    std::atomic<size_t> segment_memory_max;

    explicit segment_limits(const segment_options& opts) noexcept
      : segment_count_max(opts.segment_count_max),
        segment_docs_max(opts.segment_docs_max),
        segment_memory_max(opts.segment_memory_max) {}

    segment_limits& operator=(const segment_options& opts) noexcept {
      segment_count_max.store(opts.segment_count_max);
      segment_docs_max.store(opts.segment_docs_max);
      segment_memory_max.store(opts.segment_memory_max);
      return *this;
    }
  };

  using committed_state_t =
    std::shared_ptr<std::pair<std::shared_ptr<index_meta>, file_refs_t>>;
  using segment_pool_t = unbounded_object_pool<segment_context>;

  // The context containing data collected for the next commit() call
  // Note a 'segment_context' is tracked by at most 1 'flush_context', it is
  // the job of the 'documents_context' to garantee that the
  // 'segment_context' is not used once the tracker 'flush_context' is no
  // longer active.
  struct flush_context {
    // 'value' == node offset into 'pending_segment_context_'
    using freelist_t = concurrent_stack<size_t>;

    struct pending_segment_context : public freelist_t::node_type {
      // starting segment_context::document_contexts_ for this
      // flush_context range
      // [pending_segment_context::doc_id_begin_,
      // std::min(pending_segment_context::doc_id_end_,
      // segment_context::uncomitted_doc_ids_))
      const size_t doc_id_begin_;
      // ending segment_context::document_contexts_ for
      // this flush_context range
      // [pending_segment_context::doc_id_begin_,
      // std::min(pending_segment_context::doc_id_end_,
      // segment_context::uncomitted_doc_ids_))
      size_t doc_id_end_;
      // starting
      // segment_context::modification_queries_
      // for this flush_context range
      // [pending_segment_context::modification_offset_begin_,
      // std::min(pending_segment_context::::modification_offset_end_,
      // segment_context::uncomitted_modification_queries_))
      const size_t modification_offset_begin_;
      // ending
      // segment_context::modification_queries_ for
      // this flush_context range
      // [pending_segment_context::modification_offset_begin_,
      // std::min(pending_segment_context::::modification_offset_end_,
      // segment_context::uncomitted_modification_queries_))
      size_t modification_offset_end_;
      const std::shared_ptr<segment_context> segment_;

      pending_segment_context(const std::shared_ptr<segment_context>& segment,
                              size_t pending_segment_context_offset)
        : doc_id_begin_(segment->uncomitted_doc_id_begin_),
          doc_id_end_(std::numeric_limits<size_t>::max()),
          modification_offset_begin_(segment->uncomitted_modification_queries_),
          modification_offset_end_(std::numeric_limits<size_t>::max()),
          segment_(segment) {
        assert(segment);
        value = pending_segment_context_offset;
      }
    };

    // current modification/update generation
    std::atomic<size_t> generation_{0};
    // ref tracking directory used by this context (tracks all/only
    // refs for this context)
    ref_tracking_directory::ptr dir_;
    // guard for the current context during flush (write)
    // operations vs update (read)
    std::shared_mutex flush_mutex_;
    // guard for the current context during struct update operations,
    // e.g. pending_segments_, pending_segment_contexts_
    std::mutex mutex_;
    // the next context to switch to
    flush_context* next_context_;
    // complete segments to be added during next commit
    // (import)
    std::vector<import_context> pending_segments_;
    // notified when a segment has been freed
    // (guarded by mutex_)
    std::condition_variable pending_segment_context_cond_;
    // segment writers with data pending for next
    // commit (all segments that have been used by
    // this flush_context) must be std::deque to
    // garantee that element memory location does
    // not change for use with
    // 'pending_segment_contexts_freelist_'
    std::deque<pending_segment_context> pending_segment_contexts_;
    // entries from
    // 'pending_segment_contexts_' that
    // are available for reuse
    freelist_t pending_segment_contexts_freelist_;
    // set of segment names to be removed from the index upon commit
    absl::flat_hash_set<readers_cache::key_t, readers_cache::key_hash_t>
      segment_mask_;

    flush_context() = default;

    ~flush_context() noexcept { reset(); }

    // add the segment to this flush_context
    void emplace(active_segment_context&& segment,
                 uint64_t first_operation_tick);

    // add the segment to this flush_context pending segments
    // but not to freelist. So this segment would be waited upon flushing
    void AddToPending(active_segment_context& segment);

    void reset() noexcept;
  };

  struct sync_context : util::noncopyable {
    sync_context() = default;
    sync_context(sync_context&& rhs) noexcept
      : files(std::move(rhs.files)), segments(std::move(rhs.segments)) {}
    sync_context& operator=(sync_context&& rhs) noexcept {
      if (this != &rhs) {
        files = std::move(rhs.files);
        segments = std::move(rhs.segments);
      }
      return *this;
    }

    bool empty() const noexcept { return segments.empty(); }

    void register_full_sync(size_t i) { segments.emplace_back(i, 0); }

    void register_partial_sync(size_t i, std::string_view file) {
      segments.emplace_back(i, 1);
      files.emplace_back(file);
    }

    template<typename Visitor>
    bool visit(const Visitor& visitor, const index_meta& meta) const {
      // cppcheck-suppress shadowFunction
      auto begin = files.begin();

      for (auto& entry : segments) {
        auto& segment = meta[entry.first];

        if (entry.second) {
          // partial update
          assert(begin <= files.end());

          if (std::numeric_limits<size_t>::max() == entry.second) {
            // skip invalid segments
            begin += entry.second;
            continue;
          }

          for (auto end = begin + entry.second; begin != end; ++begin) {
            if (!visitor(*begin)) {
              return false;
            }
          }
        } else {
          // full sync
          for (auto& file : segment.meta.files) {
            if (!visitor(file)) {
              return false;
            }
          }
        }

        if (!visitor(segment.filename)) {
          return false;
        }
      }

      return true;
    }

    // files to sync
    std::vector<std::string> files;
    // segments to sync (index within index meta + number of files
    // to sync)
    std::vector<std::pair<size_t, size_t>> segments;
  };

  struct pending_context_t {
    // reference to flush context held until end of commit
    flush_context_ptr ctx{nullptr, nullptr};
    // index meta of next commit
    index_meta::ptr meta;
    // file names and segments to be synced during next commit
    sync_context to_sync;

    operator bool() const noexcept { return ctx && meta; }
  };

  static_assert(std::is_nothrow_move_constructible_v<pending_context_t>);
  static_assert(std::is_nothrow_move_assignable_v<pending_context_t>);

  struct pending_state_t {
    // reference to flush context held until end of commit
    flush_context_ptr ctx{nullptr, nullptr};
    // meta + references of next commit
    committed_state_t commit;

    operator bool() const noexcept { return ctx && commit; }

    void reset() noexcept {
      ctx.reset();
      commit.reset();
    }
  };

  static_assert(std::is_nothrow_move_constructible_v<pending_state_t>);
  static_assert(std::is_nothrow_move_assignable_v<pending_state_t>);

  index_writer(index_lock::ptr&& lock, index_file_refs::ref_t&& lock_file_ref,
               directory& dir, format::ptr codec, size_t segment_pool_size,
               const segment_options& segment_limits,
               const comparer* comparator,
               const column_info_provider_t& column_info,
               const feature_info_provider_t& feature_info,
               const payload_provider_t& meta_payload_provider,
               index_meta&& meta, committed_state_t&& committed_state);

  std::pair<std::vector<std::unique_lock<std::mutex>>, uint64_t> flush_pending(
    flush_context& ctx, std::unique_lock<std::mutex>& ctx_lock);

  pending_context_t flush_all(
    progress_report_callback const& progress_callback);

  flush_context_ptr get_flush_context(bool shared = true);

  // return a usable segment or a nullptr segment if
  // retry is required (e.g. no free segments available)
  active_segment_context get_segment_context(flush_context& ctx);

  // starts transaction
  bool start(progress_report_callback const& progress = nullptr);
  // finishes transaction
  void finish();
  // aborts transaction
  void abort();

  feature_info_provider_t feature_info_;
  std::vector<std::string_view> files_to_sync_;
  column_info_provider_t column_info_;
  // provides payload for new segments
  payload_provider_t meta_payload_provider_;
  const comparer* comparator_;
  // readers by segment name
  readers_cache cached_readers_;
  format::ptr codec_;
  // guard for cached_segment_readers_, commit_pool_, meta_
  // (modification during commit()/defragment()), paylaod_buf_
  std::mutex commit_lock_;
  committed_state_t committed_state_;  // last successfully committed state
  std::recursive_mutex consolidation_lock_;
  // segments that are under consolidation
  consolidating_segments_t consolidating_segments_;
  // directory used for initialization of readers
  directory& dir_;
  // collection of contexts that collect data to be
  // flushed, 2 because just swap them
  std::vector<flush_context> flush_context_pool_;
  // currently active context accumulating data to be
  // processed during the next flush
  std::atomic<flush_context*> flush_context_;
  // latest/active state of index metadata
  index_meta meta_;
  // current state awaiting commit completion
  pending_state_t pending_state_;
  // limits for use with respect to segments
  segment_limits segment_limits_;
  // a cache of segments available for reuse
  segment_pool_t segment_writer_pool_;
  // number of segments currently in use by the writer
  std::atomic<size_t> segments_active_;
  index_meta_writer::ptr writer_;
  // exclusive write lock for directory
  index_lock::ptr write_lock_;
  // track ref for lock file to preven removal
  index_file_refs::ref_t write_lock_file_ref_;
};

}  // namespace iresearch

#endif  // IRESEARCH_INDEX_WRITER_H
