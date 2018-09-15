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

#ifndef IRESEARCH_INDEXWRITER_H
#define IRESEARCH_INDEXWRITER_H

#include "field_meta.hpp"
#include "index_meta.hpp"
#include "merge_writer.hpp"
#include "segment_reader.hpp"
#include "segment_writer.hpp"

#include "formats/formats.hpp"
#include "search/filter.hpp"

#include "utils/async_utils.hpp"
#include "utils/bitvector.hpp"
#include "utils/thread_utils.hpp"
#include "utils/object_pool.hpp"
#include "utils/string.hpp"
#include "utils/noncopyable.hpp"

#include <cassert>
#include <atomic>

NS_ROOT

// ----------------------------------------------------------------------------
// --SECTION--                                             forward declarations 
// ----------------------------------------------------------------------------

class bitvector; // forward declaration
struct directory;
class directory_reader;

class readers_cache final : util::noncopyable {
 public:
  readers_cache() = default;

  segment_reader emplace(directory& dir, const segment_meta& meta);
  void clear() NOEXCEPT;
  size_t purge(const std::unordered_set<string_ref>& segments) NOEXCEPT;

 private:
  std::mutex lock_;
  std::unordered_map<std::string, segment_reader> cache_;
}; // readers_cache

//////////////////////////////////////////////////////////////////////////////
/// @enum OpenMode
/// @brief defines how index writer should be opened
//////////////////////////////////////////////////////////////////////////////
enum OpenMode {
  ////////////////////////////////////////////////////////////////////////////
  /// @brief Creates new index repository. In case if repository already
  ///        exists, all contents will be cleared.
  ////////////////////////////////////////////////////////////////////////////
  OM_CREATE = 1,

  ////////////////////////////////////////////////////////////////////////////
  /// @brief Opens existsing index repository. In case if repository does not
  ///        exists, error will be generated.
  ////////////////////////////////////////////////////////////////////////////
  OM_APPEND = 2,

  ////////////////////////////////////////////////////////////////////////////
  /// @brief Do not lock index directory. Caller is responsible for providing
  ///        and maintaining exclusive write access, otherwise it may cause
  ///        index corruption
  ////////////////////////////////////////////////////////////////////////////
  OM_NOLOCK = 4
}; // OpenMode

ENABLE_BITMASK_ENUM(OpenMode);

////////////////////////////////////////////////////////////////////////////////
/// @class index_writer 
/// @brief The object is using for indexing data. Only one writer can write to
///        the same directory simultaneously.
///        Thread safe.
////////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API index_writer : util::noncopyable {
 private:
  struct flush_context; // forward declaration
  struct segment_context; // forward declaration

  typedef std::unique_ptr<
    flush_context,
    void(*)(flush_context*) // sizeof(std::function<void(flush_context*)>) > sizeof(void(*)(flush_context*))
  > flush_context_ptr; // unique pointer required since need ponter declaration before class declaration e.g. for 'documents_context'

  typedef std::shared_ptr<
    segment_context
  > segment_context_ptr; // declaration from segment_context::ptr below
 public:

  //////////////////////////////////////////////////////////////////////////////
  /// @brief a context allowing index modification operations
  /// @note the object is non-thread-safe, each thread should use its own
  ///       separate instance
  //////////////////////////////////////////////////////////////////////////////
  class IRESEARCH_API documents_context {
   public:
    ////////////////////////////////////////////////////////////////////////////
    /// @brief a wrapper around a segment_writer::document with commit/rollback
    ////////////////////////////////////////////////////////////////////////////
    class IRESEARCH_API document: public segment_writer::document {
     public:
      document(
        const flush_context_ptr& ctx,
        const segment_context_ptr& segment,
        const segment_writer::update_context& update
      );
      document(document&& other);
      ~document();

     private:
      flush_context& ctx_; // reference to flush_context for rollback operations
      segment_context_ptr segment_; // hold reference to segment to prevent if from going back into the pool
      size_t update_id_;
    };

    explicit documents_context(index_writer& writer) NOEXCEPT
      : ctx_(nullptr), writer_(writer) {
    }

    ////////////////////////////////////////////////////////////////////////////
    /// @brief create a document to filled by the caller
    ///        for insertion into the index index
    ///        applied upon return value deallocation
    /// @note the changes are not visible until commit()
    ////////////////////////////////////////////////////////////////////////////
    document insert() {
      // thread-safe to use ctx_/segment_ while have lock since active flush_context will not change
      auto ctx = update_segment(); // updates 'segment_' and 'ctx_'

      return document(
        std::move(ctx), segment_, writer_.make_update_context(*ctx_)
      );
    }

    ////////////////////////////////////////////////////////////////////////////
    /// @brief marks all documents matching the filter for removal
    /// @param filter the filter selecting which documents should be removed
    /// @note that changes are not visible until commit()
    /// @note that filter must be valid until commit()
    ////////////////////////////////////////////////////////////////////////////
    template<typename Filter>
    void remove(Filter&& filter) {
      auto ctx = writer_.get_flush_context();

      writer_.remove(*ctx, std::forward<Filter>(filter));
    }

    ////////////////////////////////////////////////////////////////////////////
    /// @brief create a document to filled by the caller
    ///        for replacement of existing documents already in the index
    ///        matching filter with the filled document
    ///        applied upon return value deallocation
    /// @param filter the filter selecting which documents should be replaced
    /// @note the changes are not visible until commit()
    /// @note that filter must be valid until commit()
    ////////////////////////////////////////////////////////////////////////////
    template<typename Filter>
    document replace(Filter&& filter) {
      // thread-safe to use ctx_/segment_ while have lock since active flush_context will not change
      auto ctx = update_segment(); // updates 'segment_' and 'ctx_'

      return document(
        std::move(ctx),
        segment_,
        writer_.make_update_context(*ctx, std::forward<Filter>(filter))
      );
    }

    ////////////////////////////////////////////////////////////////////////////
    /// @brief replace existing documents already in the index matching filter
    ///        with the documents filled by the specified functor
    /// @param filter the filter selecting which documents should be replaced
    /// @param func the insertion logic, similar in signature to e.g.:
    ///        std::function<bool(segment_writer::document&)>
    /// @note the changes are not visible until commit()
    /// @note that filter must be valid until commit()
    /// @return all fields/attributes successfully insterted
    ////////////////////////////////////////////////////////////////////////////
    template<typename Filter, typename Func>
    bool replace(Filter&& filter, Func func) {
      flush_context* ctx;
      segment_context_ptr segment;
      {
        // thread-safe to use ctx_/segment_ while have lock since active flush_context will not change
        auto ctx_ptr = update_segment(); // updates 'segment_' and 'ctx_'

        assert(ctx_ptr);
        assert(segment_);
        assert(segment_->writer_);
        ctx = ctx_ptr.get(); // make copies in case 'func' causes their reload
        segment = segment_; // make copies in case 'func' causes their reload
        segment->busy_ = true; // guarded by flush_context::flush_mutex_
      }

      auto clear_busy = make_finally([ctx, segment]()->void {
        SCOPED_LOCK(ctx->mutex_); // lock due to context modification and notification
        segment->busy_ = false; // guarded by flush_context::mutex_ @see flush_all()
        ctx->pending_segment_context_cond_.notify_all(); // in case ctx is in flush_all()
      });
      auto& writer = *(segment_->writer_);
      segment_writer::document doc(writer);
      std::exception_ptr exception;
      bitvector rollback; // doc_ids to roll back on failure
      auto update = make_update_context(*ctx_, std::forward<Filter>(filter));

      try {
        for(;;) {
          rollback.set(writer.docs_cached());
          writer.begin(update, rollback.count());
          segment->buffered_docs.store(writer.docs_cached());

          auto done = !func(doc);

          if (writer.valid()) {
            writer.commit();

            if (done) {
              return true;
            }
          }
        }
      } catch (...) {
        exception = std::current_exception(); // track exception
      }

      // .......................................................................
      // perform rollback
      // implicitly NOEXCEPT since memory reserved in the call to begin(...)
      // .......................................................................

      writer.rollback(); // mark as failed

      for (auto i = rollback.size(); i && rollback.any();) {
        if (rollback.test(--i)) {
          rollback.unset(i); // if new doc_ids at end this allows to terminate 'for' earlier
          writer.remove(i);
        }
      }

      SCOPED_LOCK(ctx->mutex_); // lock due to context modification
      ctx->modification_queries_[update.update_id].filter = nullptr; // mark invalid

      if (exception) {
        std::rethrow_exception(exception);
      }

      return false;
    }

   private:
    flush_context* ctx_; // the context that the segment_context was obtained from
    segment_context_ptr segment_;
    index_writer& writer_;

    // refresh segment if required (guarded by flush_context::flush_mutex_)
    // is is thread-safe to use ctx_/segment_ while holding 'flush_context_ptr'
    // since active 'flush_context' will not change and hence no reload required
    flush_context_ptr update_segment();
  };

  struct segment_hash {
    size_t operator()(
        const segment_meta* segment
    ) const NOEXCEPT {
      return hash_utils::hash(segment->name);
    }
  }; // segment_hash

  struct segment_equal {
    size_t operator()(
        const segment_meta* lhs,
        const segment_meta* rhs
    ) const NOEXCEPT {
      return lhs->name == rhs->name;
    }
  }; // segment_equal

  // works faster than std::unordered_set<string_ref>
  typedef std::unordered_set<
    const segment_meta*,
    segment_hash,
    segment_equal
  > consolidating_segments_t; // segments that are under consolidation

  DECLARE_SHARED_PTR(index_writer);

  ////////////////////////////////////////////////////////////////////////////
  /// @brief mark consolidation candidate segments matching the current policy
  /// @param candidates the segments that should be consolidated
  ///        in: segment candidates that may be considered by this policy
  ///        out: actual segments selected by the current policy
  /// @param dir the segment directory
  /// @param meta the index meta containing segments to be considered
  /// @param consolidating_segments segments that are currently in progress
  ///        of consolidation
  /// @note final candidates are all segments selected by at least some policy
  ////////////////////////////////////////////////////////////////////////////
  typedef std::function<void(
    std::set<const segment_meta*>& candidates,
    const directory& dir,
    const index_meta& meta,
    const consolidating_segments_t& consolidating_segments
  )> consolidation_policy_t;

  ////////////////////////////////////////////////////////////////////////////
  /// @brief name of the lock for index repository 
  ////////////////////////////////////////////////////////////////////////////
  static const std::string WRITE_LOCK_NAME;

  ////////////////////////////////////////////////////////////////////////////
  /// @brief opens new index writer
  /// @param dir directory where index will be should reside
  /// @param codec format that will be used for creating new index segments
  /// @param mode specifies how to open a writer
  /// @param memory_pool_size [expert] number of memory blocks to cache for
  ///        internal memory pool
  ////////////////////////////////////////////////////////////////////////////
  static index_writer::ptr make(
    directory& dir,
    format::ptr codec,
    OpenMode mode,
    size_t memory_pool_size = 0
  );

  ////////////////////////////////////////////////////////////////////////////
  /// @brief destructor 
  ////////////////////////////////////////////////////////////////////////////
  ~index_writer();

  ////////////////////////////////////////////////////////////////////////////
  /// @returns overall number of buffered documents in a writer 
  ////////////////////////////////////////////////////////////////////////////
  uint64_t buffered_docs() const;

  ////////////////////////////////////////////////////////////////////////////
  /// @brief Clears the existing index repository by staring an empty index.
  ///        Previously opened readers still remain valid.
  ////////////////////////////////////////////////////////////////////////////
  void clear();

  ////////////////////////////////////////////////////////////////////////////
  /// @brief merges segments accepted by the specified defragment policty into
  ///        a new segment. For all accepted segments frees the space occupied
  ///        by the doucments marked as deleted and deduplicate terms.
  /// @param policy the speicified defragmentation policy
  /// @param codec desired format that will be used for segment creation,
  ///        nullptr == use index_writer's codec
  /// @note for deffered policies during the commit stage each policy will be
  ///       given the exact same index_meta containing all segments in the
  ///       commit, however, the resulting acceptor will only be segments not
  ///       yet marked for consolidation by other policies in the same commit
  ////////////////////////////////////////////////////////////////////////////
  bool consolidate(
    const consolidation_policy_t& policy, format::ptr codec = nullptr
  );

  //////////////////////////////////////////////////////////////////////////////
  /// @return returns a context allowing index modification operations
  /// @note all document insertions will be applied to the same segment on a
  ///       best effort basis, e.g. a flush_all() will cause a segment switch
  //////////////////////////////////////////////////////////////////////////////
  documents_context documents() NOEXCEPT {
    return documents_context(*this);
  }

  ////////////////////////////////////////////////////////////////////////////
  /// @brief imports index from the specified index reader into new segment
  /// @param reader the index reader to import 
  /// @param desired format that will be used for segment creation,
  ///        nullptr == use index_writer's codec
  /// @returns true on success
  ////////////////////////////////////////////////////////////////////////////
  bool import(const index_reader& reader, format::ptr codec = nullptr);

  ////////////////////////////////////////////////////////////////////////////
  /// @brief begins the two-phase transaction
  /// @returns true if transaction has been sucessflully started
  ////////////////////////////////////////////////////////////////////////////
  bool begin();

  ////////////////////////////////////////////////////////////////////////////
  /// @brief rollbacks the two-phase transaction 
  ////////////////////////////////////////////////////////////////////////////
  void rollback();

  ////////////////////////////////////////////////////////////////////////////
  /// @brief make all buffered changes visible for readers
  ///
  /// Note that if begin() has been already called commit() is 
  /// relatively lightweight operation 
  ////////////////////////////////////////////////////////////////////////////
  void commit();

  ////////////////////////////////////////////////////////////////////////////
  /// @brief closes writer object 
  ////////////////////////////////////////////////////////////////////////////
  void close();

 private:
  typedef std::vector<index_file_refs::ref_t> file_refs_t;

  struct modification_context {
    typedef std::shared_ptr<const irs::filter> filter_ptr;

    filter_ptr filter; // keep a handle to the filter for the case when this object has ownership
    const size_t generation;
    const bool update; // this is an update modification (as opposed to remove)
    bool seen;
    modification_context(const irs::filter& match_filter, size_t gen, bool isUpdate)
      : filter(filter_ptr(), &match_filter), generation(gen), update(isUpdate), seen(false) {}
    modification_context(const filter_ptr& match_filter, size_t gen, bool isUpdate)
      : filter(match_filter), generation(gen), update(isUpdate), seen(false) {}
    modification_context(irs::filter::ptr&& match_filter, size_t gen, bool isUpdate)
      : filter(std::move(match_filter)), generation(gen), update(isUpdate), seen(false) {}
    modification_context(modification_context&& other) NOEXCEPT
      : filter(std::move(other.filter)), generation(other.generation), update(other.update), seen(other.seen) {}
    modification_context& operator=(const modification_context& other) = delete; // no default constructor
  }; // modification_context

  struct consolidation_context_t : util::noncopyable {
    consolidation_context_t() = default;

    consolidation_context_t(consolidation_context_t&& rhs) NOEXCEPT
      : consolidaton_meta(std::move(rhs.consolidaton_meta)),
        candidates(std::move(rhs.candidates)),
        merger(std::move(rhs.merger)) {
    }

    consolidation_context_t(
        std::shared_ptr<index_meta>&& consolidaton_meta,
        std::set<const segment_meta*>&& candidates,
        merge_writer&& merger) NOEXCEPT
      : consolidaton_meta(std::move(consolidaton_meta)),
        candidates(std::move(candidates)),
        merger(std::move(merger)) {
    }

    consolidation_context_t(
        std::shared_ptr<index_meta>&& consolidaton_meta,
        std::set<const segment_meta*>&& candidates) NOEXCEPT
      : consolidaton_meta(std::move(consolidaton_meta)),
        candidates(std::move(candidates)) {
    }

    std::shared_ptr<index_meta> consolidaton_meta;
    std::set<const segment_meta*> candidates;
    merge_writer merger;
  }; // consolidation_context_t

  struct import_context {
    import_context(
        index_meta::index_segment_t&& segment,
        size_t generation,
        file_refs_t&& refs,
        std::set<const segment_meta*>&& consolidation_candidates,
        std::shared_ptr<index_meta>&& consolidation_meta,
        merge_writer&& merger
    ) NOEXCEPT
      : generation(generation),
        segment(std::move(segment)),
        refs(std::move(refs)),
        consolidation_ctx(std::move(consolidation_meta), std::move(consolidation_candidates), std::move(merger)) {
    }

    import_context(
        index_meta::index_segment_t&& segment,
        size_t generation,
        file_refs_t&& refs,
        std::set<const segment_meta*>&& consolidation_candidates,
        std::shared_ptr<index_meta>&& consolidation_meta
    ) NOEXCEPT
      : generation(generation),
        segment(std::move(segment)),
        refs(std::move(refs)),
        consolidation_ctx(std::move(consolidation_meta), std::move(consolidation_candidates)) {
    }

    import_context(
        index_meta::index_segment_t&& segment,
        size_t generation,
        file_refs_t&& refs,
        std::set<const segment_meta*>&& consolidation_candidates
    ) NOEXCEPT
      : generation(generation),
        segment(std::move(segment)),
        refs(std::move(refs)),
        consolidation_ctx(nullptr, std::move(consolidation_candidates)) {
    }

    import_context(
        index_meta::index_segment_t&& segment,
        size_t generation,
        file_refs_t&& refs
    ) NOEXCEPT
      : generation(generation),
        segment(std::move(segment)),
        refs(std::move(refs)) {
    }

    import_context(
        index_meta::index_segment_t&& segment,
        size_t generation
    ) NOEXCEPT
      : generation(generation),
        segment(std::move(segment)) {
    }

    import_context(import_context&& other) NOEXCEPT
      : generation(other.generation),
        segment(std::move(other.segment)),
        refs(std::move(other.refs)),
        consolidation_ctx(std::move(other.consolidation_ctx)) {
    }

    import_context& operator=(const import_context&) = delete;

    const size_t generation;
    index_meta::index_segment_t segment;
    file_refs_t refs;
    consolidation_context_t consolidation_ctx;
  }; // import_context

  //////////////////////////////////////////////////////////////////////////////
  /// @brief the segment writer and its associated ref tracing directory
  ///        for use with an unbounded_object_pool
  /// @note the segment flows through following stages
  ///        1a) taken from pool (!busy_, !dirty) {Thread A}
  ///        2a) requested by documents() (!busy_, !dirty)
  ///        3a) documents() validates that active context is the same && !dirty_
  ///        4a) documents() sets 'busy_', guarded by flush_context::flush_mutex_
  ///        5a) documents() starts operation
  ///        6a) documents() finishes operation
  ///        7a) documents() unsets 'busy_', guarded by flush_context::mutex_ (different mutex for cond notify)
  ///        8a) documents() notifies flush_context::pending_segment_context_cond_
  ///        ... after some time ...
  ///       10a) documents() validates that active context is the same && !dirty_
  ///       11a) documents() sets 'busy_', guarded by flush_context::flush_mutex_
  ///       12a) documents() starts operation
  ///       13b) flush_all() switches active context {Thread B}
  ///       14b) flush_all() sets 'dirty_', guarded by flush_context::mutex_
  ///       15b) flush_all() checks 'busy_' and waits on flush_context::mutex_ (different mutex for cond notify)
  ///       16a) documents() finishes operation {Thread A}
  ///       17a) documents() unsets 'busy_', guarded by flush_context::mutex_ (different mutex for cond notify)
  ///       18a) documents() notifies flush_context::pending_segment_context_cond_
  ///       19b) flush_all() checks 'busy_' and continues flush {Thread B} (different mutex for cond notify)
  ///       {scenario 1} ... after some time reuse of same documents() {Thread A}
  ///       20a) documents() validates that active context is not the same
  ///       21a) documents() re-requests a new segment, i.e. continues to (1a)
  ///       {scenario 2} ... after some time reuse of same documents() {Thread A}
  ///       20a) documents() validates that active context is the same && dirty_
  ///       21a) documents() re-requests a new segment, i.e. continues to (1a)
  //////////////////////////////////////////////////////////////////////////////
  struct segment_context {
    DECLARE_SHARED_PTR(segment_context);
    std::atomic<size_t> buffered_docs; // for use with index_writer::buffered_docs() asynchronous call
    bool busy_; // true when in use by one of the documents() operations (insert/replace), guarded by the flush_context::flush_mutex_ (during set) and flush_context::mutex_ (during unset to allow notify)
    bool dirty_; // true if flush_all() started processing this segment (this segment should not be used for any new operations), guarded by the flush_context::flush_mutex_
    ref_tracking_directory dir_;
    segment_writer::ptr writer_;

    DECLARE_FACTORY(directory& dir)
    segment_context(directory& dir);
    void reset();
  };

  typedef std::shared_ptr<
    std::pair<std::shared_ptr<index_meta>,
    file_refs_t
  >> committed_state_t;

  typedef std::vector<modification_context> modification_requests_t;
  typedef unbounded_object_pool<segment_context> segment_pool_t;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief the context containing data collected for the next commit() call
  /// @note a 'segment_context' is tracked by at most 1 'flush_context', it is
  ///       the job of the 'documents_context' to garantee that the
  ///       'segment_context' is not used once the tracker 'flush_context' is no
  ///       longer active
  //////////////////////////////////////////////////////////////////////////////
  struct IRESEARCH_API flush_context {
    std::atomic<size_t> generation_{ 0 }; // current modification/update generation
    ref_tracking_directory::ptr dir_; // ref tracking directory used by this context (tracks all/only refs for this context)
    async_utils::read_write_mutex flush_mutex_; // guard for the current context during flush (write) operations vs update (read)
    modification_requests_t modification_queries_; // sequential list of modification requests (remove/update)
    std::mutex mutex_; // guard for the current context during struct update operations, e.g. modification_queries_, pending_segments_
    flush_context* next_context_; // the next context to switch to
    std::vector<import_context> pending_segments_; // complete segments to be added during next commit (import)
    std::vector<segment_context::ptr> pending_segment_contexts_; // segment writers with data pending for next commit (all segments that have been used by this flush_context)
    std::condition_variable pending_segment_context_cond_; // notified when a segment has been freed (guarded by mutex_)
    std::unordered_set<string_ref> segment_mask_; // set of segment names to be removed from the index upon commit (refs at strings in index_writer::meta_)

    flush_context() = default;

    ~flush_context() NOEXCEPT {
      reset();
    }

    void reset() NOEXCEPT;
  }; // flush_context

  struct sync_context : util::noncopyable {
    sync_context() = default;
    sync_context(sync_context&& rhs) NOEXCEPT
      : files(std::move(rhs.files)),
        segments(std::move(rhs.segments)) {
    }
    sync_context& operator=(sync_context&& rhs) NOEXCEPT {
      if (this != &rhs) {
        files = std::move(rhs.files);
        segments = std::move(rhs.segments);
      }
      return *this;
    }

    bool empty() const NOEXCEPT {
      return segments.empty();
    }

    void register_full_sync(size_t i) {
      segments.emplace_back(i, 0);
    }

    void register_partial_sync(size_t i, const std::string& file) {
      segments.emplace_back(i, 1);
      files.emplace_back(file);
    }

    template<typename Visitor>
    bool visit(const Visitor& visitor, const index_meta& meta) const {
      auto begin = files.begin();

      for (auto& entry : segments) {
        auto& segment = meta[entry.first];

        if (entry.second) {
          // partial update
          assert(begin <= files.end());

          if (integer_traits<size_t>::const_max == entry.second) {
            // skip invalid segments
            begin += entry.second;
            continue;
          }

          for (auto end = begin + entry.second; begin != end; ++begin) {
            if (!visitor(begin->get())) {
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

    std::vector<std::reference_wrapper<const std::string>> files; // files to sync
    std::vector<std::pair<size_t, size_t>> segments; // segments to sync (index within index meta + number of files to sync)
  }; // sync_context

  struct pending_context_t {
    flush_context_ptr ctx{ nullptr, nullptr }; // reference to flush context held until end of commit
    index_meta::ptr meta; // index meta of next commit
    sync_context to_sync; // file names and segments to be synced during next commit

    pending_context_t() = default;
    pending_context_t(pending_context_t&& other) NOEXCEPT
      : ctx(std::move(other.ctx)),
        meta(std::move(other.meta)),
        to_sync(std::move(other.to_sync)) {
    }
    operator bool() const NOEXCEPT { return ctx && meta; }
  }; // pending_context_t

  struct pending_state_t {
    flush_context_ptr ctx{ nullptr, nullptr }; // reference to flush context held until end of commit
    committed_state_t commit; // meta + references of next commit

    operator bool() const NOEXCEPT { return ctx && commit; }
    void reset() NOEXCEPT { ctx.reset(), commit.reset(); }
  }; // pending_state_t

  index_writer(
    index_lock::ptr&& lock, 
    index_file_refs::ref_t&& lock_file_ref,
    directory& dir, 
    format::ptr codec,
    index_meta&& meta, 
    committed_state_t&& committed_state
  ) NOEXCEPT;

  bool add_document_mask_modified_records(
    modification_requests_t& requests,
    document_mask& docs_mask,
    segment_meta& meta,
    size_t min_doc_id_generation = 0
  ); // return if any new records were added (modification_queries_ modified)

  bool add_document_mask_modified_records(
    modification_requests_t& requests,
    segment_writer& writer,
    segment_meta& meta
  ); // return if any new records were added (modification_queries_ modified)

  static bool add_document_mask_unused_updates(
    modification_requests_t& requests,
    segment_writer& writer,
    segment_meta& meta
  ); // return if any new records were added (modification_queries_ modified)

  pending_context_t flush_all();

  flush_context_ptr get_flush_context(bool shared = true);
  segment_context_ptr get_segment_context(flush_context& ctx);

  // returns context for "add" operation
  static segment_writer::update_context make_update_context(flush_context& ctx);

  // returns context for "update" operation
  static segment_writer::update_context make_update_context(flush_context& ctx, const filter& filter);
  static segment_writer::update_context make_update_context(flush_context& ctx, const std::shared_ptr<filter>& filter);
  static segment_writer::update_context make_update_context(flush_context& ctx, filter::ptr&& filter);

  // modifies context for "remove" operation
  static void remove(flush_context& ctx, const filter& filter);
  static void remove(flush_context& ctx, const std::shared_ptr<filter>& filter);
  static void remove(flush_context& ctx, filter::ptr&& filter);

  bool start(); // starts transaction
  void finish(); // finishes transaction

  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  readers_cache cached_readers_; // readers by segment name
  format::ptr codec_;
  std::mutex commit_lock_; // guard for cached_segment_readers_, commit_pool_, meta_ (modification during commit()/defragment())
  committed_state_t committed_state_; // last successfully committed state
  directory& dir_; // directory used for initialization of readers
  std::vector<flush_context> flush_context_pool_; // collection of contexts that collect data to be flushed, 2 because just swap them
  std::atomic<flush_context*> flush_context_; // currently active context accumulating data to be processed during the next flush
  index_meta meta_; // latest/active state of index metadata
  pending_state_t pending_state_; // current state awaiting commit completion
  segment_pool_t segment_writer_pool_; // a cache of segments available for reuse
  index_meta_writer::ptr writer_;
  index_lock::ptr write_lock_; // exclusive write lock for directory
  index_file_refs::ref_t write_lock_file_ref_; // track ref for lock file to preven removal
  std::recursive_mutex consolidation_lock_;
  consolidating_segments_t consolidating_segments_; // segments that are under consolidation
  IRESEARCH_API_PRIVATE_VARIABLES_END
}; // index_writer

NS_END

#endif
