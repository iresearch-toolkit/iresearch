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
 public:
  DECLARE_SHARED_PTR(index_writer);

  static const size_t THREAD_COUNT = 8;

  ////////////////////////////////////////////////////////////////////////////
  /// @brief mark consolidation candidate segments matching the current policy
  /// @param candidates the segments that should be consolidated
  ///        in: segment candidates that may be considered by this policy
  ///        out: actual segments selected by the current policy
  /// @param dir the segment directory
  /// @param meta the index meta containing segments to be considered
  /// @note final candidates are all segments selected by at least some policy
  ////////////////////////////////////////////////////////////////////////////
  typedef std::function<void(
    std::set<const segment_meta*>& candidates,
    const directory& dir,
    const index_meta& meta
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
  /// @brief inserts document to be filled by the specified functor into index
  /// @note that changes are not visible until commit()
  /// @note the specified 'func' should return false in order to break the
  ///       insertion loop
  /// @param func the insertion logic
  /// @return status of the last insert operation
  ////////////////////////////////////////////////////////////////////////////
  template<typename Func>
  bool insert(Func func) {
    auto ctx = get_flush_context(); // retain lock until end of insert(...)
    auto writer = get_segment_context(*ctx);
    segment_writer::document doc(*writer);
    bool has_next = true;

    do {
      writer->begin(make_update_context(*ctx));
      try {
        has_next = func(doc);
        writer->commit();
      } catch (...) {
        writer->rollback();
        throw;
      }
    } while (has_next);

    return writer->valid();
  }

  ////////////////////////////////////////////////////////////////////////////
  /// @brief replaces documents matching filter with the document
  ///        to be filled by the specified functor
  /// @note that changes are not visible until commit()
  /// @note that filter must be valid until commit()
  /// @param filter the document filter
  /// @param func the insertion logic
  /// @return all fields/attributes successfully insterted
  ////////////////////////////////////////////////////////////////////////////
  template<typename Filter, typename Func>
  bool update(Filter&& filter, Func func) {
    auto ctx = get_flush_context(); // retain lock until end of update(...)
    auto writer = get_segment_context(*ctx);

    writer->begin(make_update_context(*ctx, std::forward<Filter>(filter)));

    return update(*ctx, *writer, func);
  }

  ////////////////////////////////////////////////////////////////////////////
  /// @brief marks documents matching filter for removal 
  /// @note that changes are not visible until commit()
  /// @note that filter must be valid until commit()
  ///
  /// @param filter the document filter 
  ////////////////////////////////////////////////////////////////////////////
  void remove(const filter& filter); 

  ////////////////////////////////////////////////////////////////////////////
  /// @brief marks documents matching filter for removal 
  /// @note that changes are not visible until commit()
  ///
  /// @param filter the document filter 
  ////////////////////////////////////////////////////////////////////////////
  void remove(const std::shared_ptr<filter>& filter);

  ////////////////////////////////////////////////////////////////////////////
  /// @brief marks documents matching filter for removal 
  /// @note that changes are not visible until commit()
  ///
  /// @param filter the document filter 
  ////////////////////////////////////////////////////////////////////////////
  void remove(filter::ptr&& filter);

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

  typedef std::shared_ptr<
    std::pair<std::shared_ptr<index_meta>, file_refs_t>
  > committed_state_t;
  typedef std::vector<modification_context> modification_requests_t;

  struct IRESEARCH_API flush_context {
    typedef std::vector<import_context> imported_segments_t;
    typedef std::unordered_set<string_ref> segment_mask_t;
    typedef bounded_object_pool<segment_writer> segment_writers_t;

    // do not use std::shared_ptr to avoid unnecessary heap allocatons
    class ptr : util::noncopyable {
     public:
      explicit ptr(flush_context* ctx = nullptr, bool shared = false) NOEXCEPT
        : ctx(ctx), shared(shared) {
      }

      ptr(ptr&& rhs) NOEXCEPT
        : ctx(rhs.ctx), shared(rhs.shared) {
        rhs.ctx = nullptr; // take ownership
      }

      ptr& operator=(ptr&& rhs) NOEXCEPT {
        if (this != &rhs) {
          ctx = rhs.ctx;
          rhs.ctx = nullptr; // take ownership
          shared = rhs.shared;
        }
        return *this;
      }

      ~ptr() NOEXCEPT {
        reset();
      }

      void reset() NOEXCEPT {
        if (!ctx) {
          // nothing to do
          return;
        }

        if (!shared) {
          async_utils::read_write_mutex::write_mutex mutex(ctx->flush_mutex_);
          ADOPT_SCOPED_LOCK_NAMED(mutex, lock);

          ctx->reset(); // reset context and make ready for reuse
        } else {
          async_utils::read_write_mutex::read_mutex mutex(ctx->flush_mutex_);
          ADOPT_SCOPED_LOCK_NAMED(mutex, lock);
        }

        ctx = nullptr;
      }

      flush_context& operator*() const NOEXCEPT { return *ctx; }
      flush_context* operator->() const NOEXCEPT { return ctx; }
      operator bool() const NOEXCEPT { return nullptr != ctx; }

     private:
      flush_context* ctx;
      bool shared;
    }; // ptr

    std::atomic<size_t> generation_; // current modification/update generation
    ref_tracking_directory::ptr dir_; // ref tracking directory used by this context (tracks all/only refs for this context)
    async_utils::read_write_mutex flush_mutex_; // guard for the current context during flush (write) operations vs update (read)
    modification_requests_t modification_queries_; // sequential list of modification requests (remove/update)
    std::mutex mutex_; // guard for the current context during struct update operations, e.g. modification_queries_, pending_segments_
    flush_context* next_context_; // the next context to switch to
    imported_segments_t pending_segments_; // complete segments to be added during next commit (import)
    segment_mask_t segment_mask_; // set of segment names to be removed from the index upon commit (refs at strings in index_writer::meta_)
    segment_writers_t writers_pool_; // per thread segment writers

    flush_context();
    void reset();
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
    flush_context::ptr ctx; // reference to flush context held until end of commit
    index_meta::ptr meta; // index meta of next commit
    sync_context to_sync; // file names and segments to be synced during next commit
    pending_context_t() = default;
    pending_context_t(pending_context_t&& other) NOEXCEPT
      : ctx(std::move(other.ctx)),
        meta(std::move(other.meta)),
        to_sync(std::move(other.to_sync)) {
    }
    operator bool() const { return ctx && meta; }
  }; // pending_context_t

  struct pending_state_t {
    flush_context::ptr ctx; // reference to flush context held until end of commit
    index_meta::ptr meta; // index meta of next commit
    operator bool() const { return ctx && meta; }
    void reset() { ctx.reset(), meta.reset(); }
  }; // pending_state_t

  index_writer(
    index_lock::ptr&& lock, 
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

  flush_context::ptr get_flush_context(bool shared = true);
  index_writer::flush_context::segment_writers_t::ptr get_segment_context(flush_context& ctx);

  // returns context for "add" operation
  static segment_writer::update_context make_update_context(flush_context& ctx);

  // returns context for "update" operation
  segment_writer::update_context make_update_context(flush_context& ctx, const filter& filter);
  segment_writer::update_context make_update_context(flush_context& ctx, const std::shared_ptr<filter>& filter);
  segment_writer::update_context make_update_context(flush_context& ctx, filter::ptr&& filter);

  template<typename Func>
  bool update(flush_context& ctx, segment_writer& writer, Func func) {
    segment_writer::document doc(writer);

    try {
      func(doc);
      writer.commit();
    } catch (...) {
      writer.rollback();

      SCOPED_LOCK(ctx.mutex_); // lock due to context modification
      ctx.modification_queries_[writer.doc_context().update_id].filter = nullptr; // mark invalid

      throw;
    }

    if (!writer.valid()) {
      SCOPED_LOCK(ctx.mutex_); // lock due to context modification
      ctx.modification_queries_[writer.doc_context().update_id].filter = nullptr; // mark invalid
      return false;
    }

    return true;
  }

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
  index_meta_writer::ptr writer_;
  index_lock::ptr write_lock_; // exclusive write lock for directory

  std::recursive_mutex consolidation_lock_;
  std::unordered_set<const segment_meta*> consolidating_segments_; // segments that are under consolidation
  IRESEARCH_API_PRIVATE_VARIABLES_END
}; // index_writer

NS_END

#endif
