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

#ifndef IRESEARCH_INDEXWRITER_H
#define IRESEARCH_INDEXWRITER_H

#include "index_meta.hpp"
#include "field_meta.hpp"
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

struct directory;
class directory_reader;

//////////////////////////////////////////////////////////////////////////////
/// @enum OpenMode 
/// @brief defines how index writer should be opened
//////////////////////////////////////////////////////////////////////////////
enum OPEN_MODE {
  ////////////////////////////////////////////////////////////////////////////
  /// @brief Creates new index repository. In case if repository already
  ///        exists, all contents will be cleared.
  ////////////////////////////////////////////////////////////////////////////
  OM_CREATE,
  
  ////////////////////////////////////////////////////////////////////////////
  /// @brief Opens existsing index repository. In case if repository does not 
  ///        exists, error will be generated.
  ////////////////////////////////////////////////////////////////////////////
  OM_APPEND,
  
  ////////////////////////////////////////////////////////////////////////////
  /// @brief Checks whether index repository already exists. If so, opens it, 
  ///        otherwise initializes new repository
  ////////////////////////////////////////////////////////////////////////////
  OM_CREATE_APPEND
};

//////////////////////////////////////////////////////////////////////////////
/// @class index_writer 
/// @brief The object is using for indexing data. Only one writer can write to
///        the same directory simultaneously.
///        Thread safe.
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API index_writer : util::noncopyable {
 public:
  DECLARE_SPTR(index_writer);

  static const size_t THREAD_COUNT = 8;

  typedef std::function<bool(const segment_meta& meta)> consolidation_acceptor_t;
  typedef std::function<consolidation_acceptor_t(
    const directory& dir, const index_meta& meta
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
  ////////////////////////////////////////////////////////////////////////////
  static index_writer::ptr make(
    directory& dir, 
    format::ptr codec, 
    OPEN_MODE mode);
  
  ////////////////////////////////////////////////////////////////////////////
  /// @brief destructor 
  ////////////////////////////////////////////////////////////////////////////
  ~index_writer();

  ////////////////////////////////////////////////////////////////////////////
  /// @returns overall number of buffered documents in a writer 
  ////////////////////////////////////////////////////////////////////////////
  uint64_t buffered_docs() const;

  ////////////////////////////////////////////////////////////////////////////
  /// @brief inserts document specified by the range of fields [begin;end) 
  ///        into index. 
  /// @note iterator underlying value type must satisfy the Field concept
  /// @note that changes are not visible until commit()
  /// @param begin the beginning of the document
  /// @param end the end of the document
  /// @return all fields/attributes successfully insterted
  ////////////////////////////////////////////////////////////////////////////
  template<typename FieldIterator>
  bool insert(FieldIterator begin, FieldIterator end) {
    return insert(begin, end, empty::instance(), empty::instance());
  }
  
  template<typename FieldIterator, typename AttributeIterator>
  bool insert(
      FieldIterator begin, FieldIterator end,
      AttributeIterator abegin, AttributeIterator aend) {
    auto ctx = get_flush_context(); // retain lock until end of instert(...)
    auto writer = get_segment_context(*ctx);

    return writer->insert(begin, end, abegin, aend, make_update_context(*ctx));
  }

  ////////////////////////////////////////////////////////////////////////////
  /// @brief replaces documents matching filter for with the document
  ///        represented by the range of fields [begin;end)
  /// @note iterator underlying value type must satisfy the Field concept
  /// @note that changes are not visible until commit()
  /// @note that filter must be valid until commit()
  /// @param filter the document filter 
  /// @param begin the beginning of the document
  /// @param end the end of the document
  /// @return all fields/attributes successfully insterted
  ////////////////////////////////////////////////////////////////////////////
  template<typename FieldIterator>
  bool update(const filter& filter, FieldIterator begin, FieldIterator end) {
    return update(filter, begin, end, empty::instance(), empty::instance());
  }
  
  template<typename FieldIterator, typename AttributeIterator>
  bool update(
      const filter& filter, 
      FieldIterator begin, FieldIterator end,
      AttributeIterator abegin, AttributeIterator aend) {
    auto ctx = get_flush_context(); // retain lock until end of instert(...)
    auto writer = get_segment_context(*ctx);
    auto update_context = make_update_context(*ctx, filter);

    if (writer->insert(begin, end, abegin, aend, update_context)) {
      return true;
    }

    SCOPED_LOCK(ctx->mutex_); // lock due to context modification
    ctx->modification_queries_[update_context.update_id].filter = nullptr; // mark invalid

    return false;
  }

  ////////////////////////////////////////////////////////////////////////////
  /// @brief replaces documents matching filter for with the document
  ///        represented by the range of fields [begin;end)
  /// @note iterator underlying value type must satisfy the Field concept
  /// @note that changes are not visible until commit()
  /// @param filter the document filter 
  /// @param begin the beginning of the document
  /// @param end the end of the document
  ////////////////////////////////////////////////////////////////////////////
  template<typename FieldIterator>
  bool update(filter::ptr&& filter, FieldIterator begin, FieldIterator end) {
    return update(
      std::move(filter), begin, end, empty::instance(), empty::instance()
    );
  }

  template<typename FieldIterator, typename AttributeIterator>
  bool update(
      filter::ptr&& filter, 
      FieldIterator begin, FieldIterator end,
      AttributeIterator abegin, AttributeIterator aend) {
    if (!filter) {
      return false; // skip empty filters
    }

    auto ctx = get_flush_context(); // retain lock until end of instert(...)
    auto writer = get_segment_context(*ctx);
    auto update_context = make_update_context(*ctx, std::move(filter));

    if (writer->insert(begin, end, abegin, aend, update_context)) {
      return true;
    }

    SCOPED_LOCK(ctx->mutex_); // lock due to context modification
    ctx->modification_queries_[update_context.update_id].filter = nullptr; // mark invalid

    return false;
  }

  ////////////////////////////////////////////////////////////////////////////
  /// @brief replaces documents matching filter for with the document
  ///        represented by the range of fields [begin;end)
  /// @note iterator underlying value type must satisfy the Field concept
  /// @note that changes are not visible until commit()
  /// @param filter the document filter 
  /// @param begin the beginning of the document
  /// @param end the end of the document
  ////////////////////////////////////////////////////////////////////////////
  template<typename Iterator>
  bool update(
    const std::shared_ptr<filter>& filter, Iterator begin, Iterator end
  ) {
    return update(filter, begin, end, empty::instance(), empty::instance());
  }

  template<typename FieldIterator, typename AttributeIterator>
  bool update(
      const std::shared_ptr<filter>& filter, 
      FieldIterator begin, FieldIterator end,
      AttributeIterator abegin, AttributeIterator aend) {
    if (!filter) {
      return false; // skip empty filters
    }

    auto ctx = get_flush_context(); // retain lock until end of instert(...)
    auto writer = get_segment_context(*ctx);
    auto update_context = make_update_context(*ctx, filter);

    if (writer->insert(begin, end, abegin, aend, update_context)) {
      return true;
    }

    SCOPED_LOCK(ctx->mutex_); // lock due to context modification
    ctx->modification_queries_[update_context.update_id].filter = nullptr; // mark invalid

    return false;
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
  ///        a new segment. Frees the space occupied by the doucments marked 
  ///        as deleted and deduplicate terms.
  /// @param policy the speicified defragmentation policy
  /// @param immediate apply the policy immediately but only to previously
  ///        committed segments, or defer defragment until the commit stage
  ///        and apply the policy to all segments in the commit
  ////////////////////////////////////////////////////////////////////////////
  void consolidate(const consolidation_policy_t& policy, bool immediate = true);

  ////////////////////////////////////////////////////////////////////////////
  /// @brief merges segments accepted by the specified defragment policty into
  ///        a new segment. Frees the space occupied by the doucments marked 
  ///        as deleted and deduplicate terms.
  /// @param policy the speicified defragmentation policy
  /// @param immediate apply the policy immediately but only to previously
  ///        committed segments, or defer defragment until the commit stage
  ///        and apply the policy to all segments in the commit
  ////////////////////////////////////////////////////////////////////////////
  void consolidate(
    const std::shared_ptr<consolidation_policy_t>& policy, bool immediate = true
  );

  ////////////////////////////////////////////////////////////////////////////
  /// @brief merges segments accepted by the specified defragment policty into
  ///        a new segment. Frees the space occupied by the doucments marked 
  ///        as deleted and deduplicate terms.
  /// @param policy the speicified defragmentation policy
  /// @param immediate apply the policy immediately but only to previously
  ///        committed segments, or defer defragment until the commit stage
  ///        and apply the policy to all segments in the commit
  ////////////////////////////////////////////////////////////////////////////
  void consolidate(consolidation_policy_t&& policy, bool immediate = true);

  ////////////////////////////////////////////////////////////////////////////
  /// @brief imports index from the specified index reader into new segment
  /// @param reader the index reader to import 
  /// @returns true on success
  ////////////////////////////////////////////////////////////////////////////
  bool import(const index_reader& reader);

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

  // empty attribute iterator
  class empty {
   public:
    const string_ref& name() const { return string_ref::nil; }
    bool write(data_output&) const { return false; }

    CONSTEXPR static empty* instance() { return nullptr; }

   private:
    empty();
  };

  struct consolidation_context {
    consolidation_policy_t buf; // policy buffer for moved policies (private use)
    std::shared_ptr<const consolidation_policy_t> policy; // keep a handle to the policy for the case when this object has ownership
    consolidation_context(const consolidation_policy_t& consolidation_policy)
      : policy(&consolidation_policy, [](const consolidation_policy_t*)->void{}) {}
    consolidation_context(const std::shared_ptr<consolidation_policy_t>& consolidation_policy)
      : policy(consolidation_policy) {}
    consolidation_context(consolidation_policy_t&& consolidation_policy)
      : buf(std::move(consolidation_policy)) {
      policy.reset(&buf, [](const consolidation_policy_t*)->void{});
    }
    consolidation_context(consolidation_context&& other) {
      if (&other.buf == other.policy.get()) {
        buf = std::move(other.buf);
        policy.reset(&buf, [](const consolidation_policy_t*)->void{});
      } else {
        policy = std::move(other.policy);
      }
    }
    consolidation_context& operator=(const consolidation_context& other) = delete; // no default constructor
  }; // consolidation_context

  struct modification_context {
    std::shared_ptr<const iresearch::filter> filter; // keep a handle to the filter for the case when this object has ownership
    const size_t generation;
    const bool update; // this is an update modification (as opposed to remove)
    bool seen;
    modification_context(const iresearch::filter& match_filter, size_t gen, bool isUpdate)
      : filter(&match_filter, [](const iresearch::filter*)->void{}), generation(gen), update(isUpdate), seen(false) {}
    modification_context(const std::shared_ptr<iresearch::filter>& match_filter, size_t gen, bool isUpdate)
      : filter(match_filter), generation(gen), update(isUpdate), seen(false) {}
    modification_context(iresearch::filter::ptr&& match_filter, size_t gen, bool isUpdate)
      : filter(std::move(match_filter)), generation(gen), update(isUpdate), seen(false) {}
    modification_context(modification_context&& other)
      : filter(std::move(other.filter)), generation(other.generation), update(other.update), seen(other.seen) {}
    modification_context& operator=(const modification_context& other) = delete; // no default constructor
  }; // modification_context

  struct import_context {
    import_context(index_meta::index_segment_t&& v_segment, size_t&& v_generation)
      : generation(std::move(v_generation)), segment(std::move(v_segment)) {}
    import_context(import_context&& other)
      : generation(std::move(other.generation)), segment(std::move(other.segment)) {}
    import_context& operator=(const import_context&) = delete;

    const size_t generation;
    const index_meta::index_segment_t segment;
  }; // import_context

  typedef std::unordered_map<std::string, segment_reader::ptr> cached_readers_t;
  typedef std::vector<consolidation_context> consolidation_requests_t;
  typedef std::vector<modification_context> modification_requests_t;

  struct flush_context {
    typedef std::vector<import_context> imported_segments_t;
    typedef std::unordered_set<string_ref> segment_mask_t;
    typedef bounded_object_pool<segment_writer> segment_writers_t;
    typedef std::vector<std::reference_wrapper<const std::string>> sync_context_t; // file names to be synced during next commit
    DECLARE_SPTR(flush_context);

    consolidation_requests_t consolidation_policies_; // sequential list of segment merge policies to apply at the end of commit to all segments
    std::atomic<size_t> generation_; // current modification/update generation
    ref_tracking_directory::ptr dir_; // ref tracking directory used by this context (tracks all/only refs for this context)
    async_utils::read_write_mutex flush_mutex_; // guard for the current context during flush (write) operations vs update (read)
    index_meta meta_; // meta pending flush completion
    modification_requests_t modification_queries_; // sequential list of modification requests (remove/update)
    std::mutex mutex_; // guard for the current context during struct update operations, e.g. modification_queries_, pending_segments_
    flush_context* next_context_; // the next context to switch to
    imported_segments_t pending_segments_; // complete segments to be added during next commit (import)
    segment_mask_t segment_mask_; // set of segment names to be removed from the index upon commit (refs at strings in index_writer::meta_)
    sync_context_t to_sync_; // file names to be synced during next commit
    segment_writers_t writers_pool_; // per thread segment writers

    flush_context();
    void reset();
  }; // flush_context

  index_writer(
    index_lock::ptr&& lock, 
    directory& dir, 
    format::ptr codec,
    index_meta&& meta, 
    index_meta::ptr&& commited_meta
  ) NOEXCEPT;

  // on open failure returns an empty pointer
  // function access controlled by commit_lock_ since only used in
  // flush_all(...) and defragment(...)
  segment_reader::ptr get_segment_reader(const segment_meta& meta);

  bool add_document_mask_modified_records(
    modification_requests_t& requests, 
    document_mask& docs_mask,
    const segment_meta& meta,
    size_t min_doc_id_generation = 0
  ); // return if any new records were added (modification_queries_ modified)

  bool add_document_mask_modified_records(
    modification_requests_t& requests, 
    segment_writer& writer,
    const segment_meta& meta
  ); // return if any new records were added (modification_queries_ modified)

  static bool add_document_mask_unused_updates(
    modification_requests_t& requests, 
    segment_writer& writer,
    const segment_meta& meta
  ); // return if any new records were added (modification_queries_ modified)

  bool add_segment_mask_consolidated_records(
    index_meta::index_segment_t& segment,
    const index_meta& meta, // already locked for read of segment meta
    flush_context& ctx, // already locked for modification of internal structures
    const index_writer::consolidation_policy_t& policy
  ); // return if any new records were added (pending_segments_/segment_mask_ modified)

  flush_context::ptr flush_all();

  flush_context::ptr get_flush_context(bool shared = true);
  index_writer::flush_context::segment_writers_t::ptr get_segment_context(flush_context& ctx);

  // returns context for "add" operatio
  segment_writer::update_context make_update_context(flush_context& ctx);

  // returns context for "update" operation
  segment_writer::update_context make_update_context(flush_context& ctx, const filter& filter);
  segment_writer::update_context make_update_context(flush_context& ctx, const std::shared_ptr<filter>& filter);
  segment_writer::update_context make_update_context(flush_context& ctx, filter::ptr&& filter);

  bool start(); // starts transaction
  void finish(); // finishes transaction

  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  flush_context::ptr active_flush_context_; // flush pending completion
  std::vector<flush_context> flush_context_pool_; // collection of contexts that collect data to be flushed, 2 because just swap them
  std::atomic<flush_context*> flush_context_; // currently active context accumulating data to be processed during the next flush
  index_meta meta_; // index metadata
  cached_readers_t cached_segment_readers_; // readers by segment name
  std::mutex commit_lock_; // guard for cached_segment_readers_, commit_pool_, meta_ (modification during commit()/defragment())
  index_meta::ptr commited_meta_; // last successfully committed meta 
  index_lock::ptr write_lock_; // exclusive write lock for directory
  format::ptr codec_;
  directory& dir_; // directory used for initialization of readers
  file_refs_t file_refs_; // file refs for the commited_meta_
  index_meta_writer::ptr writer_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
}; // index_writer

NS_END

#endif