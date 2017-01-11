//
// IResearch search engine 
// 
// Copyright (c) 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#ifndef IRESEARCH_FORMAT_BURST_TRIE_H
#define IRESEARCH_FORMAT_BURST_TRIE_H

#include "formats.hpp"
#include "formats_10_attributes.hpp"
#include "index/field_meta.hpp"

#include "store/data_output.hpp"
#include "store/memory_directory.hpp"
#include "store/store_utils.hpp"
#include "utils/buffers.hpp"
#include "utils/hash_utils.hpp"

#if defined(_MSC_VER)
  // NOOP
#elif defined (__GNUC__)
  #pragma GCC diagnostic ignored "-Wsign-compare"
  #pragma GCC diagnostic ignored "-Wunused-local-typedefs"
#endif

  #include "utils/fst_decl.hpp"

#if defined(_MSC_VER)
  // NOOP
#elif defined (__GNUC__)
  #pragma GCC diagnostic pop
  #pragma GCC diagnostic pop
#endif

#include "utils/noncopyable.hpp"

#include <list>

NS_ROOT
NS_BEGIN(burst_trie)

class field_reader;

NS_BEGIN( detail )

// -------------------------------------------------------------------
// FST predeclaration
// -------------------------------------------------------------------

class fst_buffer;

/* -------------------------------------------------------------------
* entry
* ------------------------------------------------------------------*/

enum EntryType : byte_type {
  ET_TERM = 0U,
  ET_BLOCK
};

class block_meta;
struct term_t;
struct block_t;

struct entry : private util::noncopyable {
 public:
  entry(const iresearch::bytes_ref& term, iresearch::attributes&& attrs);
  entry(const iresearch::bytes_ref& prefix, uint64_t block_start,
        const block_meta& meta, int16_t label);
  entry(entry&& rhs) NOEXCEPT;
  entry& operator=(entry&& rhs) NOEXCEPT;
  ~entry();

  iresearch::bstring data; // block prefix or term
  union {
    term_t* term;
    block_t* block;
  };
  EntryType type;

 private:
  void destroy() NOEXCEPT;
}; // entry

/* -------------------------------------------------------------------
* term_reader
* ------------------------------------------------------------------*/

class term_iterator;
typedef std::vector<const attribute::type_id*> feature_map_t;

class term_reader : public iresearch::term_reader,
                    private util::noncopyable {
 public:
  term_reader() = default;
  term_reader(term_reader&& rhs);
  virtual ~term_reader();

  bool prepare(
    std::istream& in,
    const feature_map_t& features,
    field_reader& owner
  );

  virtual seek_term_iterator::ptr iterator() const override;
  virtual const field_meta& meta() const override { return field_; }
  virtual size_t size() const override { return terms_count_; }
  virtual uint64_t docs_count() const override { return doc_count_; }
  virtual const bytes_ref& min() const override { return min_term_ref_; }
  virtual const bytes_ref& max() const override { return max_term_ref_; }
  virtual const iresearch::attributes& attributes() const NOEXCEPT override {
    return attrs_; 
  }

 private:
  friend class term_iterator;

  iresearch::attributes attrs_;
  bstring min_term_;
  bstring max_term_;
  bytes_ref min_term_ref_;
  bytes_ref max_term_ref_;
  uint64_t terms_count_;
  uint64_t doc_count_;
  uint64_t doc_freq_;
  uint64_t term_freq_;
  field_meta field_;
  fst::VectorFst< byte_arc >* fst_; // TODO: use compact fst here!!!
  field_reader* owner_;
};

NS_END // detail

/* -------------------------------------------------------------------
* field_writer
* ------------------------------------------------------------------*/

class field_writer final : public iresearch::field_writer{
 public:
  static const int32_t FORMAT_MIN = 0;
  static const int32_t FORMAT_MAX = FORMAT_MIN;
  static const uint32_t DEFAULT_MIN_BLOCK_SIZE = 25;
  static const uint32_t DEFAULT_MAX_BLOCK_SIZE = 48;

  static const string_ref FORMAT_TERMS;
  static const string_ref TERMS_EXT;
  static const string_ref FORMAT_TERMS_INDEX;
  static const string_ref TERMS_INDEX_EXT;

  field_writer( iresearch::postings_writer::ptr&& pw,
                uint32_t min_block_size = DEFAULT_MIN_BLOCK_SIZE,
                uint32_t max_block_size = DEFAULT_MAX_BLOCK_SIZE );

  virtual ~field_writer();
  virtual void prepare( const iresearch::flush_state& state ) override;
  virtual void end() override;
  virtual void write( 
    const std::string& name,
    iresearch::field_id norm,
    const iresearch::flags& features,
    iresearch::term_iterator& terms 
  ) override;

 private:
  void write_segment_features(data_output& out, const flags& features);
  void write_field_features(data_output& out, const flags& features) const;

  void begin_field(const iresearch::flags& field);
  void end_field(
    const std::string& name,
    iresearch::field_id norm,
    const iresearch::flags& features, 
    uint64_t total_doc_freq, 
    uint64_t total_term_freq, 
    size_t doc_count
  );

  void write_term_entry( const detail::entry& e, size_t prefix, bool leaf );
  void write_block_entry( const detail::entry& e, size_t prefix, uint64_t block_start );
  static void merge_blocks( std::list< detail::entry >& blocks );
  /* prefix - prefix length ( in last_term )
  * begin - index of the first entry in the block
  * end - index of the last entry in the block
  * meta - block metadata
  * label - block lead label ( if present ) */
  void write_block( std::list< detail::entry >& blocks,
                    size_t prefix, size_t begin,
                    size_t end, const detail::block_meta& meta,
                    int16_t label );
  /* prefix - prefix length ( in last_term
  * count - number of entries to write into block */
  void write_blocks( size_t prefix, size_t count );
  void push( const iresearch::bytes_ref& term );

  static const size_t DEFAULT_SIZE = 8;

  std::unordered_map<const attribute::type_id*, size_t> feature_map_;
  iresearch::memory_output suffix; /* term suffix column */
  iresearch::memory_output stats; /* term stats column */
  iresearch::index_output::ptr terms_out; /* output stream for terms */
  iresearch::index_output::ptr index_out; /* output stream for indexes*/
  iresearch::postings_writer::ptr pw; /* postings writer */
  std::vector< detail::entry > stack;
  std::unique_ptr<detail::fst_buffer> fst_buf_; // pimpl buffer used for building FST for fields
  bstring last_term; // last pushed term
  std::vector<size_t> prefixes;
  std::pair<bool, bstring> min_term; // current min term in a block
  bstring max_term; // current max term in a block
  uint64_t term_count;    /* count of terms */
  size_t fields_count{};
  uint32_t min_block_size;
  uint32_t max_block_size;
};

/* -------------------------------------------------------------------
* field_reader
* ------------------------------------------------------------------*/

class field_reader final : public iresearch::field_reader {
 public:
  explicit field_reader(iresearch::postings_reader::ptr&& pr);
  virtual bool prepare(const reader_state& state) override;
  virtual const iresearch::term_reader* field(const string_ref& field) const override;
  virtual iresearch::field_iterator::ptr iterator() const override;
  virtual size_t size() const override;

 private:
  friend class detail::term_iterator;

  std::vector<detail::term_reader> fields_;
  std::unordered_map<hashed_string_ref, term_reader*> name_to_field_;
  std::vector<const detail::term_reader*> fields_mask_;
  iresearch::postings_reader::ptr pr_;
  iresearch::index_input::ptr terms_in_;
}; // field_reader

NS_END
NS_END

#endif
