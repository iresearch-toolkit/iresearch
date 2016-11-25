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

#ifndef IRESEARCH_FORMAT_BURST_TRIE_H
#define IRESEARCH_FORMAT_BURST_TRIE_H

#include "formats.hpp"
#include "formats_10_attributes.hpp"

#include "store/data_output.hpp"
#include "store/memory_directory.hpp"
#include "store/store_utils.hpp"
#include "utils/buffers.hpp"

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

class field_iterator;
class term_iterator;

class term_reader : public iresearch::term_reader,
                    private util::noncopyable {
 public:
  term_reader() = default;
  term_reader(term_reader&& rhs);
  virtual ~term_reader();

  bool prepare(
    index_input& meta_in, 
    std::istream& fst_in, 
    const field_meta& field, 
    field_reader& owner
  );

  virtual seek_term_iterator::ptr iterator() const override;
  virtual const field_meta& meta() const override { return *field_; }
  virtual size_t size() const override { return terms_count_; }
  virtual uint64_t docs_count() const override { return doc_count_; }
  virtual const bytes_ref& min() const override { return min_term_ref_; }
  virtual const bytes_ref& max() const override { return max_term_ref_; }
  virtual const iresearch::attributes& attributes() const override { 
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
  const field_meta* field_;
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
    iresearch::field_id id,
    const iresearch::flags& features,
    iresearch::term_iterator& terms 
  ) override;

 private:
  struct field_meta : private util::noncopyable {
    field_meta(
      bstring&& min_term,
      bstring&& max_term,
      iresearch::field_id id,
      uint64_t index_start,
      uint64_t term_count,
      size_t doc_count,
      uint64_t doc_freq,
      uint64_t term_freq,
      bool write_freq
    );
    field_meta(field_meta&& rhs);

    void write(data_output& out) const;

    bstring min_term; // min term
    bstring max_term; // max term
    uint64_t index_start; // where field index starts
    uint64_t doc_freq; // size of postings
    size_t doc_count; // number of documents that have at least one term for field
    uint64_t term_freq; // total number of tokens for field
    uint64_t term_count; // number of terms for field
    iresearch::field_id id; // field identifier
    bool write_freq;
  };

  void begin_field(const iresearch::flags& field);
  void end_field(
    field_id id, 
    const iresearch::flags& field, 
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

  iresearch::memory_output suffix; /* term suffix column */
  iresearch::memory_output stats; /* term stats column */
  iresearch::index_output::ptr terms_out; /* output stream for terms */
  iresearch::index_output::ptr index_out; /* output stream for indexes*/
  iresearch::postings_writer::ptr pw; /* postings writer */
  std::vector< detail::entry > stack;
  std::vector< field_meta > fields; /* finished fields metadata */
  std::unique_ptr<detail::fst_buffer> fst_buf_; // pimpl buffer used for building FST for fields
  bstring last_term; // last pushed term
  std::vector<size_t> prefixes;
  std::pair<bool, bstring> min_term; // current min term in a block
  bstring max_term; // current max term in a block
  uint64_t term_count;    /* count of terms */
  uint32_t min_block_size;
  uint32_t max_block_size;
};

/* -------------------------------------------------------------------
* field_reader
* ------------------------------------------------------------------*/

class field_reader final : public iresearch::field_reader {
 public:
  explicit field_reader(iresearch::postings_reader::ptr&& pr);
  virtual void prepare(const reader_state& state) override;
  virtual const iresearch::term_reader* terms(field_id field) const override;
  virtual size_t size() const override;

 private:
  friend class detail::term_iterator;
  friend class detail::field_iterator;

  std::vector<detail::term_reader> fields_;
  std::vector<const detail::term_reader*> fields_mask_;
  iresearch::postings_reader::ptr pr_;
  iresearch::index_input::ptr terms_in_;
};

NS_END
NS_END

#endif
