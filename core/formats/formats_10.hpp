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

#ifndef IRESEARCH_FORMATS_10_H
#define IRESEARCH_FORMATS_10_H

#include "formats.hpp"
#include "skip_list.hpp"
#include "format_compress.hpp"

#include "formats_10_attributes.hpp"

#include "analysis/token_attributes.hpp"

#include "store/data_output.hpp"
#include "store/memory_directory.hpp"
#include "store/store_utils.hpp"
#include "store/checksum_io.hpp"

#include "index/field_meta.hpp"
#include "utils/compression.hpp"
#include "utils/bit_utils.hpp"
#include "utils/bitset.hpp"
#include "utils/memory.hpp"
#include "utils/noncopyable.hpp"
#include "utils/type_limits.hpp"

#include <list>

#if defined(_MSC_VER)
  #pragma warning(disable : 4244)
  #pragma warning(disable : 4245)
#elif defined (__GNUC__)
  // NOOP
#endif

#include <boost/crc.hpp>

#if defined(_MSC_VER)
  #pragma warning(default: 4244)
  #pragma warning(default: 4245)
#elif defined (__GNUC__)
  // NOOP
#endif

#if defined(_MSC_VER)
  #pragma warning(disable : 4351)
#endif

NS_ROOT
NS_BEGIN( version10 )

// compiled features supported by current format
class features {
 public:
  enum Mask : uint32_t {
    POS = 3, POS_OFFS = 7, POS_PAY = 11, POS_OFFS_PAY = 15
  };

  features() = default;

  explicit features(const flags& in) NOEXCEPT {
    set_bit<0>(in.check<iresearch::frequency>(), mask_);
    set_bit<1>(in.check<iresearch::position>(), mask_);
    set_bit<2>(in.check<iresearch::offset>(), mask_);
    set_bit<3>(in.check<iresearch::payload>(), mask_);
  }

  bool freq() const { return check_bit<0>(mask_); }
  bool position() const { return check_bit<1>(mask_); }
  bool offset() const { return check_bit<2>(mask_); }
  bool payload() const { return check_bit<3>(mask_); }
  operator Mask() const { return static_cast<Mask>(mask_); }

 private:
  byte_type mask_{};
}; // features

/* -------------------------------------------------------------------
 * index_meta_writer 
 * ------------------------------------------------------------------*/

struct index_meta_writer final: public iresearch::index_meta_writer {
  static const string_ref FORMAT_NAME;
  static const string_ref FORMAT_PREFIX;
  static const string_ref FORMAT_PREFIX_TMP;

  static const int32_t FORMAT_MIN = 0;
  static const int32_t FORMAT_MAX = FORMAT_MIN;

  virtual std::string filename(const index_meta& meta) const override;
  using iresearch::index_meta_writer::prepare;
  virtual bool prepare(directory& dir, index_meta& meta) override;
  virtual void commit() override;
  virtual void rollback() NOEXCEPT override;
 private:
  directory* dir_ = nullptr;
  index_meta* meta_ = nullptr;
};

/* -------------------------------------------------------------------
 * index_meta_reader
 * ------------------------------------------------------------------*/

struct index_meta_reader final: public iresearch::index_meta_reader {
  virtual bool last_segments_file(
    const directory& dir, std::string& name
  ) const override;

  virtual void read( 
    const directory& dir,
    index_meta& meta,
    const string_ref& filename = string_ref::nil // null == use meta
  ) override;
};

/* -------------------------------------------------------------------
 * segment_meta_writer 
 * ------------------------------------------------------------------*/

struct segment_meta_writer final : public iresearch::segment_meta_writer{
  static const string_ref FORMAT_EXT;
  static const string_ref FORMAT_NAME;

  static const int32_t FORMAT_MIN = 0;
  static const int32_t FORMAT_MAX = FORMAT_MIN;

  virtual std::string filename(const segment_meta& meta) const override;
  virtual void write(directory& dir, const segment_meta& meta) override;
};

/* -------------------------------------------------------------------
 * segment_meta_reader
 * ------------------------------------------------------------------*/

struct segment_meta_reader final : public iresearch::segment_meta_reader{
  virtual void read(
    const directory& dir,  
    segment_meta& meta,
    const string_ref& filename = string_ref::nil // null == use meta
  ) override;
};

/* -------------------------------------------------------------------
 * document_mask_writer
 * ------------------------------------------------------------------*/

class document_mask_reader;
class document_mask_writer final: public iresearch::document_mask_writer {
 public:
  static const string_ref FORMAT_EXT;
  static const string_ref FORMAT_NAME;

  static const int32_t FORMAT_MIN = 0;
  static const int32_t FORMAT_MAX = FORMAT_MIN;

  virtual ~document_mask_writer();
  virtual std::string filename(const segment_meta& meta) const override;
  virtual void prepare(directory& dir, const segment_meta& meta) override;
  virtual void begin(uint32_t count) override;
  virtual void write(const doc_id_t& mask) override;
  virtual void end() override;

private:
  friend document_mask_reader;
  index_output::ptr out_;
};

/* -------------------------------------------------------------------
 * document_mask_reader
 * ------------------------------------------------------------------*/

class document_mask_reader final: public iresearch::document_mask_reader {
public:
  virtual ~document_mask_reader();

  virtual bool prepare(directory const& dir, segment_meta const& meta) override;
  virtual uint32_t begin() override;
  virtual void read(iresearch::doc_id_t& mask) override;
  virtual void end() override;

private:
  checksum_index_input<::boost::crc_32_type> in_;
};

/* -------------------------------------------------------------------
 * field_meta_reader
 * ------------------------------------------------------------------*/

class field_meta_reader final : public iresearch::field_meta_reader{
 public:
  virtual ~field_meta_reader();

  virtual void prepare(const directory& dir, const string_ref& seg_name) override;
  virtual size_t begin() override;
  virtual void read(iresearch::field_meta& meta) override;
  virtual void end() override;

 private:
  void read_segment_features();
  void read_field_features(flags& features);

  std::vector<const attribute::type_id*> feature_map_;
  checksum_index_input<::boost::crc_32_type> in_;
};
    
/* -------------------------------------------------------------------
 * field_meta_writer
 * ------------------------------------------------------------------*/

class field_meta_writer final : public iresearch::field_meta_writer{
 public:
  static const string_ref FORMAT_EXT;
  static const string_ref FORMAT_NAME;

  static const int32_t FORMAT_MIN = 0;
  static const int32_t FORMAT_MAX = FORMAT_MIN;

  virtual ~field_meta_writer();

  virtual void prepare(const flush_state& state) override;
  virtual void write(
    field_id id, 
    const std::string& name, 
    const flags& features,
    field_id norm
  ) override;
  virtual void end() override;

 private:
  void write_segment_features(const flags& features);
  void write_field_features(const flags& features) const;

  index_output::ptr out;
  std::unordered_map<const attribute::type_id*, size_t> feature_map_;
};

/* -------------------------------------------------------------------
 * stored_fields_writer 
 * ------------------------------------------------------------------*/

class stored_fields_writer final : public iresearch::stored_fields_writer {
 public:
  static const uint32_t MAX_BUFFERED_DOCS = 128;
  static const int32_t FORMAT_MIN = 0;
  static const int32_t FORMAT_MAX = FORMAT_MIN;

  static const string_ref FIELDS_EXT;
  static const string_ref FORMAT_FIELDS;
  static const string_ref INDEX_EXT;

  stored_fields_writer(uint32_t buf_size, uint32_t block_size);

  virtual void prepare(directory& dir,  const string_ref& seg_name) override;
  virtual bool write(const serializer& serializer) override;
  virtual void end(const serializer* header) override;
  virtual void finish() override;
  virtual void reset() override;

 private:
  void flush();

  compressing_index_writer index;
  compressor compres;
  bytes_output seg_buf_; // per segment buffer
  index_output::ptr fields_out; // fields output stream
  index_output::ptr index_out_; // index output stream
  uint32_t bodies_[MAX_BUFFERED_DOCS]{}; // document bodies length
  uint32_t headers_[MAX_BUFFERED_DOCS]{}; // headers length
  uint32_t last_offset_;
  uint32_t doc_base;
  uint32_t num_buffered_docs;
  uint32_t buf_size; // block size
  uint32_t block_max_size_{}; // actual max block size
  uint32_t num_blocks_; // number of flushed blocks
  uint32_t num_incomplete_blocks_; // number of incomplete flushed blocks
};

/* -------------------------------------------------------------------
 * stored_fields_reader
 * ------------------------------------------------------------------*/

class stored_fields_reader final : public iresearch::stored_fields_reader {
 public:
  virtual void prepare(const reader_state& state) override;

  // expects 0-based doc id's
  virtual bool visit(doc_id_t doc, const visitor_f& visitor) override;

 private:
  class compressing_document_reader: util::noncopyable { // noncopyable due to index_
   public:
    compressing_document_reader():
      base_(type_limits<type_t::doc_id_t>::invalid()),
      size_(0) {
    }

    void prepare(
      const directory& dir, 
      const std::string& name, 
      uint32_t num_blocks, 
      uint32_t num_incomplete_blocks, 
      uint32_t block_size
    );

    inline bool contains(doc_id_t doc) const {
      return doc >= base_ && doc < (base_ + size_);
    }

    void load_block(uint64_t block_ptr);

    // expects 0-based doc id's
    bool visit(
      doc_id_t doc, // document to visit
      uint64_t start_ptr, // where block starts
      const visitor_f& visitor // document visitor
    );

   private: 
    //compressing_data_input data_in_;
    decompressor decomp_;
    bstring buf_; // compressed data
    bstring data_; // uncompressed data
    bytes_ref view_; // view on valid data
    bytes_ref_input header_; // header stream
    bytes_ref_input body_; // body stream
    uint32_t offsets_[stored_fields_writer::MAX_BUFFERED_DOCS]{}; // document offsets 
    uint32_t headers_[stored_fields_writer::MAX_BUFFERED_DOCS]{}; // document header lengths
    index_input::ptr fields_in_;
    doc_id_t base_; // document base
    uint32_t size_; /* number of documents in a block */
    uint32_t num_blocks_; /* number of flushed blocks */
    uint32_t num_incomplete_blocks_; /* number of incomplete flushed blocks */
    uint32_t block_max_size_{}; // size of the biggest block
  };

  compressing_document_reader docs_;
  compressed_index<uint64_t> index_;
  const fields_meta* fields_;
};

/* -------------------------------------------------------------------
* postings_writer
* 
* Assume that doc_count = 28, skip_n = skip_0 = 12
*  
*  |       block#0       | |      block#1        | |vInts|
*  d d d d d d d d d d d d d d d d d d d d d d d d d d d d (posting list)
*                          ^                       ^       (level 0 skip point)
*
* ------------------------------------------------------------------*/
class IRESEARCH_PLUGIN postings_writer final: public iresearch::postings_writer {
 public:
  static const string_ref TERMS_FORMAT_NAME;
  static const int32_t TERMS_FORMAT_MIN = 0;
  static const int32_t TERMS_FORMAT_MAX = TERMS_FORMAT_MIN;
   
  static const string_ref DOC_FORMAT_NAME;
  static const string_ref DOC_EXT;
  static const string_ref POS_FORMAT_NAME;
  static const string_ref POS_EXT;
  static const string_ref PAY_FORMAT_NAME;
  static const string_ref PAY_EXT;

  static const int32_t FORMAT_MIN = 0;
  static const int32_t FORMAT_MAX = FORMAT_MIN;

  static const uint32_t MAX_SKIP_LEVELS = 10;
  static const uint32_t BLOCK_SIZE = 128;
  static const uint32_t SKIP_N = 8;

  postings_writer(bool volatile_attributes);
  virtual ~postings_writer();
  
  /*------------------------------------------
  * const_attributes_provider 
  * ------------------------------------------*/
  
  virtual const iresearch::attributes& attributes() const override final {
    return attrs_;
  }

  /*------------------------------------------
  * postings_writer
  * ------------------------------------------*/

  virtual void prepare(index_output& out, const iresearch::flush_state& state) override;
  virtual void begin_field(const iresearch::flags& meta) override;
  virtual void write(doc_iterator& docs, iresearch::attributes& out) override;
  virtual void begin_block() override;
  virtual void encode(data_output& out, const iresearch::attributes& attrs) override;
  virtual void end() override;

 private:
  struct stream {
    uint64_t skip_ptr[MAX_SKIP_LEVELS]{};   /* skip data */
    index_output::ptr out;                  /* output stream*/
    uint64_t start{};                       /* start position of block */
    uint64_t end{};                         /* end position of block */
  }; // stream

  struct doc_stream : stream {
    void flush(uint64_t* buf, bool freq);
    bool full() const { return BLOCK_SIZE == size; }
    void next(doc_id_t id) { last = id, ++size; }
    void doc( int32_t delta ) { deltas[size] = delta; }
    void freq( uint32_t frq ) { freqs[size] = frq; }

    doc_id_t deltas[BLOCK_SIZE]{}; // document deltas
    doc_id_t skip_doc[MAX_SKIP_LEVELS]{};
    std::unique_ptr<uint32_t[]> freqs; /* document frequencies */
    doc_id_t last{ type_limits<type_t::doc_id_t>::invalid() }; // last buffered document id
    doc_id_t block_last{}; // last document id in a block
    uint32_t size{};            /* number of buffered elements */
  }; // doc_stream

  struct pos_stream : stream {
    DECLARE_PTR(pos_stream);

    void flush(uint32_t* buf);

    bool full() const { return BLOCK_SIZE == size; }
    void next( int32_t pos ) { last = pos, ++size; }
    void pos( int32_t pos ) { buf[size] = pos; }

    uint32_t buf[BLOCK_SIZE]{};        /* buffer to store position deltas */
    uint32_t last{};                   /* last buffered position */
    uint32_t block_last{};             /* last position in a block */
    uint32_t size{};                   /* number of buffered elements */
  }; // pos_stream

  struct pay_stream : stream {
    DECLARE_PTR(pay_stream);

    void flush_payload(uint32_t* buf);
    void flush_offsets(uint32_t* buf);

    void payload(uint32_t i, const bytes_ref& pay);
    void offsets(uint32_t i, uint32_t start, uint32_t end);

    bstring pay_buf_; // buffer for payload
    uint32_t pay_sizes[BLOCK_SIZE]{};             /* buffer to store payloads sizes */
    uint32_t offs_start_buf[BLOCK_SIZE]{};        /* buffer to store start offsets */
    uint32_t offs_len_buf[BLOCK_SIZE]{};          /* buffer to store offset lengths */
    size_t block_last{};                          /* last payload buffer length in a block */
    uint32_t last{};                              /* last start offset */
  }; // pay_stream 

  void write_skip(size_t level, index_output& out);
  void begin_term();
  void begin_doc(doc_id_t id, const frequency* freq);
  void add_position( uint32_t pos, const offset* offs, const payload* pay );
  void end_doc();
  void end_term(
      version10::term_meta& state,
      const frequency* tfreq);

  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  skip_writer skip_;
  iresearch::attributes attrs_;
  uint64_t buf[BLOCK_SIZE]; // buffer for encoding (worst case)
  version10::term_meta last_state;    /* last final term state*/
  doc_stream doc;           /* document stream */
  pos_stream::ptr pos_;      /* proximity stream */
  pay_stream::ptr pay_;      /* payloads and offsets stream */
  uint64_t docs_count{};      /* count of processed documents */
  bitset docs_;             /* bit set of all processed documents */
  features features_; /* features supported by current field */
  bool volatile_attributes_ = false; // attribute value memory locations may change after next()
  IRESEARCH_API_PRIVATE_VARIABLES_END
};

/* -------------------------------------------------------------------
* postings_reader
* ------------------------------------------------------------------*/

class IRESEARCH_PLUGIN postings_reader final: public iresearch::postings_reader {
 public:
  virtual void prepare(index_input& in, const reader_state& state) override;

  virtual void decode(
    data_input& in, 
    const flags& field,
    attributes& attrs
  ) override;

  virtual doc_iterator::ptr iterator(
    const flags& field,
    const attributes& attrs,
    const flags& features
  ) override;

 private:
  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  const document_mask* docs_mask_{};
  index_input::ptr doc_in_;
  index_input::ptr pos_in_;
  index_input::ptr pay_in_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
};

/* -------------------------------------------------------------------
 * format
 * ------------------------------------------------------------------*/

class IRESEARCH_PLUGIN format final : public iresearch::format {
 public:
  DECLARE_FORMAT_TYPE();
  DECLARE_FACTORY_DEFAULT();
  format();

  virtual index_meta_writer::ptr get_index_meta_writer() const override;
  virtual index_meta_reader::ptr get_index_meta_reader() const override;

  virtual segment_meta_writer::ptr get_segment_meta_writer() const override;
  virtual segment_meta_reader::ptr get_segment_meta_reader() const override;

  virtual document_mask_writer::ptr get_document_mask_writer() const override;
  virtual document_mask_reader::ptr get_document_mask_reader() const override;

  virtual field_meta_reader::ptr get_field_meta_reader() const override;
  virtual field_meta_writer::ptr get_field_meta_writer() const override;

  virtual field_writer::ptr get_field_writer(bool volatile_attributes = false) const override;
  virtual field_reader::ptr get_field_reader() const override;

  virtual stored_fields_writer::ptr get_stored_fields_writer() const override;
  virtual stored_fields_reader::ptr get_stored_fields_reader() const override;
  
  virtual column_meta_writer::ptr get_column_meta_writer() const override;
  virtual column_meta_reader::ptr get_column_meta_reader() const override;

  virtual columnstore_writer::ptr get_columnstore_writer() const override;
  virtual columnstore_reader::ptr get_columnstore_reader() const override;
};

NS_END
NS_END

#endif
