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

#include "shared.hpp"
#include "formats_burst_trie.hpp"
#include "format_utils.hpp"

#include "analysis/token_attributes.hpp"

#include "index/iterators.hpp"
#include "index/index_meta.hpp"
#include "index/field_meta.hpp"
#include "index/file_names.hpp"
#include "index/index_meta.hpp"

#include "store/checksum_io.hpp"
#include "utils/timer_utils.hpp"
#include "utils/fst.hpp"
#include "utils/fst_utils.hpp"
#include "utils/fst_decl.hpp"
#include "utils/bit_utils.hpp"
#include "utils/bitset.hpp"
#include "utils/attributes.hpp"

#if defined(_MSC_VER)
  // NOOP
#elif defined (__GNUC__)
  #pragma GCC diagnostic ignored "-Wunused-local-typedefs"
#endif

#include <fst/equivalent.h>

#if defined(_MSC_VER)
  // NOOP
#elif defined (__GNUC__)
  #pragma GCC diagnostic pop
#endif

#if defined(_MSC_VER)
  #pragma warning(disable : 4291)
#elif defined (__GNUC__)
  // NOOP
#endif

#include <fst/matcher.h>

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

#include <cassert>

NS_ROOT

NS_BEGIN( burst_trie )

NS_BEGIN( detail )

/* -------------------------------------------------------------------
* helper functions
* ------------------------------------------------------------------*/

inline void prepare_output(
    std::string& str,
    index_output::ptr& out,
    const flush_state& state,
    const string_ref& ext,
    const string_ref& format,
    const int32_t version ) {
  assert( !out );

  file_name(str, state.name, ext);
  out = state.dir->create(str);
  format_utils::write_header(*out, format, version);
}

inline void prepare_input(
    std::string& str,
    index_input::ptr& in,
    const reader_state& state,
    const string_ref& ext,
    const string_ref& format,
    const int32_t min_ver,
    const int32_t max_ver) {
  assert(!in);

  file_name(str, state.meta->name, ext);
  in = state.dir->open(str);
  format_utils::check_header(*in, format, min_ver, max_ver);
}

class block_meta {
 public:
  block_meta() : meta_(0) { }

  explicit block_meta(uint8_t meta) : meta_(meta) { }

  /* block has terms */
  bool terms() const { return check_bit<ET_TERM>(meta_); }

  /* block has sub-blocks */
  bool blocks() const { return check_bit<ET_BLOCK>(meta_); }

  void type(EntryType type) { set_bit(meta_, type); }

  /* block is floor block */
  bool floor() const { return check_bit<ET_BLOCK + 1>(meta_); }

  void floor(bool b) { set_bit<ET_BLOCK + 1>(b, meta_); }

  explicit operator byte_type() const { return meta_; }

  void reset() {
    unset_bit<ET_TERM>(meta_);
    unset_bit<ET_BLOCK>(meta_);
  }

 private:
  /* 0 - has terms
   * 1 - has sub blocks
   * 2 - is floor block */
  uint8_t meta_;
};

// -------------------------------------------------------------------
// resetable FST buffer
// -------------------------------------------------------------------

class fst_buffer: public vector_byte_fst {
 public:
  template<typename Data>
  fst_buffer& reset(const Data& data) {
    fst_byte_builder builder(*this);

    builder.reset();

    for (auto& fst_data: data) {
      builder.add(fst_data.prefix, fst_data.weight);
    }

    builder.finish();

    return *this;
  }
};

/* -------------------------------------------------------------------
* term_t
* ------------------------------------------------------------------*/

struct term_t : private util::noncopyable {
  term_t(iresearch::attributes &&attrs)
    : attrs(std::move(attrs)) {
  }

  iresearch::attributes attrs;
};

/* -------------------------------------------------------------------
* block_t 
* ------------------------------------------------------------------*/

struct block_t : private util::noncopyable {
  struct prefixed_output : iresearch::weight_output {
    explicit prefixed_output(bstring&& prefix): prefix(prefix) {}

    bstring prefix;
  }; // prefixed_output

  static const int16_t INVALID_LABEL = -1;
  
  block_t( uint64_t block_start, const block_meta& meta, int16_t label )
    : start( block_start ),
      label( label ),
      meta( meta ) {
  }

  std::list<prefixed_output> index; /* fst index data */
  uint64_t start; /* file pointer */
  int16_t label; /* block lead label */
  block_meta meta; /* block metadata */
};

/* -------------------------------------------------------------------
* entry
* ------------------------------------------------------------------*/

entry::entry(
  const iresearch::bytes_ref& term, iresearch::attributes&& attrs
): data(term.c_str(), term.size()), type(ET_TERM) {
  assert( !term.null() );
  this->term = new term_t( std::move( attrs ) );
}

entry::entry(
  const iresearch::bytes_ref& prefix,
  uint64_t block_start,
  const block_meta& meta,
  int16_t label
): data(prefix.c_str(), prefix.size()), type(ET_BLOCK) {
  if (block_t::INVALID_LABEL != label) {
    data.append(1, static_cast<byte_type>(label & 0xFF));
  }

  block = new block_t( block_start, meta, label );
}

entry::entry( entry&& rhs ) NOEXCEPT
  : data( std::move( rhs.data ) ),
    term( rhs.term ),
    type( rhs.type ) {
  /* prevent deletion */
  rhs.term = 0;
}

entry& entry::operator=(entry&& rhs) NOEXCEPT{
  if (this == &rhs) {
    return *this;
  }

  data = std::move(rhs.data);

  destroy();
  term = rhs.term, rhs.term = 0;
  type = rhs.type;
  return *this;
}

void entry::destroy() NOEXCEPT{
  switch ( type ) {
    case ET_TERM: delete term; break;
    case ET_BLOCK: delete block; break;
    default: assert( false ); break;
  }
}

entry::~entry() {
  destroy();
}

/* -------------------------------------------------------------------
* block_iterator : decl
* ------------------------------------------------------------------*/

class block_iterator : util::noncopyable {
 public:
  static const uint64_t UNDEFINED = integer_traits<uint64_t>::const_max;

  block_iterator(byte_weight&& header, size_t prefix, term_iterator* owner);
  block_iterator(uint64_t start, size_t prefix, term_iterator* owner);

  void load();

  void next_block() {
    assert(sub_count_);
    cur_start_ = cur_end_;
    if (sub_count_ != UNDEFINED) {
      --sub_count_;
    }
    dirty_ = true;
  }

  void next();
  void end();
  void reset();

  const version10::term_meta& state() const { return state_; }
  bool dirty() const { return dirty_; }
  const block_meta& meta() const { return cur_meta_; }
  size_t prefix() const { return prefix_; }
  uint64_t sub_count() const { return sub_count_; }
  EntryType type() const { return cur_type_; }
  uint64_t pos() const { return cur_ent_; }
  uint64_t terms_seen() const { return term_count_; }
  uint64_t count() const { return ent_count_; }
  uint64_t sub_start() const { return cur_block_start_; }
  uint64_t start() const { return start_; }
  bool block_end() const { return cur_ent_ == ent_count_; }

  SeekResult scan_to_term(const bytes_ref& term);

  /*  scan to floor block */
  void scan_to_block(const bytes_ref& term);

  /* scan to entry with the following start address*/
  void scan_to_block(uint64_t ptr);

  /* read attributes */
  void load_data(const field_meta& meta, iresearch::postings_reader& pr);

 private:
  inline void refresh_term(uint64_t suffix);
  inline void refresh_term(uint64_t suffix, uint64_t start);
  inline void read_entry_leaf();
  void read_entry_nonleaf();

  SeekResult scan_to_term_nonleaf(const bytes_ref& term, uint64_t& suffix, uint64_t& start);
  SeekResult scan_to_term_leaf(const bytes_ref& term, uint64_t& suffix, uint64_t& start);

  //TODO: check layout
  weight_input header_in_; /* reader for block header */
  bytes_input suffix_in_; /* suffix input stream */
  bytes_input stats_in_; /* stats input stream */
  term_iterator* owner_;
  version10::term_meta state_;
  uint64_t cur_ent_{}; /* current entry in a block */
  uint64_t ent_count_{}; /* number of entries in a current block */
  uint64_t start_; /* initial block start pointer */
  uint64_t cur_start_; /* current block start pointer */
  uint64_t cur_end_; /* block end pointer */
  uint64_t term_count_{}; /* number terms in a block, that we have seen */
  uint64_t cur_stats_ent_{}; /* current position of loaded stats */
  uint64_t cur_block_start_{ UNDEFINED }; /* start pointer of the current sub-block entry*/
  size_t prefix_; /* block prefix length */
  uint64_t sub_count_; /* number of sub-blocks */
  int16_t next_label_{ block_t::INVALID_LABEL }; /* next label (of the next sub-block)*/
  EntryType cur_type_; /* term or block */
  block_meta meta_; /* initial block metadata */
  block_meta cur_meta_; /* current block metadata */
  bool dirty_{ true }; /* current block is dirty */
  bool leaf_{ false }; /* current block is leaf block */
};

/* -------------------------------------------------------------------
* term_iterator : decl
* ------------------------------------------------------------------*/

struct cookie : attribute {
  DECLARE_ATTRIBUTE_TYPE();

  cookie(const version10::term_meta& meta,
         uint64_t term_freq)
    : attribute(cookie::type()),
      meta(meta),
      term_freq(term_freq) {
  }

  virtual void clear() override {
    meta.clear();
    term_freq = 0;
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief declaration/implementation of DECLARE_FACTORY_DEFAULT()
  //////////////////////////////////////////////////////////////////////////////
  static ptr make(
    const version10::term_meta& meta, uint64_t term_freq) {
    return ptr(new cookie(meta, term_freq));
  }

  version10::term_meta meta; /* term metadata */
  uint64_t term_freq; /* length of the positions list */
}; // cookie

DEFINE_ATTRIBUTE_TYPE(cookie);

class term_iterator : public iresearch::seek_term_iterator {
 public:
  explicit term_iterator(const term_reader* owner);

  virtual void read() override;
  virtual bool next() override;
  const iresearch::attributes& attributes() const override { return attrs_; }
  const bytes_ref& value() const override { return term_; }
  virtual SeekResult seek_ge(const bytes_ref& term) override;
  virtual bool seek(const bytes_ref& term) override;
  virtual bool seek(
      const bytes_ref& term,
      const iresearch::attribute& cookie) override {
    assert(detail::cookie::type() == cookie.type());
    /* copy state */
    auto& state = static_cast<const detail::cookie&>(cookie);
    *state_ = state.meta;
    if (freq_) {
      freq_->value = state.term_freq;
    }
    /* copy term */
    term_.reset();
    term_ += term;
    /* reset seek state */
    sstate_.resize(0);
    /* mark block as invalid */
    cur_block_ = nullptr;
    return true;
  }
  virtual attribute::ptr cookie() const override {
    return detail::cookie::make(
      *state_, freq_ ? freq_->value : 0
    );
  }
  virtual doc_iterator::ptr postings( const flags& features ) const override;
  index_input& terms_input() const;

 private:
  friend class block_iterator;

  struct arc {
    typedef fst_byte_builder::stateid_t stateid_t;

    arc() : block{} { }

    arc(arc&& rhs)
      : state(rhs.state), 
        weight(std::move(rhs.weight)),
        block(rhs.block) {
      rhs.block = 0;
    }
    
    arc(stateid_t state, const byte_weight& weight, block_iterator* block)
      : state(state), weight(weight), block(block) {
    }

    stateid_t state;
    byte_weight weight;
    block_iterator* block;
  }; // arc

  typedef std::vector<arc> seek_state_t;
  typedef std::deque<block_iterator> block_stack_t; // does not invalidate addresses
  
  ptrdiff_t seek_cached(
    size_t& prefix, arc::stateid_t& state,
    byte_weight& weight, const bytes_ref& term
  );

  /* Seeks to the specified term using FST
   * There may be several sutuations: 
   *   1. There is no term in a block (SeekResult::NOT_FOUND)
   *   2. There is no term in a block and we have
   *      reached the end of the block (SeekResult::END)
   *   3. We have found term in a block (SeekResult::FOUND)    
   *
   * Note, that search may end up on a BLOCK entry. In all cases
   * "owner_->term_" will be refreshed with the valid number of
   * common bytes */ 
  SeekResult seek_equal(const bytes_ref& term);

  inline block_iterator* pop_block() {
    block_stack_.pop_back();
    return &block_stack_.back();
  }

  inline block_iterator* push_block(byte_weight&& out, size_t prefix) {
    assert(out.Size());
    block_stack_.emplace_back(std::move(out), prefix, this);
    return &block_stack_.back();
  }

  inline block_iterator* push_block(uint64_t start, size_t prefix) {
    block_stack_.emplace_back(start, prefix, this);
    return &block_stack_.back();
  }

  iresearch::attributes attrs_;
  seek_state_t sstate_;
  block_stack_t block_stack_;
  block_iterator* cur_block_;
  const term_reader* owner_;
  version10::term_meta* state_;
  frequency* freq_;
  mutable index_input::ptr terms_in_;
  bytes_builder term_;
};

/* -------------------------------------------------------------------
* block_iterator : impl
* ------------------------------------------------------------------*/

block_iterator::block_iterator( byte_weight&& header, size_t prefix, 
                                term_iterator* owner )
  : header_in_(std::move(header)),
    owner_(owner),
    prefix_(prefix),
    sub_count_(0) {
  assert(owner_);
  cur_meta_ = meta_ = block_meta(header_in_.read_byte());
  cur_start_ = start_ = header_in_.read_vlong();
  if (meta_.floor()) {
    sub_count_ = header_in_.read_vlong();
    next_label_ = header_in_.read_byte();
  }
}

block_iterator::block_iterator( uint64_t start, size_t prefix, 
                                term_iterator* owner )
  : owner_(owner),
    start_(start),
    cur_start_(start),
    prefix_(prefix),
    sub_count_(UNDEFINED) {
  assert( owner_ );
}

void block_iterator::load() {
  if ( !dirty_ ) {
    return;
  }

  index_input& in = owner_->terms_input();
  in.seek( cur_start_ );
  if ( shift_unpack_64( in.read_vint(), ent_count_ ) ) {
    sub_count_ = 0; // no sub-blocks
  }
  uint64_t block_size;
  leaf_ = shift_unpack_64( in.read_vlong(), block_size );
  suffix_in_.read_from( in, block_size );

  const uint64_t stats_size = in.read_vlong();
  if ( stats_size ) {
    stats_in_.read_from( in, stats_size );
  }
  cur_end_ = in.file_pointer();
  cur_ent_ = 0;
  cur_block_start_ = UNDEFINED;
  term_count_ = 0;
  cur_stats_ent_ = 0;
  dirty_ = false;
}

inline void block_iterator::refresh_term(uint64_t suffix, uint64_t start) {
  auto& term = owner_->term_;
  term.reset(prefix_);
  term.append(suffix_in_.c_str() + start, suffix);
}

inline void block_iterator::refresh_term(uint64_t suffix) {
  auto& term = owner_->term_;
  term.oversize(prefix_ + suffix);
  term.reset(prefix_ + suffix);
  suffix_in_.read_bytes(term.data() + prefix_, suffix);
}

void block_iterator::read_entry_leaf() {
  assert(leaf_ && cur_ent_ < ent_count_);
  cur_type_ = ET_TERM; // always term
  refresh_term(suffix_in_.read_vlong());
  ++term_count_;
}

void block_iterator::read_entry_nonleaf() {
  assert(!leaf_ && cur_ent_ < ent_count_);

  uint64_t suffix;
  cur_type_ = shift_unpack_64(suffix_in_.read_vlong(), suffix) 
    ? ET_BLOCK 
    : ET_TERM;
  refresh_term(suffix);

  switch (cur_type_) {
    case ET_TERM: ++term_count_; break;
    case ET_BLOCK: cur_block_start_ = cur_start_ - suffix_in_.read_vlong(); break;
    default: assert(false); break;
  }
}

void block_iterator::next() {
  assert(!dirty_ && cur_ent_ < ent_count_);
  if (leaf_) {
    read_entry_leaf();
  } else {
    read_entry_nonleaf();
  }
  ++cur_ent_;
}

SeekResult block_iterator::scan_to_term_leaf(
    const bytes_ref& term, 
    uint64_t& suffix, 
    uint64_t& start) {
  assert(leaf_);
  assert(!dirty_);

  for (; cur_ent_ < ent_count_;) {
    assert(starts_with(term, owner_->term_));
    ++cur_ent_;
    ++term_count_;
    cur_type_ = ET_TERM;
    suffix = suffix_in_.read_vlong();
    start = suffix_in_.file_pointer(); /* start of the current suffix */
    suffix_in_.skip(suffix); /* skip to the next term */

    const size_t term_len = prefix_ + suffix;
    const size_t max = std::min(term.size(), term_len); /* max limit of comparison */

    ptrdiff_t cmp;
    bool stop = false;
    for (size_t tpos = prefix_, spos = start;;) {
      if (tpos < max) {
        cmp = suffix_in_.c_str()[spos] - term[tpos];
        ++tpos, ++spos;
      } else {
        assert(tpos == max);
        cmp = term_len - term.size();
        stop = true;
      }

      if (cmp < 0) { 
        /* we before the target, move to next entry */
        break;
      } else if (cmp > 0) { 
        /* we after the target, not found */
        return SeekResult::NOT_FOUND;
      } else if (stop) { // && cmp == 0
        /* match! */
        return SeekResult::FOUND;
      }
    }
  }
  
  /* we have reached the end of the block */
  return SeekResult::END;
}

SeekResult block_iterator::scan_to_term_nonleaf(
    const bytes_ref& term, 
    uint64_t& suffix, 
    uint64_t& start) {
  assert(!leaf_);
  assert(!dirty_);

  for (; cur_ent_ < ent_count_;) {
    assert(starts_with(term, owner_->term_));
    ++cur_ent_;
    cur_type_ = shift_unpack_64(suffix_in_.read_vlong(), suffix) ? ET_BLOCK : ET_TERM;
    start = suffix_in_.file_pointer();
    suffix_in_.skip(suffix); /* skip to the next entry*/

    const size_t term_len = prefix_ + suffix;
    const size_t max = std::min(term.size(), term_len); /* max limit of comparison */

    switch (cur_type_) {
      case ET_TERM: ++term_count_; break;
      case ET_BLOCK: cur_block_start_ = cur_start_ - suffix_in_.read_vlong(); break;
      default: assert(false); break;
    }

    ptrdiff_t cmp;
    for (size_t tpos = prefix_, spos = start;;) {
      bool stop = false;
      if (tpos < max) {
        cmp = suffix_in_.c_str()[spos] - term[tpos];
        ++tpos, ++spos;
      } else {
        assert(tpos == max);
        cmp = term_len - term.size();
        stop = true;
      }

      if (cmp < 0) { 
        /* we before the target, move to next entry */
        break;
      } else if (cmp > 0) { 
        /* we after the target, not found */
        return SeekResult::NOT_FOUND;
      } else if (stop) { /* && cmp == 0 */
        /* match! */
        return SeekResult::FOUND;
      }
    }
  }

  /* we have reached the end of the block */
  return SeekResult::END;
}

SeekResult block_iterator::scan_to_term(const bytes_ref& term) {
  assert(!dirty_);

  if (cur_ent_ == ent_count_) {
    /* have reached the end of the block */
    return SeekResult::END;
  }

  uint64_t suffix, start;
  const SeekResult res = leaf_
    ? scan_to_term_leaf(term, suffix, start)
    : scan_to_term_nonleaf(term, suffix, start);

  refresh_term(suffix, start);
  return res;
}

void block_iterator::scan_to_block(const bytes_ref& term) {
  assert(sub_count_ != UNDEFINED);

  if (!sub_count_ || !meta_.floor() || term.size() <= prefix_) {
    /* no sub-blocks, nothing to do */
    return;
  }

  const byte_type label = term[prefix_];
  if (label < next_label_) {
    /* search does not required */
    return;
  }

  // TODO: better to use binary search here
  uint64_t start = cur_start_;
  for (;;) {
    start = start_ + header_in_.read_vlong();
    cur_meta_ = block_meta(header_in_.read_byte());
    --sub_count_;

    if (0 == sub_count_) {
      next_label_ = block_t::INVALID_LABEL;
      break;
    } else {
      next_label_ = header_in_.read_byte();
      if (label < next_label_) {
        break;
      }
    }
  }

  if (start != cur_start_) {
    cur_start_ = start;
    cur_ent_ = 0;
    dirty_ = true;
  }
}

void block_iterator::scan_to_block(uint64_t start) {
  if (leaf_) return; /* should be non leaf block */
  if (cur_block_start_ == start) return; /* nothing to do */
  const uint64_t target = cur_start_ - start; /* delta */
  for (; cur_ent_ < ent_count_;) {
    ++cur_ent_;
    uint64_t suffix;
    const EntryType type = shift_unpack_64(suffix_in_.read_vlong(), suffix) ? ET_BLOCK : ET_TERM;
    suffix_in_.skip(suffix);

    switch (type) {
      case ET_TERM:
        ++term_count_;
        break;
      case ET_BLOCK:
        if (suffix_in_.read_vlong() == target) {
          cur_block_start_ = target;
          return;
        }
        break;
      default:
        assert(false);
        break;
    }
  }
  assert(false);
}

void block_iterator::load_data(const field_meta& meta, iresearch::postings_reader& pr) {
  assert(ET_TERM == cur_type_);

  if (cur_stats_ent_ >= term_count_) {
    return;
  }

  auto& state = *owner_->state_;
  if (0 == cur_stats_ent_) {
    /* clear state at the beginning */
    state.clear();
  } else {
    state = state_;
  }

  for (; cur_stats_ent_ < term_count_; ++cur_stats_ent_) {
    pr.decode(stats_in_, meta.features, owner_->attrs_);
  }

  state_ = state;
}

void block_iterator::reset() {
  if ( sub_count_ != UNDEFINED ) {
    sub_count_ = 0;
  }
  next_label_ = block_t::INVALID_LABEL;
  cur_start_ = start_;
  cur_meta_ = meta_;
  if ( meta_.floor() ) {
    assert( sub_count_ != UNDEFINED );
    header_in_.reset();
    header_in_.read_byte(); // skip meta
    header_in_.read_vlong(); // skip address
    sub_count_ = header_in_.read_vlong();
    next_label_ = header_in_.read_byte();
  }
  dirty_ = true;
}

/* -------------------------------------------------------------------
* term_iterator : impl
* ------------------------------------------------------------------*/

term_iterator::term_iterator(const term_reader* owner):
  attrs_(2), // version10::term_meta + frequency
  cur_block_(nullptr),
  owner_(owner),
  freq_(nullptr) {
  assert(owner_);
  state_ = attrs_.add<version10::term_meta>();
  if (owner_->field_->features.check<frequency>()) {
    freq_ = attrs_.add<frequency>();
  }
}

void term_iterator::read() {
  /* read attributes */
  cur_block_->load_data(
    *owner_->field_,
    *owner_->owner_->pr_
  );
}

bool term_iterator::next() {
  /* iterator at the beginning or seek to cached state was called */
  if (!cur_block_) {
    if (term_.empty()) {
      /* iterator at the beginning */
      const vector_byte_fst& fst = *owner_->fst_;
      cur_block_ = push_block(fst.Final(fst.Start()), 0);
      cur_block_->load();
    } else {
      // seek to the term with the specified state was called from
      // term_iterator::seek(const bytes_ref&, const attribute&),
      // need create temporary "bytes_ref" here, since "seek" calls
      // term_.reset() internally,
      // note, that since we do not create extra copy of term_
      // make sure that it does not reallocate memory !!!
#ifdef IRESEARCH_DEBUG
      const SeekResult res = seek_equal(bytes_ref(term_));
      assert(SeekResult::FOUND == res);
#else
      seek_equal(bytes_ref(term_));
#endif
    }
  }

  /* pop finished blocks */
  while (cur_block_->block_end()) {
    if (cur_block_->sub_count() > 0) {
      cur_block_->next_block();
      cur_block_->load();
    } else if (&block_stack_.front() == cur_block_) { /* root */
      term_.reset();
      cur_block_->reset();
      sstate_.resize(0);
      return false;
    } else {
      const uint64_t start = cur_block_->start();
      cur_block_ = pop_block();
      *state_ = cur_block_->state();
      if (cur_block_->dirty() || cur_block_->sub_start() != start) {
        /* here we currently on non block that was not loaded yet */
        cur_block_->scan_to_block(term_); /* to sub-block */
        cur_block_->load();
        cur_block_->scan_to_block(start);
      }
    }

    sstate_.resize(std::min(sstate_.size(), cur_block_->prefix()));
  }

  /* push new block or next term */
  for (cur_block_->next();
       EntryType::ET_BLOCK == cur_block_->type();
       cur_block_->next()) {
    cur_block_ = push_block(cur_block_->sub_start(), term_.size());
    cur_block_->load();
  }

  return true;
}

#if defined(_MSC_VER)
  #pragma warning( disable : 4706 )
#elif defined (__GNUC__)
  #pragma GCC diagnostic ignored "-Wparentheses"
#endif

ptrdiff_t term_iterator::seek_cached( 
    size_t& prefix, arc::stateid_t& state,
    byte_weight& weight, const bytes_ref& target) {
  assert(!block_stack_.empty());
  const byte_type* pterm = term_.c_str();
  const byte_type* ptarget = target.c_str();

  // reset current block to root
  auto* cur_block = &block_stack_.front();

  // determine common prefix between target term and current
  {
    auto begin = sstate_.begin();
    auto end = begin + std::min(target.size(), sstate_.size());

    for (;begin != end && *pterm == *ptarget; ++begin, ++pterm, ++ptarget) {
      fst_utils::append(weight, begin->weight);
      state = begin->state;
      cur_block = begin->block;
    }

    prefix = size_t(pterm - term_.c_str());
  }

  // inspect suffix and determine our current position 
  // with respect to target term (before, after, equal)
  ptrdiff_t cmp = std::char_traits<byte_type>::compare(
    pterm, ptarget, 
    std::min(target.size(), term_.size()) - prefix);

  if (!cmp) {
    cmp = term_.size() - target.size();
  }

  if (cmp) {
    cur_block_ = cur_block; // update cir_block if not on the same block

    // truncate block_stack_ to match path
    while (!block_stack_.empty() && &(block_stack_.back()) != cur_block_) {
      block_stack_.pop_back();
    }
  }

  // cmp < 0 : target term is after the current term
  // cmp == 0 : target term is current term
  // cmp > 0 : target term is before current term
  return cmp;
}

SeekResult term_iterator::seek_equal(const bytes_ref& term) {
  assert(owner_->fst_);

  const vector_byte_fst& fst = *owner_->fst_;
  typedef decltype(fst.Final(0)) weight_t;

  size_t prefix = 0; /* number of current symbol to process */
  arc::stateid_t state = fst.Start(); /* start state */
  byte_weight out; /* aggregated fst output */

  if (cur_block_) {
    const auto cmp = seek_cached(prefix, state, out, term);
    if (cmp > 0) {
      /* target term is before the current term */
      cur_block_->reset();
    } else if (0 == cmp) {
      /* we already at current term */
      return SeekResult::FOUND;
    }
  } else {
    cur_block_ = push_block(fst.Final(state), prefix);
  }

  term_.oversize(term.size());
  term_.reset(prefix); /* reset to common seek prefix */
  sstate_.resize(prefix); /* remove invalid cached arcs */

  bool found = fst_byte_builder::final != state;
  typedef fst::SortedMatcher<vector_byte_fst> matcher_t;
  fst::ExplicitMatcher<matcher_t> matcher(fst, fst::MATCH_INPUT); // avoid implicit loops
  while (found && prefix < term.size()) {
    matcher.SetState(state);
    if (found = matcher.Find(term[prefix])) {
      const byte_arc& arc = matcher.Value();
      term_ += byte_type(arc.ilabel);
      fst_utils::append(out, arc.weight);
      ++prefix;

      const weight_t weight = fst.Final(state = arc.nextstate);
      if (weight_t::One() != weight && !fst_utils::is_zero(weight)) {
        cur_block_ = push_block(fst::Times(out, weight), prefix);
      } else if (fst_byte_builder::final == state) {
        cur_block_ = push_block(std::move(out), prefix);
        found = false;
      }

      /* cache found arcs, we can reuse it in further seek's, 
       * avoiding expensive FST lookups */
      sstate_.emplace_back(state, arc.weight, cur_block_);
    }
  }

  assert(cur_block_);
  sstate_.resize(cur_block_->prefix());
  cur_block_->scan_to_block(term);
  if (!cur_block_->meta().terms()) {
    /* current block does not contain terms */
    term_.reset(prefix);
    return SeekResult::NOT_FOUND;
  }
  cur_block_->load();
  return cur_block_->scan_to_term(term);
}

SeekResult term_iterator::seek_ge(const bytes_ref& term) {
  switch (seek_equal(term)) {
    case SeekResult::FOUND:
      // we have found the specified term
      return SeekResult::FOUND;
    case SeekResult::NOT_FOUND:
      assert(cur_block_);
      // in case of dirty block we should just load it and call next,
      // block may be dirty here if it was denied by seek_equal 
      // when it has no terms
      if (!cur_block_->dirty()) {
        // we are on the term or block after the specified term 
        if (ET_TERM == cur_block_->type()) {
          // we already on the greater term 
          return SeekResult::NOT_FOUND;
        }

        // in case of block entry we should step into 
        // and move to the next term 
        cur_block_ = push_block(cur_block_->sub_start(), term_.size());
      }
      cur_block_->load();
    case SeekResult::END:
      /* we are in the end of the block */
      return next()
        ? SeekResult::NOT_FOUND /* have moved to the next entry*/
        : SeekResult::END; /* have no more terms */
  }

  assert(false);
  return SeekResult::END;
}

bool term_iterator::seek(const bytes_ref& term) {
  return SeekResult::FOUND == seek_equal(term);
}

#if defined(_MSC_VER)
  #pragma warning( default : 4706 )
#elif defined (__GNUC__)
  #pragma GCC diagnostic pop
#endif

doc_iterator::ptr term_iterator::postings(const flags& features) const {
  const field_meta& field = *owner_->field_;
  postings_reader& pr = *owner_->owner_->pr_;
  if (cur_block_) {
    /* read attributes */
    cur_block_->load_data(field, pr);
  }
  return pr.iterator(field.features, attrs_, features);
}

index_input& term_iterator::terms_input() const {
  if (!terms_in_) {
    terms_in_ = owner_->owner_->terms_in_->clone();
  }
  return *terms_in_;
}

/* -------------------------------------------------------------------
* term_reader : impl
* ------------------------------------------------------------------*/

term_reader::term_reader(term_reader&& rhs)
  : min_term_(std::move(rhs.min_term_)),
    max_term_(std::move(rhs.max_term_)),
    terms_count_(rhs.terms_count_),
    doc_count_(rhs.doc_count_),
    doc_freq_(rhs.doc_freq_),
    term_freq_(rhs.term_freq_),
    field_(rhs.field_),
    fst_(rhs.fst_),
    owner_(rhs.owner_) {
  min_term_ref_ = min_term_;
  max_term_ref_ = max_term_;
  rhs.min_term_ref_ = bytes_ref::nil;
  rhs.max_term_ref_ = bytes_ref::nil;
  rhs.terms_count_ = 0;
  rhs.doc_count_ = 0;
  rhs.doc_freq_ = 0;
  rhs.term_freq_ = 0;
  rhs.field_ = nullptr;
  rhs.fst_ = nullptr;
  rhs.owner_ = nullptr;
}

term_reader::~term_reader() {
  delete fst_;
}

const field_meta& term_reader::field() const {
  return *field_;
}

seek_term_iterator::ptr term_reader::iterator() const {
  return seek_term_iterator::make<detail::term_iterator>( this );
}
  
bool term_reader::prepare(
    index_input& meta_in, 
    std::istream& fst_in, 
    const field_meta& field, 
    field_reader& owner) {

  // load field metadata
  terms_count_ = meta_in.read_vlong();
  doc_count_ = meta_in.read_vlong();
  doc_freq_ = meta_in.read_vlong();
  min_term_ = read_string<bstring>(meta_in);
  min_term_ref_ = min_term_;
  max_term_ = read_string<bstring>(meta_in);
  max_term_ref_ = max_term_;
  term_freq_ = field.features.check<frequency>()
    ? meta_in.read_vlong()
    : 0; // TODO: not 0 but reserved value

  fst_ = vector_byte_fst::Read(fst_in, fst::FstReadOptions());
  assert(fst_);

  owner_ = &owner;
  field_ = &field;
  return true;
}

NS_END // detail

/* -------------------------------------------------------------------
* field_writer
* ------------------------------------------------------------------*/

/* field_writer::field_meta */

field_writer::field_meta::field_meta(
  iresearch::bstring&& min_term,
  iresearch::bstring&& max_term,
  iresearch::field_id id,
  uint64_t index_start,
  uint64_t term_count,
  size_t doc_count,
  uint64_t doc_freq,
  uint64_t term_freq,
  bool write_freq
)
  : min_term(std::move(min_term)),
    max_term(std::move(max_term)),
    index_start(index_start),
    doc_freq(doc_freq),
    doc_count(doc_count),
    term_freq(term_freq),
    term_count(term_count),
    id(id),
    write_freq(write_freq) {
}

field_writer::field_meta::field_meta(field_writer::field_meta&& rhs)
  : min_term(std::move(rhs.min_term)),
    max_term(std::move(rhs.max_term)),
    index_start(rhs.index_start),
    doc_freq(rhs.doc_freq),
    doc_count(rhs.doc_count),
    term_freq(rhs.term_freq),
    term_count(rhs.term_count),
    id(rhs.id),
    write_freq(rhs.write_freq) {
  rhs.index_start = 0;
  rhs.doc_freq = 0;
  rhs.doc_count = 0;
  rhs.term_freq = 0;
  rhs.term_count = 0;
  rhs.id = type_limits<type_t::field_id_t>::invalid();
  rhs.write_freq = false;
}

void field_writer::field_meta::write(data_output& out) const {
  out.write_vint(static_cast<uint32_t>(id));
  out.write_vlong(term_count);
  out.write_vlong(doc_count);
  out.write_vlong(doc_freq);
  write_string(out, min_term);
  write_string(out, max_term);
  if (write_freq) {
    out.write_vlong(term_freq);
  }
}

const string_ref field_writer::FORMAT_TERMS = "block_tree_terms_dict";
const string_ref field_writer::TERMS_EXT = "tm";
const string_ref field_writer::FORMAT_TERMS_INDEX = "block_tree_terms_index";
const string_ref field_writer::TERMS_INDEX_EXT = "ti";

void field_writer::write_term_entry( const detail::entry& e, size_t prefix, bool leaf ) {
  using namespace detail;

  const size_t suf_size = e.data.size() - prefix;
  suffix.stream.write_vlong( leaf ? suf_size : shift_pack_64( suf_size, false ) );
  suffix.stream.write_bytes( e.data.c_str() + prefix, suf_size );

  pw->encode( stats.stream, e.term->attrs );
}

void field_writer::write_block_entry(
    const detail::entry& e,
    size_t prefix,
    uint64_t block_start ) {
    const size_t suf_size = e.data.size() - prefix;
  suffix.stream.write_vlong( shift_pack_64( suf_size, true ) );
  suffix.stream.write_bytes( e.data.c_str() + prefix, suf_size );

  /* current block start pointer
  * should be greater */
  assert( block_start > e.block->start );
  suffix.stream.write_vlong( block_start - e.block->start );
}

void field_writer::write_block(
    std::list< detail::entry >& blocks, size_t prefix,
    size_t begin, size_t end,
    const detail::block_meta& meta,
    int16_t label ) {
  assert( end > begin );
  using namespace detail;

  /* begin of the block */
  const uint64_t block_start = terms_out->file_pointer();

  /* write block header */
  terms_out->write_vint( 
    shift_pack_32( static_cast< uint32_t >( end - begin ),
                end == stack.size() ) 
  );

  /* write block entries */
  const uint64_t leaf = !meta.blocks();

  std::list< detail::block_t::prefixed_output > index;

  pw->begin_block();

  for ( size_t i = begin; i < end; ++i ) {
    entry& e = stack[i];
    assert(starts_with(e.data, bytes_ref(last_term.c_str(), prefix)));

    switch ( e.type ) {
      case detail::ET_TERM:
        write_term_entry( e, prefix, leaf > 0 );
        break;
      case detail::ET_BLOCK: {
        write_block_entry( e, prefix, block_start );
        index.splice( index.end(), e.block->index );
      } break;
      default:
        assert( false );
        break;
    }
  }

  suffix.stream.flush();
  stats.stream.flush();

  terms_out->write_vlong( shift_pack_64(static_cast<uint64_t>(suffix.stream.file_pointer()), leaf > 0 ) );
  suffix.file >> *terms_out;

  terms_out->write_vlong(static_cast<uint64_t>(stats.stream.file_pointer()));
  stats.file >> *terms_out;

  suffix.stream.reset();
  stats.stream.reset();

  /* add new block to the list of created blocks */
  blocks.emplace_back(bytes_ref(last_term.c_str(), prefix), block_start, meta, label);

  if ( !index.empty() ) {
    blocks.back().block->index = std::move( index );
  }
}

void field_writer::merge_blocks( std::list< detail::entry >& blocks ) {
  assert( !blocks.empty() );
  using namespace detail;

  std::list<entry>::iterator it = blocks.begin();
  entry& root = *it;

  root.block->index.emplace_front(std::move(root.data));

  // First byte in block header must not be equal to fst::kStringInfinity
  // Consider the following:
  //   StringWeight0 -> { fst::kStringInfinity 0x11 ... }
  //   StringWeight1 -> { fst::KStringInfinity 0x22 ... }
  //   CommonPrefix = fst::Plus(StringWeight0, StringWeight1) -> { fst::kStringInfinity }
  //   Suffix = fst::Divide(StringWeight1, CommonPrefix) -> { fst::kStringBad }
  // But actually Suffix should be equal to { 0x22 ... }
  assert(static_cast<byte_type>(root.block->meta) != fst::kStringInfinity);

  /* will be just several bytes here */
  block_t::prefixed_output& out = *root.block->index.begin();
  out.write_byte(static_cast<byte_type>(root.block->meta)); // block metadata
  out.write_vlong(root.block->start); // start pointer of the block

  if (root.block->meta.floor()) {
    out.write_vlong(static_cast< uint64_t >(blocks.size()-1));
    for ( ++it; it != blocks.end(); ++it ) {
      const block_t* block = it->block;
      assert( block->label != block_t::INVALID_LABEL );
      assert( block->start > root.block->start );

      const uint64_t start_delta = it->block->start - root.block->start;
      out.write_byte( static_cast< byte_type >( block->label & 0xFF ) );
      out.write_vlong( start_delta );
      out.write_byte( static_cast< byte_type >( block->meta ) );

      root.block->index.splice(
        root.block->index.end(),
        it->block->index
      );
    }
  } else {
    for ( ++it; it != blocks.end(); ++it ) {
      root.block->index.splice(
        root.block->index.end(),
        it->block->index
      );
    }
  }
}

void field_writer::write_blocks( size_t prefix, size_t count ) {
  /* only root node able to write whole stack */
  assert( prefix || count == stack.size() );
  using namespace detail;

  /* block metadata */
  detail::block_meta meta;

  /* created blocks */
  std::list< entry > blocks;

  const size_t end = stack.size();
  const size_t begin = end - count;
  size_t block_start = begin; /* begin of current block to write */

  int16_t last_label = block_t::INVALID_LABEL; /* last lead suffix label */
  int16_t next_label = block_t::INVALID_LABEL; /* next lead suffix label in current block */
  for ( size_t i = begin; i < end; ++i ) {
    const entry& e = stack[i];

    const int16_t label = e.data.size() == prefix
      ? block_t::INVALID_LABEL
      : e.data[prefix];

    if ( last_label != label ) {
      const size_t block_size = i - block_start;

      if ( block_size >= min_block_size
           && end - block_start > max_block_size ) {
        meta.floor( block_size < count );
        write_block( blocks, prefix, block_start, i, meta, next_label );
        next_label = label;
        meta.reset();
        block_start = i;
      }

      last_label = label;
    }

    meta.type( e.type );
  }

  /* write remaining block */
  if ( block_start < end ) {
    meta.floor( end - block_start < count );
    write_block( blocks, prefix, block_start, end, meta, next_label );
  }

  /* merge blocks into 1st block */
  merge_blocks( blocks );

  /* remove processed entries from the
   * top of the stack */
  stack.erase( stack.begin() + begin, stack.end() );

  /* move root block from temporary storage
   * to the top of the stack */
  if (!blocks.empty()) {
    stack.emplace_back(std::move(blocks.front()));
  }
}

void field_writer::push( const bytes_ref& term ) {
  const size_t limit = std::min( last_term.size(), term.size() );

  /* find common prefix */
  size_t pos = 0;
  while ( pos < limit && term[pos] == last_term[pos] ) {
    ++pos;
  }

  for ( size_t i = last_term.empty() ? 0 : last_term.size() - 1; i > pos; ) {
    --i; /* should use it here as we use size_t */
    const size_t top = stack.size() - prefixes[i];
    if (top > min_block_size) {
      write_blocks(i + 1, top);
      prefixes[i] -= (top - 1);
    }
  }

  prefixes.resize(term.size());
  std::fill(prefixes.begin() + pos, prefixes.end(), stack.size());
  last_term.assign(term.c_str(), term.size());
}

field_writer::field_writer(
    iresearch::postings_writer::ptr&& pw,
    uint32_t min_block_size,
    uint32_t max_block_size )
  : pw(std::move(pw)),
    fst_buf_(new detail::fst_buffer()),
    prefixes( DEFAULT_SIZE, 0 ),
    term_count( 0 ),
    min_block_size( min_block_size ),
    max_block_size( max_block_size ) {
  assert( this->pw );
  assert( min_block_size > 1 );
  assert( min_block_size <= max_block_size );
  assert( 2 * ( min_block_size - 1 ) <= max_block_size );
  min_term.first = false;
}

field_writer::~field_writer() { }

void field_writer::prepare( const iresearch::flush_state& state ) {
  // reset writer state
  fields.clear();
  last_term.clear();
  max_term.clear();
  min_term.first = false;
  min_term.second.clear();
  prefixes.assign(DEFAULT_SIZE, 0);
  stack.clear();
  stats.reset();
  suffix.reset();
  term_count = 0;

  fields.reserve( state.fields_count );

  // prepare terms and index output
  std::string str;
  detail::prepare_output(str, terms_out, state, TERMS_EXT, FORMAT_TERMS, FORMAT_MAX);
  detail::prepare_output(str, index_out, state, TERMS_INDEX_EXT, FORMAT_TERMS_INDEX, FORMAT_MAX);

  // prepare postings writer
  pw->prepare(*terms_out, state);
}

void field_writer::write(
    iresearch::field_id id,
    const iresearch::flags& field,
    iresearch::term_iterator& terms) {
  REGISTER_TIMER_DETAILED();
  begin_field(field);

  uint64_t sum_dfreq = 0;
  uint64_t sum_tfreq = 0;

  const version10::documents* docs = pw->attributes().get<version10::documents>();
  assert(docs);

  /* aggregated by postings writer term attributes */
  attributes attrs;

  for (; terms.next();) {
    auto postings = terms.postings(flags::empty_instance());
    pw->write(*postings, attrs);

    const term_meta* meta = attrs.add<term_meta>();
    const frequency *tfreq = nullptr;
    if (field.check<frequency>()) {
      tfreq = attrs.add<frequency>();
    }

    sum_dfreq += meta->docs_count;
    if (tfreq) {
      sum_tfreq += tfreq->value;
    }

    if (meta->docs_count) {
      const bytes_ref &term = terms.value();
      push(term);

      /* push term to the top of the stack */
      stack.emplace_back(term, std::move(attrs));

      if (!min_term.first) {
        min_term.first = true;
        min_term.second = term;
      }

      max_term.assign(term.c_str(), term.size());

      /* increase processed term count */
      ++term_count;
    }
  }

  end_field(id, field, sum_dfreq, sum_tfreq, docs->value->count());
}

void field_writer::begin_field(const iresearch::flags& field) {
  assert(terms_out);
  assert(index_out);

  // at the beginning of the field
  // there should be no pending
  // entries at all
  assert(stack.empty());
//  assert(blocks_.empty());

  // reset first field term
  min_term.first = false;
  min_term.second.clear();
  term_count = 0;

  pw->begin_field(field);
}

void field_writer::end_field(
    field_id id,
    const iresearch::flags& field,
    uint64_t total_doc_freq,
    uint64_t total_term_freq,
    size_t doc_count) {
  assert( terms_out );
  assert( index_out );
  using namespace detail;

  if (term_count > 0) {
    // cause creation of all final blocks
    push(bytes_ref::nil);

    // write root block with empty prefix
    write_blocks(0, stack.size());

    assert(1 == stack.size());
    const entry& root = *stack.begin();

    // build fst
    assert(fst_buf_);
    auto& fst = fst_buf_->reset(root.block->index);

    // save fst
    const size_t index_start = index_out->file_pointer();
    {
      output_buf isb(index_out.get());
      std::ostream os(&isb);
      fst.Write(os, fst::FstWriteOptions());
    }

    fields.emplace_back(
      std::move(min_term.second),
      std::move(max_term),
      id,
      static_cast<uint64_t>(index_start),
      term_count,
      doc_count,
      total_doc_freq,
      total_term_freq,
      field.check<frequency>()
    );

    stack.clear();
  }
}

void field_writer::end() {
  assert( terms_out );
  assert( index_out );

  /* finish postings */
  pw->end();

  const size_t terms_start = terms_out->file_pointer();
  const size_t index_start = index_out->file_pointer();

  /* finish fields */
  terms_out->write_vint(static_cast<uint32_t>(fields.size()));
  for (const field_meta& fm : fields) {
    fm.write(*terms_out);
    index_out->write_vlong(fm.index_start);
  }

  terms_out->write_long(static_cast<uint64_t>(terms_start));
  format_utils::write_footer(*terms_out);
  terms_out.reset(); // ensure stream is closed

  index_out->write_long(static_cast<uint64_t>(index_start));
  format_utils::write_footer(*index_out);
  index_out.reset(); // ensure stream is closed
}

/* -------------------------------------------------------------------
* field_reader
* ------------------------------------------------------------------*/

field_reader::field_reader(iresearch::postings_reader::ptr&& pr)
  : pr_(std::move(pr)) {
  assert( pr_ );
}

void field_reader::prepare( const reader_state& state ) {
  std::string str;

  // prepare terms input
  {
    // check term header
    detail::prepare_input(
      str, terms_in_, state,
      field_writer::TERMS_EXT,
      field_writer::FORMAT_TERMS,
      field_writer::FORMAT_MIN,
      field_writer::FORMAT_MAX
    );
  
    // prepare postings reader
    pr_->prepare(*terms_in_, state);

    /* Since terms dictionary are too large
     * it is too costly to verify checksum of
     * the entire file. Here we perform cheap
     * error detection which could recognize
     * some forms of corruption. */
    format_utils::read_checksum(*terms_in_);

    /* seek to term start */
    terms_in_->seek(
      terms_in_->length() - format_utils::FOOTER_LEN - sizeof(uint64_t)
    );

    const uint64_t terms_start = terms_in_->read_long();
    terms_in_->seek(terms_start);
  }

  {
    /* check index header */
    index_input::ptr index_in;
    detail::prepare_input(
      str, index_in, state,
      field_writer::TERMS_INDEX_EXT,
      field_writer::FORMAT_TERMS_INDEX,
      field_writer::FORMAT_MIN,
      field_writer::FORMAT_MAX
    );

    /* check index checksum */
    format_utils::check_checksum<boost::crc_32_type>(*index_in);
    {
      /* seek to index start */
      index_in->seek(
        index_in->length() - format_utils::FOOTER_LEN - sizeof(uint64_t)
      );

      const uint64_t index_start = index_in->read_long();
      index_in->seek(index_start);
    }

    //
    // There may be stored only fields which are not encountered here
    // Since we access to field_reader by id, we store pointers to 
    // indexed fields in separate array of size same as the overall 
    // number of fields (indexed or stored).
    //
    // It's maybe better to sequentially store indexed fields in order
    // to provide dense field identifiers (and store readers within a
    // single array), but there are some problems with stored fields 
    // since when field metadata writes to directory stored document 
    // headers (which are contains field identifiers for  now) are 
    // already written.
    //

    auto fst_in = index_in->clone();
    input_buf isb(fst_in.get());
    std::istream input(&isb); // stream for reading fst

    // read terms for each indexed field
    size_t size = terms_in_->read_vint();
    fields_.reserve(size);
    fields_mask_.resize(state.fields->size());
    while (size) {
      const field_meta* field = state.fields->find(terms_in_->read_vint());

      if (!field) {
        throw index_error(); // TODO: corrupted index: no field id
      }

      fields_.emplace_back();
      auto& reader = fields_.back();

      fst_in->seek(index_in->read_vlong()); // seek to the beginning of fst for field

      if (!reader.prepare(*terms_in_, input, *field, *this)) {
        throw index_error(); // TODO: corrupted index
      }

      fields_mask_[field->id] = &reader;

      --size;
    }
  }
}

const iresearch::term_reader* field_reader::terms(field_id field) const {
  if (field >= fields_mask_.size()) {
    return nullptr;
  }

  return fields_mask_[field];
}

size_t field_reader::size() const {
  return fields_.size();
}

NS_END /* burst_trie */
NS_END /* root */
