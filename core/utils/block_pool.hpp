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

#ifndef IRESEARCH_BLOCK_POOL_H
#define IRESEARCH_BLOCK_POOL_H

#include <vector>
#include <functional>
#include <algorithm>
#include <memory>
#include <cmath>
#include <iterator> 
#include <cassert>
#include <cstring>

#include "memory.hpp"
#include "ebo.hpp"
#include "misc.hpp"

NS_ROOT

template< typename ContType >
class block_pool_const_iterator : public std::iterator < std::random_access_iterator_tag, typename ContType::value_type > {
  public:
  typedef ContType container;
  typedef std::iterator< std::random_access_iterator_tag, typename container::value_type > base;

  typedef typename base::pointer pointer;
  typedef typename base::difference_type difference_type;
  typedef typename base::reference reference;
  typedef const typename container::value_type& const_reference;
  typedef typename container::const_pointer const_pointer;

  explicit block_pool_const_iterator(container* pool)
    : pool_(pool),
      block_start_(pool_->size()) {
  }

  block_pool_const_iterator(container* pool, size_t offset)
    : pool_(pool),
      block_start_(0) {
    reset(offset);
  }

  block_pool_const_iterator& operator++() {
    seek_relative(1);
    return *this;
  }

  block_pool_const_iterator& operator--() {
    seek_relative(-1);
    return *this;
  }

  block_pool_const_iterator operator--(int) {
    block_pool_const_iterator state;
    --*this;
    return state;
  }

  block_pool_const_iterator operator++(int) {
    block_pool_const_iterator state;
    ++*this;
    return state;
  }

  const_reference operator*() const { return *pos_; }

  const_reference operator[](difference_type offset) const {
    auto& rep = pool_->get_rep();
    const auto pos = pos_ + offset;
    if (pos < begin_ || pos >= end_) {
      return pool_->at(block_start_ + std::distance(begin_, pos));
    }

    return *pos;
  }

  block_pool_const_iterator& operator+=(difference_type offset) {
    seek_relative(offset);
    return *this;
  }

  block_pool_const_iterator& operator-=(difference_type offset) {
    seek_relative(offset);
    return *this;
  }

  block_pool_const_iterator operator+(difference_type offset) const {
    return block_pool_const_iterator(pool_, block_start_ + std::distance(begin_, pos_) + offset);
  }

  block_pool_const_iterator operator-(difference_type offset) const {
    return block_pool_const_iterator(pool_, block_start_ + std::distance(begin_, pos_) - offset);
  }

  difference_type operator-(const block_pool_const_iterator& rhs) const {
    assert(pool_ == rhs.pool_);
    return pool_offset() - rhs.pool_offset();
  }

  bool operator==(const block_pool_const_iterator& rhs) const {
    assert(pool_ == rhs.pool_);
    return pool_offset() == rhs.pool_offset();
  }

  bool operator!=(const block_pool_const_iterator& rhs) const {
    return !(*this == rhs);
  }

  bool operator>(const block_pool_const_iterator& rhs) const {
    assert(pool_ == rhs.pool_);
    return pool_offset() > rhs.pool_offset();
  }

  bool operator<(const block_pool_const_iterator& rhs) const {
    assert(pool_ == rhs.pool_);
    return pool_offset() < rhs.pool_offset();
  }

  bool operator<=(const block_pool_const_iterator& rhs) const {
    return !(*this > rhs);
  }

  bool operator>= (const block_pool_const_iterator& rhs) const {
    return !(*this < rhs);
  }

  bool eof() const { return block_start_ + std::distance(begin_, pos_) == pool_->size(); }

  const_pointer buffer() const {
    return pos_;
  }

  size_t remain() const { return pool_->block_size() - std::distance(begin_, pos_); }

  size_t offset() const { return std::distance(begin_, pos_); }

  size_t block_offset() const { return block_start_; }

  size_t pool_offset() const { return block_start_ + std::distance(begin_, pos_); }

  void refresh() {
    auto& rep = pool_->get_rep();
    assert(rep.block_len_);
    const auto pos = std::distance(begin_, pos_);
    begin_ = rep.buffers_[block_start_ / rep.block_len_].get();
    end_ = begin_ + rep.block_len_;
    pos_ = begin_ + pos;
  }

  void reset(size_t offset) {
    if (offset >= pool_->size()) {
      block_start_ = pool_->size();
      begin_ = end_ = pos_ = nullptr;
      return;
    }

    auto& rep = pool_->get_rep();
    const size_t idx = offset / rep.block_len_;
    assert(idx < rep.buffers_.size());

    const size_t pos = offset % rep.block_len_;
    assert(pos < rep.block_len_);

    begin_ = rep.buffers_[idx].get();
    end_ = begin_ + rep.block_len_;
    block_start_ = idx * rep.block_len_;
    pos_ = begin_ + pos;
  }

  const container& parent() const { return pool_; }

 protected:
  void seek_relative(difference_type offset) {
    pos_ += offset;
    if (pos_ < begin_ || pos_ >= end_) {
      reset(block_start_ + std::distance(begin_, pos_));
    }
  }

  container* pool_;
  pointer begin_{};
  pointer end_{};
  pointer pos_{};
  size_t block_start_;
}; // block_pool_const_iterator 

template< typename ContType >
block_pool_const_iterator< ContType > operator+(
    size_t offset,
    const block_pool_const_iterator< ContType >& it) {
  return it + offset;
}

template< typename ContType >
class block_pool_iterator : public block_pool_const_iterator < ContType > {
 public:
  typedef ContType container;
  typedef block_pool_const_iterator< container > base;
  typedef typename base::pointer pointer;
  typedef typename base::difference_type difference_type;
  typedef typename base::reference reference;

  using base::operator-;

  explicit block_pool_iterator(container* pool)
    : base(pool) {}

  block_pool_iterator(container* pool, size_t offset)
    : base(pool, offset) {}

  block_pool_iterator& operator++() {
    return static_cast<block_pool_iterator&>(base::operator++());
  }

  block_pool_iterator& operator--() {
    return static_cast<block_pool_iterator&>(base::operator--());
  }

  block_pool_iterator operator--(int) {
    block_pool_iterator state(*this);
    --*this;
    return state;
  }

  block_pool_iterator operator++(int) {
    block_pool_iterator state(*this);
    ++*this;
    return state;
  }

  reference operator*() {
    return const_cast<reference>(*static_cast<base&>(*this));
  }

  reference operator[](difference_type offset) {
    return const_cast<reference>(static_cast<base&>(*this)[offset]);
  }

  block_pool_iterator& operator+=(difference_type offset) {
    return static_cast<block_pool_iterator&>(base::operator+=(offset));
  }

  block_pool_iterator& operator-=(difference_type offset) {
    return static_cast<block_pool_iterator&>(base::operator-=(offset));
  }

  block_pool_iterator operator+(difference_type offset) const {
    return block_pool_iterator(this->pool_, this->pool_offset() + offset);
  }

  block_pool_iterator operator-(difference_type offset) const {
    return block_pool_iterator(this->pool_, this->pool_offset() - offset);
  }

  pointer buffer() { return pos_; }

  container& parent() { return const_cast<container&>(*this->pool_); }
}; // block_pool_iterator 

template< typename ContType >
block_pool_iterator< ContType > operator+(
    size_t offset,
    const block_pool_iterator< ContType >& it) {
  return it + offset;
}

template< typename ContType >
class block_pool_reader : public std::iterator < std::input_iterator_tag, typename ContType::value_type > {
 public:
  typedef ContType container;
  typedef typename container::const_iterator const_iterator;
  typedef typename container::const_reference const_reference;
  typedef typename container::const_pointer const_pointer;
  typedef typename container::pointer pointer;
  typedef typename container::value_type value_type;

  block_pool_reader(const container& pool, size_t offset)
    : where_(pool, offset) {}

  explicit
    block_pool_reader(const const_iterator& where)
    : where_(where) {}

  ~block_pool_reader() {}

  const_reference operator*() const {
    assert(!where_.eof());
    return *where_;
  }

  const_pointer operator->() const { return &(operator*()); };

  block_pool_reader& operator++() {
    assert(!eof());

    ++where_;
    return *this;
  }

  block_pool_reader operator++(int) {
    assert(!eof());

    block_pool_reader tmp = *this;
    ++where_;
    return tmp;
  }

  bool eof() const { return where_.pool_offset() == where_.parent().size(); }

  size_t read(pointer b, size_t len) {
    assert(!eof());
    assert(b != nullptr);

    size_t items_read = 0;
    size_t to_copy = 0;
    const size_t block_size = where_.parent().block_size();

    while (len) {
      if (where_.eof()) {
        break;
      }

      to_copy = std::min(block_size - where_.offset(), len);
      memcpy(b, where_.buffer(), to_copy * sizeof(value_type));

      len -= to_copy;
      where_ += to_copy;
      b += to_copy;
      items_read += to_copy;
    }

    return items_read;
  }

 private:
  const_iterator where_;
}; // block_pool_reader 

NS_LOCAL

struct level {
  size_t next; // next level
  size_t size; // size of the level in bytes
}; // level

const level LEVELS[] = {
  { 1, 5 }, { 2, 14 }, { 3, 20 }, { 4, 30 }, { 5, 40 },
  { 6, 40 }, { 7, 80 }, { 8, 80 }, { 9, 120 }, { 9, 200 }
};

const byte_type MAGIC = 16;
const byte_type MAGIC_MASK = MAGIC - 1;

NS_END

template< typename ContType >
class block_pool_sliced_reader : public std::iterator < std::input_iterator_tag, typename ContType::value_type > {
 public:
  typedef ContType container;
  typedef typename container::const_iterator const_iterator;
  typedef typename container::pointer pointer;
  typedef typename container::const_pointer const_pointer;
  typedef typename container::const_reference const_reference;
  typedef typename container::value_type value_type;

  block_pool_sliced_reader(const container& pool)
    : where_(pool) {}

  block_pool_sliced_reader(const const_iterator& where, size_t end)
    : where_(where), end_(end) {
    init();
  }

  block_pool_sliced_reader(const container& pool, size_t offset, size_t end)
    : where_(pool, offset), end_(end) {
    init();
  }

  ~block_pool_sliced_reader() {}

  const_reference operator*() const {
    assert(!eof());
    return *where_;
  }

  const_pointer operator->() const { return &(operator*()); };

  block_pool_sliced_reader& operator++() {
    assert(!eof());
    next();
    return *this;
  }

  block_pool_sliced_reader operator++(int) {
    assert(!eof());
    block_pool_sliced_reader tmp = *this;
    next();
    return tmp;
  }

  bool eof() const {
    assert(where_.pool_offset() <= end_);
    return where_.pool_offset() == end_;
  }

  inline const_iterator& position() const { return where_; }

  inline size_t pool_offset() const { return where_.pool_offset(); }

  inline container& parent() { return where_.parent(); }

  inline const container& parent() const { return where_.parent(); }

  size_t read(pointer b, size_t len) {
    assert(!eof());
    assert(b != nullptr);

    size_t to_copy = 0;
    size_t items_read = 0;

    while (len) {
      to_copy = std::min(len, size_t(left_));
      memcpy(b, where_.buffer(), to_copy * sizeof(value_type));

      b += to_copy;
      where_ += to_copy;
      len -= to_copy;
      left_ -= to_copy;
      items_read += to_copy;

      if (!left_) {
        next_slice();
      }
    }

    return items_read;
  }

 private:
  void next_slice() {
    // TODO: check for overflow. max_size = MAX(uint32_t)-address_size-1
    // base case where last slice of pool which does not have address footer
    if (where_.pool_offset() + sizeof(uint32_t) >= end_) {
      left_ = end_ - where_.pool_offset();
    } else {
      level_ = LEVELS[level_].next;

      const size_t next_address = bytes_io<uint32_t>::read(where_);

      where_.reset(next_address);
      left_ = std::min(end_ - where_.pool_offset(),
                       LEVELS[level_].size - sizeof(uint32_t));
    }
  }

  inline void next() {
    ++where_;
    --left_;

    if (!left_) {
      next_slice();
    }
  }

  inline void init() {
    assert(end_ >= 0 && where_.pool_offset() <= end_);

    left_ = std::min(end_ - where_.pool_offset(),
                     LEVELS[level_].size - sizeof(uint32_t));
  }

  const_iterator where_;
  size_t level_{};
  size_t end_{};
  size_t left_{};
}; // block_pool_sliced_reader 

template< typename ContType >
class block_pool_sliced_inserter;

template< typename ContType >
class block_pool_inserter : public std::iterator < std::output_iterator_tag, void, void, void, void > {
 public:
  typedef ContType container;
  typedef typename container::iterator iterator;
  typedef typename container::const_pointer const_pointer;
  typedef typename container::const_reference const_reference;
  typedef typename container::value_type value_type;

  block_pool_inserter(const iterator& where) : where_(where) {}

  ~block_pool_inserter() {}

  inline size_t pool_offset() const { return where_.pool_offset(); }

  inline iterator& position() { return where_; }

  inline container& parent() { return where_.parent(); }

  inline const container& parent() const { return where_.parent(); }

  block_pool_inserter& operator=(const_reference value) {
    resize();
    *where_ = value;
    ++where_;
    return *this;
  }

  block_pool_inserter& operator*() { return *this; }

  block_pool_inserter& operator++(int) { return *this; }

  block_pool_inserter& operator++() { return *this; }

  void write(const_pointer b, size_t len) {
    assert(b || !len);

    size_t to_copy = 0;

    while (len) {
      resize();

      to_copy = std::min(where_.parent().block_size() - where_.offset(), len);

      std::memcpy(where_.buffer(), b, to_copy * sizeof(value_type));
      len -= to_copy;
      where_ += to_copy;
      b += to_copy;
    }
  }

  void seek(size_t offset) {
    if (offset >= where_.parent().size()) {
      where_.parent().next_buffer();
    }

    where_.reset(offset);
  }

  void skip(size_t offset) { seek(where_.pool_offset() + offset); }

  // returns offset of the beginning of the allocated slice in the pool
  size_t alloc_slice() {
    const size_t size = LEVELS[0].size;

    alloc_slice(size); // reserve next slice
    const size_t slice_start = where_.pool_offset();
    where_ += size;
    where_[-1] = MAGIC;
    return slice_start;
  }

 private:
  friend class block_pool_sliced_inserter < ContType >;

  inline void resize() {
    if (where_.eof()) {
      where_.parent().next_buffer();
      where_.refresh();
    }
  }

  void alloc_slice(size_t size) {
    auto pool_size = where_.parent().size();
    auto slice_start = where_.pool_offset() + size;
    auto next_block_start = where_.block_offset() + where_.parent().block_size();

    // if need to increase pool size
    if (slice_start >= pool_size) {
      where_.parent().next_buffer();

      if (slice_start == pool_size) {
        where_.refresh();
      } else {
        // do not span slice over 2 blocks, start slice at start of block allocated above
        where_.reset(where_.parent().block_offset(where_.parent().count() - 1));
      }
    } else if (slice_start > next_block_start) {
      // can reuse existing pool but slice is not in the current buffer
      // ensure iterator points to the correct buffer in the pool
      where_.reset(next_block_start);
    }

    // initialize slice
    std::memset(where_.buffer(), 0, sizeof(value_type)*size);
  }

  size_t alloc_slice(iterator& pos) {
    const size_t level = *pos & MAGIC_MASK;
    assert(level < sizeof(LEVELS)/sizeof(LEVELS[0]));
    const size_t next_level = LEVELS[level].next;
    const size_t size = LEVELS[next_level].size;
    const size_t addr_offset = sizeof(uint32_t) - 1;

    alloc_slice(size); // reserve next slice

    /* copy data to new slice */
    std::copy(pos - addr_offset, pos, where_);

    /* write next address at the end of current slice */
    {
      /* write gets non-const reference. need explicit copy here */
      block_pool_inserter it = pos - addr_offset;
      bytes_io< uint32_t>::write(it, uint32_t(where_.pool_offset()));
    }

    pos.reset(where_.pool_offset() + addr_offset);
    where_ += size;
    where_[-1] = static_cast<byte_type>(MAGIC | next_level);

    return size - sizeof(uint32_t);
  }

  iterator where_;
}; // block_pool_inserter 

template< typename ContType >
class block_pool_sliced_inserter : public std::iterator < std::output_iterator_tag, void, void, void, void > {
 public:
  typedef ContType container;
  typedef typename container::iterator iterator;
  typedef typename container::const_pointer const_pointer;
  typedef typename container::const_reference const_reference;
  typedef typename container::value_type value_type;

  block_pool_sliced_inserter(block_pool_inserter< container >* writer, const iterator& where)
    : where_(where), writer_(writer) {
    assert(writer);
  }

  block_pool_sliced_inserter(block_pool_inserter< container >* writer, size_t offset)
    : where_(&writer->parent(), offset), writer_(writer) {
    assert(writer);
  }

  ~block_pool_sliced_inserter() {}

  inline size_t pool_offset() const { return where_.pool_offset(); }

  inline iterator& position() { return where_; }

  inline container& parent() { return where_.parent(); }

  block_pool_sliced_inserter& operator=(const_reference value) {
    if (*where_) {
      writer_->alloc_slice(where_);
    }

    *where_ = value;
    ++where_;
    return *this;
  }

  block_pool_sliced_inserter& operator*() { return *this; }

  block_pool_sliced_inserter& operator++(int) { return *this; }

  block_pool_sliced_inserter& operator++() { return *this; }

  void write(const_pointer b, size_t len) {
    // find end of the slice
    for (; 0 == *where_ && len; --len, ++where_, ++b) {
      *where_ = *b;
    }

    // chunked copy
    while (len) {
      const size_t size = writer_->alloc_slice(where_);
      const size_t to_copy = std::min(size, len);
      std::memcpy(where_.buffer(), b, to_copy * sizeof(typename container::value_type));
      where_ += to_copy;
      len -= to_copy;
      b += to_copy;
    }
  }

 private:
  iterator where_;
  block_pool_inserter< container >* writer_;
}; // block_pool_sliced_inserter 

template< typename T, typename AllocType = std::allocator<T>>
class block_pool {
 public:
  typedef AllocType allocator;
  typedef typename allocator::value_type value_type;
  typedef typename allocator::reference reference;
  typedef typename allocator::const_reference const_reference;
  typedef typename allocator::pointer pointer;
  typedef typename allocator::const_pointer const_pointer;

  typedef block_pool_iterator< block_pool< value_type, allocator > > iterator;
  typedef block_pool_const_iterator< block_pool< value_type, allocator > > const_iterator;

  typedef block_pool_reader< block_pool< value_type, allocator> > reader;
  typedef block_pool_sliced_reader< block_pool< value_type, allocator> > sliced_reader;

  typedef block_pool_inserter< block_pool< value_type, allocator > > inserter;
  typedef block_pool_sliced_inserter< block_pool< value_type, allocator > > sliced_inserter;

  explicit block_pool(size_t block_size, const allocator& alloc = allocator())
    : rep_(rep{ block_size }, alloc) {}

  ~block_pool() {}

  inline void next_buffer() {
    auto& rep = get_rep();
    buffer_ptr ptr(
      get_allocator().allocate(rep.block_len_),
      [this] (T* ptr) { get_allocator().deallocate(ptr, get_rep().block_len_); }
    );

    rep.buffers_.emplace_back(std::move(ptr));
  }

  inline void next_buffer(size_t count) {
    while (count--) {
      next_buffer();
    }
  }

  iterator write(iterator where, value_type b) {
    if (where.eof()) {
      next_buffer();
      where.refresh();
    }

    *where = b;
    return ++where;
  }

  iterator write(iterator where, const_pointer b, size_t len) {
    assert(b);
    auto& rep = get_rep();

    size_t to_copy = 0;
    while (len) {
      if (where.eof()) {
        next_buffer();
        where.refresh();
      }

      to_copy = std::min(rep.block_len_ - where.offset(), len);

      memcpy(where.buffer(), b, to_copy * sizeof(value_type));
      len -= to_copy;
      where += to_copy;
      b += to_copy;
    }

    return where;
  }


  iterator read(iterator where, pointer b) {
    if (where.eof()) {
      return end();
    }

    *b = *where;
    return ++where;
  }

  iterator read(iterator where, pointer b, size_t len) const {
    assert(b);
    auto& rep = get_rep();

    size_t to_copy = 0;

    while (len) {

      if (where.eof()) {
        break;
      }

      to_copy = std::min(rep.block_len_ - where.offset(), len);
      memcpy(b, where.buffer(), to_copy * sizeof(value_type));

      len -= to_copy;
      where += to_copy;
      b += to_copy;
    }

    return where;
  }

  void clear() { get_rep().buffers_.clear(); }

  inline const_reference at(size_t offset) const {
    return const_cast<block_pool*>(this)->at(offset);
  }

  reference at(size_t offset) {
    assert(offset < this->size());
    auto& rep = get_rep();

    const size_t idx = offset / rep.block_len_;
    const size_t pos = offset % rep.block_len_;

    return *(rep.buffers_[idx].get() + pos);
  }

  inline reference operator[](size_t offset) { return at(offset); }

  inline const_reference operator[](size_t offset) const {
    return at(offset);
  }

  inline const_iterator seek(size_t offset) const {
    return const_iterator(const_cast<block_pool*>(this), offset);
  }

  inline const_iterator begin() const {
    return const_iterator(const_cast<block_pool*>(this), 0);
  }

  inline const_iterator end() const {
    return const_iterator(const_cast<block_pool*>(this));
  }

  inline iterator seek(size_t offset) {
    return iterator(this, offset);
  }

  inline iterator begin() { return iterator(this, 0); }

  inline iterator end() { return iterator(this); }

  inline size_t block_size() const { return get_rep().block_len_; }

  inline pointer buffer(size_t i) {
    auto& rep = get_rep();
    assert(i < rep.buffers_.size());
    return rep.buffers_[i];
  }

  inline const_pointer buffer(size_t i) const {
    auto& rep = get_rep();
    assert(i < rep.buffers_.size());
    return rep.buffers_[i];
  }

  inline size_t block_offset(size_t i) const {
    auto& rep = get_rep();
    assert(i < rep.buffers_.size());
    return rep.block_len_ * i;
  }

  inline size_t count() const { return get_rep().buffers_.size(); }

  inline size_t size() const {
    auto& rep = get_rep();
    return rep.block_len_ * rep.buffers_.size();
  }

 private:
  friend iterator;
  friend const_iterator;

  typedef std::shared_ptr<value_type> buffer_ptr;

  // data repository
  struct rep {
    explicit rep(size_t block_len) : block_len_(block_len) {}

    std::vector<buffer_ptr> buffers_;
    size_t block_len_;
  };

  inline const rep& get_rep() const { return rep_.first(); }
  inline rep& get_rep() { return rep_.first(); }
  inline const allocator& get_allocator() const { return rep_.second(); }
  inline allocator& get_allocator() { return rep_.second(); }

  compact_pair<rep, allocator> rep_;
}; // block_pool

NS_END

#endif
