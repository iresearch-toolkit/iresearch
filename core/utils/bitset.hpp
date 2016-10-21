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

#ifndef IRESEARCH_BITSET_H
#define IRESEARCH_BITSET_H

#include "shared.hpp"
#include "bit_utils.hpp"
#include "math_utils.hpp"

NS_ROOT

class bitset {
 public:
  typedef size_t word_t;
  typedef size_t index_t;

  explicit bitset(size_t bits = 0)
    : bits_(bits),
    words_(bit_to_words(bits_)) {
    if (words_) {
      data_ = new word_t[words_];
      clear();
    }
  }

  bitset(bitset&& rhs) NOEXCEPT
    : bits_(rhs.bits_),
    words_(rhs.words_),
    data_(rhs.data_) {
    rhs.bits_ = 0;
    rhs.words_ = 0;
    rhs.data_ = nullptr;
  }

  bitset& operator=(bitset&& rhs) NOEXCEPT {
    if (this != &rhs) {
      bits_ = rhs.bits_;
      rhs.bits_ = 0;
      words_ = rhs.words_;
      rhs.words_ = 0;
      data_ = rhs.data_;
      rhs.data_ = nullptr;
    }

    return *this;
  }

  bitset(const bitset&) = delete;
  bitset& operator=(const bitset&) = delete;

  // returns number of bits in bitset
  const size_t size() const { return bits_; }

  const size_t words() const { return words_; }
  const word_t* data() const { return data_; }
  word_t* data() { return data_; }

  void set(size_t i) NOEXCEPT {
    assert(i < bits_);
    const auto wi = word(i);
    set_bit(data_[wi], bit(i, wi));
  }

  void unset(size_t i) NOEXCEPT {
    assert(i < bits_);
    const auto wi = word(i);
    unset_bit(data_[wi], bit(i, wi));
  }

  void reset(size_t i, bool set) NOEXCEPT {
    assert(i < bits_);
    const auto wi = word(i);
    set_bit(data_[wi], bit(i, wi), set);
  }

  bool test(size_t i) const NOEXCEPT {
    assert(i < bits_);
    const auto wi = word(i);
    return check_bit(data_[wi], bit(i, wi));
  }

  bool any() const {
    return std::any_of(
      data_, data_ + words_,
      [] (word_t w) { return w != 0; }
    );
  }

  bool none() const {
    return !any();
  }

  bool all() const {
    return (count() == size());
  }

  void clear() {
    std::memset(data_, 0, sizeof(word_t)*words_);
  }

  // counts bits set
  word_t count() const NOEXCEPT {
    return std::accumulate(
      data_, data_ + words_, word_t(0),
      [] (word_t v, word_t w) {
        return v + math::math_traits<word_t>::pop(w);
    });
  }

  ~bitset() {
    delete[] data_;
  }

  private:
  FORCE_INLINE static size_t word(size_t i) NOEXCEPT {
    return i / bits_required<word_t>();
  }

  FORCE_INLINE static size_t bit(size_t i, size_t wi) NOEXCEPT {
    return i - bits_required<word_t>()*wi;
  }

  FORCE_INLINE static size_t bit_to_words(size_t bits) NOEXCEPT {
    return bits ? ((bits - 1) / bits_required<word_t>()) + 1 : 0;
  }

  size_t bits_;    // number of bits in a bitset
  size_t words_;   // number of words used for storing data
  word_t* data_{}; // words array
}; // bitset

NS_END

#endif