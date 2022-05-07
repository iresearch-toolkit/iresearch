////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#include "proxy_filter.hpp"

#include <bit>

#include "cost.hpp"
#include "score.hpp"
#include "utils/bitset.hpp"

namespace iresearch {

// Bitset expecting doc iterator to be able only to move forward.
// So in case of "seek" to the still unfilled word
// internally it does bunch of "next" calls.
class lazy_filter_bitset : private util::noncopyable {
 public:
  using word_t = size_t;

  explicit lazy_filter_bitset(const sub_reader& segment,
                              const filter::prepared& filter,
                              const Order& order,
                              ExecutionMode mode,
                              const attribute_provider* ctx) noexcept {
    const size_t bits = segment.docs_count() + doc_limits::min();
    real_doc_itr_ = segment.mask(filter.execute(segment, order, mode, ctx));
    words_ = bitset::bits_to_words(bits);
    cost_ = cost::extract(*real_doc_itr_);
    set_ = memory::make_unique<word_t[]>(words_);
    std::memset(set_.get(), 0, sizeof(word_t) * words_);
    real_doc_ = irs::get<document>(*real_doc_itr_);
    begin_ = set_.get();
    end_ = begin_;
  }

  bool get(size_t word_idx, word_t* data) {
    constexpr auto kBits{bits_required<word_t>()};
    assert(set_);
    if (word_idx >= words_) {
      return false;
    }

    word_t* requested = set_.get() + word_idx;
    if (requested >= end_) {
      auto block_limit = ((word_idx + 1) * kBits) - 1;
      while (real_doc_itr_->next()) {
        auto doc_id = real_doc_->value;
        set_bit(set_[doc_id / kBits], doc_id % kBits);
        if (doc_id >= block_limit) {
          break;  // we've filled requested word
        }
      }
      end_ = requested + 1;
    }
    *data = *requested;
    return true;
  }

  cost::cost_t get_cost() const noexcept { return cost_; }

 private:
  std::unique_ptr<word_t[]> set_;
  const word_t* begin_{nullptr};
  const word_t* end_{nullptr};
  doc_iterator::ptr real_doc_itr_;
  const document* real_doc_{nullptr};
  size_t words_{0};
  cost::cost_t cost_;
};

class lazy_filter_bitset_iterator final : public doc_iterator,
                                          private util::noncopyable {
 public:
  explicit lazy_filter_bitset_iterator(lazy_filter_bitset& bitset) noexcept
      : bitset_(bitset), cost_(bitset_.get_cost()) {
    reset();
  }

  bool next() override {
    while (!word_) {
      if (bitset_.get(word_idx_, &word_)) {
        ++word_idx_;  // move only if ok. Or we could be overflowed!
        base_ += bits_required<lazy_filter_bitset::word_t>();
        doc_.value = base_ - 1;
        continue;
      }
      doc_.value = doc_limits::eof();
      word_ = 0;
      return false;
    }
    const doc_id_t delta = doc_id_t(std::countr_zero(word_));
    assert(delta < bits_required<lazy_filter_bitset::word_t>());
    word_ = (word_ >> delta) >> 1;
    doc_.value += 1 + delta;
    return true;
  }

  doc_id_t seek(doc_id_t target) override {
    word_idx_ = target / bits_required<lazy_filter_bitset::word_t>();
    if (bitset_.get(word_idx_, &word_)) {
      const doc_id_t bit_idx =
          target % bits_required<lazy_filter_bitset::word_t>();
      base_ = word_idx_ * bits_required<lazy_filter_bitset::word_t>();
      word_ >>= bit_idx;
      doc_.value = base_ - 1 + bit_idx;
      ++word_idx_;  // mark this word as consumed
      // FIXME consider inlining to speedup
      next();
      return doc_.value;
    } else {
      doc_.value = doc_limits::eof();
      word_ = 0;
      return doc_.value;
    }
  }

  doc_id_t value() const noexcept final { return doc_.value; }

  attribute* get_mutable(type_info::type_id id) noexcept override {
    if (type<document>::id() == id) {
      return &doc_;
    }
    return type<cost>::id() == id ? &cost_ : nullptr;
  }

  void reset() noexcept {
    word_idx_ = 0;
    word_ = 0;
    base_ = doc_limits::invalid() -
            bits_required<lazy_filter_bitset::word_t>();  // before the
                                                          // first word
    doc_.value = doc_limits::invalid();
  }

 private:
  lazy_filter_bitset& bitset_;
  cost cost_;
  document doc_;
  doc_id_t word_idx_{0};
  lazy_filter_bitset::word_t word_{0};
  doc_id_t base_{doc_limits::invalid()};
};

struct proxy_query_cache {
  explicit proxy_query_cache(filter::ptr&& ptr)
      : real_filter_(std::move(ptr)) {}

  absl::flat_hash_map<const sub_reader*, std::unique_ptr<lazy_filter_bitset>>
      readers_;
  filter::prepared::ptr prepared_real_filter_;
  filter::ptr real_filter_;
};

class proxy_query final : public filter::prepared {
 public:
  explicit proxy_query(proxy_filter::cache_ptr cache) : cache_(cache) {
    assert(cache_->prepared_real_filter_);
  }

  doc_iterator::ptr execute(const sub_reader& rdr,
                            const Order& order,
                            ExecutionMode mode,
                            const attribute_provider* ctx) const override {
    // first try to find segment in cache.
    auto& [unused, cached] = *cache_->readers_.emplace(&rdr, nullptr).first;

    if (!cached) {
      cached = std::make_unique<lazy_filter_bitset>(
          rdr, *cache_->prepared_real_filter_, order, mode, ctx);
    }

    assert(cached);
    return memory::make_managed<lazy_filter_bitset_iterator>(*cached);
  }

 private:
  mutable proxy_filter::cache_ptr cache_;
};

DEFINE_FACTORY_DEFAULT(proxy_filter);

proxy_filter::proxy_filter() noexcept : filter(irs::type<proxy_filter>::get()) {}

filter::prepared::ptr proxy_filter::prepare(
    const index_reader& rdr, const Order& ord, boost_t boost,
    const attribute_provider* ctx) const {
  if (!cache_ || !cache_->real_filter_) {
    assert(false);
    return filter::prepared::empty();
  }
  if (!ord.empty()) {
    // Currently we do not support caching scores.
    // Proxy filter should not be used with scorers!
    assert(false);
    return filter::prepared::empty();
  }
  if (!cache_->prepared_real_filter_) {
    cache_->prepared_real_filter_ =
        cache_->real_filter_->prepare(rdr, ord, boost, ctx);
  }
  return memory::make_managed<proxy_query>(cache_);
}

filter& proxy_filter::cache_filter(filter::ptr&& ptr) {
  cache_ = std::make_shared<proxy_query_cache>(std::move(ptr));
  assert(cache_->real_filter_);
  return *cache_->real_filter_;
}

}  // namespace iresearch
