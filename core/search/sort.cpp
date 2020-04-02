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

#include "sort.hpp"

#include "shared.hpp"
#include "analysis/token_attributes.hpp"
#include "index/index_reader.hpp"
#include "utils/memory_pool.hpp"

NS_ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                                      filter_boost
// -----------------------------------------------------------------------------

filter_boost::filter_boost() noexcept
  : basic_attribute<boost_t>(1.f) {
}

REGISTER_ATTRIBUTE(filter_boost);
DEFINE_ATTRIBUTE_TYPE(filter_boost);

// ----------------------------------------------------------------------------
// --SECTION--                                                             sort
// ----------------------------------------------------------------------------

sort::sort(const type_id& type) noexcept
  : type_(&type) {
}

// ----------------------------------------------------------------------------
// --SECTION--                                                            order 
// ----------------------------------------------------------------------------

const order& order::unordered() {
  static order ord;
  return ord;
}

void order::remove(const type_id& type) {
  order_.erase(
    std::remove_if(
      order_.begin(), order_.end(),
      [type] (const entry& e) { return type == e.sort().type(); }
  ));
}

const order::prepared& order::prepared::unordered() {
  static order::prepared ord;
  return ord;
}

order::prepared::prepared(order::prepared&& rhs) noexcept
  : order_(std::move(rhs.order_)),
    features_(std::move(rhs.features_)),
    score_size_(rhs.score_size_),
    stats_size_(rhs.stats_size_) {
  rhs.score_size_ = 0;
  rhs.stats_size_ = 0;
}

order::prepared& order::prepared::operator=(order::prepared&& rhs) noexcept {
  if (this != &rhs) {
    order_ = std::move(rhs.order_);
    features_ = std::move(rhs.features_);
    score_size_ = rhs.score_size_;
    rhs.score_size_ = 0;
    stats_size_ = rhs.stats_size_;
    rhs.stats_size_ = 0;
  }

  return *this;
}

bool order::operator==(const order& other) const {
  if (order_.size() != other.order_.size()) {
    return false;
  }

  for (size_t i = 0, count = order_.size(); i < count; ++i) {
    auto& entry = order_[i];
    auto& other_entry = other.order_[i];

    auto& sort = entry.sort_;
    auto& other_sort = other_entry.sort_;

    // FIXME TODO operator==(...) should be specialized for every sort child class based on init config
    if (!sort != !other_sort
        || (sort
            && (sort->type() != other_sort->type()
                || entry.reverse_ != other_entry.reverse_))) {
      return false;
    }
  }

  return true;
}

order& order::add(bool reverse, const sort::ptr& sort) {
  assert(sort);
  order_.emplace_back(sort, reverse);

  return *this;
}

order::prepared order::prepare() const {
  order::prepared pord;
  pord.order_.reserve(order_.size());

  size_t stats_align = 0;
  size_t score_align = 0;

  for (auto& entry : order_) {
    auto prepared = entry.sort().prepare();

    if (!prepared) {
      // skip empty sorts
      continue;
    }

    const auto score_size = prepared->score_size();
    assert(score_size.second <= alignof(MAX_ALIGN_T));
    assert(math::is_power2(score_size.second)); // math::is_power2(0) returns true

    const auto stats_size = prepared->stats_size();
    assert(stats_size.second <= alignof(MAX_ALIGN_T));
    assert(math::is_power2(stats_size.second)); // math::is_power2(0) returns true

    stats_align = std::max(stats_align, stats_size.second);
    score_align = std::max(score_align, score_size.second);

    pord.score_size_ = memory::align_up(pord.score_size_, score_size.second);
    pord.stats_size_ = memory::align_up(pord.stats_size_, stats_size.second);
    pord.features_ |= prepared->features();

    pord.order_.emplace_back(
      std::move(prepared),
      pord.score_size_,
      pord.stats_size_,
      entry.reverse()
    );

    pord.score_size_ += memory::align_up(score_size.first, score_size.second);
    pord.stats_size_ += memory::align_up(stats_size.first, stats_size.second);
  }

  pord.stats_size_ = memory::align_up(pord.stats_size_, stats_align);
  pord.score_size_ = memory::align_up(pord.score_size_, score_align);

  return pord;
}


// ----------------------------------------------------------------------------
// --SECTION--                                                          scorers
// ----------------------------------------------------------------------------

order::prepared::scorers::scorers(
    const prepared_order_t& buckets,
    const sub_reader& segment,
    const term_reader& field,
    const byte_type* stats_buf,
    const attribute_view& doc,
    boost_t boost
) {
  scorers_.reserve(buckets.size());

  for (auto& entry: buckets) {
    assert(stats_buf);
    assert(entry.bucket); // ensured by order::prepared
    const auto& bucket = *entry.bucket;

    auto scorer = bucket.prepare_scorer(
      segment, field, stats_buf + entry.stats_offset, doc, boost
    );

    if (scorer.second) {
      // skip empty scorers
      scorers_.emplace_back(std::move(scorer.first), scorer.second, entry.score_offset);
    }
  }
}

order::prepared::scorers::scorers(order::prepared::scorers&& other) noexcept
  : scorers_(std::move(other.scorers_)) {
}

order::prepared::scorers& order::prepared::scorers::operator=(
    order::prepared::scorers&& other
) noexcept {
  if (this != &other) {
    scorers_ = std::move(other.scorers_);
  }

  return *this;
}

void order::prepared::scorers::score(byte_type* scr) const {
  for (auto& scorer : scorers_) {
    assert(scorer.func);
    (*scorer.func)(scorer.ctx.get(), scr + scorer.offset);
  }
}

void order::prepared::prepare_collectors(
    byte_type* stats_buf,
    const index_reader& index
) const {
  for (auto& entry: order_) {
    assert(entry.bucket); // ensured by order::prepared
    entry.bucket->collect(stats_buf + entry.stats_offset, index, nullptr, nullptr);
  }
}

void order::prepared::prepare_score(byte_type* score) const {
  for (auto& sort : order_) {
    assert(sort.bucket);
    sort.bucket->prepare_score(score + sort.score_offset);
  }
}

void order::prepared::prepare_stats(byte_type* stats) const {
  for (auto& sort : order_) {
    assert(sort.bucket);
    sort.bucket->prepare_stats(stats + sort.stats_offset);
  }
}

bool order::prepared::less(const byte_type* lhs, const byte_type* rhs) const {
  if (!lhs) {
    return rhs != nullptr; // lhs(nullptr) == rhs(nullptr)
  }

  if (!rhs) {
    return true; // nullptr last
  }

  for (const auto& sort: order_) {
    assert(sort.bucket); // ensured by order::prepared
    const auto& bucket = *sort.bucket;
    const auto* lhs_begin = lhs + sort.score_offset;
    const auto* rhs_begin = rhs + sort.score_offset;

    if (bucket.less(lhs_begin, rhs_begin)) {
      return !sort.reverse;
    }

    if (bucket.less(rhs_begin, lhs_begin)) {
      return sort.reverse;
    }
  }

  return false;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                        collectors
// -----------------------------------------------------------------------------

field_collectors::field_collectors(const order::prepared& buckets)
  : collectors_base<sort::field_collector::ptr>(buckets.size(), buckets) {
  auto begin = collectors_.begin();
  for (auto& bucket : buckets) {
    // FIXME
    // in case of entry.bucket->prepare_field_collector() returned nullptr,
    // put dummy obj to avoid null checks in collect

    *begin = bucket.bucket->prepare_field_collector();
    ++begin;
  }
  assert(begin == collectors_.end());
}


void field_collectors::collect(const sub_reader& segment,
                               const term_reader& field) const {
  switch (collectors_.size()) {
    case 0:
      return;
    case 1:
      collectors_.front()->collect(segment, field);
      return;
    case 2:
      collectors_.front()->collect(segment, field);
      collectors_.back()->collect(segment, field);
      return;
    default:
      for (auto& collector : collectors_) {
        collector->collect(segment, field);
      }
  }
}

void field_collectors::finish(byte_type* stats_buf, const index_reader& index) const {
  // special case where term statistics collection is not applicable
  // e.g. by_column_existence filter
  assert(buckets_->size() == collectors_.size());

  for (size_t i = 0, count = collectors_.size(); i < count; ++i) {
    auto& sort = (*buckets_)[i];
    assert(sort.bucket); // ensured by order::prepare

    sort.bucket->collect(
      stats_buf + sort.stats_offset, // where stats for bucket start
      index,
      collectors_[i].get(),
      nullptr
    );
  }
}

term_collectors::term_collectors(const order::prepared& buckets, size_t size)
  : collectors_base<sort::term_collector::ptr>(buckets.size()*size, buckets) {
  // add term collectors from each bucket
  // layout order [t0.b0, t0.b1, ... t0.bN, t1.b0, t1.b1 ... tM.BN]
  auto begin = collectors_.begin();
  for (size_t i = 0; i < size; ++i) {
    for (auto& entry: buckets) {
      assert(entry.bucket); // ensured by order::prepare

      // FIXME
      // in case of entry.bucket->prepare_term_collector() returned nullptr,
      // put dummy obj to avoid null checks in collect

      *begin = entry.bucket->prepare_term_collector();
      ++begin;
    }
  }
  assert(begin == collectors_.end());
}


void term_collectors::collect(
    const sub_reader& segment, const term_reader& field,
    size_t term_idx, const attribute_view& attrs) const {
  // collector may be null if prepare_term_collector() returned nullptr
  const size_t count = buckets_->size();

  switch (count) {
    case 0:
      return;
    case 1: {
      assert(term_idx < collectors_.size());
      auto* collector = collectors_[term_idx].get();
      if (collector) collector->collect(segment, field, attrs);
      return;
    }
    case 2: {
      assert(term_idx + 1 < collectors_.size());
      auto* collector0 = collectors_[term_idx].get();
      if (collector0) collector0->collect(segment, field, attrs);
      auto* collector1 = collectors_[term_idx + 1].get();
      if (collector1) collector1->collect(segment, field, attrs);
      return;
    }
    default: {
      const size_t term_offset_count = term_idx * count;
      for (size_t i = 0; i < count; ++i) {
        const auto idx = term_offset_count + i;
        assert(idx < collectors_.size()); // enforced by allocation in the constructor
        auto* collector = collectors_[idx].get();

        if (collector) { // may be null if prepare_term_collector() returned nullptr
          collector->collect(segment, field, attrs);
        }
      }
      return;
    }
  }
}


size_t term_collectors::push_back() {
  const size_t size = buckets_->size();

  switch (size) {
    case 0:
      return 0;
    case 1: {
      const auto term_offset = collectors_.size();
      assert((*buckets_)[0].bucket); // ensured by order::prepare
      collectors_.emplace_back((*buckets_)[0].bucket->prepare_term_collector());
      return term_offset;
    }
    case 2: {
      const auto term_offset = collectors_.size() / 2;
      assert((*buckets_)[0].bucket); // ensured by order::prepare
      collectors_.emplace_back((*buckets_)[0].bucket->prepare_term_collector());
      assert((*buckets_)[1].bucket); // ensured by order::prepare
      collectors_.emplace_back((*buckets_)[1].bucket->prepare_term_collector());
      return term_offset;
    }
    default: {
      const auto term_offset = collectors_.size() / size;
      collectors_.reserve(collectors_.size() + size);
      for (auto& entry: (*buckets_)) {
        assert(entry.bucket); // ensured by order::prepare
        collectors_.emplace_back(entry.bucket->prepare_term_collector());
      }
      return term_offset;
    }
  }
}

void term_collectors::finish(byte_type* stats_buf,
                             const field_collectors& field_collectors,
                             const index_reader& index) const {
  auto bucket_count = buckets_->size();
  assert(collectors_.size() % bucket_count == 0); // enforced by allocation in the constructor

  for (size_t i = 0, count = collectors_.size(); i < count; ++i) {
    auto bucket_offset = i % bucket_count;
    auto& sort = (*buckets_)[bucket_offset];
    assert(sort.bucket); // ensured by order::prepare

    assert(i % bucket_count < field_collectors.size());
    sort.bucket->collect(
      stats_buf + sort.stats_offset, // where stats for bucket start
      index,
      field_collectors[bucket_offset],
      collectors_[i].get()
    );
  }
}

NS_END
