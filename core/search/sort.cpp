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

namespace {

using namespace irs;

template<typename Iterator>
Order Prepare(Iterator begin, Iterator end) {
  Order ord;
  ord.buckets.reserve(std::distance(begin, end));

  size_t stats_align = 0;

  for (size_t score_index = 0; begin != end; ++begin) {
    auto prepared = begin->prepare();

    if (!prepared) {
      // skip empty sorts
      continue;
    }

    // cppcheck-suppress shadowFunction
    const auto stats_size = prepared->stats_size();
    assert(stats_size.second <= alignof(std::max_align_t));
    assert(math::is_power2(stats_size.second)); // math::is_power2(0) returns true

    stats_align = std::max(stats_align, stats_size.second);

    ord.stats_size = memory::align_up(ord.stats_size, stats_size.second);
    ord.features |= prepared->features();

    ord.buckets.emplace_back(
      std::move(prepared),
      score_index,
      ord.stats_size);

    ord.stats_size += memory::align_up(stats_size.first, stats_size.second);
    ++score_index;
  }

  ord.stats_size = memory::align_up(ord.stats_size, stats_align);
  ord.score_size = sizeof(score_t)*ord.buckets.size();

  return ord;
}

void default_score(score_ctx*, score_t*) noexcept { }

}

namespace iresearch {

REGISTER_ATTRIBUTE(filter_boost);

/*static*/ const score_f score_function::kDefaultScoreFunc{&::default_score};

score_function::score_function() noexcept
  : func_{kDefaultScoreFunc} {
}

score_function::score_function(score_function&& rhs) noexcept
  : ctx_(std::move(rhs.ctx_)),
    func_(rhs.func_) {
  rhs.func_ = kDefaultScoreFunc;
}

score_function& score_function::operator=(score_function&& rhs) noexcept {
  if (this != &rhs) {
    ctx_ = std::move(rhs.ctx_);
    func_ = rhs.func_;
    rhs.func_ = kDefaultScoreFunc;
  }
  return *this;
}

sort::sort(const type_info& type) noexcept
  : type_(type.id()) {
}

Order Order::Prepare(std::span<const sort::ptr> order) {
  using PtrIterator = ptr_iterator<decltype(order)::iterator>;

  return ::Prepare(PtrIterator{std::begin(order)}, PtrIterator{std::end(order)});
}

Order Order::Prepare(std::span<const sort*> order) {
  using PtrIterator = ptr_iterator<decltype(order)::iterator>;

  return ::Prepare(PtrIterator{std::begin(order)}, PtrIterator{std::end(order)});
}

const Order Order::kUnordered;


std::vector<Scorer> PrepareScorers(const Order& order,
                                    const sub_reader& segment,
                                    const term_reader& field,
                                    const byte_type* stats_buf,
                                    score_t* score_buf,
                                    const attribute_provider& doc,
                                    boost_t boost) {
  std::vector<Scorer> scorers;
  scorers.reserve(order.buckets.size());

  for (auto& entry : order.buckets) {
    assert(stats_buf);
    assert(entry.bucket); // ensured by Order
    const auto& bucket = *entry.bucket;

    auto scorer = bucket.prepare_scorer(
      segment, field,
      stats_buf + entry.stats_offset,
      score_buf,
      doc, boost);

    if (scorer) {
      // skip empty scorers
      scorers.emplace_back(std::move(scorer), &entry);
    }

    if (score_buf) {
      ++score_buf;
    }
  }

  return scorers;
}

void PrepareCollectors(
    std::span<const OrderBucket> order,
    byte_type* stats_buf,
    const index_reader& index) {
  for (auto& entry: order) {
    assert(entry.bucket); // ensured by Order
    entry.bucket->collect(stats_buf + entry.stats_offset, index, nullptr, nullptr);
  }
}

}
