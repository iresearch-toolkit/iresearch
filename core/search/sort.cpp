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

#include "analysis/token_attributes.hpp"
#include "index/index_reader.hpp"
#include "shared.hpp"
#include "utils/memory_pool.hpp"

namespace {

using namespace irs;

template<typename Iterator>
std::tuple<std::vector<OrderBucket>, size_t, IndexFeatures> Prepare(
    Iterator begin, Iterator end) {
  std::vector<OrderBucket> buckets;
  buckets.reserve(std::distance(begin, end));

  IndexFeatures features{};
  size_t stats_size{};
  size_t stats_align{};

  for (; begin != end; ++begin) {
    auto prepared = begin->prepare();

    if (!prepared) {
      // skip empty sorts
      continue;
    }

    // cppcheck-suppress shadowFunction
    const auto [bucket_stats_size, bucket_stats_align] = prepared->stats_size();
    assert(bucket_stats_align <= alignof(std::max_align_t));
    assert(math::is_power2(
        bucket_stats_align));  // math::is_power2(0) returns true

    stats_align = std::max(stats_align, bucket_stats_align);

    stats_size = memory::align_up(stats_size, bucket_stats_align);
    features |= prepared->features();

    buckets.emplace_back(std::move(prepared), stats_size);

    stats_size += memory::align_up(bucket_stats_size, bucket_stats_align);
  }

  stats_size = memory::align_up(stats_size, stats_align);

  return {std::move(buckets), stats_size, features};
}

void DefaultScore(score_ctx* ctx, score_t* res) noexcept {
  assert(res);
  // FIXME(gnusi): use std::bit_cast when available
  std::memset(res, 0, reinterpret_cast<size_t>(ctx));
}

}  // namespace

namespace iresearch {

REGISTER_ATTRIBUTE(filter_boost);

/*static*/ const score_f ScoreFunction::kDefault{&::DefaultScore};

ScoreFunction::ScoreFunction() noexcept : func_{kDefault} {}

ScoreFunction::ScoreFunction(ScoreFunction&& rhs) noexcept
    : ctx_(std::move(rhs.ctx_)), func_(rhs.func_) {
  rhs.func_ = kDefault;
}

ScoreFunction& ScoreFunction::operator=(ScoreFunction&& rhs) noexcept {
  if (this != &rhs) {
    ctx_ = std::move(rhs.ctx_);
    func_ = rhs.func_;
    rhs.func_ = kDefault;
  }
  return *this;
}

sort::sort(const type_info& type) noexcept : type_(type.id()) {}

Order Order::Prepare(std::span<const sort::ptr> order) {
  return std::apply(
      [](auto&&... args) {
        return Order{std::forward<decltype(args)>(args)...};
      },
      ::Prepare(ptr_iterator{std::begin(order)},
                ptr_iterator{std::end(order)}));
}

Order Order::Prepare(std::span<const sort*> order) {
  return std::apply(
      [](auto&&... args) {
        return Order{std::forward<decltype(args)>(args)...};
      },
      ::Prepare(ptr_iterator{std::begin(order)},
                ptr_iterator{std::end(order)}));
}

const Order Order::kUnordered;

std::vector<ScoreFunction> PrepareScorers(std::span<const OrderBucket> buckets,
                                          const sub_reader& segment,
                                          const term_reader& field,
                                          const byte_type* stats_buf,
                                          const attribute_provider& doc,
                                          boost_t boost) {
  std::vector<ScoreFunction> scorers;
  scorers.reserve(buckets.size());

  for (auto& entry : buckets) {
    const auto& bucket = *entry.bucket;

    if (IRS_UNLIKELY(!entry.bucket)) {
      continue;
    }

    auto scorer = bucket.prepare_scorer(
        segment, field, stats_buf + entry.stats_offset, doc, boost);

    if (IRS_LIKELY(scorer)) {
      scorers.emplace_back(std::move(scorer));
    } else {
      scorers.emplace_back(ScoreFunction::Default(sizeof(score_t)));
    }
  }

  return scorers;
}

void PrepareCollectors(std::span<const OrderBucket> order, byte_type* stats_buf,
                       const index_reader& index) {
  for (auto& entry : order) {
    if (IRS_LIKELY(entry.bucket)) {
      entry.bucket->collect(stats_buf + entry.stats_offset, index, nullptr,
                            nullptr);
    }
  }
}

}  // namespace iresearch
