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

#ifndef IRESEARCH_SORT_H
#define IRESEARCH_SORT_H

#include <vector>

#include "index/index_features.hpp"
#include "utils/attributes.hpp"
#include "utils/attribute_provider.hpp"
#include "utils/math_utils.hpp"
#include "utils/iterator.hpp"

namespace iresearch {

struct collector;
struct data_output;
struct OrderBucket;
struct index_reader;
struct sub_reader;
struct term_reader;

// Represents no boost value.
constexpr boost_t kNoBoost{1.f};

// Represents an addition to score from filter specific to a particular
// document. May vary from document to document.
struct filter_boost final : attribute {
  static constexpr string_ref type_name() noexcept {
    return "iresearch::filter_boost";
  }

  boost_t value{kNoBoost};
};

// Stateful object used for computing the document score based on the
// stored state.
struct score_ctx {
  score_ctx() = default;
  score_ctx(score_ctx&&) = default;
  score_ctx& operator=(score_ctx&&) = default;
  virtual ~score_ctx() = default;
};

using score_f = void(*)(score_ctx* ctx, score_t* res) noexcept;

// Convenient wrapper around score_f and score_ctx.
class score_function : util::noncopyable {
 public:
  static const score_f kDefaultScoreFunc;

  score_function() noexcept;
  score_function(memory::managed_ptr<score_ctx>&& ctx, const score_f func) noexcept
    : ctx_(std::move(ctx)), func_(func) {
  }
  score_function(std::unique_ptr<score_ctx>&& ctx, const score_f func) noexcept
    : score_function(memory::to_managed<score_ctx>(std::move(ctx)), func) {
  }
  score_function(score_ctx* ctx, const score_f func) noexcept
    : score_function(memory::to_managed<score_ctx, false>(std::move(ctx)), func) {
  }
  score_function(score_function&& rhs) noexcept;
  score_function& operator=(score_function&& rhs) noexcept;

  FORCE_INLINE void operator()(score_t* res) const noexcept {
    assert(func_);
    return func_(ctx_.get(), res);
  }

  bool operator==(std::nullptr_t) const noexcept {
    return static_cast<bool>(*this);
  }

  bool operator!=(std::nullptr_t) const noexcept {
    return !(*this == nullptr);
  }

  bool operator==(const score_function& rhs) const noexcept {
    return ctx_ == rhs.ctx_ && func_ == rhs.func_;
  }

  bool operator!=(const score_function& rhs) const noexcept {
    return !(*this == rhs);
  }

  const score_ctx* ctx() const noexcept { return ctx_.get(); }
  score_f func() const noexcept { return func_; }

  void reset(memory::managed_ptr<score_ctx>&& ctx, const score_f func) noexcept {
    ctx_ = std::move(ctx);
    func_ = func;
  }

  void reset(std::unique_ptr<score_ctx>&& ctx, const score_f func) noexcept {
    ctx_ = memory::to_managed<score_ctx>(std::move(ctx));
    func_ = func;
  }

  void reset(score_ctx* ctx, const score_f func) noexcept {
    ctx_ = memory::to_managed<score_ctx, false>(ctx);
    func_ = func;
  }

  explicit operator bool() const noexcept {
    return nullptr != func_;
  }

 private:
  memory::managed_ptr<score_ctx> ctx_;
  score_f func_;
};

// Base class for all user-side sort entries.
// Stats are meant to be trivially constructible and will be
// zero initialized before usage.
class sort {
 public:
  using ptr = std::unique_ptr<sort>;

  // Object used for collecting index statistics, for a specific matched
  // field, that are required by the scorer for scoring individual
  // documents.
  class field_collector {
    public:
     using ptr = std::unique_ptr<field_collector>;

     virtual ~field_collector() = default;

     // Collect field related statistics, i.e. field used in the filter
     // `segment` is the segment being processed (e.g. for columnstore).
     // `field` is  the field matched by the filter in the 'segment'.
     // Called once for every field matched by a filter per each segment.
     // Always called on each matched 'field' irrespective of if it
     // contains a matching 'term'.
     virtual void collect(const sub_reader& segment,
                          const term_reader& field) = 0;

     // Clear collected stats
     virtual void reset() = 0;

     // Collect field related statistics from a serialized
     // representation as produced by write(...) below.
     virtual void collect(bytes_ref in) = 0;

     // Serialize the internal data representation into 'out'.
     virtual void write(data_output& out) const = 0;
  };

  // Object used for collecting index statistics, for a specific matched
  // term of a field, that are required by the scorer for scoring
  // individual documents.
  class term_collector {
   public:
    using ptr = std::unique_ptr<term_collector>;

    virtual ~term_collector() = default;

    // Collect term related statistics, i.e. term used in the filter.
    // `segment` is the segment being processed (e.g. for columnstore)
    // `field` is the the field matched by the filter in the 'segment'.
    // `term_attrs` is the attributes of the matched term in the field.
    // Called once for every term matched by a filter in the 'field'
    // per each segment.
    // Only called on a matched 'term' in the 'field' in the 'segment'.
    virtual void collect(
      const sub_reader& segment,
      const term_reader& field,
      const attribute_provider& term_attrs) = 0;

    // Clear collected stats
    virtual void reset() = 0;

    // Collect term related statistics from a serialized
    // representation as produced by write(...) below.
    virtual void collect(bytes_ref in) = 0;

    // Serialize the internal data representation into 'out'.
    virtual void write(data_output& out) const = 0;
  };

  // Possible variants of merging multiple scores
  enum class MergeType {
    // Aggregate multiple scores
    AGGREGATE = 0,

    // Find max among multiple scores
    MAX
  };

  // Base class for all prepared(compiled) sort entries.
  class prepared {
   public:
    using ptr = std::unique_ptr<prepared>;

    virtual ~prepared() = default;

    // Store collected index statistics into 'stats' of the
    // current 'filter'
    // `filter_attrs` is out-parameter to store statistics for later use in
    // calls to score(...).
    // `index` is the full index to collect statistics on
    // `field` is the field level statistics collector as returned from
    //  prepare_field_collector()
    // `term` is the term level statistics collector as returned from
    //  prepare_term_collector()
    // Called once on the 'index' for every field+term matched by the
    // filter.
    // Called once on the 'index' for every field with null term stats
    // if term is not applicable, e.g. by_column_existence filter.
    // Called exactly once if field/term collection is not applicable,
    // e.g. collecting statistics over the columnstore.
    // Called after all calls to collector::collect(...) on each segment.
    virtual void collect(
      byte_type* stats,
      const index_reader& index,
      const field_collector* field,
      const term_collector* term) const = 0;

    // The index features required for proper operation of this sort::prepared
    virtual IndexFeatures features() const = 0;

    // Create an object to be used for collecting index statistics, one
    // instance per matched field.
    // Returns nullptr == no statistics collection required
    virtual field_collector::ptr prepare_field_collector() const = 0;

    // Create a stateful scorer used for computation of document scores
    virtual score_function prepare_scorer(
      const sub_reader& segment,
      const term_reader& field,
      const byte_type* stats,
      score_t* score,
      const attribute_provider& doc_attrs,
      boost_t boost) const = 0;

    // Create an object to be used for collecting index statistics, one
    // instance per matched term.
    // Returns nullptr == no statistics collection required.
    virtual term_collector::ptr prepare_term_collector() const = 0;

    // Number of bytes (first) and alignment (first) required to store stats
    // Alignment must satisfy the following requirements:
    //   - be a power of 2
    //   - be less or equal than alignof(MAX_ALIGN_T))
    virtual std::pair<size_t, size_t> stats_size() const = 0;
  };

  explicit sort(const type_info& type) noexcept;
  virtual ~sort() = default;

  constexpr type_info::type_id type() const noexcept { return type_; }

  virtual prepared::ptr prepare() const = 0;

 private:
  type_info::type_id type_;
};

struct OrderBucket : util::noncopyable {
  OrderBucket(sort::prepared::ptr&& bucket,
              size_t score_index,
              size_t stats_offset) noexcept
    : bucket(std::move(bucket)),
      score_index{score_index},
      stats_offset{stats_offset} {
    assert(this->bucket);
  }

  OrderBucket(OrderBucket&&) = default;
  OrderBucket& operator=(OrderBucket&&) = default;

  sort::prepared::ptr bucket; // prepared score
  size_t score_index; // scorer index
  size_t stats_offset; // offset in stats buffer
};

// Set of compiled sort entries
struct Order final : private util::noncopyable {
  static const Order kUnordered;

  static Order Prepare(std::span<const sort::ptr> order);
  static Order Prepare(std::span<const sort*> order);
  static Order Prepare(const sort& order) {
    const auto* p = &order;
    return Prepare(std::span{&p, 1});
  }

  Order() = default;
  Order(Order&&) = default;
  Order& operator=(Order&&) = default;

  std::vector<OrderBucket> buckets;
  size_t score_size{ };
  size_t stats_size{ };
  IndexFeatures features{ IndexFeatures::NONE };
};

static_assert(std::is_nothrow_move_constructible_v<OrderBucket>);
static_assert(std::is_nothrow_move_assignable_v<OrderBucket>);

struct NoopAggregator{
  constexpr size_t size() const noexcept { return 0; }
  void operator()(score_t*, const score_t*) const noexcept { }
  void operator()(score_t*, const score_t**, size_t) noexcept { }
};

template<typename Merger, size_t Size>
struct Aggregator {
  constexpr size_t size() const noexcept {
    return Size;
  }

  void operator()(score_t* dst, const score_t* src) const noexcept {
    for (size_t i = 0; i < Size; ++i) {
      Merger::Merge(i, dst, src);
    }
  }

  void operator()(score_t* dst, const score_t** src, size_t size) const noexcept {
    for (size_t i = 0; i < Size; ++i) {
      Merger::Merge(i, dst, src, size);
    }
  }
};

template<typename Merger>
struct Aggregator<Merger, std::numeric_limits<size_t>::max()> {
  explicit Aggregator(size_t size) noexcept
    : count{size} {}

  size_t size() const noexcept {
    return count;
  }

  void operator()(score_t* dst, const score_t* src) const noexcept {
    for (size_t i = 0; i < count; ++i) {
      Merger::Merge(i, dst, src);
    }
  }

  void operator()(score_t* dst, const score_t** src, size_t size) const noexcept {
    for (size_t i = 0; i < size; ++i) {
      Merger::Merge(i, dst, src, size);
    }
  }

  size_t count;
};

struct SumMerger {
  static void Merge(size_t idx,
                    score_t* RESTRICT dst,
                    const score_t* RESTRICT src) noexcept {
    dst[idx] += src[idx];
  }

  static void Merge(size_t idx, score_t* dst,
                    const score_t** src_begin, size_t size) noexcept {
    auto& casted_dst = dst[idx];
    casted_dst = {};

    const auto** src_end = src_begin + size;
    const auto** src_next = src_begin + 4;
    for (; src_next <= src_end; src_begin = src_next, src_next += 4) {
      casted_dst += src_begin[0][idx]
                 +  src_begin[1][idx]
                 +  src_begin[2][idx]
                 +  src_begin[3][idx];
    }

    switch (std::distance(src_end, src_next)) {
      case 0:
        break;
      case 1:
        casted_dst += src_begin[0][idx]
                   +  src_begin[1][idx]
                   +  src_begin[2][idx];
        break;
      case 2:
        casted_dst += src_begin[0][idx]
                   +  src_begin[1][idx];
        break;
      case 3:
        casted_dst += src_begin[0][idx];
        break;
    }
  }
};

struct MaxMerger {
  static void Merge(size_t idx, score_t* dst,
                    const score_t** src_begin, size_t size) noexcept {
    auto& casted_dst = dst[idx];

    switch (size) {
      case 0:
        casted_dst = {};
        break;
      case 1:
        casted_dst = src_begin[0][idx];
        break;
      case 2:
        casted_dst = std::max(src_begin[0][idx],
                              src_begin[1][idx]);
        break;
      default:
        casted_dst = (*src_begin)[idx];
        const auto* src_end = src_begin + size;
        for (++src_begin; src_begin != src_end; ) {
          casted_dst = std::max(*src_begin++[idx], casted_dst);
        }
        break;
    }
  }

  static void Merge(size_t idx,
                    score_t* RESTRICT dst,
                    const score_t* RESTRICT src) noexcept {
    auto& casted_dst = dst[idx];
    auto& casted_src = src[idx];

    if (casted_dst < casted_src) {
      casted_dst = casted_src;
    }
  }
};

template<typename Visitor>
auto ResoveMergeType(sort::MergeType type,
                     size_t num_buckets,
                     Visitor&& visitor) {
  constexpr size_t kRuntimeSize = std::numeric_limits<size_t>::max();

  auto impl = [&]<typename Merger>(){
    switch (num_buckets) {
      case 0:
        return visitor(NoopAggregator{});
      case 1:
        return visitor(Aggregator<Merger, 1>{});
      case 2:
        return visitor(Aggregator<Merger, 2>{});
      case 3:
        return visitor(Aggregator<Merger, 3>{});
      case 4:
        return visitor(Aggregator<Merger, 4>{});
      default:
        return visitor(Aggregator<Merger, kRuntimeSize>{num_buckets});
    }
  };

  switch (type) {
    case sort::MergeType::AGGREGATE:
      return impl.template operator()<SumMerger>();
    case sort::MergeType::MAX:
      return impl.template operator()<MaxMerger>();
    default:
      assert(false);
      return visitor(NoopAggregator{});
  }
}

// Template score for base class for all prepared(compiled) sort entries
template<typename StatsType>
class PreparedSortBase : public sort::prepared {
 public:
  static_assert(std::is_trivially_constructible_v<StatsType>,
                "StatsTypemust be trivially constructible");

  using stats_t = StatsType;

  FORCE_INLINE static const stats_t& stats_cast(const byte_type* buf) noexcept {
    assert(buf);
    return *reinterpret_cast<const stats_t*>(buf);
  }

  FORCE_INLINE static stats_t& stats_cast(byte_type* buf) noexcept {
    return const_cast<stats_t&>(stats_cast(const_cast<const byte_type*>(buf)));
  }

  // Returns number of bytes and alignment required to store stats
  inline std::pair<size_t, size_t> stats_size() const noexcept final {
    static_assert(alignof(stats_t) <= alignof(std::max_align_t),
                  "alignof(stats_t) must be <= alignof(std::max_align_t)");

    static_assert(math::is_power2(alignof(stats_t)),
                  "alignof(stats_t) must be a power of 2");

    return std::make_pair(sizeof(stats_t), alignof(stats_t));
  }
};

template<>
class PreparedSortBase<void> : public sort::prepared {
 public:
  virtual sort::field_collector::ptr prepare_field_collector() const override {
    return nullptr;
  }

  virtual sort::term_collector::ptr prepare_term_collector() const override {
    return nullptr;
  }

  virtual void collect(
      byte_type*,
      const index_reader&,
      const sort::field_collector*,
      const sort::term_collector*) const override {
    // NOOP
  }

  virtual inline std::pair<size_t, size_t> stats_size() const noexcept override final {
    return std::make_pair(size_t(0), size_t(0));
  }
};

struct Scorer {
  Scorer(score_function&& func, const OrderBucket* bucket) noexcept
      : func(std::move(func)),
        bucket(bucket) {
    assert(this->func);
    assert(this->bucket);
  }

  score_function func;
  const OrderBucket* bucket;
};

// A convenient class for doc_iterators to invoke scorer functions
// on scorers in each order bucket
class Scorers : private util::noncopyable { // noncopyable required by MSVC
 public:
  Scorers() = default;
  Scorers(const Order& buckets,
          const sub_reader& segment,
          const term_reader& field,
          const byte_type* stats,
          score_t* score,
          const attribute_provider& doc,
          boost_t boost);
  Scorers(Scorers&&) = default;
  Scorers& operator=(Scorers&&) = default;

  std::vector<Scorer> scorers; // scorer + offset
};

static_assert(std::is_nothrow_move_constructible_v<Scorers>);
static_assert(std::is_nothrow_move_assignable_v<Scorers>);

// Prepare empty collectors, i.e. call collect(...) on each of the
// buckets without explicitly collecting field or term statistics,
// e.g. for 'all' filter
void PrepareCollectors(std::span<const OrderBucket> order,
                       byte_type* stats,
                       const index_reader& index);


}

#endif
