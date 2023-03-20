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

#pragma once

#include <set>

#include "index/index_features.hpp"
#include "search/score_function.hpp"
#include "utils/attribute_provider.hpp"
#include "utils/iterator.hpp"
#include "utils/math_utils.hpp"
#include "utils/memory_pool.hpp"
#include "utils/small_vector.hpp"

namespace irs {

struct data_output;
struct IndexReader;
class memory_index_output;
struct SubReader;
struct ColumnProvider;
struct term_reader;

// Represents no boost value.
inline constexpr score_t kNoBoost{1.F};

// Represents an addition to score from filter specific to a particular
// document. May vary from document to document.
struct filter_boost final : attribute {
  static constexpr std::string_view type_name() noexcept {
    return "irs::filter_boost";
  }

  score_t value{kNoBoost};
};

// Object used for collecting index statistics, for a specific matched
// field, that are required by the scorer for scoring individual
// documents.
class FieldCollector {
 public:
  using ptr = std::unique_ptr<FieldCollector>;

  virtual ~FieldCollector() = default;

  // Collect field related statistics, i.e. field used in the filter
  // `segment` is the segment being processed (e.g. for columnstore).
  // `field` is  the field matched by the filter in the 'segment'.
  // Called once for every field matched by a filter per each segment.
  // Always called on each matched 'field' irrespective of if it
  // contains a matching 'term'.
  virtual void collect(const SubReader& segment, const term_reader& field) = 0;

  // Clear collected stats
  virtual void reset() = 0;

  // Collect field related statistics from a serialized
  // representation as produced by write(...) below.
  virtual void collect(bytes_view in) = 0;

  // Serialize the internal data representation into 'out'.
  virtual void write(data_output& out) const = 0;
};

// Object used for collecting index statistics, for a specific matched
// term of a field, that are required by the scorer for scoring
// individual documents.
struct TermCollector {
  using ptr = std::unique_ptr<TermCollector>;

  virtual ~TermCollector() = default;

  // Collect term related statistics, i.e. term used in the filter.
  // `segment` is the segment being processed (e.g. for columnstore)
  // `field` is the the field matched by the filter in the 'segment'.
  // `term_attrs` is the attributes of the matched term in the field.
  // Called once for every term matched by a filter in the 'field'
  // per each segment.
  // Only called on a matched 'term' in the 'field' in the 'segment'.
  virtual void collect(const SubReader& segment, const term_reader& field,
                       const attribute_provider& term_attrs) = 0;

  // Clear collected stats
  virtual void reset() = 0;

  // Collect term related statistics from a serialized
  // representation as produced by write(...) below.
  virtual void collect(bytes_view in) = 0;

  // Serialize the internal data representation into 'out'.
  virtual void write(data_output& out) const = 0;
};

struct WandSource : attribute_provider {
  using ptr = std::unique_ptr<WandSource>;

  virtual void Read(data_input& in) = 0;
};

struct WandWriter {
  using ptr = std::unique_ptr<WandWriter>;

  static constexpr byte_type kMaxSize = 127;

  virtual ~WandWriter() = default;

  virtual bool Prepare(
    const ColumnProvider& reader,
    const std::map<irs::type_info::type_id, field_id>& features,
    const attribute_provider& attrs) = 0;

  virtual void Reset() = 0;

  virtual void Update() = 0;

  virtual void Write(size_t level, memory_index_output& out) = 0;

  virtual byte_type Size(size_t level) const = 0;
};

// Base class for all scorers.
// Stats are meant to be trivially constructible and will be
// zero initialized before usage.
struct Scorer {
  using ptr = std::unique_ptr<Scorer>;

  virtual ~Scorer() = default;

  // Store collected index statistics into 'stats' of the
  // current 'filter'
  // `filter_attrs` is out-parameter to store statistics for later use in
  // calls to score(...).
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
  virtual void collect(byte_type* stats, const FieldCollector* field,
                       const TermCollector* term) const = 0;

  // The index features required for proper operation of this sort::prepared
  virtual IndexFeatures index_features() const = 0;

  // Create an object to be used for collecting index statistics, one
  // instance per matched field.
  // Returns nullptr == no statistics collection required
  virtual FieldCollector::ptr prepare_field_collector() const = 0;

  // Create a stateful scorer used for computation of document scores
  virtual ScoreFunction prepare_scorer(
    const ColumnProvider& segment,
    const std::map<irs::type_info::type_id, field_id>& features,
    const byte_type* stats, const attribute_provider& doc_attrs,
    score_t boost) const = 0;

  // Create an object to be used for collecting index statistics, one
  // instance per matched term.
  // Returns nullptr == no statistics collection required.
  virtual TermCollector::ptr prepare_term_collector() const = 0;

  // Create an object to be used for writing wand entries to the skip list.
  // max_levels - max number of levels in the skip list
  virtual WandWriter::ptr prepare_wand_writer(size_t max_levels) const = 0;

  virtual WandSource::ptr prepare_wand_source() const = 0;

  // Number of bytes (first) and alignment (first) required to store stats
  // Alignment must satisfy the following requirements:
  //   - be a power of 2
  //   - be less or equal than alignof(MAX_ALIGN_T))
  virtual std::pair<size_t, size_t> stats_size() const = 0;

  virtual bool equals(const Scorer& other) const noexcept {
    return type() == other.type();
  }

  virtual irs::type_info::type_id type() const noexcept = 0;
};

// Possible variants of merging multiple scores
enum class ScoreMergeType {
  // Do nothing
  kNoop = 0,

  // Sum multiple scores
  kSum,

  // Find max amongst multiple scores
  kMax,

  // Find min amongst multiple scores
  kMin
};

struct ScorerBucket {
  ScorerBucket(const Scorer& bucket, size_t stats_offset) noexcept
    : bucket(&bucket), stats_offset{stats_offset} {
    IRS_ASSERT(this->bucket);
  }

  const Scorer* bucket;  // prepared score
  size_t stats_offset;   // offset in stats buffer
};

static_assert(std::is_nothrow_move_constructible_v<ScorerBucket>);
static_assert(std::is_nothrow_move_assignable_v<ScorerBucket>);

// Set of compiled scorers
class Scorers final : private util::noncopyable {
 public:
  static const Scorers kUnordered;

  static Scorers Prepare(std::span<const Scorer*> scorers) {
    return Prepare(scorers.begin(), scorers.end());
  }
  static Scorers Prepare(std::span<const Scorer::ptr> scorers) {
    return Prepare(scorers.begin(), scorers.end());
  }
  static Scorers Prepare(const Scorer* scorer) {
    return Prepare(std::span{&scorer, 1});
  }
  static Scorers Prepare(const Scorer& scorer) { return Prepare(&scorer); }

  Scorers() = default;
  Scorers(Scorers&&) = default;
  Scorers& operator=(Scorers&&) = default;

  bool empty() const noexcept { return buckets_.empty(); }
  std::span<const ScorerBucket> buckets() const noexcept {
    return {buckets_.data(), buckets_.size()};
  }
  size_t score_size() const noexcept { return score_size_; }
  size_t stats_size() const noexcept { return stats_size_; }
  IndexFeatures features() const noexcept { return features_; }

 private:
  using ScorerBuckets = SmallVector<ScorerBucket, 2>;

  template<typename Iterator>
  static Scorers Prepare(Iterator begin, Iterator end);

  size_t PushBack(const Scorer& scorer);

  ScorerBuckets buckets_;
  size_t score_size_{};
  size_t stats_size_{};
  IndexFeatures features_{IndexFeatures::NONE};
};

template<typename Iterator>
Scorers Scorers::Prepare(Iterator begin, Iterator end) {
  IRS_ASSERT(begin <= end);

  size_t stats_align{};
  Scorers scorers;
  scorers.buckets_.reserve(end - begin);

  for (; begin != end; ++begin) {
    const auto& scorer = *begin;

    if (IRS_UNLIKELY(!scorer)) {
      continue;
    }

    stats_align = std::max(stats_align, scorers.PushBack(*scorer));
  }
  scorers.stats_size_ = memory::align_up(scorers.stats_size_, stats_align);

  return scorers;
}

struct NoopAggregator {
  constexpr size_t size() const noexcept { return 0; }
};

template<typename Merger, size_t Size>
struct Aggregator : Merger {
  static_assert(Size > 0);

  constexpr size_t size() const noexcept { return Size; }

  constexpr size_t byte_size() const noexcept { return Size * sizeof(score_t); }

  constexpr score_t* temp() noexcept { return buf.data(); }

  void operator()(score_t* dst, const score_t* src) const noexcept {
    for (size_t i = 0; i < Size; ++i) {
      Merger::operator()(i, dst, src);
    }
  }

  std::array<score_t, Size> buf;
};

template<typename Merger>
struct Aggregator<Merger, std::numeric_limits<size_t>::max()> : Merger {
  explicit Aggregator(size_t size) noexcept : count{size} {
    IRS_ASSERT(size);
    buf.resize(byte_size());
  }

  size_t size() const noexcept { return count; }

  size_t byte_size() const noexcept { return size() * sizeof(score_t); }

  score_t* temp() noexcept { return reinterpret_cast<score_t*>(buf.data()); }

  void operator()(score_t* dst, const score_t* src) const noexcept {
    for (size_t i = 0; i < count; ++i) {
      Merger::operator()(i, dst, src);
    }
  }

  size_t count;
  bstring buf;
};

template<typename Aggregator>
struct HasScoreHelper : std::true_type {};

template<>
struct HasScoreHelper<NoopAggregator> : std::false_type {};

template<typename Aggregator>
inline constexpr bool HasScore_v = HasScoreHelper<Aggregator>::value;

struct SumMerger {
  void operator()(size_t idx, score_t* IRS_RESTRICT dst,
                  const score_t* IRS_RESTRICT src) const noexcept {
    dst[idx] += src[idx];
  }
};

struct MaxMerger {
  void operator()(size_t idx, score_t* IRS_RESTRICT dst,
                  const score_t* IRS_RESTRICT src) const noexcept {
    auto& casted_dst = dst[idx];
    auto& casted_src = src[idx];

    if (casted_dst < casted_src) {
      casted_dst = casted_src;
    }
  }
};

struct MinMerger {
  void operator()(size_t idx, score_t* IRS_RESTRICT dst,
                  const score_t* IRS_RESTRICT src) const noexcept {
    auto& casted_dst = dst[idx];
    auto& casted_src = src[idx];

    if (casted_src < casted_dst) {
      casted_dst = casted_src;
    }
  }
};

template<typename Visitor>
auto ResoveMergeType(ScoreMergeType type, size_t num_buckets,
                     Visitor&& visitor) {
  constexpr size_t kRuntimeSize = std::numeric_limits<size_t>::max();

  auto impl = [&]<typename Merger>() {
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
    case ScoreMergeType::kNoop:
      return visitor(NoopAggregator{});
    case ScoreMergeType::kSum:
      return impl.template operator()<SumMerger>();
    case ScoreMergeType::kMax:
      return impl.template operator()<MaxMerger>();
    case ScoreMergeType::kMin:
      return impl.template operator()<MinMerger>();
    default:
      IRS_ASSERT(false);
      return visitor(NoopAggregator{});
  }
}

// Template score for base class for all prepared(compiled) sort entries
template<typename Impl, typename StatsType = void>
class ScorerBase : public Scorer {
 public:
  static_assert(std::is_same_v<StatsType, void> ||
                std::is_trivially_constructible_v<StatsType>);

  WandWriter::ptr prepare_wand_writer(size_t) const override { return nullptr; }

  WandSource::ptr prepare_wand_source() const override { return nullptr; }

  irs::type_info::type_id type() const noexcept final {
    return irs::type<Impl>::id();
  }

  FieldCollector::ptr prepare_field_collector() const override {
    return nullptr;
  }

  TermCollector::ptr prepare_term_collector() const override { return nullptr; }

  void collect(byte_type*, const FieldCollector*,
               const TermCollector*) const override {}

  IRS_FORCE_INLINE static const StatsType* stats_cast(
    const byte_type* buf) noexcept {
    IRS_ASSERT(buf);
    return reinterpret_cast<const StatsType*>(buf);
  }

  IRS_FORCE_INLINE static StatsType* stats_cast(byte_type* buf) noexcept {
    return const_cast<StatsType*>(
      stats_cast(const_cast<const byte_type*>(buf)));
  }

  // Returns number of bytes and alignment required to store stats
  IRS_FORCE_INLINE std::pair<size_t, size_t> stats_size() const noexcept final {
    if constexpr (std::is_same_v<StatsType, void>) {
      return {size_t{0}, size_t{0}};
    } else {
      static_assert(alignof(StatsType) <= alignof(std::max_align_t));
      static_assert(math::is_power2(alignof(StatsType)));

      return {sizeof(StatsType), alignof(StatsType)};
    }
  }
};

}  // namespace irs
