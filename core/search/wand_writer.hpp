////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "analysis/token_attributes.hpp"
#include "index/norm.hpp"
#include "search/sort.hpp"
#include "store/memory_directory.hpp"

#include <absl/container/inlined_vector.h>

namespace irs {

template<typename ValueProducer>
class WandWriterImpl final : public WandWriter {
 public:
  WandWriterImpl(const Scorer& scorer, size_t max_levels)
    : score_levels_{max_levels}, producer_{scorer} {
    IRS_ASSERT(!score_levels_.empty());
  }

  bool Prepare(const ColumnProvider& reader, const feature_map_t& features,
               const attribute_provider& attrs) final {
    return producer_.Prepare(reader, features, attrs);
  }

  void Reset() noexcept final {
    for (auto& level : score_levels_) {
      level = {};
    }
  }

  void Update() noexcept final {
    score_t score;
    producer_.GetScore(&score);

    Update(score_levels_.front(), score,
           [&]() { return producer_.GetValue(); });
  }

  void Write(size_t level, memory_index_output& out) final {
    IRS_ASSERT(level < score_levels_.size());
    auto& score = score_levels_[level];
    if (level < score_levels_.size() - 1) {
      // Accumulate score on less granular level
      Update(score_levels_[level + 1], score.score,
             [&]() { return score.value; });
    }
    ValueProducer::Write(score.value, out);
    score.score = 0.f;
  }

  byte_type Size(size_t level) const noexcept final {
    IRS_ASSERT(level < score_levels_.size());
    const auto& entry = score_levels_[level];
    return ValueProducer::Size(entry.value);
  }

 private:
  using ValueType = typename ValueProducer::Value;

  struct Entry {
    score_t score{};
    ValueType value;
  };

  template<typename Func>
  void Update(Entry& lhs, score_t rhs_score, Func&& func) noexcept {
    if (lhs.score < rhs_score) {
      lhs.score = rhs_score;
      lhs.value = func();
    }
  }

  std::vector<Entry> score_levels_;
  IRS_NO_UNIQUE_ADDRESS ValueProducer producer_;
};

class ValueProducerBase {
 public:
  void GetScore(score_t* score) const noexcept { func_(score); }

  explicit ValueProducerBase(const Scorer& scorer) : scorer_{&scorer} {
    stats_.resize(scorer.stats_size().first);
    scorer.collect(stats_.data(), nullptr, nullptr);
  }

 protected:
  bool Prepare(const ColumnProvider& reader, const feature_map_t& features,
               const attribute_provider& attrs) {
    func_ =
      scorer_->prepare_scorer(reader, features, stats_.data(), attrs, kNoBoost);
    return static_cast<bool>(func_);
  }

 private:
  absl::InlinedVector<byte_type, 16> stats_;
  ScoreFunction func_;
  const Scorer* scorer_;
};

class FreqNormProducer : public ValueProducerBase {
 public:
  struct Value {
    uint32_t freq{};
    uint32_t norm{};
  };

  static void Write(Value value, memory_index_output& out) {
    out.write_vint(value.freq);
    out.write_vint(value.norm);
  }

  static size_t Size(Value value) noexcept {
    return bytes_io<uint32_t>::vsize(value.freq) +
           bytes_io<uint32_t>::vsize(value.norm);
  }

  explicit FreqNormProducer(const Scorer& scorer) : ValueProducerBase{scorer} {}

  bool Prepare(const ColumnProvider& reader, const feature_map_t& features,
               const attribute_provider& attrs) {
    freq_ = irs::get<frequency>(attrs);

    if (!freq_) {
      return false;
    }

    auto norm = features.find(irs::type<Norm2>::id());

    if (norm == features.end() || !field_limits::valid(norm->second)) {
      return false;
    }

    // FIXME(gnusi): norms
    return ValueProducerBase::Prepare(reader, features, attrs);
  }

  Value GetValue() const noexcept { return {.freq = freq_->value}; }

 private:
  const irs::frequency* freq_{};
};

class FreqProducer : public ValueProducerBase {
 public:
  struct Value {
    uint32_t freq{};
  };

  static void Write(Value value, memory_index_output& out) {
    out.write_vint(value.freq);
  }

  static size_t Size(Value value) noexcept {
    return bytes_io<uint32_t>::vsize(value.freq);
  }

  explicit FreqProducer(const Scorer& scorer) : ValueProducerBase{scorer} {}

  bool Prepare(const ColumnProvider& reader, const feature_map_t& features,
               const attribute_provider& attrs) {
    freq_ = irs::get<frequency>(attrs);

    if (!freq_) {
      return false;
    }

    return ValueProducerBase::Prepare(reader, features, attrs);
  }

  Value GetValue() const noexcept { return {.freq = freq_->value}; }

 private:
  const frequency* freq_{};
};

template<bool HasNorms>
using WandProducer =
  std::conditional_t<HasNorms, FreqNormProducer, FreqProducer>;

}  // namespace irs
