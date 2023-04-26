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

#include <algorithm>

#include "analysis/token_attributes.hpp"
#include "index/norm.hpp"
#include "search/scorer.hpp"
#include "store/memory_directory.hpp"

#include <absl/container/inlined_vector.h>

namespace irs {

template<typename ValueProducer>
class WandWriterImpl final : public WandWriter {
 public:
  WandWriterImpl(const Scorer& scorer, size_t max_levels)
    : score_levels_{max_levels + 1}, producer_{scorer} {
    IRS_ASSERT(max_levels != 0);
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

    Update(score_levels_.front(), score, [&] { return producer_.GetValue(); });
  }

  void Write(size_t level, memory_index_output& out) final {
    IRS_ASSERT(level + 1 < score_levels_.size());
    auto& score = score_levels_[level];
    // Accumulate score on less granular level
    Update(score_levels_[level + 1], score.score, [&] { return score.value; });

    IRS_ASSERT(
      std::is_sorted(score_levels_.begin(), score_levels_.begin() + level + 2));
    ValueProducer::Write(score.value, out);
    score.score = 0.f;
  }

  void WriteRoot(index_output& out) final {
    auto max = std::max_element(score_levels_.begin(), score_levels_.end());
    IRS_ASSERT(max != score_levels_.end());
    ValueProducer::Write(max->value, out);
  }

  byte_type Size(size_t level) const noexcept final {
    IRS_ASSERT(level + 1 < score_levels_.size());
    const auto& entry = score_levels_[level];
    return ValueProducer::Size(entry.value);
  }

  byte_type SizeRoot() const noexcept final {
    const auto& entry = score_levels_.back();
    return ValueProducer::Size(entry.value);
  }

 private:
  using ValueType = typename ValueProducer::Value;

  struct Entry {
    score_t score{};
    ValueType value;

    friend bool operator<(const Entry& lhs, const Entry& rhs) noexcept {
      return lhs.score < rhs.score;
    }
  };

  template<typename Func>
  void Update(Entry& lhs, score_t rhs_score, Func&& func) noexcept {
    if (lhs.score < rhs_score) {
      lhs.score = rhs_score;
      lhs.value = func();
    }
  }

  absl::InlinedVector<Entry, 9 + 1> score_levels_;
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
    uint32_t delta_norm{};
  };

  template<typename Output>
  static void Write(Value value, Output& out) {
    out.write_vint(value.freq);
    out.write_vint(value.delta_norm);
  }

  static size_t Size(Value value) noexcept {
    return bytes_io<uint32_t>::vsize(value.freq) +
           bytes_io<uint32_t>::vsize(value.delta_norm);
  }

  explicit FreqNormProducer(const Scorer& scorer) : ValueProducerBase{scorer} {}

  bool Prepare(const ColumnProvider& reader, const feature_map_t& features,
               const attribute_provider& attrs) {
    freq_ = irs::get<frequency>(attrs);

    if (!freq_) {
      return false;
    }

    const auto* doc = irs::get<irs::document>(attrs);

    if (IRS_UNLIKELY(!doc)) {
      return false;
    }

    const auto norm = features.find(irs::type<Norm2>::id());

    if (norm == features.end() || !field_limits::valid(norm->second)) {
      return false;
    }

    Norm2::Context ctx;
    if (!ctx.Reset(reader, norm->second, *doc)) {
      return false;
    }

    norm_ = Norm2::MakeReader(std::move(ctx), [&](auto&& reader) {
      return fu2::unique_function<Norm2::ValueType()>{std::move(reader)};
    });

    return ValueProducerBase::Prepare(reader, features, attrs);
  }

  Value GetValue() const noexcept {
    const auto freq = freq_->value;
    const auto norm = norm_();
    IRS_ASSERT(freq <= norm);
    return {.freq = freq, .delta_norm = norm - freq};
  }

 private:
  const irs::frequency* freq_{};
  mutable fu2::unique_function<uint32_t()> norm_;
};

class FreqNormSource final : public WandSource {
 public:
  attribute* get_mutable(type_info::type_id type) final {
    if (irs::type<frequency>::id() == type) {
      return &freq_;
    }
    if (irs::type<Norm2>::id() == type) {
      return &norm_;
    }
    return nullptr;
  }

  void Read(data_input& in) final {
    freq_.value = in.read_vint();
    norm_.value = freq_.value + in.read_vint();
  }

 private:
  frequency freq_;
  Norm2 norm_;
};

class FreqProducer : public ValueProducerBase {
 public:
  struct Value {
    uint32_t freq{};
  };

  template<typename Output>
  static void Write(Value value, Output& out) {
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

class FreqSource final : public WandSource {
 public:
  attribute* get_mutable(type_info::type_id type) final {
    if (irs::type<frequency>::id() == type) {
      return &freq_;
    }
    return nullptr;
  }

  void Read(data_input& in) final { freq_.value = in.read_vint(); }

 private:
  frequency freq_;
};

template<bool HasNorms>
using WandProducer =
  std::conditional_t<HasNorms, FreqNormProducer, FreqProducer>;

template<bool HasNorms>
using WandAttributes = std::conditional_t<HasNorms, FreqNormSource, FreqSource>;

}  // namespace irs
