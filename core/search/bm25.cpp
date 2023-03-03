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
////////////////////////////////////////////////////////////////////////////////

#include "bm25.hpp"

#include "analysis/token_attributes.hpp"
#include "index/field_meta.hpp"
#include "index/index_reader.hpp"
#include "index/norm.hpp"
#include "search/wand_writer.hpp"
#include "utils/math_utils.hpp"
#include "velocypack/Builder.h"
#include "velocypack/Parser.h"
#include "velocypack/Slice.h"
#include "velocypack/velocypack-aliases.h"
#include "velocypack/vpack.h"

namespace {

const auto kSQRT = irs::cache_func<uint32_t, 2048>(
  0, [](uint32_t i) noexcept { return std::sqrt(static_cast<float_t>(i)); });

irs::ScorerFactory::ptr make_from_object(const VPackSlice slice) {
  IRS_ASSERT(slice.isObject());

  auto scorer = std::make_unique<irs::bm25_sort>();

  {
    // optional float
    const auto* key = "b";

    if (slice.hasKey(key)) {
      if (!slice.get(key).isNumber()) {
        IR_FRMT_ERROR(
          "Non-float value in '%s' while constructing bm25 scorer from VPack "
          "arguments",
          key);

        return nullptr;
      }

      scorer->b(slice.get(key).getNumber<float>());
    }
  }

  {
    // optional float
    const auto* key = "k";

    if (slice.hasKey(key)) {
      if (!slice.get(key).isNumber()) {
        IR_FRMT_ERROR(
          "Non-float value in '%s' while constructing bm25 scorer from VPack "
          "arguments",
          key);

        return nullptr;
      }

      scorer->k(slice.get(key).getNumber<float>());
    }
  }

  return scorer;
}

irs::ScorerFactory::ptr make_from_array(const VPackSlice slice) {
  IRS_ASSERT(slice.isArray());

  VPackArrayIterator array(slice);
  VPackValueLength size = array.size();
  if (size > 2) {
    // wrong number of arguments
    IR_FRMT_ERROR(
      "Wrong number of arguments while constructing bm25 scorer from VPack "
      "arguments (must be <= 2)");
    return nullptr;
  }

  // default args
  auto k = irs::bm25_sort::K();
  auto b = irs::bm25_sort::B();
  int i = 0;
  for (auto arg_slice : array) {
    switch (i) {
      case 0:  // parse `k` coefficient
        if (!arg_slice.isNumber<decltype(k)>()) {
          IR_FRMT_ERROR(
            "Non-float value at position '%u' while constructing bm25 scorer "
            "from VPack arguments",
            i);
          return nullptr;
        }

        k = static_cast<float_t>(arg_slice.getNumber<decltype(k)>());
        ++i;
        break;
      case 1:  // parse `b` coefficient
        if (!arg_slice.isNumber<decltype(b)>()) {
          IR_FRMT_ERROR(
            "Non-float value at position '%u' while constructing bm25 scorer "
            "from VPack arguments",
            i);
          return nullptr;
        }

        b = static_cast<float_t>(arg_slice.getNumber<decltype(b)>());
        break;
    }
  }

  return std::make_unique<irs::bm25_sort>(k, b);
}

irs::ScorerFactory::ptr make_vpack(const VPackSlice slice) {
  switch (slice.type()) {
    case VPackValueType::Object:
      return make_from_object(slice);
    case VPackValueType::Array:
      return make_from_array(slice);
    default:  // wrong type
      IR_FRMT_ERROR(
        "Invalid VPack arguments passed while constructing bm25 scorer");
      return nullptr;
  }
}

irs::ScorerFactory::ptr make_vpack(std::string_view args) {
  if (irs::IsNull(args)) {
    // default args
    return std::make_unique<irs::bm25_sort>();
  } else {
    VPackSlice slice(reinterpret_cast<const uint8_t*>(args.data()));
    return make_vpack(slice);
  }
}

irs::ScorerFactory::ptr make_json(std::string_view args) {
  if (irs::IsNull(args)) {
    // default args
    return std::make_unique<irs::bm25_sort>();
  } else {
    try {
      auto vpack = VPackParser::fromJson(args.data(), args.size());
      return make_vpack(vpack->slice());
    } catch (const VPackException& ex) {
      IR_FRMT_ERROR(
        "Caught error '%s' while constructing VPack from JSON for bm25 "
        "scorer",
        ex.what());
    } catch (...) {
      IR_FRMT_ERROR(
        "Caught error while constructing VPack from JSON for bm25 scorer");
    }
    return nullptr;
  }
}

REGISTER_SCORER_JSON(irs::bm25_sort, make_json);
REGISTER_SCORER_VPACK(irs::bm25_sort, make_vpack);

struct byte_ref_iterator {
  using iterator_category = std::input_iterator_tag;
  using value_type = irs::byte_type;
  using pointer = value_type*;
  using reference = value_type&;
  using difference_type = void;

  const irs::byte_type* end_;
  const irs::byte_type* pos_;

  explicit byte_ref_iterator(irs::bytes_view in)
    : end_(in.data() + in.size()), pos_(in.data()) {}

  irs::byte_type operator*() {
    if (pos_ >= end_) {
      throw irs::io_error("invalid read past end of input");
    }

    return *pos_;
  }

  void operator++() { ++pos_; }
};

struct field_collector final : public irs::FieldCollector {
  // number of documents containing the matched
  // field (possibly without matching terms)
  uint64_t docs_with_field = 0;
  // number of terms for processed field
  uint64_t total_term_freq = 0;

  void collect(const irs::SubReader& /*segment*/,
               const irs::term_reader& field) final {
    docs_with_field += field.docs_count();

    auto* freq = irs::get<irs::frequency>(field);

    if (freq) {
      total_term_freq += freq->value;
    }
  }

  void reset() noexcept final {
    docs_with_field = 0;
    total_term_freq = 0;
  }

  void collect(irs::bytes_view in) final {
    byte_ref_iterator itr(in);
    auto docs_with_field_value = irs::vread<uint64_t>(itr);
    auto total_term_freq_value = irs::vread<uint64_t>(itr);

    if (itr.pos_ != itr.end_) {
      throw irs::io_error("input not read fully");
    }

    docs_with_field += docs_with_field_value;
    total_term_freq += total_term_freq_value;
  }

  void write(irs::data_output& out) const final {
    out.write_vlong(docs_with_field);
    out.write_vlong(total_term_freq);
  }
};

struct term_collector final : public irs::TermCollector {
  // number of documents containing the matched term
  uint64_t docs_with_term = 0;

  void collect(const irs::SubReader& /*segment*/,
               const irs::term_reader& /*field*/,
               const irs::attribute_provider& term_attrs) final {
    auto* meta = irs::get<irs::term_meta>(term_attrs);

    if (meta) {
      docs_with_term += meta->docs_count;
    }
  }

  void reset() noexcept final { docs_with_term = 0; }

  void collect(irs::bytes_view in) final {
    byte_ref_iterator itr(in);
    auto docs_with_term_value = irs::vread<uint64_t>(itr);

    if (itr.pos_ != itr.end_) {
      throw irs::io_error("input not read fully");
    }

    docs_with_term += docs_with_term_value;
  }

  void write(irs::data_output& out) const final {
    out.write_vlong(docs_with_term);
  }
};

}  // namespace

namespace irs {

// BM25 similarity
// bm25(doc, term) = idf(term) * ((k + 1) * tf(doc, term)) / (k * (1 - b + b *
// |doc|/avgDL) + tf(doc, term))
//
// Inverted document frequency
// idf(term) = log(1 + (#documents with this field - #documents with this term +
// 0.5)/(#documents with this term + 0.5))
//
// Term frequency
//   Norm2: tf(doc, term) = frequency(doc, term);
//   Norm:  tf(doc, term) = sqrt(frequency(doc, term));
//
// Document length
//   Norm2: |doc| # of terms in a field within a document
//   Norm:  |doc| = 1 / sqrt(# of terms in a field within a document)
//
// Average document length
// avgDL = sum(field_term_count) / (#documents with this field)

namespace bm25 {

// Empty frequency
const frequency kEmptyFreq;

struct stats final {
  // precomputed idf value
  float_t idf;
  // precomputed k*(1-b)
  float_t norm_const;
  // precomputed k*b/avgD
  float_t norm_length;
  // precomputed 1/(k*(1-b+b*|doc|/avgDL)) for |doc| E [0..255]
  float_t norm_cache[256];
};

struct BM15Context : public irs::score_ctx {
  BM15Context(float_t k, irs::score_t boost, const bm25::stats& stats,
              const frequency* freq, const filter_boost* fb = nullptr) noexcept
    : freq{freq ? freq : &kEmptyFreq},
      filter_boost{fb},
      num{boost * (k + 1) * stats.idf},
      norm_const{k} {
    IRS_ASSERT(this->freq);
  }

  const frequency* freq;  // document frequency
  const irs::filter_boost* filter_boost;
  float_t num;  // partially precomputed numerator : boost * (k + 1) * idf
  float_t norm_const;  // 'k' factor
};

template<typename Norm>
struct BM25Context final : public BM15Context {
  BM25Context(float_t k, irs::score_t boost, const bm25::stats& stats,
              const frequency* freq, Norm&& norm,
              const irs::filter_boost* filter_boost = nullptr) noexcept
    : BM15Context{k, boost, stats, freq, filter_boost},
      norm{std::move(norm)},
      norm_length{stats.norm_length},
      norm_cache{stats.norm_cache} {
    IRS_ASSERT(stats.norm_const);
    norm_const = stats.norm_const;
  }

  Norm norm;
  float_t norm_length;  // precomputed 'k*b/avgD'
  const float_t* norm_cache;
};

enum class NormType {
  // Norm2 values
  kNorm2 = 0,
  // Norm2 values fit 1-byte
  kNorm2Tiny,
  // Old norms, 1/sqrt(|doc|)
  kNorm
};

template<typename Reader, NormType Type>
struct NormAdapter : Reader {
  static constexpr auto kType = Type;

  explicit NormAdapter(Reader&& reader) : Reader{std::move(reader)} {}

  IRS_FORCE_INLINE auto operator()() -> std::invoke_result_t<Reader> {
    if constexpr (kType < NormType::kNorm) {
      // norms are stored |doc| as uint32_t
      return Reader::operator()();
    } else {
      // norms are stored 1/sqrt(|doc|) as float
      return 1.f / Reader::operator()();
    }
  }
};

template<NormType Type, typename Reader>
auto MakeNormAdapter(Reader&& reader) {
  return NormAdapter<Reader, Type>(std::move(reader));
}

template<typename Ctx>
struct MakeScoreFunctionImpl {
  template<bool HasFilterBoost, typename... Args>
  static ScoreFunction Make(Args&&... args);
};

template<>
struct MakeScoreFunctionImpl<BM15Context> {
  using Ctx = BM15Context;

  template<bool HasFilterBoost, typename... Args>
  static auto Make(Args&&... args) {
    return ScoreFunction::Make<Ctx>(
      [](irs::score_ctx* ctx, irs::score_t* res) noexcept {
        IRS_ASSERT(res);
        IRS_ASSERT(ctx);

        auto& state = *static_cast<Ctx*>(ctx);

        const float_t tf = static_cast<float_t>(state.freq->value);

        float_t c0;
        if constexpr (HasFilterBoost) {
          IRS_ASSERT(state.filter_boost);
          c0 = state.filter_boost->value * state.num;
        } else {
          c0 = state.num;
        }

        const float_t c1 = state.norm_const;

        *res = c0 - c0 / (1.f + tf / c1);
      },
      std::forward<Args>(args)...);
  }
};

template<typename Norm>
struct MakeScoreFunctionImpl<BM25Context<Norm>> {
  using Ctx = BM25Context<Norm>;

  template<bool HasFilterBoost, typename... Args>
  static auto Make(Args&&... args) {
    return ScoreFunction::Make<Ctx>(
      [](irs::score_ctx* ctx, irs::score_t* res) noexcept {
        IRS_ASSERT(res);
        IRS_ASSERT(ctx);

        auto& state = *static_cast<Ctx*>(ctx);

        float_t tf;
        if constexpr (Norm::kType < NormType::kNorm) {
          tf = static_cast<float_t>(state.freq->value);
        } else {
          tf = ::kSQRT.get<true>(state.freq->value);
        }

        float_t c0;
        if constexpr (HasFilterBoost) {
          IRS_ASSERT(state.filter_boost);
          c0 = state.filter_boost->value * state.num;
        } else {
          c0 = state.num;
        }

        if constexpr (NormType::kNorm2Tiny == Norm::kType) {
          static_assert(std::is_same_v<uint32_t, decltype(state.norm())>);
          const float_t inv_c1 =
            state.norm_cache[state.norm() & uint32_t{0xFF}];

          *res = c0 - c0 / (1.f + tf * inv_c1);
        } else {
          const float_t c1 =
            state.norm_const + state.norm_length * state.norm();

          *res = c0 - c0 * c1 / (c1 + tf);
        }
      },
      std::forward<Args>(args)...);
  }
};

template<typename Ctx, typename... Args>
ScoreFunction MakeScoreFunction(const filter_boost* filter_boost,
                                Args&&... args) noexcept {
  if (filter_boost) {
    return MakeScoreFunctionImpl<Ctx>::template Make<true>(
      std::forward<Args>(args)..., filter_boost);
  }

  return MakeScoreFunctionImpl<Ctx>::template Make<false>(
    std::forward<Args>(args)...);
}

class sort final : public irs::ScorerBase<bm25::stats> {
 public:
  sort(float_t k, float_t b, bool boost_as_score) noexcept
    : k_{k}, b_{b}, boost_as_score_{boost_as_score} {}

  void collect(byte_type* stats_buf, const irs::FieldCollector* field,
               const irs::TermCollector* term) const final {
    auto& stats = stats_cast(stats_buf);

    const auto* field_ptr = down_cast<field_collector>(field);
    const auto* term_ptr = down_cast<term_collector>(term);

    // nullptr possible if e.g. 'all' filter
    const auto docs_with_field = field_ptr ? field_ptr->docs_with_field : 0;
    // nullptr possible if e.g.'by_column_existence' filter
    const auto docs_with_term = term_ptr ? term_ptr->docs_with_term : 0;
    // nullptr possible if e.g. 'all' filter
    const auto total_term_freq = field_ptr ? field_ptr->total_term_freq : 0;

    // precomputed idf value
    stats.idf += float_t(std::log1p((docs_with_field - docs_with_term + 0.5) /
                                    (docs_with_term + 0.5)));
    IRS_ASSERT(stats.idf >= 0.f);

    // - stats were already initialized
    // - BM15 without norms
    if (IsBM15()) {
      stats.norm_const = 1.f;
      return;
    }

    // precomputed length norm
    const float_t kb = k_ * b_;

    stats.norm_const = k_ - kb;
    if (total_term_freq && docs_with_field) {
      const float_t avg_dl = float_t(total_term_freq) / docs_with_field;
      stats.norm_length = kb / avg_dl;
    } else {
      stats.norm_length = kb;
    }

    for (uint32_t i = 0; auto& norm : stats.norm_cache) {
      norm = 1.f / (stats.norm_const + stats.norm_length * i++);
    }
  }

  IndexFeatures index_features() const noexcept final {
    return IndexFeatures::FREQ;
  }

  std::span<const type_info::type_id> features() const noexcept final {
    if (!IsBM15()) {
      static const irs::type_info::type_id kFeature{irs::type<Norm2>::id()};
      return {&kFeature, 1};
    }

    return {};
  }

  FieldCollector::ptr prepare_field_collector() const final {
    return std::make_unique<field_collector>();
  }

  ScoreFunction prepare_scorer(const SubReader& segment,
                               const term_reader& field,
                               const byte_type* query_stats,
                               const attribute_provider& doc_attrs,
                               score_t boost) const final {
    auto* freq = irs::get<frequency>(doc_attrs);

    if (!freq) {
      if (!boost_as_score_ || 0.f == boost) {
        return ScoreFunction::Invalid();
      }

      // if there is no frequency then all the scores
      // will be the same (e.g. filter irs::all)
      return ScoreFunction::Constant(boost);
    }

    auto& stats = stats_cast(query_stats);
    auto* filter_boost = irs::get<irs::filter_boost>(doc_attrs);

    if (!IsBM15()) {
      auto* doc = irs::get<document>(doc_attrs);

      if (IRS_UNLIKELY(!doc)) {
        // We need 'document' attribute to be exposed.
        return ScoreFunction::Invalid();
      }

      auto prepare_norm_scorer =
        [&]<typename Norm>(Norm&& norm) -> ScoreFunction {
        return MakeScoreFunction<BM25Context<Norm>>(
          filter_boost, k_, boost, stats, freq, std::move(norm));
      };

      const auto& features = field.meta().features;

      if (auto it = features.find(irs::type<Norm2>::id());
          it != features.end()) {
        if (Norm2ReaderContext ctx; ctx.Reset(segment, it->second, *doc)) {
          if (ctx.max_num_bytes == sizeof(byte_type)) {
            return Norm2::MakeReader(std::move(ctx), [&](auto&& reader) {
              return prepare_norm_scorer(
                MakeNormAdapter<NormType::kNorm2Tiny>(std::move(reader)));
            });
          }

          return Norm2::MakeReader(std::move(ctx), [&](auto&& reader) {
            return prepare_norm_scorer(
              MakeNormAdapter<NormType::kNorm2>(std::move(reader)));
          });
        }
      }

      if (auto it = features.find(irs::type<Norm>::id());
          it != features.end()) {
        if (NormReaderContext ctx; ctx.Reset(segment, it->second, *doc)) {
          return prepare_norm_scorer(
            MakeNormAdapter<NormType::kNorm>(Norm::MakeReader(std::move(ctx))));
        }
      }

      // No norms, pretend all fields have the same length 1.
      return prepare_norm_scorer(
        MakeNormAdapter<NormType::kNorm2Tiny>([]() { return 1U; }));
    }

    // BM15
    return MakeScoreFunction<BM15Context>(filter_boost, k_, boost, stats, freq);
  }

  WandWriter::ptr prepare_wand_writer(size_t max_levels) const final {
    return std::make_unique<WandWriterImpl<FreqProducer>>(*this, max_levels);
  }

  TermCollector::ptr prepare_term_collector() const final {
    return std::make_unique<term_collector>();
  }

 private:
  // Norms are not needed for BM15
  bool IsBM15() const noexcept { return b_ == 0.f; }

  float_t k_;
  float_t b_;
  bool boost_as_score_;
};

}  // namespace bm25

bm25_sort::bm25_sort(float_t k /*= 1.2f*/, float_t b /*= 0.75f*/,
                     bool boost_as_score /*= false*/) noexcept
  : ScorerFactory{irs::type<bm25_sort>::get()},
    k_{k},
    b_{b},
    boost_as_score_{boost_as_score} {}

/*static*/ void bm25_sort::init() {
  REGISTER_SCORER_JSON(bm25_sort, make_json);    // match registration above
  REGISTER_SCORER_VPACK(bm25_sort, make_vpack);  // match registration above
}

Scorer::ptr bm25_sort::prepare() const {
  return std::make_unique<bm25::sort>(k_, b_, boost_as_score_);
}

}  // namespace irs
