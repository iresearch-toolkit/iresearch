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

#include "tfidf.hpp"

#include <velocypack/Parser.h>
#include <velocypack/Slice.h>
#include <velocypack/velocypack-aliases.h>
#include <velocypack/vpack.h>

#include <cmath>
#include <string_view>

#include "analysis/token_attributes.hpp"
#include "index/field_meta.hpp"
#include "index/index_reader.hpp"
#include "index/norm.hpp"
#include "search/scorer_impl.h"
#include "search/scorers.hpp"
#include "search/wand_writer.hpp"
#include "utils/math_utils.hpp"
#include "utils/misc.hpp"

namespace irs {
namespace {

// TODO(MBkkt) deduplicate with bm25.cpp
const auto kSQRT = cache_func<uint32_t, 2048>(
  0, [](uint32_t i) noexcept { return std::sqrt(static_cast<float_t>(i)); });

const auto kRSQRT = cache_func<uint32_t, 2048>(1, [](uint32_t i) noexcept {
  return 1.f / std::sqrt(static_cast<float_t>(i));
});

Scorer::ptr make_from_bool(const VPackSlice slice) {
  IRS_ASSERT(slice.isBool());

  return std::make_unique<TFIDF>(slice.getBool());
}

constexpr std::string_view WITH_NORMS_PARAM_NAME("withNorms");

Scorer::ptr make_from_object(const VPackSlice slice) {
  IRS_ASSERT(slice.isObject());

  auto normalize = TFIDF::WITH_NORMS();

  {
    // optional bool
    if (slice.hasKey(WITH_NORMS_PARAM_NAME)) {
      if (!slice.get(WITH_NORMS_PARAM_NAME).isBool()) {
        IR_FRMT_ERROR(
          "Non-boolean value in '%s' while constructing tfidf scorer from "
          "VPack arguments",
          WITH_NORMS_PARAM_NAME.data());

        return nullptr;
      }

      normalize = slice.get(WITH_NORMS_PARAM_NAME).getBool();
    }
  }

  return std::make_unique<TFIDF>(normalize);
}

Scorer::ptr make_from_array(const VPackSlice slice) {
  IRS_ASSERT(slice.isArray());

  VPackArrayIterator array = VPackArrayIterator(slice);
  VPackValueLength size = array.size();

  if (size > 1) {
    // wrong number of arguments
    IR_FRMT_ERROR(
      "Wrong number of arguments while constructing tfidf scorer from VPack "
      "arguments (must be <= 1)");
    return nullptr;
  }

  // default args
  auto norms = TFIDF::WITH_NORMS();

  // parse `withNorms` optional argument
  for (auto arg_slice : array) {
    if (!arg_slice.isBool()) {
      IR_FRMT_ERROR(
        "Non-bool value on position `0` while constructing tfidf scorer from "
        "VPack arguments");
      return nullptr;
    }

    norms = arg_slice.getBool();
  }

  return std::make_unique<TFIDF>(norms);
}

Scorer::ptr make_vpack(const VPackSlice slice) {
  switch (slice.type()) {
    case VPackValueType::Bool:
      return make_from_bool(slice);
    case VPackValueType::Object:
      return make_from_object(slice);
    case VPackValueType::Array:
      return make_from_array(slice);
    default:  // wrong type
      IR_FRMT_ERROR(
        "Invalid VPack arguments passed while constructing tfidf scorer, "
        "arguments");
      return nullptr;
  }
}

Scorer::ptr make_vpack(std::string_view args) {
  if (irs::IsNull(args)) {
    // default args
    return std::make_unique<TFIDF>();
  } else {
    VPackSlice slice(reinterpret_cast<const uint8_t*>(args.data()));
    return make_vpack(slice);
  }
}

Scorer::ptr make_json(std::string_view args) {
  if (irs::IsNull(args)) {
    // default args
    return std::make_unique<TFIDF>();
  } else {
    try {
      auto vpack = VPackParser::fromJson(args.data(), args.size());
      return make_vpack(vpack->slice());
    } catch (const VPackException& ex) {
      IR_FRMT_ERROR(
        "Caught error '%s' while constructing VPack from JSON for tfidf "
        "scorer",
        ex.what());
    } catch (...) {
      IR_FRMT_ERROR(
        "Caught error while constructing VPack from JSON for tfidf scorer");
    }
    return nullptr;
  }
}

REGISTER_SCORER_JSON(TFIDF, make_json);
REGISTER_SCORER_VPACK(TFIDF, make_vpack);

IRS_FORCE_INLINE float_t tfidf(uint32_t freq, float_t idf) noexcept {
  return kSQRT.get<true>(freq) * idf;
}

struct EmptyNorm final {};

template<typename Norm>
struct TFIDFContext final : public score_ctx {
  TFIDFContext(Norm&& norm, score_t boost, TFIDFStats idf,
               const frequency* freq,
               const filter_boost* filter_boost = nullptr) noexcept
    : freq{freq ? *freq : kEmptyFreq},
      filter_boost{filter_boost},
      idf{boost * idf.value},
      norm{std::move(norm)} {
    IRS_ASSERT(freq);
  }

  TFIDFContext(const TFIDFContext&) = delete;
  TFIDFContext& operator=(const TFIDFContext&) = delete;

  const frequency& freq;
  const irs::filter_boost* filter_boost;
  float_t idf;  // precomputed : boost * idf
  IRS_NO_UNIQUE_ADDRESS Norm norm;
};

template<typename Reader, NormType Type>
struct TFIDFNormAdapter final {
  explicit TFIDFNormAdapter(Reader&& reader) : reader{std::move(reader)} {}

  IRS_FORCE_INLINE decltype(auto) operator()() {
    if constexpr (Type < NormType::kNorm) {
      return kRSQRT.get<Type != NormType::kNorm2Tiny>(reader());
    } else {
      return reader();
    }
  }

  IRS_NO_UNIQUE_ADDRESS Reader reader;
};

template<NormType Type, typename Reader>
auto MakeTFIDFNormAdapter(Reader&& reader) {
  return TFIDFNormAdapter<Reader, Type>(std::move(reader));
}

}  // namespace

template<typename Norm>
struct MakeScoreFunctionImpl<TFIDFContext<Norm>> {
  using Ctx = TFIDFContext<Norm>;

  template<bool HasFilterBoost, typename... Args>
  static auto Make(Args&&... args) {
    return ScoreFunction::Make<Ctx>(
      [](score_ctx* ctx, score_t* res) noexcept {
        IRS_ASSERT(res);
        IRS_ASSERT(ctx);

        auto& state = *static_cast<Ctx*>(ctx);

        // FIXME(gnusi): we don't need idf for WAND evaluation
        float_t idf;
        if constexpr (HasFilterBoost) {
          IRS_ASSERT(state.filter_boost);
          idf = state.idf * state.filter_boost->value;
        } else {
          idf = state.idf;
        }

        if constexpr (std::is_same_v<Norm, EmptyNorm>) {
          *res = tfidf(state.freq.value, idf);
        } else {
          *res = tfidf(state.freq.value, idf) * state.norm();
        }
      },
      std::forward<Args>(args)...);
  }
};

void TFIDF::collect(byte_type* stats_buf, const FieldCollector* field,
                    const TermCollector* term) const {
  const auto* field_ptr = down_cast<FieldCollectorImpl<false>>(field);
  const auto* term_ptr = down_cast<TermCollectorImpl>(term);

  // nullptr possible if e.g. 'all' filter
  const auto docs_with_field = field_ptr ? field_ptr->docs_with_field : 0;
  // nullptr possible if e.g.'by_column_existence' filter
  const auto docs_with_term = term_ptr ? term_ptr->docs_with_term : 0;
  // TODO(MBkkt) SEARCH-464 IRS_ASSERT(docs_with_field >= docs_with_term);

  auto* idf = stats_cast(stats_buf);
  idf->value += static_cast<float_t>(
    std::log1p((docs_with_field + 1.0) / (docs_with_term + 1.0)));
  // TODO(MBkkt) SEARCH-444 IRS_ASSERT(idf.value >= 0.f);
}

void TFIDF::get_features(std::set<type_info::type_id>& features) const {
  if (normalize()) {
    features.emplace(irs::type<Norm2>::id());
  }
}

ScoreFunction TFIDF::prepare_scorer(const ColumnProvider& segment,
                                    const irs::feature_map_t& features,
                                    const byte_type* stats_buf,
                                    const attribute_provider& doc_attrs,
                                    score_t boost) const {
  auto* freq = irs::get<frequency>(doc_attrs);

  if (!freq) {
    if (!boost_as_score_ || 0.f == boost) {
      return ScoreFunction::Invalid();
    }

    // if there is no frequency then all the
    // scores will be the same (e.g. filter irs::all)
    return ScoreFunction::Constant(boost);
  }

  const auto* stats = stats_cast(stats_buf);
  auto* filter_boost = irs::get<irs::filter_boost>(doc_attrs);

  // add norm attribute if requested
  if (normalize_) {
    auto prepare_norm_scorer =
      [&]<typename Norm>(Norm&& norm) -> ScoreFunction {
      return MakeScoreFunction<TFIDFContext<Norm>>(
        filter_boost, std::move(norm), boost, *stats, freq);
    };

    // Check if norms are present in attributes
    if (auto* norm = irs::get<Norm2>(doc_attrs); norm) {
      return prepare_norm_scorer(MakeTFIDFNormAdapter<NormType::kNorm2>(
        [norm]() noexcept { return norm->value; }));
    }

    // Fallback to reading from columnstore
    auto* doc = irs::get<document>(doc_attrs);

    if (IRS_UNLIKELY(!doc)) {
      // we need 'document' attribute to be exposed
      return ScoreFunction::Invalid();
    }

    if (auto it = features.find(irs::type<Norm2>::id()); it != features.end()) {
      if (Norm2::Context ctx; ctx.Reset(segment, it->second, *doc)) {
        if (ctx.max_num_bytes == sizeof(byte_type)) {
          return Norm2::MakeReader(std::move(ctx), [&](auto&& reader) {
            return prepare_norm_scorer(
              MakeTFIDFNormAdapter<NormType::kNorm2Tiny>(std::move(reader)));
          });
        }

        return Norm2::MakeReader(std::move(ctx), [&](auto&& reader) {
          return prepare_norm_scorer(
            MakeTFIDFNormAdapter<NormType::kNorm2>(std::move(reader)));
        });
      }
    }

    if (auto it = features.find(irs::type<Norm>::id()); it != features.end()) {
      if (Norm::Context ctx; ctx.Reset(segment, it->second, *doc)) {
        return prepare_norm_scorer(MakeTFIDFNormAdapter<NormType::kNorm>(
          Norm::MakeReader(std::move(ctx))));
      }
    }
  }

  return MakeScoreFunction<TFIDFContext<EmptyNorm>>(filter_boost, EmptyNorm{},
                                                    boost, *stats, freq);
}

TermCollector::ptr TFIDF::prepare_term_collector() const {
  return std::make_unique<TermCollectorImpl>();
}

FieldCollector::ptr TFIDF::prepare_field_collector() const {
  return std::make_unique<FieldCollectorImpl<false>>();
}

WandWriter::ptr TFIDF::prepare_wand_writer(size_t max_levels) const {
  return ResolveBool(
    normalize_,
    [&]<bool HasNorms>(
      std::integral_constant<bool, HasNorms>) -> WandWriter::ptr {
      return std::make_unique<WandWriterImpl<WandProducer<HasNorms>>>(
        *this, max_levels);
    });
}

WandSource::ptr TFIDF::prepare_wand_source() const {
  return ResolveBool(normalize_,
                     [&]<bool HasNorms>(std::integral_constant<bool, HasNorms>)
                       -> WandSource::ptr {
                       return std::make_unique<WandAttributes<HasNorms>>();
                     });
}

bool TFIDF::equals(const Scorer& other) const noexcept {
  if (!Scorer::equals(other)) {
    return false;
  }
  const auto& p = down_cast<TFIDF>(other);
  return p.normalize_ == normalize_;
}

void TFIDF::init() {
  REGISTER_SCORER_JSON(TFIDF, make_json);    // match registration above
  REGISTER_SCORER_VPACK(TFIDF, make_vpack);  // match registration above
}

}  // namespace irs
