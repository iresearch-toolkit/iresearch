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

#include <cmath>
#include <string_view>

#include "analysis/token_attributes.hpp"
#include "index/field_meta.hpp"
#include "index/index_reader.hpp"
#include "index/norm.hpp"
#include "scorers.hpp"
#include "utils/math_utils.hpp"
#include "utils/misc.hpp"
#include "velocypack/Builder.h"
#include "velocypack/Parser.h"
#include "velocypack/Slice.h"
#include "velocypack/velocypack-aliases.h"
#include "velocypack/vpack.h"

namespace {

const auto kSQRT = irs::cache_func<uint32_t, 2048>(
  0, [](uint32_t i) noexcept { return std::sqrt(static_cast<float_t>(i)); });

const auto kRSQRT = irs::cache_func<uint32_t, 2048>(1, [](uint32_t i) noexcept {
  return 1.f / std::sqrt(static_cast<float_t>(i));
});

irs::sort::ptr make_from_bool(const VPackSlice slice) {
  IRS_ASSERT(slice.isBool());

  return std::make_unique<irs::tfidf_sort>(slice.getBool());
}

constexpr std::string_view WITH_NORMS_PARAM_NAME("withNorms");

irs::sort::ptr make_from_object(const VPackSlice slice) {
  IRS_ASSERT(slice.isObject());

  auto scorer = std::make_unique<irs::tfidf_sort>();

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

      scorer->normalize(slice.get(WITH_NORMS_PARAM_NAME).getBool());
    }
  }

  return scorer;
}

irs::sort::ptr make_from_array(const VPackSlice slice) {
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
  auto norms = irs::tfidf_sort::WITH_NORMS();

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

  return std::make_unique<irs::tfidf_sort>(norms);
}

irs::sort::ptr make_vpack(const VPackSlice slice) {
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

irs::sort::ptr make_vpack(std::string_view args) {
  if (irs::IsNull(args)) {
    // default args
    return std::make_unique<irs::tfidf_sort>();
  } else {
    VPackSlice slice(reinterpret_cast<const uint8_t*>(args.data()));
    return make_vpack(slice);
  }
}

irs::sort::ptr make_json(std::string_view args) {
  if (irs::IsNull(args)) {
    // default args
    return std::make_unique<irs::tfidf_sort>();
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

REGISTER_SCORER_JSON(irs::tfidf_sort, make_json);
REGISTER_SCORER_VPACK(irs::tfidf_sort, make_vpack);

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

struct field_collector final : public irs::sort::field_collector {
  // number of documents containing the matched
  // field (possibly without matching terms)
  uint64_t docs_with_field = 0;

  void collect(const irs::SubReader& /*segment*/,
               const irs::term_reader& field) override {
    docs_with_field += field.docs_count();
  }

  void reset() noexcept override { docs_with_field = 0; }

  void collect(irs::bytes_view in) override {
    byte_ref_iterator itr(in);
    auto docs_with_field_value = irs::vread<uint64_t>(itr);

    if (itr.pos_ != itr.end_) {
      throw irs::io_error("input not read fully");
    }

    docs_with_field += docs_with_field_value;
  }

  void write(irs::data_output& out) const override {
    out.write_vlong(docs_with_field);
  }
};

struct term_collector final : public irs::sort::term_collector {
  // number of documents containing the matched term
  uint64_t docs_with_term = 0;

  void collect(const irs::SubReader& /*segment*/,
               const irs::term_reader& /*field*/,
               const irs::attribute_provider& term_attrs) override {
    auto* meta = irs::get<irs::term_meta>(term_attrs);

    if (meta) {
      docs_with_term += meta->docs_count;
    }
  }

  void reset() noexcept override { docs_with_term = 0; }

  void collect(irs::bytes_view in) override {
    byte_ref_iterator itr(in);
    auto docs_with_term_value = irs::vread<uint64_t>(itr);

    if (itr.pos_ != itr.end_) {
      throw irs::io_error("input not read fully");
    }

    docs_with_term += docs_with_term_value;
  }

  void write(irs::data_output& out) const override {
    out.write_vlong(docs_with_term);
  }
};

IRS_FORCE_INLINE float_t tfidf(uint32_t freq, float_t idf) noexcept {
  return idf * kSQRT.get<true>(freq);
}

}  // namespace

namespace irs {
namespace tfidf {

// empty frequency
const frequency kEmptyFreq;

struct idf final {
  float_t value;
};

struct ScoreContext : public irs::score_ctx {
  ScoreContext(irs::score_t boost, const tfidf::idf& idf, const frequency* freq,
               const irs::filter_boost* filter_boost = nullptr) noexcept
    : freq{freq ? *freq : kEmptyFreq},
      filter_boost{filter_boost},
      idf{boost * idf.value} {
    IRS_ASSERT(freq);
  }

  ScoreContext(const ScoreContext&) = delete;
  ScoreContext& operator=(const ScoreContext&) = delete;

  const frequency& freq;
  const irs::filter_boost* filter_boost;
  float_t idf;  // precomputed : boost * idf
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
struct NormAdapter {
  static constexpr auto kType = Type;

  explicit NormAdapter(Reader&& reader) : reader{std::move(reader)} {}

  IRS_FORCE_INLINE float_t operator()() {
    if constexpr (kType < NormType::kNorm) {
      return kRSQRT.get<kType != NormType::kNorm2Tiny>(reader());
    } else {
      return reader();
    }
  }

  Reader reader;
};

template<NormType Type, typename Reader>
auto MakeNormAdapter(Reader&& reader) {
  return NormAdapter<Reader, Type>(std::move(reader));
}

template<typename Norm>
struct NormScoreContext final : public ScoreContext {
  NormScoreContext(Norm&& norm, score_t boost, const tfidf::idf& idf,
                   const frequency* freq,
                   const irs::filter_boost* filter_boost = nullptr) noexcept
    : ScoreContext{boost, idf, freq, filter_boost}, norm{std::move(norm)} {}

  Norm norm;
};

template<typename Ctx>
struct MakeScoreFunctionImpl {
  template<bool HasFilterBoost, typename... Args>
  static ScoreFunction Make(Args&&... args) {
    return {std::make_unique<Ctx>(std::forward<Args>(args)...),
            [](irs::score_ctx* ctx, irs::score_t* res) noexcept {
              IRS_ASSERT(res);
              IRS_ASSERT(ctx);

              auto& state = *static_cast<Ctx*>(ctx);

              float_t idf;
              if constexpr (HasFilterBoost) {
                IRS_ASSERT(state.filter_boost);
                idf = state.idf * state.filter_boost->value;
              } else {
                idf = state.idf;
              }

              if constexpr (std::is_same_v<Ctx, ScoreContext>) {
                *res = ::tfidf(state.freq.value, idf);
              } else {
                *res = ::tfidf(state.freq.value, idf) * state.norm();
              }
            }};
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

class sort final : public irs::PreparedSortBase<tfidf::idf> {
 public:
  explicit sort(bool normalize, bool boost_as_score) noexcept
    : normalize_{normalize}, boost_as_score_{boost_as_score} {}

  void collect(byte_type* stats_buf, const irs::IndexReader& /*index*/,
               const irs::sort::field_collector* field,
               const irs::sort::term_collector* term) const override {
    auto& idf = stats_cast(stats_buf);

    const auto* field_ptr = down_cast<field_collector>(field);
    const auto* term_ptr = down_cast<term_collector>(term);

    // nullptr possible if e.g. 'all' filter
    const auto docs_with_field = field_ptr ? field_ptr->docs_with_field : 0;
    // nullptr possible if e.g.'by_column_existence' filter
    const auto docs_with_term = term_ptr ? term_ptr->docs_with_term : 0;

    idf.value += float_t(
      std::log((docs_with_field + 1) / double_t(docs_with_term + 1)) + 1.0);
    // TODO(MBkkt) SEARCH-444 IRS_ASSERT(idf.value >= 0.f);
  }

  IndexFeatures features() const noexcept override {
    return IndexFeatures::FREQ;
  }

  field_collector::ptr prepare_field_collector() const override {
    return std::make_unique<field_collector>();
  }

  virtual ScoreFunction prepare_scorer(const SubReader& segment,
                                       const term_reader& field,
                                       const byte_type* stats_buf,
                                       const attribute_provider& doc_attrs,
                                       score_t boost) const override {
    auto* freq = irs::get<frequency>(doc_attrs);

    if (!freq) {
      if (!boost_as_score_ || 0.f == boost) {
        return ScoreFunction::Invalid();
      }

      // if there is no frequency then all the
      // scores will be the same (e.g. filter irs::all)
      return ScoreFunction::Constant(boost);
    }

    auto& stats = stats_cast(stats_buf);
    auto* filter_boost = irs::get<irs::filter_boost>(doc_attrs);

    // add norm attribute if requested
    if (normalize_) {
      auto* doc = irs::get<document>(doc_attrs);

      if (IRS_UNLIKELY(!doc)) {
        // we need 'document' attribute to be exposed
        return ScoreFunction::Invalid();
      }

      auto prepare_norm_scorer =
        [&]<typename Norm>(Norm&& norm) -> ScoreFunction {
        return MakeScoreFunction<NormScoreContext<Norm>>(
          filter_boost, std::move(norm), boost, stats, freq);
      };

      const auto& features = field.meta().features;

      if (auto it = features.find(irs::type<Norm2>::id());
          it != features.end()) {
        if (Norm2::Context ctx; ctx.Reset(segment, it->second, *doc)) {
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
        if (Norm::Context ctx; ctx.Reset(segment, it->second, *doc)) {
          return prepare_norm_scorer(
            MakeNormAdapter<NormType::kNorm>(Norm::MakeReader(std::move(ctx))));
        }
      }
    }

    return MakeScoreFunction<ScoreContext>(filter_boost, boost, stats, freq);
  }

  term_collector::ptr prepare_term_collector() const override {
    return std::make_unique<term_collector>();
  }

 private:
  bool normalize_;
  bool boost_as_score_;
};  // sort

}  // namespace tfidf

tfidf_sort::tfidf_sort(bool normalize, bool boost_as_score) noexcept
  : sort{irs::type<tfidf_sort>::get()},
    normalize_{normalize},
    boost_as_score_{boost_as_score} {}

/*static*/ void tfidf_sort::init() {
  REGISTER_SCORER_JSON(tfidf_sort, make_json);    // match registration above
  REGISTER_SCORER_VPACK(tfidf_sort, make_vpack);  // match registration above
}

sort::prepared::ptr tfidf_sort::prepare() const {
  return std::make_unique<tfidf::sort>(normalize_, boost_as_score_);
}

}  // namespace irs
