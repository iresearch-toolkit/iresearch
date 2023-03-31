#include <cstdint>
#include <iterator>
#include <type_traits>

#include "analysis/token_attributes.hpp"
#include "error/error.hpp"
#include "search/scorer.hpp"
#include "utils/string.hpp"

namespace irs {

struct ByteRefIterator final {
  using iterator_category = std::input_iterator_tag;
  using value_type = byte_type;
  using pointer = value_type*;
  using reference = value_type&;
  using difference_type = void;

  const byte_type* end_;
  const byte_type* pos_;

  explicit ByteRefIterator(bytes_view in) noexcept
    : end_{in.data() + in.size()}, pos_{in.data()} {}

  byte_type operator*() {
    if (pos_ >= end_) {
      throw io_error{"invalid read past end of input"};
    }

    return *pos_;
  }

  void operator++() noexcept { ++pos_; }
};

struct TermCollectorImpl final : TermCollector {
  // number of documents containing the matched term
  uint64_t docs_with_term = 0;

  void collect(const SubReader& /*segment*/, const term_reader& /*field*/,
               const attribute_provider& term_attrs) final {
    auto* meta = get<term_meta>(term_attrs);

    if (meta) {
      docs_with_term += meta->docs_count;
    }
  }

  void reset() noexcept final { docs_with_term = 0; }

  void collect(bytes_view in) final {
    ByteRefIterator itr{in};
    auto docs_with_term_value = vread<uint64_t>(itr);

    if (itr.pos_ != itr.end_) {
      throw io_error{"input not read fully"};
    }

    docs_with_term += docs_with_term_value;
  }

  void write(data_output& out) const final { out.write_vlong(docs_with_term); }
};

struct Empty final {};

template<bool Freq>
struct FieldCollectorImpl final : FieldCollector {
  // number of documents containing the matched field
  // (possibly without matching terms)
  uint64_t docs_with_field{};
  // number of terms for processed field
  IRS_NO_UNIQUE_ADDRESS std::conditional_t<Freq, uint64_t, Empty>
    total_term_freq{};

  void collect(const SubReader& /*segment*/,
               const term_reader& field) noexcept final {
    this->docs_with_field += field.docs_count();
    if constexpr (Freq) {
      if (auto* freq = get<frequency>(field); freq != nullptr) {
        this->total_term_freq += freq->value;
      }
    }
  }

  void reset() noexcept final {
    this->docs_with_field = 0;
    if constexpr (Freq) {
      this->total_term_freq = 0;
    }
  }

  void collect(bytes_view in) final {
    ByteRefIterator itr{in};
    const auto docs_with_field_value = vread<uint64_t>(itr);
    if constexpr (Freq) {
      const auto total_term_freq_value = vread<uint64_t>(itr);
      if (itr.pos_ != itr.end_) {
        throw io_error{"input not read fully"};
      }
      this->docs_with_field += docs_with_field_value;
      this->total_term_freq += total_term_freq_value;
    } else {
      if (itr.pos_ != itr.end_) {
        throw io_error{"input not read fully"};
      }
      this->docs_with_field += docs_with_field_value;
    }
  }

  void write(data_output& out) const final {
    out.write_vlong(this->docs_with_field);
    if constexpr (Freq) {
      out.write_vlong(this->total_term_freq);
    }
  }
};

inline constexpr frequency kEmptyFreq;

template<typename Ctx>
struct MakeScoreFunctionImpl {
  template<bool HasFilterBoost, typename... Args>
  static ScoreFunction Make(Args&&... args);
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

enum class NormType {
  // Norm2 values
  kNorm2 = 0,
  // Norm2 values fit 1-byte
  kNorm2Tiny,
  // Old norms, 1/sqrt(|doc|)
  kNorm
};

}  // namespace irs
