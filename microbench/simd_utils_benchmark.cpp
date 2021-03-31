#include <benchmark/benchmark.h>

#include "store/store_utils.hpp"
#include "utils/simd_utils.hpp"

namespace {

void BM_delta_encode(benchmark::State& state) {
  uint32_t values[8192];
  for (auto _ : state) {
    std::iota(std::begin(values), std::end(values), 0);
    irs::encode::delta::encode(std::begin(values), std::end(values));
  }
}

BENCHMARK(BM_delta_encode);

void BM_delta_encode_simd(benchmark::State& state) {
  HWY_ALIGN uint32_t values[8192];
  for (auto _ : state) {
    std::iota(std::begin(values), std::end(values), 0);
    irs::simd::delta_encode<IRESEARCH_COUNTOF(values)>(irs::simd::vu32, values, 0U);
  }
}

BENCHMARK(BM_delta_encode_simd);

void BM_avg_encode(benchmark::State& state) {
  uint32_t values[8192];
  for (auto _ : state) {
    std::iota(std::begin(values), std::end(values), 0);
    irs::encode::avg::encode(std::begin(values), std::end(values));
  }
}

BENCHMARK(BM_avg_encode);

void BM_avg_encode_simd(benchmark::State& state) {
  HWY_ALIGN uint32_t values[8192];
  for (auto _ : state) {
    std::iota(std::begin(values), std::end(values), 0);
    irs::simd::avg_encode32<IRESEARCH_COUNTOF(values)>(values);
  }
}

BENCHMARK(BM_avg_encode_simd);

void BM_zigzag_simd(benchmark::State& state) {
  using namespace hwy::HWY_NAMESPACE;

  int32_t values[1024];
  std::iota(std::begin(values), std::end(values), 0);
  HWY_ALIGN uint32_t encoded[1024];
  HWY_ALIGN int32_t decoded[1024];
  for (auto _ : state) {
    auto* v = values;
    for (auto* e = encoded; e != std::end(encoded);) {
      auto vv = Load(irs::simd::vi32, v);
      auto ve = irs::simd::zig_zag_encode32(vv);
      Store(ve, irs::simd::vu32, e);
      e += MaxLanes(irs::simd::vu32);
      v += MaxLanes(irs::simd::vu32);
    }

    auto* e = encoded;
    for (auto* d = decoded; d != std::end(decoded);) {
      auto ve = Load(irs::simd::vu32, e);
      auto vd = irs::simd::zig_zag_decode32(ve);
      Store(vd, irs::simd::vi32, d);
      e += MaxLanes(irs::simd::vu32);
      d += MaxLanes(irs::simd::vu32);
    }
  }
}

BENCHMARK(BM_zigzag_simd);

}
