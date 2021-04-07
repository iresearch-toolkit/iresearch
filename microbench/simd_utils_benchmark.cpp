#include <benchmark/benchmark.h>

extern "C" {
#include <simdcomputil.h>
}

#include "store/store_utils.hpp"
#include "utils/simd_utils.hpp"

namespace {

constexpr size_t BLOCK_SIZE = 65536;

template<typename T>
std::unique_ptr<T[]> allocate(size_t size) {
  return std::make_unique<T[]>(size);
}

template<typename T>
std::unique_ptr<T, void(*)(T*)> allocate_aligned(size_t size, size_t align) {
  return {
    reinterpret_cast<T*>(aligned_alloc(align, size)),
    [](T* ptr) { free(ptr); }
  };
}

struct HWY_ALIGN aligned_type { };

// -----------------------------------------------------------------------------
// --SECTION--                                                             delta
// -----------------------------------------------------------------------------

/*
void BM_delta_encode(benchmark::State& state) {
  const size_t size = state.range(0);
  auto values = allocate<uint32_t>(size);
  auto begin = values.get();
  auto end = begin + state.range(0);
  for (auto _ : state) {
    std::iota(begin, end, ::rand());
    irs::encode::delta::encode(begin, end);
  }
}

BENCHMARK(BM_delta_encode)->RangeMultiplier(2)->Range(128, 65536);

void BM_delta_encode_simd_aligned(benchmark::State& state) {
  const size_t size = state.range(0);
  auto values = allocate_aligned<uint32_t>(size, alignof(aligned_type));
  auto begin = values.get();
  auto end = begin + state.range(0);
  for (auto _ : state) {
    std::iota(begin, end, ::rand());
    irs::simd::delta_encode<true>(begin, size, 0U);
  }
}

BENCHMARK(BM_delta_encode_simd_aligned)->RangeMultiplier(2)->Range(128, 65536);

void BM_delta_encode_simd_unaligned(benchmark::State& state) {
  uint32_t values[BLOCK_SIZE];
  for (auto _ : state) {
    std::iota(std::begin(values), std::end(values), 0);
    irs::simd::delta_encode<IRESEARCH_COUNTOF(values), false>(values, 0U);
  }
}

BENCHMARK(BM_delta_encode_simd_unaligned)->RangeMultiplier(2)->Range(128, 65536);
*/
// -----------------------------------------------------------------------------
// --SECTION--                                                        avg_encode
// -----------------------------------------------------------------------------

void BM_avg_encode(benchmark::State& state) {
  uint32_t values[BLOCK_SIZE];
  for (auto _ : state) {
    std::iota(std::begin(values), std::end(values), ::rand());
    irs::encode::avg::encode(std::begin(values), std::end(values));
  }
}

BENCHMARK(BM_avg_encode);

void BM_avg_encode_simd_aligned(benchmark::State& state) {
  HWY_ALIGN uint32_t values[BLOCK_SIZE];
  for (auto _ : state) {
    std::iota(std::begin(values), std::end(values), ::rand());
    irs::simd::avg_encode<IRESEARCH_COUNTOF(values), true>(values);
  }
}

BENCHMARK(BM_avg_encode_simd_aligned);

void BM_avg_encode_simd_unaligned(benchmark::State& state) {
  uint32_t values[BLOCK_SIZE];
  for (auto _ : state) {
    std::iota(std::begin(values), std::end(values), ::rand());
    irs::simd::avg_encode<IRESEARCH_COUNTOF(values), false>(values);
  }
}

BENCHMARK(BM_avg_encode_simd_unaligned);

// -----------------------------------------------------------------------------
// --SECTION--                                                          maxmin64
// -----------------------------------------------------------------------------

void BM_maxmin64(benchmark::State& state) {
  uint64_t values[BLOCK_SIZE];
  std::iota(std::begin(values), std::end(values), ::rand());
  for (auto _ : state) {
    auto min = values[0];
    auto max = min;

    for (auto* begin = values + 1; begin != std::end(values); ++begin) {
      min = std::min(min, *begin);
      max = std::max(max, *begin);
    }

    benchmark::DoNotOptimize(min);
    benchmark::DoNotOptimize(max);
  }
}

BENCHMARK(BM_maxmin64);

void BM_maxmin64_simd_aligned(benchmark::State& state) {
  HWY_ALIGN uint64_t values[BLOCK_SIZE];
  std::iota(std::begin(values), std::end(values), ::rand());
  for (auto _ : state) {
    auto maxmin = irs::simd::maxmin<IRESEARCH_COUNTOF(values), true>(values);
    benchmark::DoNotOptimize(maxmin);
  }
}

BENCHMARK(BM_maxmin64_simd_aligned);

void BM_maxmin64_simd_unaligned(benchmark::State& state) {
  uint64_t values[BLOCK_SIZE];
  std::iota(std::begin(values), std::end(values), ::rand());
  for (auto _ : state) {
    auto maxmin = irs::simd::maxmin<IRESEARCH_COUNTOF(values), false>(values);
    benchmark::DoNotOptimize(maxmin);
  }
}

BENCHMARK(BM_maxmin64_simd_unaligned);

// -----------------------------------------------------------------------------
// --SECTION--                                                          maxmin32
// -----------------------------------------------------------------------------

void BM_maxmin32(benchmark::State& state) {
  uint32_t values[BLOCK_SIZE];
  std::iota(std::begin(values), std::end(values), ::rand());
  for (auto _ : state) {
    auto min = values[0];
    auto max = min;

    for (auto* begin = values + 1; begin != std::end(values); ++begin) {
      min = std::min(min, *begin);
      max = std::max(max, *begin);
    }

    benchmark::DoNotOptimize(min);
    benchmark::DoNotOptimize(max);
  }
}

BENCHMARK(BM_maxmin32);

void BM_maxmin32_simd_aligned(benchmark::State& state) {
  HWY_ALIGN uint32_t values[BLOCK_SIZE];
  std::iota(std::begin(values), std::end(values), ::rand());
  for (auto _ : state) {
    auto maxmin = irs::simd::maxmin<IRESEARCH_COUNTOF(values), true>(values);
    benchmark::DoNotOptimize(maxmin);
  }
}

BENCHMARK(BM_maxmin32_simd_aligned);

void BM_maxmin32_simd_unaligned(benchmark::State& state) {
  uint32_t values[BLOCK_SIZE];
  std::iota(std::begin(values), std::end(values), ::rand());
  for (auto _ : state) {
    auto maxmin = irs::simd::maxmin<IRESEARCH_COUNTOF(values), false>(values);
    benchmark::DoNotOptimize(maxmin);
  }
}

BENCHMARK(BM_maxmin32_simd_unaligned);

// -----------------------------------------------------------------------------
// --SECTION--                                                         maxbits64
// -----------------------------------------------------------------------------

void BM_maxbits64(benchmark::State& state) {
  uint64_t values[BLOCK_SIZE];
  std::iota(std::begin(values), std::end(values), ::rand());
  for (auto _ : state) {
    auto bits = irs::packed::bits_required_64(std::begin(values), std::end(values));
    benchmark::DoNotOptimize(bits);
  }
}

BENCHMARK(BM_maxbits64);

void BM_maxbits64_simd_aligned(benchmark::State& state) {
  HWY_ALIGN uint64_t values[BLOCK_SIZE];
  std::iota(std::begin(values), std::end(values), ::rand());
  for (auto _ : state) {
    auto bits = irs::simd::maxbits<IRESEARCH_COUNTOF(values), true>(values);
    benchmark::DoNotOptimize(bits);
  }
}

BENCHMARK(BM_maxbits64_simd_aligned);

void BM_maxbits64_simd_unaligned(benchmark::State& state) {
  uint64_t values[BLOCK_SIZE];
  std::iota(std::begin(values), std::end(values), ::rand());
  for (auto _ : state) {
    auto maxbits = irs::simd::maxbits<IRESEARCH_COUNTOF(values), false>(values);
    benchmark::DoNotOptimize(maxbits);
  }
}

BENCHMARK(BM_maxbits64_simd_unaligned);

// -----------------------------------------------------------------------------
// --SECTION--                                                         maxbits32
// -----------------------------------------------------------------------------

void BM_maxbits32(benchmark::State& state) {
  uint32_t values[BLOCK_SIZE];
  std::iota(std::begin(values), std::end(values), ::rand());

  for (auto _ : state) {
    auto bits = irs::packed::bits_required_32(std::begin(values), std::end(values));
    benchmark::DoNotOptimize(bits);
  }
}

BENCHMARK(BM_maxbits32);

void BM_maxbits32_lemire(benchmark::State& state) {
  uint32_t values[BLOCK_SIZE];
  std::iota(std::begin(values), std::end(values), ::rand());

  for (auto _ : state) {
    uint32_t bits = 0;
    for (auto begin = values; begin != std::end(values); begin += SIMDBlockSize) {
      bits = std::max(maxbits(begin), bits);
    }
    benchmark::DoNotOptimize(bits);
  }
}

BENCHMARK(BM_maxbits32_lemire);

void BM_maxbits32_simd_aligned(benchmark::State& state) {
  HWY_ALIGN uint32_t values[BLOCK_SIZE];
  std::iota(std::begin(values), std::end(values), ::rand());

  for (auto _ : state) {
    auto maxbits = irs::simd::maxbits<IRESEARCH_COUNTOF(values), false>(values);
    benchmark::DoNotOptimize(maxbits);
  }
}

BENCHMARK(BM_maxbits32_simd_aligned);

void BM_maxbits32_simd_unaligned(benchmark::State& state) {
  uint32_t values[BLOCK_SIZE];
  std::iota(std::begin(values), std::end(values), ::rand());

  for (auto _ : state) {
    auto maxbits = irs::simd::maxbits<IRESEARCH_COUNTOF(values), false>(values);
    benchmark::DoNotOptimize(maxbits);
  }
}

BENCHMARK(BM_maxbits32_simd_unaligned);

}
