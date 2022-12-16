////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>
#include <benchmark/benchmark.h>

#include <numeric>

namespace {

template<typename T>
void fill(T& map, size_t size, int seed) {
  ::srand(seed);
  while (size) {
    const auto kk = rand();
    map.emplace(kk, kk);
    --size;
  }
}

/// @brief type for register numbers/ids

void BM_hash_std(benchmark::State& state) {
  int seed = 41;

  std::unordered_map<uint32_t, uint64_t> map;
  fill(map, state.range(0), seed);

  for (auto _ : state) {
    for (int i = 0; i < 100; ++i) {
      srand(seed);
      for (int64_t i = 0; i < state.range(0); ++i) {
        auto it = map.find(rand());
        if (it == map.end()) {
          std::abort();
        }
        benchmark::DoNotOptimize(it);
      }
    }
  }
}

void BM_hash_absl_flat(benchmark::State& state) {
  int seed = 41;

  absl::flat_hash_map<uint32_t, uint64_t> map;
  fill(map, state.range(0), seed);

  for (auto _ : state) {
    for (int i = 0; i < 100; ++i) {
      srand(seed);
      for (int64_t i = 0; i < state.range(0); ++i) {
        auto it = map.find(rand());
        if (it == map.end()) {
          std::abort();
        }
        benchmark::DoNotOptimize(it);
      }
    }
  }
}

void BM_hash_absl_node(benchmark::State& state) {
  int seed = 41;

  absl::node_hash_map<uint32_t, uint64_t> map;
  fill(map, state.range(0), seed);

  for (auto _ : state) {
    for (int i = 0; i < 100; ++i) {
      srand(seed);
      for (int64_t i = 0; i < state.range(0); ++i) {
        auto it = map.find(rand());
        if (it == map.end()) {
          std::abort();
        }
        benchmark::DoNotOptimize(it);
      }
    }
  }
}

BENCHMARK(BM_hash_absl_flat)->RangeMultiplier(2)->Range(0, 100);
BENCHMARK(BM_hash_absl_node)->RangeMultiplier(2)->Range(0, 100);
BENCHMARK(BM_hash_std)->RangeMultiplier(2)->Range(0, 100);

void BM_lower_bound(benchmark::State& state) {
  std::vector<uint32_t> nums(state.range(0));
  std::iota(nums.begin(), nums.end(), 42);

  for (auto _ : state) {
    for (auto v : nums) {
      const auto it = std::upper_bound(nums.begin(), nums.end(), v);
      benchmark::DoNotOptimize(it);
    }
  }
}

BENCHMARK(BM_lower_bound)->DenseRange(0, 256, 8);

void BM_linear_scan(benchmark::State& state) {
  std::vector<uint32_t> nums(state.range(0));
  std::iota(nums.begin(), nums.end(), 42);

  for (auto _ : state) {
    for (auto v : nums) {
      const auto it = std::find(nums.begin(), nums.end(), v);
      benchmark::DoNotOptimize(it);
    }
  }
}

BENCHMARK(BM_linear_scan)->DenseRange(0, 256, 8);

}  // namespace
