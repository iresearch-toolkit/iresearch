#include <absl/crc/crc32c.h>
#include <benchmark/benchmark.h>

#include <string>

#include "utils/crc.hpp"

namespace {

std::string TestString(size_t len) {
  std::string result;
  result.reserve(len);
  for (size_t i = 0; i < len; ++i) {
    result.push_back(static_cast<char>(i % 256));
  }
  return result;
}

void BM_Calculate(benchmark::State& state) {
  int len = state.range(0);
  std::string data = TestString(len);
  for (auto s : state) {
    benchmark::DoNotOptimize(data);
    absl::crc32c_t crc = absl::ComputeCrc32c(data);
    benchmark::DoNotOptimize(crc);
  }
}

BENCHMARK(BM_Calculate)->Arg(0)->Arg(1)->Arg(100)->Arg(10000)->Arg(500000);

void BM_Calculate2(benchmark::State& state) {
  int len = state.range(0);
  std::string data = TestString(len);
  for (auto s : state) {
    benchmark::DoNotOptimize(data);
    iresearch::crc32c crc;
    crc.process_bytes(data.data(), data.size());
    benchmark::DoNotOptimize(crc.checksum());
  }
}

BENCHMARK(BM_Calculate2)->Arg(0)->Arg(1)->Arg(100)->Arg(10000)->Arg(500000);

void BM_Extend(benchmark::State& state) {
  int len = state.range(0);
  std::string extension = TestString(len);
  absl::crc32c_t base{0xC99465AA};  // CRC32C of "Hello World"
  for (auto s : state) {
    benchmark::DoNotOptimize(base);
    benchmark::DoNotOptimize(extension);
    absl::crc32c_t crc = absl::ExtendCrc32c(base, extension);
    benchmark::DoNotOptimize(crc);
  }
}

void BM_Extend2(benchmark::State& state) {
  int len = state.range(0);
  std::string extension = TestString(len);
  iresearch::crc32c base{0xC99465AA};  // CRC32C of "Hello World"
  for (auto s : state) {
    benchmark::DoNotOptimize(base);
    benchmark::DoNotOptimize(extension);
    base.process_bytes(extension.data(), extension.size());
    benchmark::DoNotOptimize(base.checksum());
  }
}

BENCHMARK(BM_Extend)->Arg(0)->Arg(1)->Arg(100)->Arg(10000)->Arg(500000);
BENCHMARK(BM_Extend2)->Arg(0)->Arg(1)->Arg(100)->Arg(10000)->Arg(500000);

}  // namespace
