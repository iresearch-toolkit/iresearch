#include <random>
#include <thread>
#include <vector>

#include "store/memory_directory.hpp"

static constexpr size_t kThreads = 8;
static constexpr size_t kFiles = 1000;
std::uniform_int_distribution<size_t> kFileSizePower{8, 24};

static void WriteFile(std::mt19937_64& rng) {
  irs::memory_file file;
  irs::memory_index_output output{file};
  const auto size = size_t{1} << kFileSizePower(rng);
  for (size_t i = 0; i != size; ++i) {
    output.write_byte(42);
  }
}

int main() {
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (size_t i = 0; i != kThreads; ++i) {
    threads.emplace_back([i] {
      std::mt19937_64 rng(43 * i);
      for (size_t i = 0; i != kFiles; ++i) {
        WriteFile(rng);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}
