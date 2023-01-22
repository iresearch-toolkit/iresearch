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
/// @author Valery Mironov
////////////////////////////////////////////////////////////////////////////////
#include <benchmark/benchmark.h>

#include "index/heap_iterator.hpp"
#include "index/tree_iterator.h"

namespace {

template<typename T>
class MinHeapContext {
 public:
  // advance
  bool operator()(const size_t i) const { return false; }

  // compare
  bool operator()(const size_t lhs, const size_t rhs) const { return false; }
};

void TestIntHeapOld(benchmark::State& state) {
  irs::ExternalHeapIterator<MinHeapContext<int>> it{{}};
}

void TestIntHeapMake(benchmark::State& state) {
  irs::ExternalHeapIteratorMake<MinHeapContext<int>> it{{}};
}

void TestIntHeapShift(benchmark::State& state) {
  irs::ExternalHeapIteratorSift<MinHeapContext<int>> it{{}};
}

void TestIntTree(benchmark::State& state) {
  irs::ExternalTreeIterator<MinHeapContext<int>> it{{}};
}

BENCHMARK(TestIntHeapOld);
BENCHMARK(TestIntHeapMake);
BENCHMARK(TestIntHeapShift);
BENCHMARK(TestIntTree);

void TestDoubleHeapOld(benchmark::State& state) {
  irs::ExternalHeapIterator<MinHeapContext<int>> it{{}};
}

void TestDoubleHeapMake(benchmark::State& state) {
  irs::ExternalHeapIteratorMake<MinHeapContext<int>> it{{}};
}

void TestDoubleHeapShift(benchmark::State& state) {
  irs::ExternalHeapIteratorSift<MinHeapContext<int>> it{{}};
}

void TestDoubleTree(benchmark::State& state) {
  irs::ExternalTreeIterator<MinHeapContext<int>> it{{}};
}

BENCHMARK(TestDoubleHeapOld);
BENCHMARK(TestDoubleHeapMake);
BENCHMARK(TestDoubleHeapShift);
BENCHMARK(TestDoubleTree);

void TestShortStringHeapOld(benchmark::State& state) {
  irs::ExternalHeapIterator<MinHeapContext<int>> it{{}};
}

void TestShortStringHeapMake(benchmark::State& state) {
  irs::ExternalHeapIteratorMake<MinHeapContext<int>> it{{}};
}

void TestShortStringHeapShift(benchmark::State& state) {
  irs::ExternalHeapIteratorSift<MinHeapContext<int>> it{{}};
}

void TestShortStringTree(benchmark::State& state) {
  irs::ExternalTreeIterator<MinHeapContext<int>> it{{}};
}

BENCHMARK(TestShortStringHeapOld);
BENCHMARK(TestShortStringHeapMake);
BENCHMARK(TestShortStringHeapShift);
BENCHMARK(TestShortStringTree);

void TestLongStringHeapOld(benchmark::State& state) {
  irs::ExternalHeapIterator<MinHeapContext<int>> it{{}};
}

void TestLongStringHeapMake(benchmark::State& state) {
  irs::ExternalHeapIteratorMake<MinHeapContext<int>> it{{}};
}

void TestLongStringHeapShift(benchmark::State& state) {
  irs::ExternalHeapIteratorSift<MinHeapContext<int>> it{{}};
}

void TestLongStringTree(benchmark::State& state) {
  irs::ExternalTreeIterator<MinHeapContext<int>> it{{}};
}

BENCHMARK(TestLongStringHeapOld);
BENCHMARK(TestLongStringHeapMake);
BENCHMARK(TestLongStringHeapShift);
BENCHMARK(TestLongStringTree);

}  // namespace
