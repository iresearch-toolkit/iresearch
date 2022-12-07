////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#pragma once

#include <algorithm>
#include <vector>

#include "iterators.hpp"
#include "shared.hpp"

namespace iresearch {

// External heap iterator
// ----------------------------------------------------------------------------
//      [0] <-- begin
//      [1]      |
//      [2]      | head (heap)
//      [3]      |
//      [4] <-- lead_
//      [5]      |
//      [6]      | lead (list of accepted iterators)
//      ...      |
//      [n] <-- end
// ----------------------------------------------------------------------------
template<typename Context>
class ExternalHeapIterator {
 public:
  explicit ExternalHeapIterator(Context&& ctx) : ctx_{std::move(ctx)} {}

  void reset(size_t size) {
    heap_.resize(size);
    std::iota(heap_.begin(), heap_.end(), size_t{0});
    lead_ = size;
  }

  bool next() {
    if (heap_.empty()) {
      return false;
    }

    auto begin = std::begin(heap_);

    while (lead_) {
      auto it = std::end(heap_) - lead_--;

      if (!ctx_(*it)) {  // advance iterator
        if (!remove_lead(it)) {
          IRS_ASSERT(heap_.empty());
          return false;
        }

        continue;
      }

      std::push_heap(begin, ++it, ctx_);
    }

    IRS_ASSERT(!heap_.empty());
    std::pop_heap(begin, std::end(heap_), ctx_);
    lead_ = 1;

    return true;
  }

  size_t value() const noexcept {
    IRS_ASSERT(!heap_.empty());
    return heap_.back();
  }

  size_t size() const noexcept { return heap_.size(); }

 private:
  bool remove_lead(std::vector<size_t>::iterator it) {
    if (&*it != &heap_.back()) {
      std::swap(*it, heap_.back());
    }
    heap_.pop_back();
    return !heap_.empty();
  }

  IRS_NO_UNIQUE_ADDRESS Context ctx_;
  std::vector<size_t> heap_;
  size_t lead_{};
};

}  // namespace iresearch
