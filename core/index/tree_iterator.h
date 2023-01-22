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

#pragma once

#include <algorithm>
#include <vector>

#include "iterators.hpp"
#include "shared.hpp"

namespace irs {
// Note, costs:
// sift_up: log(N) comparison
// sift_down: 2*log(N) comparison
// tree update: log(N) comparison

// Same as ExternalHeapIterator code, but probably better, because
// use make_heap (N/2 * sift_down) instead of N * push_heap
template<typename Context>
class ExternalHeapIteratorMake final {
 public:
  explicit ExternalHeapIteratorMake(Context&& ctx) : ctx_{std::move(ctx)} {}

  void reset(size_t size) {
    size_ = size;
    heap_.clear();
  }

  bool next() {
    if (IRS_UNLIKELY(heap_.empty())) {
      return lazy_reset();
    }
    std::pop_heap(heap_.begin(), heap_.end(), ctx_);
    if (IRS_UNLIKELY(!ctx_(heap_.back()))) {
      heap_.pop_back();
      return !heap_.empty();
    }
    std::push_heap(heap_.begin(), heap_.end(), ctx_);
    return true;
  }

  size_t value() const noexcept {
    IRS_ASSERT(!heap_.empty());
    return heap_.front();
  }

  size_t size() const noexcept { return heap_.size(); }

 protected:
  bool lazy_reset() {
    if (IRS_LIKELY(size_ == 0)) {
      return false;
    }
    heap_.resize(size_);
    std::iota(heap_.begin(), heap_.end(), size_t{0});
    // remove all useless iterators
    auto it = std::remove_if(heap_.begin(), heap_.end(), [&](auto index) {
      return !ctx_(index);  // first advance
    });
    // TODO(MBkkt) heap_.resize(it - heap_.begin())?
    heap_.erase(it, heap_.end());
    std::make_heap(heap_.begin(), heap_.end(), ctx_);
    size_ = 0;
    return !heap_.empty();
  }

  IRS_NO_UNIQUE_ADDRESS Context ctx_;
  std::vector<size_t> heap_;
  // TODO(MBkkt) not suited interface, better suite "do while" approach:
  //   if (!it.reset(...)) return;
  //   do { it.value(); } while (it.next());
  //  I think something like can make all iterators better
  size_t size_{0};
};

// Same as ExternalHeapIteratorMake, but probably better, because
// use raw sift_down instead of pop_heap + push_heap
template<typename Context>
class ExternalHeapIteratorSift final {
 public:
  explicit ExternalHeapIteratorSift(Context&& ctx) : ctx_{std::move(ctx)} {}

  void reset(size_t size) {
    size_ = size;
    heap_.clear();
  }

  bool next() {
    if (IRS_UNLIKELY(heap_.empty())) {
      return lazy_reset();
    }
    if (IRS_UNLIKELY(!ctx_(heap_.front()))) {
      std::swap(heap_.front(), heap_.back());
      heap_.pop_back();
    }
    const ptrdiff_t len = heap_.size();
    SiftDown(heap_.begin(), len, heap_.begin());
    return len != 0;
  }

  size_t value() const noexcept {
    IRS_ASSERT(!heap_.empty());
    return heap_.front();
  }

  size_t size() const noexcept { return heap_.size(); }

 protected:
  bool lazy_reset() {
    if (IRS_LIKELY(size_ == 0)) {
      return false;
    }
    heap_.resize(size_);
    std::iota(heap_.begin(), heap_.end(), size_t{0});
    // remove all useless iterators
    auto it = std::remove_if(heap_.begin(), heap_.end(), [&](auto index) {
      return !ctx_(index);  // first advance
    });
    // TODO heap_.resize(it - heap_.begin())?
    heap_.erase(it, heap_.end());
    MakeHeap(heap_.begin(), heap_.end());
    size_ = 0;
    return !heap_.empty();
  }

  // Copied from libcxx under Apache 2.0 license
  // https://github.com/llvm/llvm-project/blob/main/libcxx/include/__algorithm/sift_down.h
  using It = std::vector<size_t>::iterator;

  void MakeHeap(It first, It last) {
    const ptrdiff_t n = last - first;
    if (n > 1) {
      // start from the first parent, there is no need to consider children
      for (ptrdiff_t start = (n - 2) / 2; start >= 0; --start) {
        SiftDown(first, n, first + start);
      }
    }
  }

  void SiftDown(It first, ptrdiff_t len, It start) {
    // left-child of __start is at 2 * __start + 1
    // right-child of __start is at 2 * __start + 2
    ptrdiff_t child = start - first;
    if (len < 2 || (len - 2) / 2 < child) {
      return;
    }

    child = 2 * child + 1;
    auto child_i = first + child;

    if ((child + 1) < len && ctx_(*child_i, *(child_i + 1))) {
      // right-child exists and is greater than left-child
      ++child_i;
      ++child;
    }

    // check if we are in heap-order
    if (ctx_(*child_i, *start)) {
      // we are, __start is larger than its largest child
      return;
    }
    auto top = std::move(*start);
    do {
      // we are not in heap-order, swap the parent with its largest child
      *start = std::move(*child_i);
      start = child_i;

      if ((len - 2) / 2 < child) {
        break;
      }

      // recompute the child based off of the updated parent
      child = 2 * child + 1;
      child_i = first + child;

      if ((child + 1) < len && ctx_(*child_i, *(child_i + 1))) {
        // right-child exists and is greater than left-child
        ++child_i;
        ++child;
      }

      // check if we are in heap-order
    } while (!ctx_(*child_i, top));
    *start = std::move(top);
  }

  IRS_NO_UNIQUE_ADDRESS Context ctx_;
  std::vector<size_t> heap_;
  // TODO(MBkkt) not suited interface, better suite "do while" approach:
  //   if (!it.reset(...)) return;
  //   do { it.value(); } while (it.next());
  //  I think something like can make all iterators better
  size_t size_{0};
};

template<typename Context>
class ExternalTreeIterator final {
 public:
  explicit ExternalTreeIterator(Context&& ctx) : ctx_{std::move(ctx)} {}

  void reset(size_t size) {
    size_ = size;
    curr_ = 0;
    tree_.clear();
  }

  bool next() {
    if (IRS_UNLIKELY(curr_ == 0)) {
      return lazy_reset();
    }
    auto position = size_ + tree_[1];
    if (IRS_UNLIKELY(!ctx_(tree_[1]))) {
      tree_[position] = kInvalid;
      --curr_;
    }
    position /= 2;
    while (position != 0) {
      compute(position);
      position /= 2;
    }
    return curr_ != 0;
  }

  size_t value() const noexcept {
    IRS_ASSERT(curr_ != 0);
    IRS_ASSERT(tree_[1] != kInvalid);
    return tree_[1];
  }

  size_t size() const noexcept { return curr_; }

 private:
  static constexpr auto kInvalid = std::numeric_limits<size_t>::max();

  bool lazy_reset() {
    if (IRS_LIKELY(!tree_.empty() || size_ == 0)) {
      return false;
    }
    curr_ = size_;
    // Bottom-up max segment tree construction
    tree_.resize(size_ * 2);
    // stub node
    tree_[0] = kInvalid;
    auto fill = [&](size_t index) {
      if (ctx_(index)) {
        return index;
      }
      --curr_;
      return kInvalid;
    };
    // fill leaf
    for (size_t i = 0; i != size_; ++i) {
      tree_[i + size_] = fill(i);
    }
    // fill tree
    for (ptrdiff_t i = size_ - 1; i > 0; --i) {
      compute(i);
    }
    return curr_ != 0;
  }

  void compute(size_t i) {
    const auto lhs = tree_[i * 2];
    const auto rhs = tree_[i * 2 + 1];
    if (rhs == kInvalid) {
      tree_[i] = lhs;
    } else if (lhs == kInvalid) {
      tree_[i] = rhs;
    } else {
      tree_[i] = ctx_(lhs, rhs) ? lhs : rhs;
    }
  }

  IRS_NO_UNIQUE_ADDRESS Context ctx_;
  std::vector<size_t> tree_;
  // TODO(MBkkt) not suited interface, better suite "do while" approach:
  //   if (!it.reset(...)) return;
  //   do { it.value(); } while (it.next());
  //  I think something like this can make all iterators better
  size_t size_{0};
  size_t curr_{0};
};

}  // namespace irs
