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
#include <cstdint>
#include <span>

#include "shared.hpp"

namespace irs {

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
  using Value = typename Context::Value;

 public:
  explicit ExternalHeapIterator(Context&& ctx) : ctx_{std::move(ctx)} {}

  bool Reset(std::span<Value> values) noexcept {
    auto it = std::remove_if(values.begin(), values.end(),
                             [&](auto& value) { return !ctx_(value); });
    values_ = values.subspan(0, static_cast<size_t>(values.end() - it));
    if (values_.empty()) {
      return false;
    }
    std::make_heap(values_.begin(), values_.end(), ctx_);
    return true;
  }

  bool Next() noexcept {
    if (IRS_UNLIKELY(values_.empty())) {
      return false;
    }
    std::pop_heap(values_.begin(), values_.end(), ctx_);
    if (IRS_UNLIKELY(!ctx_(values_.back()))) {
      values_ = values_.subspan(0, values_.size() - 1);
      return !values_.empty();
    }
    std::push_heap(values_.begin(), values_.end(), ctx_);
    return true;
  }

  Value& Lead() const noexcept {
    IRS_ASSERT(!values_.empty());
    return values_.back();
  }

  Value* Data() const noexcept { return values_.data(); }
  size_t Size() const noexcept { return values_.size(); }

 private:
  IRS_NO_UNIQUE_ADDRESS Context ctx_;
  std::span<Value> values_;
};

}  // namespace irs
