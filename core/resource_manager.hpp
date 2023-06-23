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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "shared.hpp"
#include "utils/assert.hpp"

namespace irs {
struct IResourceManager {
  static IResourceManager kNoopManager;

  enum Call {
    kFileDescriptors,
    kCachedColumns,
    kTransactions,
    kConsolidations,
    kReaders,
  };

  static_assert(sizeof(size_t) <= sizeof(uint64_t));

  IRS_FORCE_INLINE bool Increase(Call call, size_t v) noexcept {
    IRS_ASSERT(v <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
    switch (call) {
      case kFileDescriptors:
        return ChangeFileDescritors(static_cast<int64_t>(v));
      case kCachedColumns:
        return ChangeCachedColumnsMemory(static_cast<int64_t>(v));
      case kReaders:
        return ChangeIndexPinnedMemory(static_cast<int64_t>(v));
      case kTransactions:
        return ChangeTransactionPinnedMemory(static_cast<int64_t>(v));
      case kConsolidations:
        return ChangeConsolidationPinnedMemory(static_cast<int64_t>(v));
    }
    IRS_ASSERT(false);
    // MSVC issues warning here
    return false;
  }

  IRS_FORCE_INLINE void Decrease(Call call, size_t v) noexcept {
    IRS_ASSERT(v <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
    [[maybe_unused]] bool res;
    switch (call) {
      case kFileDescriptors:
        res = ChangeFileDescritors(-static_cast<int64_t>(v));
        break;
      case kCachedColumns:
        res = ChangeCachedColumnsMemory(-static_cast<int64_t>(v));
        break;
      case kReaders:
        res = ChangeIndexPinnedMemory(-static_cast<int64_t>(v));
        break;
      case kTransactions:
        res = ChangeTransactionPinnedMemory(-static_cast<int64_t>(v));
        break;
      case kConsolidations:
        res = ChangeConsolidationPinnedMemory(-static_cast<int64_t>(v));
        break;
    }
    IRS_ASSERT(res);
  }

 protected:
  virtual bool ChangeFileDescritors(int64_t) noexcept { return true; }

  virtual bool ChangeCachedColumnsMemory(int64_t) noexcept { return true; }

  // memory allocated by Inserts/Removes/Replaces:
  // Buffered column values, docs contexts, pending removals
  // fst buffer for inserted data, commits
  virtual bool ChangeTransactionPinnedMemory(int64_t) noexcept { return true; }

  // memory allocated by index readers, 
  virtual bool ChangeIndexPinnedMemory(int64_t) noexcept { return true; }

  // consolidation (including fst buffer for consolidated segments)
  virtual bool ChangeConsolidationPinnedMemory(int64_t) noexcept { return true; }
};
}  // namespace irs
