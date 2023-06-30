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
#include "utils/misc.hpp"
#include <memory>


#if (defined(__clang__) || defined(_MSC_VER) || \
     defined(__GNUC__) &&                       \
       (__GNUC__ > 8))  // GCCs <=  don't have "<version>" header
#include <version>
#endif

#ifdef __cpp_lib_memory_resource
#include <memory_resource>
#endif

namespace irs {
struct IResourceManager {
  static IResourceManager kNoopManager;

  enum Call {
    kNoop,
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
      case kNoop:
        IRS_ASSERT(false);
        return true;
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
      case kNoop:
        res = true;
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
  virtual bool ChangeConsolidationPinnedMemory(int64_t) noexcept {
    return true;
  }
};

template<typename Allocator>
class ManagedAllocator : private Allocator {
 public:
  using difference_type = typename std::allocator_traits<Allocator>::difference_type;
  using propagate_on_container_move_assignment = typename std::allocator_traits<
    Allocator>::propagate_on_container_move_assignment;
  using size_type = typename std::allocator_traits<Allocator>::size_type;
  using value_type = typename std::allocator_traits<Allocator>::value_type;

  explicit ManagedAllocator()
    : rm_{IResourceManager::kNoopManager}, call_{IResourceManager::kNoop} {}

  template<typename... Args>
  ManagedAllocator(IResourceManager& rm,
                   IResourceManager::Call call,
                   Args&&... args)
    : Allocator(std::forward<Args>(args)...), rm_{rm}, call_{call} {}

  ManagedAllocator(ManagedAllocator&& other) noexcept
    : Allocator(other.RawAllocator()),
    rm_{other.ResourceManager()}, call_{other.ResourceCall()} {}

   ManagedAllocator(const ManagedAllocator& other) noexcept
    : Allocator(other.RawAllocator()),
      rm_{other.ResourceManager()},
      call_{other.ResourceCall()} {}

  ManagedAllocator& operator=(ManagedAllocator&& other) noexcept {
    static_cast<Allocator&>(*this) = std::move(static_cast<Allocator&>(other));
    rm_ = other.ResourceManager();
    call_ = other.ResourceCall();
    return *this;
  }

  template<typename A>
  ManagedAllocator(const ManagedAllocator<A>& other) noexcept
    : Allocator(other.RawAllocator()),
      rm_{other.ResourceManager()},
      call_{other.ResourceCall()} {}

  value_type* allocate(std::size_t n) {
    rm_.Increase(call_, sizeof(value_type) * n);
    auto call = call_;
    Finally cleanup = [&]() noexcept {
      rm_.Decrease(call, sizeof(value_type) * n);
    };
    auto res = Allocator::allocate(n);
    call = IResourceManager::kNoop;
    return res;
  }

  void deallocate(value_type* p, std::size_t n) {
    rm_.Decrease(call_, sizeof(value_type) * n);
    Allocator::deallocate(p, n);
  }

  const Allocator& RawAllocator() const noexcept {
    return static_cast<const Allocator&>(*this);
  }

  template<typename A>
  bool operator==(const ManagedAllocator<A>& other) const noexcept {
    return RawAllocator() == other.RawAllocator() &&
           call_ == other.call_ &&
           &rm_ == &other.rm_;
  }

  template<typename A>
  bool operator!=(const ManagedAllocator<A>& other) const noexcept {
    return !(*this == other);
  }

  IResourceManager& ResourceManager() const noexcept { return rm_; }

  IResourceManager::Call ResourceCall() const noexcept { return call_; }

 private:
  IResourceManager& rm_;
  IResourceManager::Call call_;
};

template<typename T>
struct ManagedTypedAllocator : ManagedAllocator<std::allocator<T>> {
  using ManagedAllocator<std::allocator<T>>::ManagedAllocator;
};

#ifdef __cpp_lib_polymorphic_allocator
template<typename T>
struct ManagedTypedPmrAllocator
  : ManagedAllocator<std::pmr::polymorphic_allocator<T>> {
  using ManagedAllocator<std::pmr::polymorphic_allocator<T>>::ManagedAllocator;
};
#endif

}  // namespace irs
