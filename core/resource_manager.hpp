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

#if (defined(__clang__) || defined(_MSC_VER) || \
     defined(__GNUC__) &&                       \
       (__GNUC__ > 8))  // GCCs <=  don't have "<version>" header
#include <version>
#endif

#ifdef __cpp_lib_memory_resource
#include <memory_resource>
#endif

#include "utils/managed_allocator.hpp"

namespace irs {
struct IResourceManager {
  static IResourceManager kNoop;
#ifdef IRESEARCH_DEBUG
  static IResourceManager kForbidden;
#endif

  IResourceManager() = default;
  virtual ~IResourceManager() = default;

  IResourceManager(const IResourceManager&) = delete;
  IResourceManager operator=(const IResourceManager&) = delete;

  virtual bool Increase(size_t) noexcept { return true; }

  virtual void Decrease(size_t) noexcept {}

  IRS_FORCE_INLINE void DecreaseChecked(size_t v) noexcept {
    if (v != 0) {
      Decrease(v);
    }
  }
};

struct ResourceManagementOptions {
  static ResourceManagementOptions kDefault;

  IResourceManager* transactions{&IResourceManager::kNoop};
  IResourceManager* readers{&IResourceManager::kNoop};
  IResourceManager* consolidations{&IResourceManager::kNoop};
  IResourceManager* file_descriptors{&IResourceManager::kNoop};
  IResourceManager* cached_columns{&IResourceManager::kNoop};
};

template<typename T>
struct ManagedTypedAllocator
  : ManagedAllocator<std::allocator<T>, IResourceManager> {
  using Base = ManagedAllocator<std::allocator<T>, IResourceManager>;
  explicit ManagedTypedAllocator()
    : Base(
#ifdef IRESEARCH_DEBUG
        IResourceManager::kForbidden
#else
        IResourceManager::kNoop
#endif
      ) {
  }
  using Base::Base;
};

#ifdef __cpp_lib_polymorphic_allocator
template<typename T>
struct ManagedTypedPmrAllocator
  : ManagedAllocator<std::pmr::polymorphic_allocator<T>, IResourceManager> {
  using ManagedAllocator<std::pmr::polymorphic_allocator<T>,
                         IResourceManager>::ManagedAllocator;
};
#endif

}  // namespace irs
