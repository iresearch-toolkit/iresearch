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

#if (defined(__clang__) || defined(_MSC_VER) || \
     defined(__GNUC__) &&                       \
       (__GNUC__ > 8))  // GCCs <=  don't have "<version>" header
#include <version>
#endif

#ifdef __cpp_lib_memory_resource
#include <memory_resource>
#endif

#include "misc.hpp"
#include "resource_manager.hpp"

namespace irs {
template<typename Allocator, typename Manager = IResourceManager>
class ManagedAllocator : private Allocator {
 public:
  using difference_type =
    typename std::allocator_traits<Allocator>::difference_type;
  using propagate_on_container_move_assignment = typename std::allocator_traits<
    Allocator>::propagate_on_container_move_assignment;
  using propagate_on_container_copy_assignment = typename std::allocator_traits<
    Allocator>::propagate_on_container_copy_assignment;
  using propagate_on_container_swap =
    typename std::allocator_traits<Allocator>::propagate_on_container_swap;
  using size_type = typename std::allocator_traits<Allocator>::size_type;
  using value_type = typename std::allocator_traits<Allocator>::value_type;

  explicit ManagedAllocator()
    : rm_{
#ifdef IRESEARCH_DEBUG
        &Manager::kForbidden
#else
        &Manager::kNoop
#endif
      } {
  }

  template<typename... Args>
  ManagedAllocator(Manager& rm, Args&&... args)
    : Allocator(std::forward<Args>(args)...), rm_(&rm) {}

  ManagedAllocator(ManagedAllocator&& other) noexcept
    : Allocator(other), rm_(&other.ResourceManager()) {}

  ManagedAllocator(const ManagedAllocator& other) noexcept
    : Allocator(other), rm_(&other.ResourceManager()) {}

  ManagedAllocator& operator=(ManagedAllocator&& other) noexcept {
    static_cast<Allocator&>(*this) = std::move(static_cast<Allocator&>(other));
    rm_ = &other.ResourceManager();
    return *this;
  }

  ManagedAllocator& operator=(const ManagedAllocator& other) noexcept {
    static_cast<Allocator&>(*this) = static_cast<Allocator&>(other);
    rm_ = &other.ResourceManager();
    return *this;
  }

  template<typename A>
  ManagedAllocator(const ManagedAllocator<A>& other) noexcept
    : Allocator(other.RawAllocator()), rm_{&other.ResourceManager()} {}

  value_type* allocate(std::size_t n) {
    rm_->Increase(sizeof(value_type) * n);
    Finally cleanup = [&]() noexcept {
      rm_->DecreaseChecked(sizeof(value_type) * n);
    };
    auto* res = Allocator::allocate(n);
    n = 0;
    return res;
  }

  void deallocate(value_type* p, std::size_t n) noexcept {
    Allocator::deallocate(p, n);
    rm_->Decrease(sizeof(value_type) * n);
  }

  const Allocator& RawAllocator() const noexcept {
    return static_cast<const Allocator&>(*this);
  }

  template<typename A>
  bool operator==(const ManagedAllocator<A>& other) const noexcept {
    return RawAllocator() == other.RawAllocator() && &rm_ == &other.rm_;
  }

  Manager& ResourceManager() const noexcept { return *rm_; }

 private:
  Manager* rm_;
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