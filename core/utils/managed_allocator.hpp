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

#include <memory>
#include <type_traits>

#include "misc.hpp"

namespace irs {

// It's possible to allow unsafe swap: swap without allocator propagation.
// But beware of UB in case of non equal allocators.
template<typename M, typename A, bool SafeSwap = true>
class ManagedAllocator {
  using Traits = std::allocator_traits<A>;

 public:
  using value_type = typename Traits::value_type;
  using size_type = typename Traits::size_type;
  using difference_type = typename Traits::difference_type;
  using propagate_on_container_move_assignment =
    typename Traits::propagate_on_container_move_assignment;
  using propagate_on_container_copy_assignment =
    typename Traits::propagate_on_container_copy_assignment;
  using propagate_on_container_swap = std::bool_constant<SafeSwap>;

  template<typename... Args>
  ManagedAllocator(M& manager, Args&&... args) noexcept(
    std::is_nothrow_constructible_v<A, Args&&...>)
    : manager_{&manager}, allocator_{std::forward<Args>(args)...} {}

  ManagedAllocator(ManagedAllocator&& other) noexcept(
    std::is_nothrow_move_constructible_v<A>)
    : manager_{other.manager_}, allocator_{std::move(other.allocator_)} {}

  ManagedAllocator(const ManagedAllocator& other) noexcept(
    std::is_nothrow_copy_constructible_v<A>)
    : manager_{other.manager_}, allocator_{other.allocator_} {}

  // TODO(MBkkt) Assign:
  //  Is it safe in case of other == &this? Looks like yes
  //  Maybe swap idiom?

  ManagedAllocator& operator=(ManagedAllocator&& other) noexcept(
    std::is_nothrow_move_assignable_v<A>) {
    manager_ = other.manager_;
    allocator_ = std::move(other.allocator_);
    return *this;
  }

  ManagedAllocator& operator=(const ManagedAllocator& other) noexcept(
    std::is_nothrow_copy_assignable_v<A>) {
    manager_ = other.manager_;
    allocator_ = other.allocator_;
    return *this;
  }

  template<typename T>
  ManagedAllocator(const ManagedAllocator<M, T, SafeSwap>& other) noexcept(
    std::is_nothrow_constructible_v<A, const T&>)
    : manager_{&other.Manager()}, allocator_{other.Allocator()} {}

  value_type* allocate(size_type n) {
    manager_->Increase(sizeof(value_type) * n);
    Finally cleanup = [&]() noexcept {
      manager_->DecreaseChecked(sizeof(value_type) * n);
    };
    auto* p = allocator_.allocate(n);
    n = 0;
    return p;
  }

  void deallocate(value_type* p, size_type n) noexcept {
    allocator_.deallocate(p, n);
    IRS_ASSERT(n != 0);
    manager_->Decrease(sizeof(value_type) * n);
  }

  M& Manager() const noexcept { return *manager_; }
  const A& Allocator() const noexcept { return allocator_; }

  template<typename T>
  bool operator==(
    const ManagedAllocator<M, T, SafeSwap>& other) const noexcept {
    return manager_ == &other.Manager() && allocator_ == other.Allocator();
  }

 private:
  M* manager_;
  IRS_NO_UNIQUE_ADDRESS A allocator_;
};

}  // namespace irs
