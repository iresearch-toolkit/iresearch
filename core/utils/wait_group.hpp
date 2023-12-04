////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Valery Mironov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <atomic>

#include <absl/synchronization/mutex.h>

namespace irs {

// TODO(MBkkt) Considered to replace with YACLib
struct WaitGroup {
  explicit WaitGroup(size_t counter = 0) noexcept : counter_{2 * counter + 1} {}

  void Add(size_t counter = 1) noexcept {
    counter_.fetch_add(2 * counter, std::memory_order_relaxed);
  }

  void Done(size_t counter = 1) noexcept {
    if (counter_.fetch_sub(2 * counter, std::memory_order_acq_rel) ==
        2 * counter) {
      absl::MutexLock{&m_};
    }
  }

  // Multiple parallel Wait not supported, if needed check YACLib
  void Wait(size_t counter = 0) noexcept {
    if (counter_.fetch_sub(1, std::memory_order_acq_rel) != 1) {
      absl::Condition counter_zero{
        +[](std::atomic_size_t* counter) noexcept {
          return counter->load(std::memory_order_acquire) == 0;
        },
        &counter_};
      m_.LockWhen(counter_zero);
      m_.Unlock();
    }
    // We can put acquire here and remove above, but is it worth?
    Reset(counter);
  }

  // It shouldn't used for synchronization
  size_t Count() const noexcept {
    return counter_.load(std::memory_order_relaxed) / 2;
  }

  void Reset(size_t counter) noexcept {
    counter_.store(2 * counter + 1, std::memory_order_relaxed);
  }

  auto& Mutex() noexcept { return m_; }

 private:
  std::atomic_size_t counter_;
  absl::Mutex m_;
};

}  // namespace irs
