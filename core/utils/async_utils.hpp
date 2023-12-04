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
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <function2/function2.hpp>
#include <queue>
#include <thread>

#include "noncopyable.hpp"
#include "shared.hpp"
#include "string.hpp"
#include "thread_utils.hpp"

#include <absl/synchronization/mutex.h>

namespace irs::async_utils {

//////////////////////////////////////////////////////////////////////////////
/// @brief spinlock implementation for Win32 since std::mutex cannot be used
///        in calls going through dllmain()
//////////////////////////////////////////////////////////////////////////////
class busywait_mutex final {
 public:
  void lock() noexcept;
  bool try_lock() noexcept;
  void unlock() noexcept;

 private:
  std::atomic_bool locked_{false};
};

template<bool UseDelay = true>
class ThreadPool {
 public:
  using Char = std::remove_pointer_t<thread_name_t>;
  using Clock = std::chrono::steady_clock;
  using Func = fu2::unique_function<void()>;

  ThreadPool() = default;
  explicit ThreadPool(size_t threads, basic_string_view<Char> name = {});
  ~ThreadPool() { stop(true); }

  void start(size_t threads, basic_string_view<Char> name = {});
  bool run(Func&& fn, absl::Duration delay = {});
  void stop(bool skip_pending = false) noexcept;  // always a blocking call
  size_t tasks_active() const {
    absl::MutexLock lock{&m_};
    return state_ / 2;
  }
  size_t tasks_pending() const {
    absl::MutexLock lock{&m_};
    return tasks_.size();
  }
  size_t threads() const {
    absl::MutexLock lock{&m_};
    return threads_.size();
  }
  // 1st - tasks active(), 2nd - tasks pending(), 3rd - threads()
  std::tuple<size_t, size_t, size_t> stats() const {
    absl::MutexLock lock{&m_};
    return {state_ / 2, tasks_.size(), threads_.size()};
  }

 private:
  struct Task {
    explicit Task(absl::Time at, Func&& fn) : at{at}, fn{std::move(fn)} {}

    absl::Time at;
    Func fn;

    bool operator<(const Task& rhs) const noexcept { return rhs.at < at; }
  };

  void Work();

  bool WasStop() const noexcept { return state_ % 2 != 0; }

  bool NeedsWork() const noexcept { return !tasks_.empty() || WasStop(); }

  bool NeedsWorkAt() const noexcept {
    if constexpr (UseDelay) {
      return (!tasks_.empty() && tasks_.top().at <= absl::Now()) || WasStop();
    } else {
      return false;
    }
  }

  std::vector<std::thread> threads_;
  mutable absl::Mutex m_;
  std::conditional_t<UseDelay, std::priority_queue<Task>, std::queue<Func>>
    tasks_;
  // stop flag and active tasks counter
  uint64_t state_ = 0;
};

}  // namespace irs::async_utils
