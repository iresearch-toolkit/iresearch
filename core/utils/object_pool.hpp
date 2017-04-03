//
// IResearch search engine 
// 
// Copyright (c) 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#ifndef IRESEARCH_OBJECT_POOL_H
#define IRESEARCH_OBJECT_POOL_H

#include <algorithm>
#include <atomic>
#include <functional>

#include "memory.hpp"
#include "shared.hpp"
#include "thread_utils.hpp"
#include "misc.hpp"

NS_ROOT

template<typename Ptr>
class atomic_base {
 public:
  Ptr& atomic_exchange(Ptr* p, Ptr& r) const {
    SCOPED_LOCK(mutex_);
    p->swap(r);
    return r;
  }

  void atomic_store(Ptr* p, Ptr& r) const  {
    SCOPED_LOCK(mutex_);
    p->swap(r);
  }

  const Ptr& atomic_load(const Ptr* p) const {
    SCOPED_LOCK(mutex_);
    return *p;
  }

 private:
  mutable std::mutex mutex_;
};

// GCC prior the 5.0 does not support std::atomic_exchange(std::shared_ptr<T>*, std::shared_ptr<T>)
#if !defined(__GNUC__) || (__GNUC__ >= 5)
template<typename T>
class atomic_base<std::shared_ptr<T>> {
 public:
  static std::shared_ptr<T> atomic_exchange(std::shared_ptr<T>* p, std::shared_ptr<T>& r) {
    return std::atomic_exchange(p, r);
  }

  static void atomic_store(std::shared_ptr<T>* p, std::shared_ptr<T>& r) {
    std::atomic_store(p, r);
  }

  static std::shared_ptr<T> atomic_load(const std::shared_ptr<T>* p) {
    return std::atomic_load(p);
  }
};
#endif

template<typename T>
class move_on_copy { 
 public:
  move_on_copy(T&& value) : value_(std::move(value)) {}
  move_on_copy(const move_on_copy& rhs) : value_(std::move(rhs.value_)) {}

  T& value() { return value_; }
  const T& value() const { return value_; }

 private:
  move_on_copy& operator=(move_on_copy&&) = delete;
  move_on_copy& operator=(const move_on_copy&) = delete;
  
  mutable T value_;
};

template<typename T>
move_on_copy<T> make_move_on_copy(T&& value) {
  static_assert(std::is_rvalue_reference<decltype(value)>::value, "parameter should be an rvalue");
  return move_on_copy<T>(std::move(value));
}

//////////////////////////////////////////////////////////////////////////////
/// @brief a fixed size pool of objects
///        if the pool is empty then a new object is created via make(...)
///        if an object is available in a pool then in is returned but tracked
///        by the pool
///        when the object is released it is placed back into the pool
//////////////////////////////////////////////////////////////////////////////
template<typename T>
class bounded_object_pool : atomic_base<typename T::ptr> {
 public:
  typedef atomic_base<typename T::ptr> atomic_utils;
  DECLARE_SPTR(typename T::ptr::element_type);

  explicit bounded_object_pool(size_t size)
    : free_count_(std::max(size_t(1), size)), 
      pool_(std::max(size_t(1), size)), 
      used_(std::max(size_t(1), size)) {
    for (auto& flag : used_) {
      flag.clear();
    }
  }

  template<typename... Args>
  ptr emplace(Args&&... args) {
    for(;;) {
      // linear search for an unused slot in the pool
      for(size_t i = 0, count = used_.size(); i < count; ++i) {
        if (lock(i)) {
          auto& slot = pool_[i];

          if (!slot) {
            try {
              auto value = T::make(std::forward<Args>(args)...);
              atomic_utils::atomic_store(&slot, value);
            } catch (...) {
              unlock(i);
              throw;
            }
          }

          return ptr(
            slot.get(), 
            [this, i](typename ptr::element_type*) { unlock(i); }
          );
        }
      }

      wait_for_free_slots();
    }
  }

  size_t size() const NOEXCEPT { return pool_.size(); }

  template<typename Visitor>
  bool visit(const Visitor& visitor, bool shared = false) {
    return const_cast<const bounded_object_pool&>(*this).visit(visitor, shared);
  }

  template<typename Visitor>
  bool visit(const Visitor& visitor, bool shared = false) const {
    // shared visitation does not acquire exclusive lock on object
    // it is up to the caller to guarantee proper synchronization
    if (shared) {
      for (size_t i = 0, count = pool_.size(); i < count; ++i) {
        auto obj = atomic_utils::atomic_load(&pool_[i]).get();

        if (obj && !visitor(*obj)) {
          return false;
        }
      }

      return true;
    }

    std::vector<bool> used(pool_.size(), false);
    auto size = used.size();

    auto unlock_all = make_finally([this, &used] () {
      for (size_t i = 0, count = used.size(); i < count; ++i) {
        if (used[i]) {
          unlock(i);
        }
      }
    });

    for (;;) {
      for (size_t i = 0, count = pool_.size(); i < count; ++i) {
        if (used[i]) {
          continue;
        }

        if (lock(i)) {
          used[i] = true;

          auto obj = atomic_utils::atomic_load(&pool_[i]).get();

          if (obj && !visitor(*obj)) {
            return false;
          }

          if (1 == size) {
            return true;
          }

          --size;
        }
      }

      wait_for_free_slots();
    }

    return true;
  }

 private:
  void wait_for_free_slots() const {
    SCOPED_LOCK_NAMED(mutex_, lock);

    if (!free_count_) {
      cond_.wait_for(lock, std::chrono::milliseconds(1000));
    }
  }

  bool lock(size_t i) const {
    if (!used_[i].test_and_set()) {
      --free_count_;
      return true;
    }
    return false;
  }
  
  void unlock(size_t i) const {   
    used_[i].clear();
    SCOPED_LOCK(mutex_);
    ++free_count_; // under lock to ensure cond_.wait_for(...) not triggered
    cond_.notify_all();
  }

  mutable std::condition_variable cond_;
  mutable std::atomic<size_t> free_count_;
  mutable std::mutex mutex_;
  std::vector<typename T::ptr> pool_;
  mutable std::vector<std::atomic_flag> used_;
};

//////////////////////////////////////////////////////////////////////////////
/// @brief a fixed size pool of objects
///        if the pool is empty then a new object is created via make(...)
///        if an object is available in a pool then in is returned and no
///        longer tracked by the pool
///        when the object is released it is placed back into the pool if
///        space in the pool is available
//////////////////////////////////////////////////////////////////////////////
template<typename T>
class unbounded_object_pool
  : private atomic_base<typename T::ptr>,
    private atomic_base<std::shared_ptr<std::atomic<bool>>> {
 public:
  DECLARE_SPTR(typename T::ptr::element_type);

  explicit unbounded_object_pool(size_t size)
    : pool_(size), reusable_(memory::make_unique<std::atomic<bool>>(true)) {
  }

  void clear() {
    auto reusable = atomic_bool_utils::atomic_load(&reusable_);

    reusable->store(false); // prevent existing element from returning into the pool
    reusable = memory::make_unique<std::atomic<bool>>(true); // mark new generation
    atomic_bool_utils::atomic_store(&reusable_, reusable);

    // linearly reset all cached instances
    for(size_t i = 0, count = pool_.size(); i < count; ++i) {
      typename T::ptr empty;
      atomic_utils::atomic_exchange(&(pool_[i]), empty);
    }
  }

  template<typename... Args>
  ptr emplace(Args&&... args) {
    auto reusable = atomic_bool_utils::atomic_load(&reusable_); // retrieve before seek/instantiate
    typename T::ptr value = nullptr;

    // liniar search for an existing entry in the pool
    for(size_t i = 0, count = pool_.size(); i < count && !value; ++i) {
      value = std::move(atomic_utils::atomic_exchange(&(pool_[i]), value));
    }

    if (!value) {
      value = T::make(std::forward<Args>(args)...);
    }

    // unique_ptr can't be copied
    auto raw = value.get();
    auto moved = make_move_on_copy(std::move(value));

    return ptr(
      raw,
      [this, moved, reusable](typename ptr::element_type*) mutable ->void {
        if (!static_cast<atomic_bool_utils*>(this)->atomic_load(&reusable)->load()) {
          return; // do not return non-reusable elements into the pool
        }

        typename T::ptr val = std::move(moved.value());

        // linear search for an empty slot in the pool
        for(size_t i = 0, count = pool_.size(); i < count && val; ++i) {
          val = std::move(atomic_utils::atomic_exchange(&(pool_[i]), val));
        }
      }
    );
  }

  size_t size() const { return pool_.size(); }

 private:
  typedef std::shared_ptr<std::atomic<bool>> reusable_t;
  typedef atomic_base<typename T::ptr> atomic_utils;
  typedef atomic_base<reusable_t> atomic_bool_utils;
  std::vector<typename T::ptr> pool_;
  reusable_t reusable_;
};

NS_END

#endif