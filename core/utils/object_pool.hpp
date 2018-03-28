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

#ifndef IRESEARCH_OBJECT_POOL_H
#define IRESEARCH_OBJECT_POOL_H

#include <algorithm>
#include <atomic>
#include <functional>
#include <vector>

#include "memory.hpp"
#include "shared.hpp"
#include "thread_utils.hpp"
#include "async_utils.hpp"
#include "misc.hpp"
#include "noncopyable.hpp"

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
    p->swap(r); // FIXME
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
  #if defined(IRESEARCH_VALGRIND) // suppress valgrind false-positives related to std::atomic_*
   public:
    // for compatibility 'std::mutex' is not moveable
    atomic_base() = default;
    atomic_base(atomic_base&&) NOEXCEPT { }
    atomic_base& operator=(atomic_base&&) NOEXCEPT { return *this; }

    std::shared_ptr<T> atomic_exchange(std::shared_ptr<T>* p, std::shared_ptr<T> r) const NOEXCEPT {
      SCOPED_LOCK(mutex_);
      return std::atomic_exchange(p, r);
    }

    void atomic_store(std::shared_ptr<T>* p, std::shared_ptr<T> r) const NOEXCEPT {
      SCOPED_LOCK(mutex_);
      std::atomic_store(p, r);
    }

    std::shared_ptr<T> atomic_load(const std::shared_ptr<T>* p) const NOEXCEPT {
      SCOPED_LOCK(mutex_);
      return std::atomic_load(p);
    }

   private:
    mutable std::mutex mutex_;
  #else
   public:
    static std::shared_ptr<T> atomic_exchange(std::shared_ptr<T>* p, std::shared_ptr<T> r) NOEXCEPT {
      return std::atomic_exchange(p, r);
    }

    static void atomic_store(std::shared_ptr<T>* p, std::shared_ptr<T> r) NOEXCEPT {
      std::atomic_store(p, r);
    }

    static std::shared_ptr<T> atomic_load(const std::shared_ptr<T>* p) NOEXCEPT {
      return std::atomic_load(p);
    }
  #endif // defined(IRESEARCH_VALGRIND)
};
#else
template<typename T>
class atomic_base<std::shared_ptr<T>> {
 public:
  // for compatibility 'std::mutex' is not moveable
  atomic_base() = default;
  atomic_base(atomic_base&&) NOEXCEPT { }
  atomic_base& operator=(atomic_base&&) NOEXCEPT { return *this; }

  std::shared_ptr<T> atomic_exchange(std::shared_ptr<T>* p, std::shared_ptr<T> r) const NOEXCEPT {
    SCOPED_LOCK(mutex_);
    p->swap(r);
    return r;
  }

  void atomic_store(std::shared_ptr<T>* p, std::shared_ptr<T> r) const NOEXCEPT {
    SCOPED_LOCK(mutex_);
    *p = r;
  }

  std::shared_ptr<T> atomic_load(const std::shared_ptr<T>* p) const NOEXCEPT {
    SCOPED_LOCK(mutex_);
    return *p;
  }

 private:
  mutable std::mutex mutex_;
};
#endif

//////////////////////////////////////////////////////////////////////////////
/// @class concurrent_forward_list
/// @brief lock-free single linked list
//////////////////////////////////////////////////////////////////////////////
template<typename T>
class concurrent_forward_list : private util::noncopyable {
 public:
  typedef T element_type;

  struct node_type {
    element_type value{};
    node_type* next{};
  };

  explicit concurrent_forward_list(node_type* head = nullptr) NOEXCEPT
    : list_(head) {
  }

  bool empty() const NOEXCEPT {
    return nullptr == list_.load();
  }

  node_type* pop_front() NOEXCEPT {
    auto* head = list_.load();
    while (head && !list_.compare_exchange_weak(head, head->next)) { }
    return head;
  }

  void push_front(node_type& new_head) NOEXCEPT {
    new_head.next = list_.load();
    while (!list_.compare_exchange_weak(new_head.next, &new_head)) { }
  }

  void swap(concurrent_forward_list& rhs) NOEXCEPT {
    auto& rhslist = rhs.list_;
    auto* head = rhslist.load();
    while (!rhslist.compare_exchange_weak(head, nullptr)) { }
    list_.store(head);
  }

 private:
  std::atomic<node_type*> list_{ nullptr };
}; // concurrent_forward_list

// -----------------------------------------------------------------------------
// --SECTION--                                                      bounded pool
// -----------------------------------------------------------------------------

//////////////////////////////////////////////////////////////////////////////
/// @brief a fixed size pool of objects
///        if the pool is empty then a new object is created via make(...)
///        if an object is available in a pool then in is returned but tracked
///        by the pool
///        when the object is released it is placed back into the pool
//////////////////////////////////////////////////////////////////////////////
template<typename T>
class bounded_object_pool : atomic_base<typename T::ptr> {
 private:
  struct slot_t : util::noncopyable {
    bounded_object_pool* owner;
    typename T::ptr ptr;
    std::atomic_flag used;
  }; // slot_t

  typedef atomic_base<typename T::ptr> atomic_utils;

 public:
  typedef typename T::ptr::element_type element_type;

  /////////////////////////////////////////////////////////////////////////////
  /// @class ptr
  /// @brief represents a control object of bounded_object_pool with
  ///        semantic similar to smart pointers
  /////////////////////////////////////////////////////////////////////////////
  class ptr : util::noncopyable {
   public:
    explicit ptr(slot_t& slot) NOEXCEPT
      : slot_(&slot) {
    }

    ptr(ptr&& rhs) NOEXCEPT
      : slot_(rhs.slot_) {
      rhs.slot_ = nullptr; // take ownership
    }

    ptr& operator=(ptr&& rhs) NOEXCEPT {
      if (this != &rhs) {
        slot_ = rhs.slot_;
        rhs.slot_ = nullptr; // take ownership
      }
      return *this;
    }

    ~ptr() NOEXCEPT {
      reset();
    }

    FORCE_INLINE void reset() NOEXCEPT {
      reset(slot_);
    }

    std::shared_ptr<element_type> release() {
      auto* raw = get();
      auto* slot = slot_;
      slot_ = nullptr; // moved

      // in case if exception occurs
      // destructor will be called
      return std::shared_ptr<element_type>(
        raw,
        [slot] (element_type*) mutable NOEXCEPT {
          reset(slot);
      });
    }

    element_type& operator*() const NOEXCEPT { return *slot_->ptr; }
    element_type* operator->() const NOEXCEPT { return get(); }
    element_type* get() const NOEXCEPT { return slot_->ptr.get(); }
    operator bool() const NOEXCEPT {
      return static_cast<bool>(slot_);
    }

   private:
    static void reset(slot_t*& slot) NOEXCEPT {
      if (!slot) {
        // nothing to do
        return;
      }

      assert(slot->owner);
      slot->owner->unlock(*slot);
      slot = nullptr; // release ownership
    }

    slot_t* slot_;
  }; // ptr

  explicit bounded_object_pool(size_t size)
    : free_count_(std::max(size_t(1), size)), 
      pool_(std::max(size_t(1), size)) {
    // initialize pool
    for (auto& slot : pool_) {
      slot.used.clear();
      slot.owner = this;
    }
  }

  template<typename... Args>
  ptr emplace(Args&&... args) {
    for(;;) {
      // linear search for an unused slot in the pool
      for (auto& slot : pool_) {
        if (lock(slot)) {
          auto& p = slot.ptr;
          if (!p) {
            try {
              auto value = T::make(std::forward<Args>(args)...);
              atomic_utils::atomic_store(&p, value);
            } catch (...) {
              unlock(slot);
              throw;
            }
          }

          return ptr(slot);
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
      for (auto& slot : pool_) {
        auto& p = slot.ptr;
        auto* obj = atomic_utils::atomic_load(&p).get();

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
          unlock(pool_[i]);
        }
      }
    });

    for (;;) {
      for (size_t i = 0, count = pool_.size(); i < count; ++i) {
        if (used[i]) {
          continue;
        }

        auto& slot = pool_[i];
        if (lock(slot)) {
          used[i] = true;

          auto& p = slot.ptr;
          auto* obj = atomic_utils::atomic_load(&p).get();

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

  // MSVC 2017.3, 2017.4 and 2017.5 incorectly decrement counter if this function is inlined during optimization
  // MSVC 2017.2 and below TODO test for both debug and release
  MSVC2017_3456_OPTIMIZED_WORKAROUND(__declspec(noinline))
  bool lock(slot_t& slot) const NOEXCEPT {
    if (!slot.used.test_and_set()) {
      --free_count_;

      return true;
    }

    return false;
  }

  // MSVC 2017.3, 2017.4 and 2017.5 incorectly increment counter if this function is inlined during optimization
  // MSVC 2017.2 and below TODO test for both debug and release
  MSVC2017_3456_OPTIMIZED_WORKAROUND(__declspec(noinline))
  void unlock(slot_t& slot) const NOEXCEPT {
    slot.used.clear();
    SCOPED_LOCK(mutex_);
    ++free_count_; // under lock to ensure cond_.wait_for(...) not triggered
    cond_.notify_all();
  }

  mutable std::condition_variable cond_;
  mutable std::atomic<size_t> free_count_;
  mutable std::mutex mutex_;
  mutable std::vector<slot_t> pool_;
}; // bounded_object_pool

// -----------------------------------------------------------------------------
// --SECTION--                                                    unbounded pool
// -----------------------------------------------------------------------------

//////////////////////////////////////////////////////////////////////////////
/// @class async_value
/// @brief convenient class storing value and associated read-write lock
//////////////////////////////////////////////////////////////////////////////
template<typename T>
class async_value {
 public:
  typedef T value_type;
  typedef async_utils::read_write_mutex::read_mutex read_lock_type;
  typedef async_utils::read_write_mutex::write_mutex write_lock_type;

  template<typename... Args>
  explicit async_value(Args&&... args)
    : value_(std::forward<Args>(args)...) {
  }

  const value_type& value() const NOEXCEPT {
    return value_;
  }

  value_type& value() NOEXCEPT {
    return value_;
  }

  read_lock_type& read_lock() const NOEXCEPT {
    return read_lock_;
  }

  write_lock_type& write_lock() const NOEXCEPT {
    return write_lock_;
  }

 protected:
  value_type value_;
  async_utils::read_write_mutex lock_;
  mutable read_lock_type read_lock_{ lock_ };
  mutable write_lock_type write_lock_{ lock_ };
}; // async_value

//////////////////////////////////////////////////////////////////////////////
/// @class unbounded_object_pool_base
/// @brief base class for all unbounded object pool implementations
//////////////////////////////////////////////////////////////////////////////
template<typename T>
class unbounded_object_pool_base : private util::noncopyable {
 public:
  typedef typename T::ptr::element_type element_type;

  size_t size() const NOEXCEPT { return pool_.size(); }

 protected:
  typedef concurrent_forward_list<typename T::ptr> forward_list;
  typedef typename forward_list::node_type node;

  explicit unbounded_object_pool_base(size_t size)
    : pool_(size), free_slots_(pool_.data()) {
    // build up linked list
    for (auto begin = pool_.begin(), next = begin + 1, end = pool_.end();
         next < end;
         begin = next, ++next) {
      begin->next = &*next;
    }
  }

  unbounded_object_pool_base(unbounded_object_pool_base&& rhs) NOEXCEPT
    : pool_(std::move(rhs.pool_)) {
  }

  template<typename... Args>
  typename T::ptr acquire(Args&&... args) {
    auto* head = free_objects_.pop_front();

    if (head) {
      auto value = std::move(head->value);
      assert(value);
      free_slots_.push_front(*head);
      return value;
    }

    return T::make(std::forward<Args>(args)...);
  }

  void release(typename T::ptr&& value) NOEXCEPT {
    auto* slot = free_slots_.pop_front();

    if (!slot) {
      // no free slots
      return;
    }

    slot->value = std::move(value);
    free_objects_.push_front(*slot);
  }

  std::vector<node> pool_;
  forward_list free_objects_; // list of created objects that are ready to be reused
  forward_list free_slots_; // list of free slots to be reused
}; // unbounded_object_pool_base

//////////////////////////////////////////////////////////////////////////////
/// @class unbounded_object_pool
/// @brief a fixed size pool of objects
///        if the pool is empty then a new object is created via make(...)
///        if an object is available in a pool then in is returned and no
///        longer tracked by the pool
///        when the object is released it is placed back into the pool if
///        space in the pool is available
///        pool owns produced object so it's not allowed to destroy before
///        all acquired objects will be destroyed
//////////////////////////////////////////////////////////////////////////////
template<typename T>
class unbounded_object_pool : public unbounded_object_pool_base<T> {
 private:
  typedef unbounded_object_pool_base<T> base_t;
  typedef typename base_t::node node;

 public:
  typedef typename base_t::element_type element_type;

  /////////////////////////////////////////////////////////////////////////////
  /// @class ptr
  /// @brief represents a control object of unbounded_object_pool with
  ///        semantic similar to smart pointers
  /////////////////////////////////////////////////////////////////////////////
  class ptr : util::noncopyable {
   public:
    ptr(typename T::ptr&& value,
        unbounded_object_pool& owner
    ) : value_(std::move(value)),
        owner_(&owner) {
    }

    ptr(ptr&& rhs) NOEXCEPT
      : value_(std::move(rhs.value_)),
        owner_(rhs.owner_) {
      rhs.owner_ = nullptr;
    }

    ptr& operator=(ptr&& rhs) NOEXCEPT {
      if (this != &rhs) {
        value_ = std::move(rhs.value_);
        owner_ = rhs.owner_;
        rhs.owner_ = nullptr;
      }
      return *this;
    }

    ~ptr() NOEXCEPT {
      reset();
    }

    FORCE_INLINE void reset() NOEXCEPT {
      reset(value_, owner_);
    }

    // FIXME handle potential bad_alloc in shared_ptr constructor
    // mark method as NOEXCEPT and move back all the stuff in case of error
    std::shared_ptr<element_type> release() {
      auto* raw = get();
      auto moved_value = make_move_on_copy(std::move(value_));
      auto* owner = owner_;
      owner_ = nullptr; // moved

      // in case if exception occurs
      // destructor will be called
      return std::shared_ptr<element_type>(
        raw,
        [owner, moved_value] (element_type*) mutable NOEXCEPT {
          reset(moved_value.value(), owner);
      });
    }

    element_type& operator*() const NOEXCEPT { return *value_; }
    element_type* operator->() const NOEXCEPT { return get(); }
    element_type* get() const NOEXCEPT { return value_.get(); }
    operator bool() const NOEXCEPT {
      return static_cast<bool>(owner_);
    }

   private:
    static void reset(typename T::ptr& obj, unbounded_object_pool*& owner) NOEXCEPT {
      if (!owner) {
        // already destroyed
        return;
      }

      owner->release(std::move(obj));
      obj = typename T::ptr{}; // reset value
      owner = nullptr; // mark as destroyed
    }

    typename T::ptr value_;
    unbounded_object_pool* owner_;
  };

  explicit unbounded_object_pool(size_t size = 0)
    : base_t(size) {
  }

  /////////////////////////////////////////////////////////////////////////////
  /// @brief clears all cached objects
  /////////////////////////////////////////////////////////////////////////////
  void clear() {
    node* head = nullptr;

    // reset all cached instances
    while (head = this->free_objects_.pop_front()) {
      head->value = typename T::ptr{}; // empty instance
      this->free_slots_.push_front(*head);
    }
  }

  template<typename... Args>
  ptr emplace(Args&&... args) {
    return ptr(this->acquire(std::forward<Args>(args)...), *this);
  }

 private:
  // disallow move
  unbounded_object_pool(unbounded_object_pool&&) = delete;
  unbounded_object_pool& operator=(unbounded_object_pool&&) = delete;

}; // unbounded_object_pool

NS_BEGIN(detail)

template<typename Pool>
struct pool_generation {
  explicit pool_generation(Pool* owner) NOEXCEPT
    : owner(owner) {
  }

  bool stale{false}; // stale mark
  Pool* owner; // current owner
}; // pool_generation

NS_END // detail

//////////////////////////////////////////////////////////////////////////////
/// @class unbounded_object_pool_volatile
/// @brief a fixed size pool of objects
///        if the pool is empty then a new object is created via make(...)
///        if an object is available in a pool then in is returned and no
///        longer tracked by the pool
///        when the object is released it is placed back into the pool if
///        space in the pool is available
///        pool may be safely destroyed even there are some produced objects
///        alive
//////////////////////////////////////////////////////////////////////////////
template<typename T>
class unbounded_object_pool_volatile
    : public unbounded_object_pool_base<T>,
      private atomic_base<std::shared_ptr<async_value<detail::pool_generation<unbounded_object_pool_volatile<T>>>>> {
 private:
  typedef unbounded_object_pool_base<T> base_t;

  typedef async_value<detail::pool_generation<unbounded_object_pool_volatile<T>>> generation_t;
  typedef std::shared_ptr<generation_t> generation_ptr_t;
  typedef atomic_base<generation_ptr_t> atomic_utils;

  typedef std::lock_guard<typename generation_t::read_lock_type> read_guard_t;
  typedef std::lock_guard<typename generation_t::write_lock_type> write_guard_t;

 public:
  typedef typename base_t::element_type element_type;

  /////////////////////////////////////////////////////////////////////////////
  /// @class ptr
  /// @brief represents a control object of unbounded_object_pool with
  ///        semantic similar to smart pointers
  /////////////////////////////////////////////////////////////////////////////
  class ptr : util::noncopyable {
   public:
    ptr(typename T::ptr&& value,
        const generation_ptr_t& gen) NOEXCEPT
      : value_(std::move(value)),
        gen_(std::move(gen)) {
    }

    ptr(ptr&& rhs) NOEXCEPT
      : value_(std::move(rhs.value_)),
        gen_(std::move(rhs.gen_)) {
    }

    ptr& operator=(ptr&& rhs) NOEXCEPT {
      if (this != &rhs) {
        value_ = std::move(rhs.value_);
        gen_ = std::move(rhs.gen_);
      }
      return *this;
    }

    ~ptr() NOEXCEPT {
      reset();
    }

    FORCE_INLINE void reset() NOEXCEPT {
      reset(value_, gen_);
    }

    std::shared_ptr<element_type> release() {
      auto* raw = get();
      auto moved_value = make_move_on_copy(std::move(value_));
      auto moved_gen = make_move_on_copy(std::move(gen_));
      assert(!gen_); // moved

      // in case if exception occurs
      // destructor will be called
      return std::shared_ptr<element_type>(
        raw,
        [moved_gen, moved_value] (element_type*) mutable NOEXCEPT {
          reset(moved_value.value(), moved_gen.value());
      });
    }

    element_type& operator*() const NOEXCEPT { return *value_; }
    element_type* operator->() const NOEXCEPT { return get(); }
    element_type* get() const NOEXCEPT { return value_.get(); }
    operator bool() const NOEXCEPT {
      return static_cast<bool>(gen_);
    }

   private:
    static void reset(typename T::ptr& obj, generation_ptr_t& gen) NOEXCEPT {
      if (!gen) {
        // already destroyed
        return;
      }

      // do not remove scope!!!
      // variable 'lock' below must be destroyed before 'gen_'
      {
        read_guard_t lock(gen->read_lock());
        auto& value = gen->value();

        if (!value.stale) {
          value.owner->release(std::move(obj));
        }
      }

      obj = typename T::ptr{}; // clear object oustide the lock
      gen = nullptr; // mark as destroyed
    }

    typename T::ptr value_;
    generation_ptr_t gen_;
  }; // ptr

  explicit unbounded_object_pool_volatile(size_t size = 0)
    : base_t(size),
      gen_(memory::make_shared<generation_t>(this)) {
  }

  // FIXME check what if
  //
  // unbounded_object_pool_volatile p0, p1;
  // thread0: p0.clear();
  // thread1: unbounded_object_pool_volatile p1(std::move(p0));
  unbounded_object_pool_volatile(unbounded_object_pool_volatile&& rhs) NOEXCEPT
    : base_t(std::move(rhs)) {
    gen_ = atomic_utils::atomic_load(&rhs.gen_);

    write_guard_t lock(gen_->write_lock());
    gen_->value().owner = this; // change owner

    this->free_slots_.swap(rhs.free_slots_);
    this->free_objects_.swap(rhs.free_objects_);
  }

  ~unbounded_object_pool_volatile() NOEXCEPT {
    // prevent existing elements from returning into the pool
    // if pool doesn't own generation anymore
    const auto gen = atomic_utils::atomic_load(&gen_);
    write_guard_t lock(gen->write_lock());

    auto& value = gen->value();

    if (value.owner == this) {
      value.stale = true;
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  /// @brief clears all cached objects and optionally prevents already created
  ///        objects from returning into the pool
  /// @param new_generation if true, prevents already created objects from
  ///        returning into the pool, otherwise just clears all cached objects
  /////////////////////////////////////////////////////////////////////////////
  void clear(bool new_generation = false) {
    // prevent existing elements from returning into the pool
    if (new_generation) {
      {
        auto gen = atomic_utils::atomic_load(&gen_);
        write_guard_t lock(gen->write_lock());
        gen->value().stale = true;
      }

      // mark new generation
      atomic_utils::atomic_store(&gen_, memory::make_shared<generation_t>(this));
    }

    typename base_t::node* head = nullptr;

    // reset all cached instances
    while (head = this->free_objects_.pop_front()) {
      head->value = typename T::ptr{}; // empty instance
      this->free_slots_.push_front(*head);
    }
  }

  template<typename... Args>
  ptr emplace(Args&&... args) {
    const auto gen = atomic_utils::atomic_load(&gen_); // retrieve before seek/instantiate

    return ptr(this->acquire(std::forward<Args>(args)...), gen);
  }

  size_t generation_size() const NOEXCEPT {
    const auto use_count = atomic_utils::atomic_load(&gen_).use_count();
    assert(use_count >= 2);
    return use_count - 2; // -1 for temporary object, -1 for this->_gen
  }

 private:
  // disallow move assignment
  unbounded_object_pool_volatile& operator=(unbounded_object_pool_volatile&&) = delete;

  generation_ptr_t gen_; // current generation
}; // unbounded_object_pool_volatile

NS_END

#endif
