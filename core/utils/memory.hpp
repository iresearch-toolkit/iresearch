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

#ifndef IRESEARCH_MEMORY_H
#define IRESEARCH_MEMORY_H

#include <memory>

#include "shared.hpp"
#include "ebo.hpp"
#include "log.hpp"
#include "math_utils.hpp"
#include "noncopyable.hpp"

NS_ROOT
NS_BEGIN(memory)

//////////////////////////////////////////////////////////////////////////////
/// @brief dump memory statistics and stack trace to stderr
//////////////////////////////////////////////////////////////////////////////
IRESEARCH_API void dump_mem_stats_trace() NOEXCEPT;

template <typename T, typename... Types>
inline typename std::enable_if<
  !std::is_array<T>::value,
  std::unique_ptr<T>
>::type make_unique(Types&&... Args) {
  try {
    return std::unique_ptr<T>(new T(std::forward<Types>(Args)...));
  } catch (std::bad_alloc&) {
    fprintf(
      stderr,
      "Memory allocation failure while creating and initializing an object of size " IR_SIZE_T_SPECIFIER " bytes\n",
      sizeof(T)
    );
    dump_mem_stats_trace();
    throw;
  }
}

template<typename T>
inline typename std::enable_if<
  std::is_array<T>::value && std::extent<T>::value == 0,
  std::unique_ptr<T>
>::type make_unique(size_t size) {
  typedef typename std::remove_extent<T>::type value_type;

  try {
    return std::unique_ptr<T>(new value_type[size]());
  } catch (std::bad_alloc&) {
    fprintf(
      stderr,
      "Memory allocation failure while creating and initializing an array of " IR_SIZE_T_SPECIFIER " objects each of size " IR_SIZE_T_SPECIFIER " bytes\n",
      size, sizeof(value_type)
    );
    dump_mem_stats_trace();
    throw;
  }
}

template<typename T, typename... Types>
typename std::enable_if<
  std::extent<T>::value != 0,
  void
>::type make_unique(Types&&...) = delete;

template<typename Alloc>
struct allocator_deleter : public compact<0, Alloc*, std::is_empty<Alloc>::value> {
  typedef compact<0, Alloc*, std::is_empty<Alloc>::value> allocator_ref;
  typedef typename allocator_ref::type allocator_type;
  typedef typename allocator_type::pointer pointer;

  allocator_deleter(const allocator_type& alloc)
    : allocator_ref(&alloc) {
  }

  void operator()(pointer p) const NOEXCEPT {
    typedef std::allocator_traits<allocator_type> traits_t;

    auto& alloc = *allocator_ref::get();

    // destroy object
    traits_t::destroy(alloc, p);

    // deallocate storage
    traits_t::deallocate(alloc, p, 1);
  }
}; // allocator_deleter

template<typename T, typename Alloc, typename... Types>
inline typename std::enable_if<
  !std::is_array<T>::value,
  std::unique_ptr<T, allocator_deleter<Alloc>>
>::type allocate_unique(const Alloc& alloc, Types&&... Args) {
  typedef std::allocator_traits<Alloc> traits_t;
  typedef typename traits_t::pointer pointer;

  pointer p;

  try {
    p = alloc.allocate(1); // allocate space for 1 object
  } catch (std::bad_alloc&) {
    fprintf(
      stderr,
      "Memory allocation failure while creating and initializing an object of size " IR_SIZE_T_SPECIFIER " bytes\n",
      sizeof(T)
    );
    dump_mem_stats_trace();
    throw;
  }

  try {
    traits_t::construct(alloc, p, std::forward<Types>(Args)...); // construct object
  } catch (...) {
    alloc.deallocate(p, 1); // free allocated storage in case of any error during construction
    throw;
  }

  return std::unique_ptr<T, allocator_deleter<Alloc>>(
    p, allocator_deleter<Alloc>(alloc)
  );
}

template<typename Alloc>
class allocator_array_deleter : public compact<0, Alloc*, std::is_empty<Alloc>::value> {
 public:
  typedef compact<0, Alloc*, std::is_empty<Alloc>::value> allocator_ref;
  typedef typename allocator_ref::type allocator_type;
  typedef typename allocator_type::pointer pointer;

  allocator_array_deleter(const allocator_type& alloc, size_t size)
    : allocator_ref(&alloc), size_(size) {
  }

  void operator()(pointer p) const NOEXCEPT {
    typedef std::allocator_traits<allocator_type> traits_t;

    auto& alloc = *allocator_ref::get();

    // destroy objects
    for (auto end = p + size_; p != end; ++p) {
      traits_t::destroy(alloc, p);
    }

    // deallocate storage
    traits_t::deallocate(alloc, p, size_);
  }

 private:
  size_t size_;
}; // allocator_array_deleter

template<typename T, typename Alloc>
typename std::enable_if<
  std::is_array<T>::value && std::extent<T>::value == 0,
  std::unique_ptr<T, allocator_array_deleter<Alloc>>
>::type allocate_unique(const Alloc& alloc, size_t size) {
  typedef std::allocator_traits<Alloc> traits_t;
  typedef typename traits_t::pointer pointer;

  if (!size) {
    return nullptr;
  }

  pointer p;

  try {
    p = alloc.allocate(size); // allocate space for 'size' object
  } catch (std::bad_alloc&) {
    fprintf(
      stderr,
      "Memory allocation failure while creating and initializing " IR_SIZE_T_SPECIFIER " object(s) of size " IR_SIZE_T_SPECIFIER " bytes\n",
      size, sizeof(T)
    );
    dump_mem_stats_trace();
    throw;
  }

  auto begin = p;

  try {
    for (auto end = begin + size; begin != end; ++begin) {
      traits_t::construct(alloc, begin); // construct object
    }
  } catch (...) {
    // destroy constructed objects
    for ( ; p != begin; --begin) {
      traits_t::destroy(alloc, begin);
    }

    // free allocated storage in case of any error during construction
    alloc.deallocate(p, size);
    throw;
  }

  return std::unique_ptr<T[], allocator_array_deleter<Alloc>>(
    p, allocator_array_deleter<Alloc>(alloc, size)
  );
}

template<typename T, typename Alloc, typename... Types>
typename std::enable_if<
  std::extent<T>::value != 0,
  void
>::type allocate_unique(const Alloc&, Types&&...) = delete;

template<typename T>
class free_list {
 public:
  typedef T               value_type;
  typedef T*              pointer;
  typedef T&              reference;
  typedef const T*        const_pointer;
  typedef const T&        const_reference;
  typedef size_t          size_type;
  typedef std::ptrdiff_t  difference_type;
  typedef std::false_type propagate_on_container_copy_assignment;
  typedef std::true_type  propagate_on_container_move_assignment;
  typedef std::true_type  propagate_on_container_swap;

  union slot {
    value_type data;
    slot* next;
  };

  static const size_t SLOT_SIZE = sizeof(slot);
  static const size_t SLOT_ALIGN = alignof(slot);

  pointer address(reference x) const NOEXCEPT {
    return std::addressof(x);
  }

  const_pointer address(const_reference x) const NOEXCEPT {
    return std::addressof(x);
  }

  static char* align(char* p) NOEXCEPT {
    return reinterpret_cast<char*>(
      (reinterpret_cast<uintptr_t>(p) - 1U + SLOT_ALIGN) & (-SLOT_ALIGN)
    );
  }

  // constructs stack of slots in the specified range denoted by [begin;end]
  static void initialize(char* begin, char* end) NOEXCEPT {
    assert(begin);
    auto* free = reinterpret_cast<slot*>(begin);
    while ((begin += SLOT_SIZE) < end) {
      free = free->next = reinterpret_cast<slot*>(begin);
    }
    free->next = nullptr; // mark last element
  }

  // push the specified element 'p' to the stack denoted by 'head'
  static void push(slot*& head, pointer p) NOEXCEPT {
    assert(p);
    auto* free = reinterpret_cast<slot*>(p);
    free->next = head;
    head = free;
  }

  // pops an element from the stack denoted by 'head'
  static pointer pop(slot*& head) NOEXCEPT {
    assert(head);
    auto* p = reinterpret_cast<pointer>(head);
    head = head->next;
    return p;
  }
}; // free_list

template<typename T, size_t BlockSize>
class memory_pool : public free_list<T>, util::noncopyable {
 public:
  typedef free_list<T> free_list_t;
  typedef typename free_list_t::pointer pointer;
  typedef typename free_list_t::const_pointer const_pointer;
  typedef typename free_list_t::size_type size_type;
  typedef typename free_list_t::slot slot;

  static const size_t BLOCK_SIZE = BlockSize;

  template <typename U> struct rebind {
    typedef memory_pool<U, BLOCK_SIZE> other;
  };

  memory_pool() = default;
  memory_pool(memory_pool&& rhs) NOEXCEPT
    : free_(rhs.free_), blocks_(rhs.blocks_) {
    rhs.free_ = nullptr;
    rhs.blocks_ = nullptr;
  }
  memory_pool& operator=(memory_pool&& rhs) NOEXCEPT {
    if (this != &rhs) {
      free_ = rhs.free_;
      blocks_ = rhs.blocks_;
      rhs.free_ = nullptr;
      rhs.blocks_ = nullptr;
    }
  }

  ~memory_pool() NOEXCEPT {
    // deallocate previously allocated blocks
    // in reverse order
    while (blocks_) {
      ::free(free_list_t::pop(blocks_));
    }
  }

  // for now pool doesn't support bulk allocation
  // 'hint' argument ignored
  pointer allocate(size_type n = 1, const_pointer hint = 0) {
    if (!free_) {
      allocate_block();
    }

    return free_list_t::pop(free_);
  }

  void deallocate(pointer p, size_type n = 1) NOEXCEPT {
    if (p) free_list_t::push(free_, p);
  }

 private:
  void allocate_block() {
    // allocate memory block
    auto* begin = reinterpret_cast<char*>(::malloc(BLOCK_SIZE));

    // use 1st slot in a block to store pointer
    // to the prevoiusly allocated block
    free_list_t::push(blocks_, reinterpret_cast<pointer>(begin));

    // align pointer to the block body
    // since all slots have the same alignment requirements and size
    // we need to align explicitly the first slot only
    // all subsequent slots will be aligned implicitly
    auto* aligned = free_list_t::align(begin + free_list_t::SLOT_SIZE);

    // initialize list of free slots in allocated block
    free_list_t::initialize(
      aligned, aligned + BLOCK_SIZE - std::distance(begin, aligned)
    );

    // store pointer to the constructed free list
    free_ = reinterpret_cast<slot*>(aligned);
  }

  static_assert(math::is_power2(BLOCK_SIZE), "BlockSize must be a power of 2");
  static_assert(BLOCK_SIZE >= 2*free_list_t::SLOT_SIZE, "BlockSize is too small");

  slot* free_{}; // list of free slots in the allocated blocks
  slot* blocks_{}; // list of the allocated blocks
}; // memory_pool

NS_END
NS_END

#define PTR_NAMED(class_type, name, ...) \
  class_type::ptr name; \
  try { \
    name.reset(new class_type(__VA_ARGS__)); \
  } catch (std::bad_alloc&) { \
    fprintf( \
      stderr, \
      "Memory allocation failure while creating and initializing an object of size %lu bytes\n", \
      sizeof(class_type) \
    ); \
    iresearch::memory::dump_mem_stats_trace(); \
    throw; \
  }

#define DECLARE_SPTR(class_name) typedef std::shared_ptr<class_name> ptr
#define DECLARE_PTR(class_name) typedef std::unique_ptr<class_name> ptr
#define DECLARE_REF(class_name) typedef std::reference_wrapper<class_name> ref
#define DECLARE_CREF(class_name) typedef std::reference_wrapper<const class_name> cref

#define DECLARE_FACTORY(class_name) \
template<typename _T, typename... _Args> \
static ptr make(_Args&&... args) { \
  typedef typename std::enable_if<std::is_base_of<class_name, _T>::value, _T>::type type; \
  try { \
    return ptr(new type(std::forward<_Args>(args)...)); \
  } catch (std::bad_alloc&) { \
    fprintf( \
      stderr, \
      "Memory allocation failure while creating and initializing an object of size %lu bytes\n", \
      sizeof(type) \
    ); \
    iresearch::memory::dump_mem_stats_trace(); \
    throw; \
  } \
}

//////////////////////////////////////////////////////////////////////////////
/// @brief default implementation of a factory method, instantiation on heap
///        NOTE: make(...) MUST be defined in CPP to ensire proper code scope
//////////////////////////////////////////////////////////////////////////////
#define DECLARE_FACTORY_DEFAULT(...) static ptr make(__VA_ARGS__);
#define DEFINE_FACTORY_DEFAULT(class_type) \
/*static*/ class_type::ptr class_type::make() { \
  PTR_NAMED(class_type, ptr); \
  return ptr; \
}

//////////////////////////////////////////////////////////////////////////////
/// @brief implementation of a factory method, using a deque to store and
///        reuse instances with the help of a skip-list style offset free_list
///        use std::deque as a non-reordering block-reserving container
///        user should #include all required dependencies e.g. <deque>,<mutex>
///        NOTE: make(...) MUST be defined in CPP to ensire proper code scope
//////////////////////////////////////////////////////////////////////////////
#define DEFINE_FACTORY_POOLED(class_type) \
/*static*/ class_type::ptr class_type::make() { \
  static const size_t free_list_empty = std::numeric_limits<size_t>::max(); \
  static size_t free_list_head = free_list_empty; \
  static std::mutex mutex; \
  static std::deque<std::pair<class_type, size_t>> pool; \
  class_type::ptr::element_type* entry; \
  size_t entry_pos; \
  std::lock_guard<std::mutex> lock(mutex); \
  if (free_list_empty == free_list_head) { \
    entry_pos = pool.size(); \
    entry = &(pool.emplace(pool.end(), class_type(), free_list_empty)->first); \
  } else { \
    auto& entry_pair = pool[free_list_head]; \
    entry = &(entry_pair.first); \
    entry_pos = free_list_head; \
    free_list_head = entry_pair.second; \
  } \
  return class_type::ptr( \
    entry, \
    [entry_pos](class_type::ptr::element_type*)->void { \
      std::lock_guard<std::mutex> lock(mutex); \
      pool[entry_pos].second = free_list_head; \
      free_list_head = entry_pos; \
    } \
  ); \
}

//////////////////////////////////////////////////////////////////////////////
/// @brief implementation of a factory method, returning a singleton instance
///        NOTE: make(...) MUST be defined in CPP to ensire proper code scope
//////////////////////////////////////////////////////////////////////////////
#define DEFINE_FACTORY_SINGLETON(class_type) \
/*static*/ class_type::ptr class_type::make() { \
  struct make_impl_t { \
    static class_type::ptr make() { PTR_NAMED(class_type, ptr); return ptr; } \
  }; \
  static auto instance = make_impl_t::make(); \
  return instance; \
}

#endif
