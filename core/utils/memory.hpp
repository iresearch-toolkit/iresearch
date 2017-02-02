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

#ifndef _MSC_VER
  #include <malloc.h>
#endif

#include "shared.hpp"

NS_ROOT
NS_BEGIN( memory )

template< typename _T, typename _alloc = std::allocator< _T > >
struct deallocator : public _alloc {
  typedef _alloc allocator_type;
  typedef typename allocator_type::pointer pointer;

  void operator()( pointer ptr ) {
    allocator_type::deallocate( ptr, 0 );
  }
};

FORCE_INLINE void malloc_statistics(FILE* out) {
  #ifdef _MSC_VER
    UNUSED(out);
  #else
    static const char* fomrat = "\
Total non-mmapped bytes (arena):       %d\n\
# of free chunks (ordblks):            %d\n\
# of free fastbin blocks (smblks):     %d\n\
# of mapped regions (hblks):           %d\n\
Bytes in mapped regions (hblkhd):      %d\n\
Max. total allocated space (usmblks):  %d\n\
Free bytes held in fastbins (fsmblks): %d\n\
Total allocated space (uordblks):      %d\n\
Total free space (fordblks):           %d\n\
Topmost releasable block (keepcost):   %d\n\
";
    auto mi = mallinfo();

    fprintf(
      out,
      fomrat,
      mi.arena,
      mi.ordblks,
      mi.smblks,
      mi.hblks,
      mi.hblkhd,
      mi.usmblks,
      mi.fsmblks,
      mi.uordblks,
      mi.fordblks,
      mi.keepcost
    );
    malloc_stats();
  #endif
}

template < class _Ty, class... _Types >
inline typename std::enable_if< 
    !std::is_array<_Ty>::value, 
    std::unique_ptr<_Ty> >::type make_unique( _Types&&... _Args ) { 
  try {
    return std::unique_ptr<_Ty>(new _Ty(std::forward<_Types>(_Args)...));
  } catch (std::bad_alloc&) {
    fprintf(
      stderr,
      "Memory allocation failure while creating and initializing an object of size %lu bytes\n",
      sizeof(_Ty)
    );
    malloc_statistics(stderr);
    throw;
  }
}

template< class _Ty > 
inline typename std::enable_if<
    std::is_array<_Ty>::value && std::extent<_Ty>::value == 0,
    std::unique_ptr<_Ty>  >::type make_unique( size_t _Size ) {
  typedef typename std::remove_extent<_Ty>::type _Elem;
  try {
    return std::unique_ptr<_Ty>(new _Elem[_Size]());
  } catch (std::bad_alloc&) {
    fprintf(
      stderr,
      "Memory allocation failure while creating and initializing an array of %lu objects each of size %lu bytes\n",
      _Size, sizeof(_Elem)
    );
    malloc_statistics(stderr);
    throw;
  }
}

template< class _Ty, class... _Types >
typename std::enable_if<
    std::extent<_Ty>::value != 0, 
    void >::type make_unique( _Types&&... ) = delete;

NS_END
NS_END

#define PTR_NAMED__(line, class_type, name, ...) \
  auto ptr ## line = iresearch::memory::make_unique<char[]>(sizeof(class_type)); \
  ::new(ptr ## line.get()) class_type(__VA_ARGS__); \
  class_type::ptr name(reinterpret_cast<class_type*>(ptr ## line.release()))
#define PTR_NAMED(class_type, name, ...) PTR_NAMED__(__LINE__, class_type, name, __VA_ARGS__)

#define DECLARE_SPTR(class_name) typedef std::shared_ptr<class_name> ptr
#define DECLARE_PTR(class_name) typedef std::unique_ptr<class_name> ptr
#define DECLARE_REF(class_name) typedef std::reference_wrapper<class_name> ref
#define DECLARE_CREF(class_name) typedef std::reference_wrapper<const class_name> cref

#define DECLARE_FACTORY(class_name) \
template<typename _T, typename... _Args> \
static ptr make(_Args&&... args) { \
  typedef typename std::enable_if<std::is_base_of<class_name, _T>::value, _T>::type type; \
  auto instance = iresearch::memory::make_unique<char[]>(sizeof(type)); \
  ::new(instance.get()) type(std::forward<_Args>(args)...); \
  return ptr(reinterpret_cast<type*>(instance.release())); \
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