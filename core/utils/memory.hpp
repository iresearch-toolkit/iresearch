//
// IResearch search engine 
// 
// Copyright © 2016 by EMC Corporation, All Rights Reserved
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

#define DECLARE_SPTR( class_name ) typedef std::shared_ptr< class_name > ptr
#define DECLARE_PTR(class_name) typedef std::unique_ptr< class_name > ptr
#define DECLARE_REF(class_name) typedef std::reference_wrapper<class_name> ref
#define DECLARE_CREF(class_name) typedef std::reference_wrapper<const class_name> cref

#define DECLARE_FACTORY(class_name) \
template< typename _Ty, typename... _Args > \
static ptr make(_Args&&... args) { \
  typedef typename std::enable_if< \
    std::is_base_of< class_name, _Ty >::value, \
    _Ty>::type type; \
  return ptr(new type( \
    std::forward< _Args >(args)... \
  )); \
}

//////////////////////////////////////////////////////////////////////////////
/// @brief default implementation of a factory method, instantiation on heap
///        NOTE: make(...) MUST be defined in CPP to ensire proper code scope
//////////////////////////////////////////////////////////////////////////////
#define DECLARE_FACTORY_DEFAULT(...) static ptr make(__VA_ARGS__);
#define DEFINE_FACTORY_DEFAULT(class_type) \
/*static*/ class_type::ptr class_type::make() { \
  return class_type::ptr(new class_type()); \
}
#define DEFINE_FACTORY_SINGLETON(class_type) \
/*static*/ class_type::ptr class_type::make() { \
  static class_type::ptr instance(new class_type()); \
  return instance; \
}

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

template < class _Ty, class... _Types >
inline typename std::enable_if< 
    !std::is_array<_Ty>::value, 
    std::unique_ptr<_Ty> >::type make_unique( _Types&&... _Args ) { 
  return ( std::unique_ptr<_Ty>( new _Ty( std::forward<_Types>( _Args )... ) ) ); 
}

template< class _Ty > 
inline typename std::enable_if<
    std::is_array<_Ty>::value && std::extent<_Ty>::value == 0,
    std::unique_ptr<_Ty>  >::type make_unique( size_t _Size ) {
  typedef typename std::remove_extent<_Ty>::type _Elem;
  return ( std::unique_ptr<_Ty>( new _Elem[_Size]() ) );
}

template< class _Ty, class... _Types >
typename std::enable_if<
    std::extent<_Ty>::value != 0, 
    void >::type make_unique( _Types&&... ) = delete;

NS_END
NS_END

#endif
