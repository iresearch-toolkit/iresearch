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

#ifndef IRESEARCH_ATTRIBUTES_H
#define IRESEARCH_ATTRIBUTES_H

#include <map>

#include "noncopyable.hpp"
#include "memory.hpp"
#include "string.hpp"
#include "timer_utils.hpp"
#include "bit_utils.hpp"
#include "type_id.hpp"
#include "noncopyable.hpp"
#include "string.hpp"

#include <unordered_set>

NS_ROOT

//////////////////////////////////////////////////////////////////////////////
/// @class attribute 
/// @brief base class for all attributes that can be deallocated via a ptr of
///        this struct type using a virtual destructor
///        an empty struct tag type with a virtual destructor
///        all derived classes must implement the following functions:
///        static const attribute::type_id& type() NOEXCEPT
///          via DECLARE_ATTRIBUTE_TYPE()/DEFINE_ATTRIBUTE_TYPE(...)
///        static ptr make(Args&&... args)
///          via DECLARE_FACTORY()/DECLARE_FACTORY_DEFAULT()
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API attribute {
  DECLARE_PTR(attribute);

  //////////////////////////////////////////////////////////////////////////////
  /// @class type_id 
  //////////////////////////////////////////////////////////////////////////////
  class IRESEARCH_API type_id: public iresearch::type_id, util::noncopyable {
   public:
    type_id(const string_ref& name): name_(name) {}
    operator const type_id*() const { return this; }
    static const type_id* get(const string_ref& name);
    const string_ref& name() const { return name_; }

   private:
    string_ref name_;
  }; // type_id

  attribute( const type_id& type ) : type_( &type ) {}

  const type_id& type() const { return *type_; }

  virtual ~attribute();
  virtual void clear() = 0;

 private:
   const type_id* type_;
};

// -----------------------------------------------------------------------------
// --SECTION--                                              Attribute definition
// -----------------------------------------------------------------------------

#define DECLARE_ATTRIBUTE_TYPE() DECLARE_TYPE_ID(iresearch::attribute::type_id)
#define DEFINE_ATTRIBUTE_TYPE_NAMED(class_type, class_name) DEFINE_TYPE_ID(class_type, iresearch::attribute::type_id) { \
  static iresearch::attribute::type_id type(class_name); \
  return type; \
}
#define DEFINE_ATTRIBUTE_TYPE(class_type) DEFINE_ATTRIBUTE_TYPE_NAMED(class_type, #class_type)

// -----------------------------------------------------------------------------
// --SECTION--                                            Attribute registration
// -----------------------------------------------------------------------------

class IRESEARCH_API attribute_registrar {
 public:
  attribute_registrar(const attribute::type_id& type);
  operator bool() const NOEXCEPT;
 private:
  bool registered_;
};

#define REGISTER_ATTRIBUTE__(attribute_name, line) static iresearch::attribute_registrar attribute_registrar ## _ ## line(attribute_name::type())
#define REGISTER_ATTRIBUTE_EXPANDER__(attribute_name, line) REGISTER_ATTRIBUTE__(attribute_name, line)
#define REGISTER_ATTRIBUTE(attribute_name) REGISTER_ATTRIBUTE_EXPANDER__(attribute_name, __LINE__)

//////////////////////////////////////////////////////////////////////////////
/// @class basic_attribute
/// @brief represents simple attribute holds a single value 
//////////////////////////////////////////////////////////////////////////////
template< typename T > 
struct IRESEARCH_API_TEMPLATE basic_attribute : attribute {
  typedef T value_t;

  basic_attribute( const attribute::type_id& type, const T& value = T() )
    : attribute(type), value( value ) { 
  }

  virtual void clear() { value = T(); }

  bool operator==( const basic_attribute& rhs ) const {
    return value == rhs.value;
  }
  
  bool operator==( const T& rhs ) const {
    return value == rhs;
  }

  T value;
};

//////////////////////////////////////////////////////////////////////////////
/// @class flags
/// @brief represents a set of features enabled for the particular field 
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API flags {
 public:  
  typedef std::unordered_set<const attribute::type_id*> type_map;

  static const flags& empty_instance();

  flags();
  flags(const flags&) = default;
  flags(flags&& rhs) NOEXCEPT;
  flags( std::initializer_list< const attribute::type_id* > flags );
  flags& operator=( std::initializer_list< const attribute::type_id* > flags );
  flags& operator=(flags&& rhs) NOEXCEPT;
  flags& operator=( const flags&) = default;

  type_map::const_iterator begin() const { return map_.begin(); }
  type_map::const_iterator end() const { return map_.end(); }

  template< typename T >
  flags& add() {
    typedef typename std::enable_if< 
      std::is_base_of< attribute, T >::value, T 
    >::type attribute_t;

    return add(attribute_t::type());
  }

  flags& add( const attribute::type_id& type ) {
    map_.insert( &type );
    return *this;
  }
  
  template< typename T >
  flags& remove() {
    typedef typename std::enable_if< 
      std::is_base_of< attribute, T >::value, T 
    >::type attribute_t;

    return remove(attribute_t::type());
  }

  flags& remove( const attribute::type_id& type ) {
    map_.erase( &type );
    return *this;
  }
  
  bool empty() const { return map_.empty(); }
  size_t size() const { return map_.size(); }
  void clear() NOEXCEPT { map_.clear(); }
  void reserve(size_t cap) { map_.reserve( cap ); }

  template< typename T >
  bool check() const NOEXCEPT {
    typedef typename std::enable_if< 
      std::is_base_of< attribute, T >::value, T 
    >::type attribute_t;

    return check(attribute_t::type());
  }

  bool check(const attribute::type_id& type) const NOEXCEPT {
    return map_.end() != map_.find( &type );
  }

  bool operator==( const flags& rhs ) const {
    return map_ == rhs.map_;
  }

  bool operator!=( const flags& rhs ) const {
    return !( *this == rhs );
  }

  flags& operator|=( const flags& rhs ) {
    std::for_each(
      rhs.map_.begin(), rhs.map_.end() ,
      [this] ( const attribute::type_id* type ) {
        add( *type );
    } );
    return *this;
  }

  flags operator&( const flags& rhs ) const {
    const type_map* lhs_map = &map_;
    const type_map* rhs_map = &rhs.map_;
    if (lhs_map->size() > rhs_map->size()) {
      std::swap(lhs_map, rhs_map);
    }
    
    flags out;
    out.reserve(lhs_map->size());

    for (auto lhs_it = lhs_map->begin(), lhs_end = lhs_map->end(); lhs_it != lhs_end; ++lhs_it) {
      auto rhs_it = rhs_map->find(*lhs_it);
      if (rhs_map->end() != rhs_it) {
        out.add(**rhs_it);
      }
    }

    return out;
  }

  flags operator|(const flags& rhs) const {
    flags out(*this);
    out.reserve(rhs.size());
    for (auto it = rhs.map_.begin(), end = rhs.map_.end(); it != end; ++it) {
      out.add(**it);
    }
    return out;
  }

  bool is_subset_of(const flags& rhs) const {
    auto& rhs_map = rhs.map_;
    for (auto entry : map_) {
      if (rhs_map.end() == rhs_map.find(entry)) {
        return false;
      }
    } 
    return true;
  } 

 private:
  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  type_map map_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
};
/*
//////////////////////////////////////////////////////////////////////////////
/// @brief common interface for attribute storage implementations
//////////////////////////////////////////////////////////////////////////////
template<typename PTR_T>
class IRESEARCH_API attribute_map {
 public:
  template <typename T>
  class ref: private util::noncopyable {
   public:
    ref(PTR_T ptr = nullptr) NOEXCEPT: ptr_(ptr) {}
    ref(ref<T>&& other) NOEXCEPT: ptr_(std::move(other.ptr_)) {}

    ref<T>& operator=(T&& ptr) NOEXCEPT { ptr_ = std::move(ptr); return *this; }
    ref<T>& operator=(ref<T>&& other) NOEXCEPT { if (this != &other) { ptr_ = std::move(other.ptr_); } return *this; }
    typename std::add_lvalue_reference<T>::type operator*() const NOEXCEPT { return *ptr_; }
    T* operator->() const NOEXCEPT { return ptr_; }
    explicit operator bool() const NOEXCEPT { return nullptr != ptr_; }

    static const ref<T>& nil() NOEXCEPT { static const ref<T> nil; return nil; }

   private:
    friend attribute_map<PTR_T>& attribute_map<PTR_T>::operator=(const attribute_map&);
    PTR_T ptr_;
  };

  attribute_map() = default;

  attribute_map(const attribute_map& other) {
    *this = std::move(other);
  }

  attribute_map(attribute_map&& other) NOEXCEPT {
    *this = std::move(other);
  }

  attribute_map<PTR_T>& operator=(const attribute_map& other) {
    if (this != &other) {
      for (auto& entry: other.map_) {
        map_.emplace(entry.first, entry.second.ptr_);
      }
    }

    return *this;
  }

  attribute_map<PTR_T>& operator=(attribute_map&& other) NOEXCEPT {
    if (this != &other) {
      map_ = std::move(other.map_);
    }

    return *this;
  }

  void clear() {
    map_.clear();
  }

  bool contains(const attribute::type_id& type) const NOEXCEPT {
    return map_.find(type) != map_.end();
  }

  template<typename T>
  inline bool contains() const NOEXCEPT {
    typedef typename std::enable_if<std::is_base_of<attribute, T >::value, T>::type type;
    return contains(type::type());
  }

  flags features() const {
    flags features;

    features.reserve(size());

    for (auto& entry: map_) {
      features.add(*entry.first);
    }

    return features;
  }

  ref<attribute>* get(const attribute::type_id& type) {
    auto itr = map_.find(&type);

    return map_.end() == itr ? nullptr : &(itr->second);
  }

  ref<attribute>& get(const attribute::type_id& type, ref<attribute>& fallback) {
    auto itr = map_.find(&type);

    return map_.end() == itr ? fallback : itr->second;
  }

  const ref<attribute>& get(
      const attribute::type_id& type,
      const ref<attribute>& fallback = ref<attribute>::nil()
  ) const {
    return const_cast<attributes*>(this)->get(type, const_cast<ref<attribute>&>(fallback));
  }

  template<typename T>
  inline ref<T>* get() {
    typedef typename std::enable_if<std::is_base_of<attribute, T>::value, T>::type type;
    auto* value = get(type::type());

    // safe to reinterpret because layout/size is identical
    return reinterpret_cast<ref<type>*>(value);
  }

  template<typename T>
  inline ref<T>& get(ref<T>& fallback) {
    typedef typename std::enable_if<std::is_base_of<attribute, T>::value, T>::type type;
    auto& value = get(type::type(), reinterpret_cast<ref<PTR_T>&>(fallback));

    // safe to reinterpret because layout/size is identical
    return reinterpret_cast<ref<type>&>(value);
  }

  template<typename T>
  inline const ref<T>& get(const ref<T>& fallback = ref<T>::nil()) const {
    typedef typename std::enable_if<std::is_base_of<attribute, T>::value, T>::type type;
    auto& value = get(type::type(), reinterpret_cast<const ref<PTR_T>&>(fallback));

    // safe to reinterpret because layout/size is identical
    return reinterpret_cast<const ref<type>&>(value);
  }

  bool remove(const attribute::type_id& type) {
    return map_.erase(&type) > 0;
  }

  template<typename T>
  inline bool remove() {
    typedef typename std::enable_if<std::is_base_of<attribute, T>::value, T>::type type;

    return remove(type::type());
  }

  template<typename Visitor>
  bool visit(const Visitor& visitor) {
    return visit(*this, visitor);
  }

  template<typename Visitor>
  bool visit(const Visitor& visitor) const {
    return visit(*this, visitor);
  }

  size_t size() const { return map_.size(); }

 protected:
  ref<attribute>& emplace(bool& inserted, const attribute::type_id& type) {
    auto res = map_utils::try_emplace(map, &type);

    inserted = res.second;

    return *(res.first);
  }

 private:
  // std::map<...> is 25% faster than std::unordered_map<...> as per profile_bulk_index test
  typedef std::map<const attribute::type_id*, ref<attribute>> map_t;

  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  map_t map_;
  IRESEARCH_API_PRIVATE_VARIABLES_END

  template<typename Attributes, typename Visitor>
  static bool visit(Attributes& attrs, const Visitor& visitor) {
    for (auto& entry : attrs.map_) {
      if (!visitor(*entry.first, entry.second)) {
        return false;
      }
    }
    return true;
  }
};

//////////////////////////////////////////////////////////////////////////////
/// @brief storage of shared_ptr to attributes
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API attribute_store: public attribute_map<attribute::ptr> {
 public:
  template<typename T, typename... Args>
  ref<T>& emplace(Args&&... args) {
    REGISTER_TIMER_DETAILED();
    typedef typename std::enable_if<std::is_base_of<attribute_base, T>::value, T>::type type;
    bool inserted;
    auto& attr = emplace(inserted, type::type());

    if (inserted) {
      attr = std::move(type::make(std::forward<Args>(args)...));
    }

    return attr;
  }
};

//////////////////////////////////////////////////////////////////////////////
/// @brief storage of data pointers to attributes
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API attribute_view: public attribute_map<void*> {
 public:
  template<typename T>
  ref<T>& emplace(const attribute::type_id& type, T& value) {
    REGISTER_TIMER_DETAILED();
    bool inserted;
    auto& attr = emplace(inserted, type);

    if (inserted) {
      attr = &value;
    }

    return attr;
  }
};
*/
//////////////////////////////////////////////////////////////////////////////
/// @class attribute_ref
/// @brief represents a reference to an attribute
//////////////////////////////////////////////////////////////////////////////
template <typename T>
class IRESEARCH_API_TEMPLATE attribute_ref {
 public:
  attribute_ref(T* value = nullptr) NOEXCEPT;
  attribute_ref(attribute::ptr&& value) NOEXCEPT;
  attribute_ref(attribute_ref<T>&& other) NOEXCEPT;
  virtual ~attribute_ref();

  attribute_ref<T>& operator=(attribute_ref<T>&& other) NOEXCEPT;
  attribute_ref<T>& operator=(const attribute_ref<T>& other) NOEXCEPT;
  typename std::add_lvalue_reference<T>::type operator*() const NOEXCEPT;
  T* operator->() const NOEXCEPT;
  operator T*() const NOEXCEPT;
  explicit operator bool() const NOEXCEPT;
  static const attribute_ref<T>& nil() NOEXCEPT;

 private:
  DECLARE_SPTR(attribute);
  ptr value_ = nullptr;
};

template <typename T>
attribute_ref<T>::attribute_ref(T* value) NOEXCEPT
  : value_(value) {
}

template <typename T>
attribute_ref<T>::attribute_ref(attribute::ptr&& value) NOEXCEPT:
  value_(std::move(value)) {
}

template <typename T>
attribute_ref<T>::attribute_ref(attribute_ref<T>&& other) NOEXCEPT:
  value_(std::move(other.value_)) {
}

template <typename T>
attribute_ref<T>::~attribute_ref() {}

template <typename T>
attribute_ref<T>& attribute_ref<T>::operator=(attribute_ref<T>&& other) NOEXCEPT {
  if (this != &other) {
    value_ = std::move(other.value_);
  }

  return *this;
}

template <typename T>
attribute_ref<T>& attribute_ref<T>::operator=(const attribute_ref<T>& other) NOEXCEPT {
  if (this != &other) {
    value_ = other.value_;
  }

  return *this;
}

template <typename T>
typename std::add_lvalue_reference<T>::type attribute_ref<T>::operator*() const NOEXCEPT {
  #ifdef IRESEARCH_DEBUG
    return dynamic_cast<T&>(*value_);
  #else
    return static_cast<T&>(*value_);
  #endif
}

template <typename T>
T* attribute_ref<T>::operator->() const NOEXCEPT {
  #ifdef IRESEARCH_DEBUG
    return dynamic_cast<T*>(value_.get());
  #else
    return static_cast<T*>(value_.get());
  #endif
}

template <typename T>
attribute_ref<T>::operator T*() const NOEXCEPT {
  return attribute_ref<T>::operator->();
}

template <typename T>
attribute_ref<T>::operator bool() const NOEXCEPT {
  return value_ ? true : false;
}

template <typename T>
/*static*/ const attribute_ref<T>& attribute_ref<T>::nil() NOEXCEPT {
  static const attribute_ref<T> nil;

  return nil;
}

//////////////////////////////////////////////////////////////////////////////
/// @class attributes
/// @brief represents a set attributes related to the particular object 
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API attributes : private util::noncopyable {
 public:
  static const attributes& empty_instance();

  attributes() = default;
  attributes(size_t reserve);
  attributes(attributes&& rhs) NOEXCEPT;

  attributes& operator=(attributes&& rhs) NOEXCEPT;

  const attribute_ref<attribute>& get(const attribute::type_id& type) const;
  attribute_ref<attribute>& get(const attribute::type_id& type);
  void remove( const attribute::type_id& type );
  void clear();

  size_t size() const { return map_.size(); }
  void clear_state();

  flags features() const { 
    flags features;
    features.reserve( size() );
    std::for_each( 
      map_.begin(), map_.end(), 
      [&features] ( const attributes_map::value_type& vt ) {
        features.add( *vt.first );
    } );
    return features;
  }

  inline bool contains( const attribute::type_id& type ) const NOEXCEPT{
    return get(type) ? true : false;
  }

  template< typename T >
  inline bool contains() const NOEXCEPT {
    typedef typename std::enable_if<
    std::is_base_of< attribute, T >::value, T
    >::type type;

    return contains(type::type());
  }

  template< typename T >
  inline attribute_ref<T>& get() {
    typedef typename std::enable_if< 
      std::is_base_of< attribute, T >::value, T 
    >::type type;

    auto& value = get(type::type());

    // safe to reinterpret because layout/size is identical
    return reinterpret_cast<attribute_ref<type>&>(value);
  }

  template< typename T >
  inline const attribute_ref<T>& get() const {
    typedef typename std::enable_if< 
      std::is_base_of< attribute, T >::value, T 
    >::type type;

    auto& value = get(type::type());

    // safe to reinterpret because layout/size is identical
    return reinterpret_cast<const attribute_ref<type>&>(value);
  }

  template< typename T >
  inline attribute_ref<T>& add() {
    typedef typename std::enable_if< 
      std::is_base_of< attribute, T >::value, T 
    >::type type;
    REGISTER_TIMER_DETAILED();
    const attribute::type_id* type_id = &type::type();
    auto it = map_.find(type_id);

    if (map_.end() == it) {
      REGISTER_TIMER_DETAILED();
      it = map_.emplace(type_id, type::make()).first;
    }

    auto& value = it->second;

    return reinterpret_cast<attribute_ref<type>&>(value);
  }

  template< typename T >
  inline void remove() {
    typedef typename std::enable_if< 
      std::is_base_of< attribute, T >::value, T 
    >::type type;

    remove(type::type());
  }
  
  template<typename Visitor>
  bool visit(const Visitor& visitor) {
    return visit(*this, visitor);
  }
  
  template<typename Visitor>
  bool visit(const Visitor& visitor) const {
    return visit(*this, visitor);
  }

 protected:
  attribute_ref<attribute>& add(const attribute::type_id& type);

 private:
  // std::map<...> is 25% faster than std::unordered_map<...> as per profile_bulk_index test
  typedef std::map<
    const attribute::type_id*,
    attribute_ref<attribute>
  > attributes_map;

  template<typename Attributes, typename Visitor>
  static bool visit(Attributes& attrs, const Visitor& visitor) {
    for (auto& entry : attrs.map_) {
      if (!visitor(*entry.first, entry.second)) {
        return false;
      }
    }
    return true;
  }

  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  attributes_map map_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
};

template< typename T >
inline T* copy_attribute( attributes& dst, const attributes& src ) {
  typedef typename std::enable_if <
    std::is_base_of< attribute, T >::value, T
  > ::type type;

  type* dsta = nullptr;
  const type* srca = src.get<type>();
  if( srca) {
    *(dsta = dst.add<type>()) = *srca;
  }

  return dsta;
}

NS_END

#endif