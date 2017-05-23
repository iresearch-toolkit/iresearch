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
#include "type_utils.hpp"
#include "bit_utils.hpp"
#include "type_id.hpp"
#include "noncopyable.hpp"
#include "string.hpp"

#include <unordered_set>

NS_ROOT

//////////////////////////////////////////////////////////////////////////////
/// @class attribute 
/// @brief base class for all attributes
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API attribute {

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

//////////////////////////////////////////////////////////////////////////////
/// @class attribute_ref
/// @brief represents a reference to an attribute
//////////////////////////////////////////////////////////////////////////////
class attributes; // forward declaration
template <typename T>
class IRESEARCH_API_TEMPLATE attribute_ref: private util::noncopyable {
 public:
  attribute_ref(T* ptr = nullptr) NOEXCEPT; // a reference to an external object
  attribute_ref(attribute_ref<T>&& other) NOEXCEPT;
  virtual ~attribute_ref();

  attribute_ref<T>& operator=(T* ptr) NOEXCEPT; // a reference to an external object
  attribute_ref<T>& operator=(attribute_ref<T>&& other) NOEXCEPT;

  typename std::add_lvalue_reference<T>::type operator*() const NOEXCEPT;
  T* operator->() const NOEXCEPT; // NOTE!!! ptr invalidated if attribute_ref is moved into a different underlying buffer
  explicit operator bool() const NOEXCEPT;
  static const attribute_ref<T>& nil() NOEXCEPT;

 private:
  struct type_traits_t {
    void(*destruct)(void* ptr);
    attribute*(*move)(void* dst, void* src);
  };

  friend attributes; // for in-buffer allocated attribute constructor and move
  size_t buf_offset_;
  T* ptr_;
  const type_traits_t* type_traits_;

  attribute_ref(bstring& buf, size_t buf_offset, const type_traits_t& traits) NOEXCEPT;
  void move(bstring& dst) NOEXCEPT;
  static const type_traits_t& traits_instance(); // instance of traits for use with external references
};

template <typename T>
attribute_ref<T>::attribute_ref(T* ptr /*= nullptr*/) NOEXCEPT
  : ptr_(ptr), type_traits_(&traits_instance()) {
}

template <typename T>
attribute_ref<T>::attribute_ref(attribute_ref<T>&& other) NOEXCEPT
  : type_traits_(&traits_instance()) {
  *this = std::move(other);
}

template <typename T>
attribute_ref<T>::attribute_ref(
    bstring& buf,
    size_t buf_offset,
    const type_traits_t& traits
) NOEXCEPT
  : buf_offset_(buf_offset),
    ptr_(reinterpret_cast<attribute*>(&buf[buf_offset])),
    type_traits_(&traits) {
  assert(buf_offset_ + sizeof(T) <= buf.size()); // value within boundaries of the new buf
}

template <typename T>
attribute_ref<T>::~attribute_ref() {
  type_traits_->destruct(ptr_);
}

template <typename T>
attribute_ref<T>& attribute_ref<T>::operator=(T* ptr) NOEXCEPT {
  if (ptr_ != ptr) {
    type_traits_->destruct(ptr_);
    ptr_ = ptr;
    type_traits_ = &traits_instance();
  }

  return *this;
}

template <typename T>
attribute_ref<T>& attribute_ref<T>::operator=(attribute_ref<T>&& other) NOEXCEPT {
  if (this != &other) {
    type_traits_->destruct(ptr_);
    buf_offset_ = std::move(other.buf_offset_);
    ptr_ = std::move(other.ptr_);
    type_traits_ = std::move(other.type_traits_);
    other.ptr_ = nullptr;
    other.type_traits_ = &traits_instance();
  }

  return *this;
}

template <typename T>
typename std::add_lvalue_reference<T>::type attribute_ref<T>::operator*() const NOEXCEPT {
  return *ptr_;
}

template <typename T>
T* attribute_ref<T>::operator->() const NOEXCEPT {
  return ptr_;
}

template <typename T>
attribute_ref<T>::operator bool() const NOEXCEPT {
  return nullptr != ptr_;
}

template <typename T>
/*static*/ const attribute_ref<T>& attribute_ref<T>::nil() NOEXCEPT {
  static const attribute_ref<T> nil;

  return nil;
}

template <typename T>
void attribute_ref<T>::move(bstring& dst) NOEXCEPT {
  ptr_ = type_traits_->move(&dst[buf_offset_], ptr_);
}

template <typename T>
/*static*/ const typename attribute_ref<T>::type_traits_t& attribute_ref<T>::traits_instance() {
  static const struct type_traits_impl: public type_traits_t {
    type_traits_impl() {
      type_traits_t::destruct = [](void*)->void {};
      type_traits_t::move = [](void*, void* src)->attribute* { return reinterpret_cast<attribute*>(src); };
    }
  } type_traits;

  return type_traits;
}

//////////////////////////////////////////////////////////////////////////////
/// @class attributes
/// @brief represents a set attributes related to the particular object 
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API attributes : private util::noncopyable {
 public:
  static const attributes& empty_instance();

  attributes() = default;
  attributes(attributes&& rhs) NOEXCEPT;

  attributes& operator=(attributes&& rhs) NOEXCEPT;

  attribute_ref<attribute>* get(const attribute::type_id& type);
  attribute_ref<attribute>& get(const attribute::type_id& type, attribute_ref<attribute>& fallback);
  const attribute_ref<attribute>& get(const attribute::type_id& type, const attribute_ref<attribute>& fallback = attribute_ref<attribute>::nil()) const;
  void remove( const attribute::type_id& type );

  //////////////////////////////////////////////////////////////////////////////
  /// @brief ordered list of types to reserve space for
  ///        type order should match calls to add<type>(...)
  //////////////////////////////////////////////////////////////////////////////
  template<typename... Types>
  void reserve() {
    buf_.reserve((std::max)(buf_.capacity(), template_traits_t<Types...>::size_aligned()));
    //map_.reserve((std::max)(map_.capacity(), template_traits_t<Types...>::count()));
  }

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

  template<typename T>
  inline attribute_ref<T>* get() {
    typedef typename std::enable_if<std::is_base_of<attribute, T>::value, T>::type type;
    auto* value = get(type::type());

    // safe to reinterpret because layout/size is identical
    return reinterpret_cast<attribute_ref<type>*>(value);
  }

  template<typename T>
  inline attribute_ref<T>& get(attribute_ref<T>& fallback) {
    typedef typename std::enable_if<std::is_base_of<attribute, T>::value, T>::type type;
    auto& value = get(type::type(), reinterpret_cast<attribute_ref<attribute>&>(fallback));

    // safe to reinterpret because layout/size is identical
    return reinterpret_cast<attribute_ref<type>&>(value);
  }

  template<typename T>
  inline const attribute_ref<T>& get(
      const attribute_ref<T>& fallback = attribute_ref<T>::nil()
  ) const {
    typedef typename std::enable_if<std::is_base_of<attribute, T>::value, T>::type type;
    auto& value = get(type::type(), reinterpret_cast<const attribute_ref<attribute>&>(fallback));

    // safe to reinterpret because layout/size is identical
    return reinterpret_cast<const attribute_ref<type>&>(value);
  }

  template< typename T >
  inline attribute_ref<T>& add() {
    static_assert(ALIGNOF(T) <= ALIGNOF(bstring), "alignof(T) > alignof(string), use smaller allignment for T, e.g. allocate complex types on heap");
    typedef typename std::enable_if< 
      std::is_base_of< attribute, T >::value, T 
    >::type type;
    REGISTER_TIMER_DETAILED();
    const attribute::type_id* type_id = &type::type();
    auto it = map_.find(type_id);

    if (map_.end() == it) {
      REGISTER_TIMER_DETAILED();
      auto size = template_traits_t<type>::size_aligned(buf_.size());

      if (buf_.capacity() >= size) {
        // underlying buffer will not get reallocated
        #ifdef IRESEARCH_DEBUG
          auto* ptr = buf_.data();
          buf_.resize(size);
          assert(buf_.data() == ptr);
        #else
          buf_.resize(size);
        #endif // IRESEARCH_DEBUG
      } else {
        // buffer needs to grow (should not happen if size properly reserved via constructor)
        bstring buf;

        buf.resize(size);

        // move all existing attributes to newly allocated storage
        // underlying array garanteed not to be changed during swap since already larger than short-string optimization
        for (auto& entry: map_) {
          entry.second.move(buf);
        }

        buf_.swap(buf);
      }

      auto type_start = buf_.size() - template_traits_t<type>::size();
      static const struct type_traits_impl: public attribute_ref<attribute>::type_traits_t {
        type_traits_impl() NOEXCEPT {
          destruct = [](void* ptr)->void { if (ptr) { reinterpret_cast<type*>(ptr)->~type(); } };
          move = [](void* dst, void* src)->attribute* { return new(dst) type(std::move(*reinterpret_cast<type*>(src))); };
        }
      } traits_instance;

      new(&buf_[type_start]) type();
      it = map_.emplace(
        type_id,
        attribute_ref<attribute>(buf_, type_start, traits_instance)
      ).first;
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
  bstring buf_;
  attributes_map map_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
};

NS_END

#endif