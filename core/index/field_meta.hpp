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

#ifndef IRESEARCH_FIELD_META_H
#define IRESEARCH_FIELD_META_H

#include "store/data_output.hpp"

#include "utils/attributes.hpp"
#include "utils/noncopyable.hpp"
#include "utils/hash_utils.hpp"
#include "utils/type_limits.hpp"

#include <unordered_map>
#include <map>
#include <mutex>

#include <boost/iterator/iterator_facade.hpp>

NS_ROOT

template<typename T>
class multi_index : util::noncopyable {
 public:
  typedef T value_type;
  typedef std::vector<value_type> items_t;
  typedef std::map<string_ref, const value_type*> by_name_sorted_t;

  class iterator : std::iterator<std::bidirectional_iterator_tag, value_type> {
   public:
     iterator(typename by_name_sorted_t::const_iterator it) : it_(it) { }
     iterator(const iterator&) = default;
     iterator& operator=(const iterator&) = default;

     iterator& operator++() { 
       ++it_; 
       return *this; 
     }

     iterator operator++(int) { 
       auto tmp = *this;
       ++*this;
       return tmp;
     }

     iterator& operator--() { 
       --it_; 
       return *this; 
     }

     iterator operator--(int) {
       auto tmp = *this;
       --*this;
       return tmp;
     }

     const value_type& operator*() const {
       return *it_->second;
     }

     const value_type* operator->() const {
       return it_->second;
     }

     bool operator==(const iterator& rhs) const {
       return it_ == rhs.it_;
     }
     
     bool operator!=(const iterator& rhs) const {
       return !(*this == rhs);
     }

   private:
    typename by_name_sorted_t::const_iterator it_;
  }; // iterator

  multi_index() = default;

  multi_index(items_t&& items) {
    by_name_t by_name;
    by_name.reserve(items.size());
    by_name_sorted_t by_name_sorted;
    string_ref_hash_t hasher;

    for (auto& item: items) {
      string_ref ref(item.name);

      by_name.emplace(
        make_hashed_ref(ref, hasher),
        &item
      );

      by_name_sorted.emplace(
        ref, &item
      );
    }

    // noexcept
    items_ = std::move(items);
    by_name_ = std::move(by_name);
    by_name_sorted_ = std::move(by_name_sorted);
  }

  multi_index(multi_index&& rhs) 
    : items_(std::move(rhs.items_)),
      by_name_(std::move(rhs.by_name_)),
      by_name_sorted_(std::move(rhs.by_name_sorted_)) { 
  }

  multi_index& operator=(multi_index&& rhs) {
    if (this != &rhs) {
      items_ = std::move(rhs.items_);
      by_name_ = std::move(rhs.by_name_);
      by_name_sorted_ = std::move(rhs.by_name_sorted_);
    }
    return *this;
  }

  const value_type* find(field_id id) const {
    if (id >= items_.size()) {
      return nullptr;
    }
    return &items_[id];
  }

  const value_type* find(const string_ref& name) const {
    auto it = by_name_.find(make_hashed_ref(name, string_ref_hash_t()));
    return by_name_.end() == it ? nullptr : it->second;
  }

  iterator begin() const { return iterator(by_name_sorted_.begin()); }
  iterator end() const { return iterator(by_name_sorted_.end()); }

  size_t size() const { return items_.size(); }
  bool empty() const { return items_.empty(); }

 private:
  typedef std::unordered_map<hashed_string_ref, const value_type*> by_name_t;

  items_t items_;
  by_name_t by_name_;
  by_name_sorted_t by_name_sorted_;
}; // multi_index

//////////////////////////////////////////////////////////////////////////////
/// @struct field_meta 
/// @brief represents field metadata
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API field_meta { 
 public:
  field_meta() = default;
  field_meta(const field_meta&) = default;
  field_meta(field_meta&& rhs);
  field_meta(const string_ref& field, field_id id, const flags& features);

  field_meta& operator=(field_meta&& rhs);
  field_meta& operator=(const field_meta&) = default;

  bool operator==(const field_meta& rhs) const;
  bool operator!=(const field_meta& rhs) const {
    return !(*this == rhs);
  }

  flags features;
  std::string name;
  field_id id{ type_limits<type_t::field_id_t>::invalid() };
}; // field_meta

//////////////////////////////////////////////////////////////////////////////
/// @class fields_meta 
/// @brief a container for fields metadata
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API fields_meta : public multi_index<field_meta> {
 public:
  typedef multi_index<field_meta> base_t;

  fields_meta() = default;
  fields_meta(base_t::items_t&& fields, flags&& features);
  fields_meta(fields_meta&& rhs);
  fields_meta& operator=(fields_meta&& rhs);

  const flags& features() const { return features_; }

 private:
  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  flags features_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
}; // fields_meta 

NS_END

#endif