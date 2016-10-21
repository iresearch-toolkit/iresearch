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

NS_ROOT

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
class IRESEARCH_API fields_meta : util::noncopyable {
 public:
  typedef std::vector<field_meta> by_id_map_t;

  fields_meta() = default;
  fields_meta(by_id_map_t&& fields);
  fields_meta(fields_meta&& rhs);
  fields_meta& operator=(fields_meta&& rhs);

  const field_meta* find(field_id id) const;
  const field_meta* find(const string_ref& name) const;

  size_t size() const { return id_to_meta_.size(); }
  bool empty() const { return id_to_meta_.empty(); }
  const flags& features() const { return features_; }

 private:
  typedef std::unordered_map<hashed_string_ref, const field_meta*> by_name_map_t;

  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  by_id_map_t id_to_meta_;
  by_name_map_t name_to_meta_;
  flags features_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
}; // fields_meta 

NS_END

#endif