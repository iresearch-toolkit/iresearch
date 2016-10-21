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

#include "shared.hpp"
#include "field_meta.hpp"

#include "utils/thread_utils.hpp"

#include "error/error.hpp"

NS_ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                         field_meta implementation 
// -----------------------------------------------------------------------------

field_meta::field_meta(field_meta&& rhs)
  : features(std::move(rhs.features)),
    name(std::move(rhs.name)),
    id(rhs.id) {
  rhs.id = type_limits<type_t::field_id_t>::invalid();
}

field_meta::field_meta(
    const string_ref& name,
    field_id id,
    const flags& features) 
  : features(features),
    name(name.c_str(), name.size()),
    id(id) {
}

field_meta& field_meta::operator=(field_meta&& rhs) {
  if (this != &rhs) {
    features = std::move(rhs.features);
    name = std::move(rhs.name);
    id = rhs.id;
    rhs.id = type_limits<type_t::field_id_t>::invalid();
  }
  return *this;
}

bool field_meta::operator==(const field_meta& rhs) const {
  return features == rhs.features && name == rhs.name;
}

// -----------------------------------------------------------------------------
// --SECTION--                                        fields_meta implementation 
// -----------------------------------------------------------------------------

fields_meta::fields_meta(fields_meta::by_id_map_t&& fields) {
  flags features;
  by_name_map_t name_to_meta;
  name_to_meta.reserve(fields.size());
  string_ref_hash_t hasher;
  
  for (auto& field : fields) {
    name_to_meta.emplace(
      make_hashed_ref(string_ref(field.name), hasher), 
      &field
    );
    features |= field.features;
  }

  // noexcept
  id_to_meta_ = std::move(fields);
  name_to_meta_ = std::move(name_to_meta);
  features_ = std::move(features);
}

fields_meta::fields_meta(fields_meta&& rhs)
  : id_to_meta_(std::move(rhs.id_to_meta_)),
    name_to_meta_(std::move(rhs.name_to_meta_)),
    features_(std::move(rhs.features_)) {
}

fields_meta& fields_meta::operator=(fields_meta&& rhs) {
  if (this != &rhs) {
    id_to_meta_ = std::move(rhs.id_to_meta_);
    name_to_meta_ = std::move(rhs.name_to_meta_);
    features_ = std::move(rhs.features_);
  }
  return *this;
}

const field_meta* fields_meta::find(field_id id) const {
  if (id >= id_to_meta_.size()) {
    return nullptr;
  }

  return &id_to_meta_[id];
}

const field_meta* fields_meta::find(const string_ref& name) const {
  auto it = name_to_meta_.find(make_hashed_ref(name, string_ref_hash_t()));
  return name_to_meta_.end() == it ? nullptr : it->second;
}

NS_END