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

#include "shared.hpp"
#include "utils/register.hpp"
#include "attributes.hpp"

#include <cassert>

NS_LOCAL

class attribute_register:
  public iresearch::generic_register<iresearch::string_ref, const iresearch::attribute::type_id*, attribute_register> {
};

NS_END

NS_ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                                         attribute
// -----------------------------------------------------------------------------

attribute::~attribute() { }

// -----------------------------------------------------------------------------
// --SECTION--                                                attribute::type_id
// -----------------------------------------------------------------------------

/*static*/ const attribute::type_id* attribute::type_id::get(
  const string_ref& name
) {
  return attribute_register::instance().get(name);
}

// -----------------------------------------------------------------------------
// --SECTION--                                                             flags 
// -----------------------------------------------------------------------------

const flags& flags::empty_instance() {
  static flags instance;
  return instance;
}

flags::flags() { }

flags::flags(flags&& rhs) NOEXCEPT
  : map_( std::move( rhs.map_ ) ) {
}

flags& flags::operator=(flags&& rhs) NOEXCEPT {
  if ( this != &rhs ) {
    map_ = std::move( rhs.map_ );
  }

  return *this;
}

flags::flags( std::initializer_list<const attribute::type_id* > flags ) {
  std::for_each( 
    flags.begin(), flags.end(), 
    [this]( const attribute::type_id* type) {
      add( *type );
  } );
}

flags& flags::operator=( std::initializer_list<const attribute::type_id* > flags ) {
  map_.clear();
  std::for_each( 
    flags.begin(), flags.end(), 
    [this]( const attribute::type_id* type) {
      add( *type );
  } );
  return *this;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                        attributes
// -----------------------------------------------------------------------------
 
const attributes& attributes::empty_instance() {
  static attributes instance(0);
  return instance;
}

attributes::attributes(size_t /*reserve*/) {
}

attributes::attributes(attributes&& rhs) NOEXCEPT
  : map_(std::move(rhs.map_)) {
}

attributes& attributes::operator=(attributes&& rhs) NOEXCEPT {
  if ( this != &rhs ) {
    map_ = std::move( rhs.map_ );
  }

  return *this;
}

attribute_ref<attribute>& attributes::get( const attribute::type_id& type ) {
  static attribute_ref<attribute> EMPTY; // const because no declared modifier methods
  attributes_map::iterator it = map_.find( &type );
  return map_.end() == it ? EMPTY : it->second;
}

void attributes::remove( const attribute::type_id& type ) {
  map_.erase( &type );
}
  
void attributes::clear() {
  map_.clear();
}

const attribute_ref<attribute>& attributes::get(const attribute::type_id& type) const {
  return const_cast< attributes* >( this )->get(type);
}

void attributes::clear_state() {
  for (auto& key_val : map_) {
    key_val.second->clear();
  }
}

attribute_ref<attribute>& attributes::add(const attribute::type_id& type) {
  return map_[&type];
}

// -----------------------------------------------------------------------------
// --SECTION--                                            attribute registration
// -----------------------------------------------------------------------------

attribute_registrar::attribute_registrar(const attribute::type_id& type) {
  attribute_register::instance().set(type.name(), &type);
}

NS_END