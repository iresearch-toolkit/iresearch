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

// list of statically loaded scorers via init()
#ifndef IRESEARCH_DLL
  #include "tfidf.hpp"
  #include "bm25.hpp"
#endif

#include "utils/register.hpp"
#include "scorers.hpp"

NS_LOCAL

const std::string FILENAME_PREFIX("libscorer-");

class scorer_register:
  public iresearch::generic_register<iresearch::string_ref, iresearch::sort::ptr(*)(), scorer_register> {
 protected:
  virtual std::string key_to_filename(const key_type& key) const override {
    std::string filename(FILENAME_PREFIX.size() + key.size(), 0);

    std::memcpy(&filename[0], FILENAME_PREFIX.c_str(), FILENAME_PREFIX.size());
    std::memcpy(&filename[0] + FILENAME_PREFIX.size(), key.c_str(), key.size());

    return filename;
  }
};

NS_END

NS_ROOT

/*static*/ sort::ptr scorers::get(const string_ref& name) {
  auto* factory = scorer_register::instance().get(name);
  return factory ? factory() : nullptr;
}

/*static*/ void scorers::init() {
  #ifndef IRESEARCH_DLL
    REGISTER_SCORER(iresearch::tfidf_sort);
    REGISTER_SCORER(iresearch::bm25_sort);
  #endif
}

/*static*/ void scorers::load_all(const std::string& path) {
  load_libraries(path, FILENAME_PREFIX, "");
}

/*static*/ bool scorers::visit(
  const std::function<bool(const string_ref&)>& visitor
) {
  return scorer_register::instance().visit(visitor);
}

// -----------------------------------------------------------------------------
// --SECTION--                                               scorer registration
// -----------------------------------------------------------------------------

scorer_registrar::scorer_registrar(
  const sort::type_id& type, sort::ptr(*factory)()
) {
  scorer_register::instance().set(type.name(), factory);
}

NS_END