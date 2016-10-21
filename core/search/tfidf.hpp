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

#ifndef IRESEARCH_TFIDF_H
#define IRESEARCH_TFIDF_H

#include "scorers.hpp"

NS_ROOT

class tfidf_sort : public sort {
public:
  DECLARE_SORT_TYPE();
  DECLARE_FACTORY_DEFAULT();

  typedef float_t score_t;

  tfidf_sort();

  virtual sort::prepared::ptr prepare() const;
};

NS_END

#endif