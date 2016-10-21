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

#ifndef IRESEARCH_ATTRIBUTES_PROVIDER_H
#define IRESEARCH_ATTRIBUTES_PROVIDER_H

#include "shared.hpp"

NS_ROOT

class attributes;

NS_BEGIN(util)

//////////////////////////////////////////////////////////////////////////////
/// @class const_attributes_provider
/// @brief base class for all objects with externally visible attributes
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API const_attributes_provider {
 public:
  virtual ~const_attributes_provider() {}
  virtual const iresearch::attributes& attributes() const = 0;
};

//////////////////////////////////////////////////////////////////////////////
/// @class attributes_provider
/// @brief base class for all objects with externally visible attributes
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API attributes_provider: public const_attributes_provider {
 public:
  virtual iresearch::attributes& attributes() = 0;
  virtual const iresearch::attributes& attributes() const override final {
    return const_cast<attributes_provider*>(this)->attributes();
  };
};

NS_END
NS_END

#endif
