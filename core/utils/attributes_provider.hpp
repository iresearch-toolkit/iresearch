////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_ATTRIBUTES_PROVIDER_H
#define IRESEARCH_ATTRIBUTES_PROVIDER_H

#include "type_id.hpp"

NS_ROOT

class attribute_store;
class attribute_view;
struct attribute;

NS_BEGIN(util)

struct IRESEARCH_API attributes_provider {
  virtual const attribute* get(type_info::type_id type) const = 0;
};

//////////////////////////////////////////////////////////////////////////////
/// @class const_attribute_store_provider
/// @brief base class for all objects with externally visible attribute_store
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API const_attribute_store_provider {
 public:
  virtual ~const_attribute_store_provider() = default;
  virtual const irs::attribute_store& attributes() const noexcept = 0;
};

//////////////////////////////////////////////////////////////////////////////
/// @class attribute_store_provider
/// @brief base class for all objects with externally visible attribute-store
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API attribute_store_provider: public const_attribute_store_provider {
 public:
  virtual irs::attribute_store& attributes() noexcept = 0;
  virtual const irs::attribute_store& attributes() const noexcept override final {
    return const_cast<attribute_store_provider*>(this)->attributes();
  };
};

//////////////////////////////////////////////////////////////////////////////
/// @class const_attribute_view_provider
/// @brief base class for all objects with externally visible attribute_view
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API const_attribute_view_provider {
 public:
  virtual ~const_attribute_view_provider() = default;
  virtual const irs::attribute_view& attributes() const noexcept = 0;
};

//////////////////////////////////////////////////////////////////////////////
/// @class attributes_provider
/// @brief base class for all objects with externally visible attribute_view
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API attribute_view_provider: public const_attribute_view_provider {
 public:
  virtual irs::attribute_view& attributes() noexcept = 0;
  virtual const irs::attribute_view& attributes() const noexcept override final {
    return const_cast<attribute_view_provider*>(this)->attributes();
  };
};

NS_END

template<typename T,
         typename = std::enable_if_t<std::is_base_of_v<attribute, T>>>
const T* get(const util::attributes_provider& attrs) {
  return static_cast<const T*>(attrs.get(type<T>::id()));
}

NS_END

#endif
