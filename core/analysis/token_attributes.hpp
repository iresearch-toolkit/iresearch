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
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_TOKEN_ATTRIBUTES_H
#define IRESEARCH_TOKEN_ATTRIBUTES_H

#include "store/data_input.hpp"

#include "index/index_reader.hpp"
#include "index/iterators.hpp"

#include "utils/attributes.hpp"
#include "utils/attributes_provider.hpp"
#include "utils/string.hpp"
#include "utils/type_limits.hpp"
#include "utils/iterator.hpp"

NS_ROOT

//////////////////////////////////////////////////////////////////////////////
/// @class offset 
/// @brief represents token offset in a stream 
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API offset final : attribute {
  static constexpr string_ref type_name() noexcept { return "offset"; }

  static const uint32_t INVALID_OFFSET = integer_traits< uint32_t >::const_max;

  void clear() {
    start = 0;
    end = 0;
  }

  uint32_t start{ 0 };
  uint32_t end{ 0 };
};

//////////////////////////////////////////////////////////////////////////////
/// @class increment 
/// @brief represents token increment in a stream 
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API increment final : basic_attribute<uint32_t> {
  static constexpr string_ref type_name() noexcept { return "increment"; }

  increment() noexcept;

  void clear() { value = 1U; }
};

//////////////////////////////////////////////////////////////////////////////
/// @class term_attribute 
/// @brief represents term value in a stream 
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API term_attribute final : basic_attribute<bytes_ref> {
  static constexpr string_ref type_name() noexcept { return "term_attribute"; }
};

//////////////////////////////////////////////////////////////////////////////
/// @class payload
/// @brief represents an arbitrary byte sequence associated with
///        the particular term position in a field
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API payload final : basic_attribute<bytes_ref> {
  // DO NOT CHANGE NAME
  static constexpr string_ref type_name() noexcept { return "payload"; }

  void clear() {
    value = bytes_ref::NIL;
  }
};

//////////////////////////////////////////////////////////////////////////////
/// @class document 
/// @brief contains a document identifier
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API document final : basic_attribute<doc_id_t> {
  // DO NOT CHANGE NAME
  static constexpr string_ref type_name() noexcept { return "document"; }

  explicit document(irs::doc_id_t doc = irs::doc_limits::invalid()) noexcept
    : basic_attribute<doc_id_t>(doc) {
  }
};

//////////////////////////////////////////////////////////////////////////////
/// @class frequency 
/// @brief how many times term appears in a document
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API frequency final : basic_attribute<uint32_t> {
  // DO NOT CHANGE NAME
  static constexpr string_ref type_name() noexcept { return "frequency"; }

  frequency() = default;
}; // frequency

//////////////////////////////////////////////////////////////////////////////
/// @class granularity_prefix
/// @brief indexed tokens are prefixed with one byte indicating granularity
///        this is marker attribute only used in field::features and by_range
///        exact values are prefixed with 0
///        the less precise the token the greater its granularity prefix value
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API granularity_prefix final : attribute {
  // DO NOT CHANGE NAME
  static constexpr string_ref type_name() noexcept {
    return "iresearch::granularity_prefix";
  }

  granularity_prefix() = default;
}; // granularity_prefix

//////////////////////////////////////////////////////////////////////////////
/// @class norm
/// @brief this marker attribute is only used in field::features in order to
///        allow evaluation of the field normalization factor 
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API norm final : stored_attribute {
  // DO NOT CHANGE NAME
  static constexpr string_ref type_name() noexcept {
    return "norm";
  }

  DECLARE_FACTORY();

  FORCE_INLINE static constexpr float_t DEFAULT() {
    return 1.f;
  }

  norm() noexcept;
  norm(norm&& rhs) noexcept;
  norm& operator=(norm&& rhs) noexcept;

  bool reset(const sub_reader& segment, field_id column, const document& doc);
  float_t read() const;
  bool empty() const;

  void clear() {
    reset();
  }

 private:
  void reset();

  //columnstore_reader::values_reader_f column_;
  doc_iterator::ptr column_it_;
  irs::pointer_wrapper<payload> payload_{ nullptr };
  const document* doc_;
}; // norm

//////////////////////////////////////////////////////////////////////////////
/// @class position 
/// @brief iterator represents term positions in a document
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API position
  : public attribute,
    public util::const_attribute_view_provider {
 public:
  typedef uint32_t value_t;

  DECLARE_REFERENCE(position);

  // DO NOT CHANGE NAME
  static constexpr string_ref type_name() noexcept { return "position"; }

  static irs::position* extract(const attribute_view& attrs) noexcept {
    return attrs.get<irs::position>().get();
  }

  const irs::attribute_view& attributes() const noexcept override {
    return attrs_;
  }

  value_t seek(value_t target) {
    while ((value_< target) && next());
    return value_;
  }

  value_t value() const noexcept {
    return value_;
  }

  virtual void reset() = 0;

  virtual bool next() = 0;

 protected:
  position(size_t reserve_attrs) noexcept;

  value_t value_{ pos_limits::invalid() };
  attribute_view attrs_;
}; // position

NS_END // ROOT

#endif
