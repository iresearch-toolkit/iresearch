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

#include "shared.hpp"
#include "token_attributes.hpp"
#include "store/store_utils.hpp"

////////////////////////////////////////////////////////////////////////////////
/// !!! DO NOT MODIFY value in DEFINE_ATTRIBUTE_TYPE(...) as it may break
/// already created indexes !!!
/// FIXME: write test
////////////////////////////////////////////////////////////////////////////////

NS_ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                                            offset
// -----------------------------------------------------------------------------

REGISTER_ATTRIBUTE(offset);

// -----------------------------------------------------------------------------
// --SECTION--                                                         increment
// -----------------------------------------------------------------------------

REGISTER_ATTRIBUTE(increment);

increment::increment() noexcept
  : basic_attribute<uint32_t>(1U) {
}

// -----------------------------------------------------------------------------
// --SECTION--                                                    term_attribute
// -----------------------------------------------------------------------------

REGISTER_ATTRIBUTE(term_attribute);

// -----------------------------------------------------------------------------
// --SECTION--                                                           payload
// -----------------------------------------------------------------------------

REGISTER_ATTRIBUTE(payload);

// -----------------------------------------------------------------------------
// --SECTION--                                                          document
// -----------------------------------------------------------------------------

REGISTER_ATTRIBUTE(document);

// -----------------------------------------------------------------------------
// --SECTION--                                                         frequency
// -----------------------------------------------------------------------------

REGISTER_ATTRIBUTE(frequency);

// -----------------------------------------------------------------------------
// --SECTION--                                                granularity_prefix
// -----------------------------------------------------------------------------

REGISTER_ATTRIBUTE(iresearch::granularity_prefix);

// -----------------------------------------------------------------------------
// --SECTION--                                                              norm
// -----------------------------------------------------------------------------

REGISTER_ATTRIBUTE(norm);

const document INVALID_DOCUMENT;

norm::norm() noexcept {
  reset();
}

norm::norm(norm&& rhs) noexcept
  : column_(std::move(rhs.column_)),
    doc_(rhs.doc_) {
  rhs.doc_ = nullptr;
}

norm& norm::operator=(norm&& rhs) noexcept {
  if (this != &rhs) {
    column_ = std::move(rhs.column_);
    doc_ = rhs.doc_;
    rhs.doc_ = nullptr;
  }
  return *this;
}

void norm::reset() {
  column_ = [](doc_id_t, bytes_ref&){ return false; };
  doc_ = &INVALID_DOCUMENT;
}

bool norm::empty() const {
  return doc_ == &INVALID_DOCUMENT;
}

bool norm::reset(const sub_reader& reader, field_id column, const document& doc) {
  const auto* column_reader = reader.column_reader(column);

  if (!column_reader) {
    return false;
  }

  column_ = column_reader->values();
  doc_ = &doc;
  return true;
}

float_t norm::read() const {
  bytes_ref value;
  if (!column_(doc_->value, value)) {
    return DEFAULT();
  }

  // TODO: create set of helpers to decode float from buffer directly
  bytes_ref_input in(value);
  return read_zvfloat(in);
}

// -----------------------------------------------------------------------------
// --SECTION--                                                          position
// -----------------------------------------------------------------------------

REGISTER_ATTRIBUTE(position);

position::position(size_t reserve_attrs) noexcept
  : attrs_(reserve_attrs) {
}

NS_END
