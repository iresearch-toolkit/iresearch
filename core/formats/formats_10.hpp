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

#ifndef IRESEARCH_FORMATS_10_H
#define IRESEARCH_FORMATS_10_H

#include "formats.hpp"

NS_ROOT
NS_BEGIN(version10)

//////////////////////////////////////////////////////////////////////////////
/// @class format
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_PLUGIN format : public irs::format {
 public:
  DECLARE_FORMAT_TYPE();
  DECLARE_FACTORY();

  format() NOEXCEPT;

  virtual index_meta_writer::ptr get_index_meta_writer() const override final;
  virtual index_meta_reader::ptr get_index_meta_reader() const override final;

  virtual segment_meta_writer::ptr get_segment_meta_writer() const override final;
  virtual segment_meta_reader::ptr get_segment_meta_reader() const override final;

  virtual document_mask_writer::ptr get_document_mask_writer() const override final;
  virtual document_mask_reader::ptr get_document_mask_reader() const override final;

  virtual field_writer::ptr get_field_writer(bool volatile_state) const override final;
  virtual field_reader::ptr get_field_reader() const override final;

  virtual column_meta_writer::ptr get_column_meta_writer() const override final;
  virtual column_meta_reader::ptr get_column_meta_reader() const override final;

  virtual columnstore_writer::ptr get_columnstore_writer() const override final;
  virtual columnstore_reader::ptr get_columnstore_reader() const override final;

  virtual postings_writer::ptr get_postings_writer(bool volatile_state) const;
  virtual postings_reader::ptr get_postings_reader() const;

 protected:
  explicit format(const irs::format::type_id& type) NOEXCEPT;
}; // format

NS_END // version10
NS_END // ROOT

#endif
