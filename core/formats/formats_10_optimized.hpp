////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_FORMATS_10_OPTIMIZED_H
#define IRESEARCH_FORMATS_10_OPTIMIZED_H

#include "formats_10.hpp"

NS_ROOT
NS_BEGIN(version10)

//////////////////////////////////////////////////////////////////////////////
/// @class format_optimized
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_PLUGIN format_optimized final : public format {
 public:
  DECLARE_FORMAT_TYPE();
  DECLARE_FACTORY();

  format_optimized() NOEXCEPT;

  virtual postings_writer::ptr get_postings_writer(bool volatile_state) const override;
  virtual postings_reader::ptr get_postings_reader() const override;
}; // format_optimized

NS_END
NS_END

#endif
