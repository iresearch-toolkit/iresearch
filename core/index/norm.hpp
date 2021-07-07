////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_NORM_H
#define IRESEARCH_NORM_H

#include "shared.hpp"
#include "analysis/token_attributes.hpp"
#include "utils/lz4compression.hpp"

namespace iresearch {

//////////////////////////////////////////////////////////////////////////////
/// @class norm
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API norm final : attribute {
  // DO NOT CHANGE NAME
  static constexpr string_ref type_name() noexcept {
    return "norm";
  }

  FORCE_INLINE static constexpr float_t DEFAULT() noexcept {
    return 1.f;
  }

  norm() noexcept;
  norm(norm&&) = default;
  norm& operator=(norm&&) = default;

  bool reset(const sub_reader& segment, field_id column, const document& doc);
  float_t read() const;
  bool empty() const noexcept;

  void clear() noexcept;

 private:
  doc_iterator::ptr column_it_;
  const payload* payload_;
  const document* doc_;
}; // norm

static_assert(std::is_nothrow_move_constructible_v<norm>);
static_assert(std::is_nothrow_move_assignable_v<norm>);

IRESEARCH_API void compute_norm(
  type_info::type_id type,
  const field_stats& stats,
  doc_id_t doc,
  columnstore_writer::values_writer_f& writer);

} // iresearch

#endif // IRESEARCH_NORM_H
