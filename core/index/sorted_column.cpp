////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#include "shared.hpp"
#include "sorted_column.hpp"
#include "utils/bitvector.hpp"
#include "utils/type_limits.hpp"
#include "utils/misc.hpp"

#include <vector>

NS_ROOT

std::pair<std::vector<irs::doc_id_t>, field_id> sorted_column::flush(
    columnstore_writer& writer,
    doc_id_t max,
    const bitvector& docs_mask,
    const less_f& less
) {
  assert(less);
  assert(index_.size() <= max);

  const bytes_ref data = data_buf_;

  // temporary push sentinel
  index_.emplace_back(type_limits<type_t::doc_id_t>::eof(), data_buf_.size());

  // prepare order array (new -> old)
  std::vector<irs::doc_id_t> new_old(max, type_limits<type_t::doc_id_t>::eof());
  doc_id_t new_doc_id = 0;
  std::for_each(
    index_.begin(), index_.end() - 1,
    [&new_old, &new_doc_id, &docs_mask](const std::pair<doc_id_t, size_t>& value) NOEXCEPT {
      const auto doc_id = value.first - type_limits<type_t::doc_id_t>::min(); // 0-based doc_id

      if (!docs_mask.test(doc_id)) {
        new_old[doc_id] = new_doc_id++;
      }
  });

  // sort data
  std::sort(
    new_old.begin(), new_old.end(),
    [data, &less, this] (doc_id_t lhs, doc_id_t rhs) {
      if (lhs == rhs) {
        return false;
      }

      const auto& lhs_value = type_limits<type_t::doc_id_t>::eof(lhs)
        ? bytes_ref::NIL
        : bytes_ref(data.c_str() + index_[lhs].second, index_[lhs+1].second);

      const auto& rhs_value = type_limits<type_t::doc_id_t>::eof(rhs)
        ? bytes_ref::NIL
        : bytes_ref(data.c_str() + index_[rhs].second, index_[rhs+1].second);

      return less(lhs_value, rhs_value);
  });

  // flush sorted data
  auto column = writer.push_column();
  auto& column_writer = column.second;

  new_doc_id = type_limits<type_t::doc_id_t>::min();
  for (const auto doc_id : new_old) {
    auto& stream = column_writer(new_doc_id++);

    if (!type_limits<type_t::doc_id_t>::eof(doc_id)) {
      stream.write_bytes(data.c_str() + index_[doc_id].second, index_[doc_id+1].second);
    }
  };

  // data have been flushed
  clear();

  return std::make_pair(std::move(new_old), column.first);
}

NS_END
