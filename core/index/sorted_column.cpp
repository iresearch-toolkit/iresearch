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

NS_LOCAL

bool check_consistency(
    const std::vector<irs::doc_id_t>& new_old,
    const std::vector<irs::doc_id_t>& old_new
) {
  if (new_old.size() != old_new.size()) {
    return false;
  }

  for (size_t i = 0, size = new_old.size(); i < size; ++i) {
    const auto doc = new_old[i];

    if (!irs::doc_limits::eof(doc) && old_new[doc] != irs::doc_id_t(i)) {
      return false;
    }
  }

  return true;
}

NS_END

NS_ROOT

doc_map::doc_map(
    std::vector<doc_id_t>&& new_old,
    std::vector<doc_id_t>&& old_new
) NOEXCEPT
  : new_old_(std::move(new_old)),
    old_new_(std::move(old_new)) {
  assert(check_consistency(new_old_, old_new_));
}

std::pair<doc_map, field_id> sorted_column::flush(
    columnstore_writer& writer,
    doc_id_t max,
    const bitvector& docs_mask,
    const less_f& less
) {
  assert(less);
  assert(index_.size() <= max);

  const bytes_ref data = data_buf_;

  // temporary push sentinel
  index_.emplace_back(doc_limits::eof(), data_buf_.size());

  // prepare order array (new -> old)
  std::vector<irs::doc_id_t> new_old(max, doc_limits::eof());

  doc_id_t new_doc_id = 0;
  std::for_each(
    index_.begin(), index_.end() - 1,
    [&new_old, &new_doc_id, &docs_mask](const std::pair<doc_id_t, size_t>& value) NOEXCEPT {
      const auto doc_id = value.first - doc_limits::min(); // 0-based doc_id

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

      const auto& lhs_value = doc_limits::eof(lhs)
        ? bytes_ref::NIL
        : bytes_ref(data.c_str() + index_[lhs].second, index_[lhs+1].second);

      const auto& rhs_value = doc_limits::eof(rhs)
        ? bytes_ref::NIL
        : bytes_ref(data.c_str() + index_[rhs].second, index_[rhs+1].second);

      return less(lhs_value, rhs_value);
  });

  std::vector<irs::doc_id_t> old_new(max, doc_limits::eof());

  for (size_t i = 0, size = new_old.size(); i < size; ++i) {
    const auto doc = new_old[i];

    if (!doc_limits::eof(doc)) {
      old_new[doc] = doc_id_t(i);
    }
  }

  // flush sorted data
  auto column = writer.push_column();
  auto& column_writer = column.second;

  new_doc_id = doc_limits::min();
  for (const auto doc_id : new_old) {
    auto& stream = column_writer(new_doc_id++);

    if (!doc_limits::eof(doc_id)) {
      stream.write_bytes(data.c_str() + index_[doc_id].second, index_[doc_id+1].second);
    }
  };

  // data have been flushed
  clear();

  return std::pair<doc_map, field_id>(
    std::piecewise_construct,
    std::forward_as_tuple(std::move(new_old), std::move(old_new)),
    std::forward_as_tuple(column.first)
  );
}

NS_END
