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
#include "utils/type_limits.hpp"
#include "utils/misc.hpp"

NS_ROOT

std::pair<doc_map, field_id> sorted_column::flush(
    columnstore_writer& writer,
    doc_id_t max,
    const comparer& less
) {
  assert(index_.size() <= max);

  const bytes_ref data = data_buf_;

  // temporarily push sentinel
  index_.emplace_back(doc_limits::eof(), data_buf_.size());

  // prepare order array (new -> old)
  // first - position in 'index_', eof() - not present
  // second - old document id, 0-based
  std::vector<std::pair<irs::doc_id_t, irs::doc_id_t>> new_old(
    max, std::make_pair(doc_limits::eof(), 0)
  );

  doc_id_t new_doc_id = 0;
  for (size_t i = 0, size = index_.size()-1; i < size; ++i) {
    const auto doc_id = index_[i].first - doc_limits::min(); // 0-based doc_id

    while (new_doc_id < doc_id) {
      new_old[new_doc_id].second = new_doc_id;
      ++new_doc_id;
    }

    new_old[new_doc_id].first = doc_id_t(i);
    new_old[new_doc_id].second = new_doc_id;
    ++new_doc_id;
  }

  // sort data
  std::sort(
    new_old.begin(), new_old.end(),
    [data, &less, this] (const std::pair<doc_id_t, doc_id_t>& lhs, const std::pair<doc_id_t, doc_id_t>& rhs) {
      if (lhs.first == rhs.first) {
        return false;
      }

      const auto& lhs_value = doc_limits::eof(lhs.first)
        ? bytes_ref::NIL
        : bytes_ref(data.c_str() + index_[lhs.first].second,
                    index_[lhs.first+1].second);

      const auto& rhs_value = doc_limits::eof(rhs.first)
        ? bytes_ref::NIL
        : bytes_ref(data.c_str() + index_[rhs.first].second,
                    index_[rhs.first+1].second);

      return less(lhs_value, rhs_value);
  });

  doc_map old_new(max);

  for (size_t i = 0, size = new_old.size(); i < size; ++i) {
    old_new[new_old[i].second] = doc_id_t(i);
  }

  // flush sorted data
  auto column = writer.push_column();
  auto& column_writer = column.second;

  new_doc_id = doc_limits::min();
  for (const auto& entry: new_old) {
    auto& stream = column_writer(new_doc_id++);

    if (!doc_limits::eof(entry.first)) {
      const auto begin = index_[entry.first].second;
      const auto end = index_[entry.first+1].second;
      assert(begin <= end);

      stream.write_bytes(data.c_str() + begin, end - begin);
    }
  };

  // data have been flushed
  clear();

  return std::pair<doc_map, field_id>(
    std::piecewise_construct,
    std::forward_as_tuple(std::move(old_new)),
    std::forward_as_tuple(column.first)
  );
}

NS_END
