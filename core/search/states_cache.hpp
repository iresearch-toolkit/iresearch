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

#ifndef IRESEARCH_STATES_CACHE_H
#define IRESEARCH_STATES_CACHE_H

#include <vector>
#include <absl/container/flat_hash_map.h>

#include "shared.hpp"
#include "index/index_reader.hpp"

namespace iresearch {

////////////////////////////////////////////////////////////////////////////////
/// @class states_cache
/// @brief generic cache for cached query states
/// @note we assume that all segment addresses are consequent,
///       i.e. segments are stored in a continious memory block
/// @todo consider changing an API so that sub_reader is indexable by an integer
///       regardless of memory layout
////////////////////////////////////////////////////////////////////////////////
/*
template<typename State>
class states_cache : private util::noncopyable {
 private:
  static constexpr size_t INVALID = std::numeric_limits<size_t>::max();

 public:
  using state_type = State;

  explicit states_cache(const index_reader& reader)
    : index_(reader.size(), INVALID),
      begin_(index_.empty() ? nullptr : &*reader.begin()) {
  }

  states_cache(states_cache&&) = default;
  states_cache& operator=(states_cache&&) = default;

  State& insert(const sub_reader& segment) {
    index_[offset(segment)] = states_.size();
    try {
      return states_.emplace_back();
    } catch (...) {
      index_[offset(segment)] = INVALID;
      throw;
    }
  }

  const State* find(const sub_reader& segment) const noexcept {
    const size_t id = index_[offset(segment)];
    return id != INVALID ? &states_[id] : nullptr;
  }

  bool empty() const noexcept { return states_.empty(); }

 private:
  size_t offset(const sub_reader& segment) const noexcept {
    assert(begin_ && begin_ <= &segment);
    return std::distance(begin_, &segment);
  }

  std::vector<size_t> index_;
  std::deque<State> states_;
  const sub_reader* begin_; // address of the very first segment
}; // states_cache
*/

template<typename State>
class states_cache : private util::noncopyable {
 private:
  using states_map = absl::flat_hash_map<const sub_reader*, State>;

 public:
  using state_type = State;

  explicit states_cache(const index_reader& reader) {
    states_.reserve(reader.size());
  }

  states_cache(states_cache&&) = default;
  states_cache& operator=(states_cache&&) = default;

  State& insert(const sub_reader& rdr) {
    return states_[&rdr];
  }

  const State* find(const sub_reader& rdr) const noexcept {
    auto it = states_.find(&rdr);
    return states_.end() == it ? nullptr : &(it->second);
  }

  bool empty() const noexcept { return states_.empty(); }

private:
  // FIXME use vector instead?
  states_map states_;
}; // states_cache

}

#endif // IRESEARCH_STATES_CACHE_H
