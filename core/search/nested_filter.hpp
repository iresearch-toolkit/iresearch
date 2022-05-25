////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_NESTED_FILTER_H
#define IRESEARCH_NESTED_FILTER_H

#include "search/filter.hpp"
#include "utils/type_limits.hpp"

namespace iresearch {

class ByNestedFilter;

struct Match {
  constexpr explicit Match(doc_id_t value) noexcept
      : Match{value, doc_limits::eof()} {}

  constexpr Match(doc_id_t min, doc_id_t max) noexcept : Min(min), Max(max) {}

  constexpr auto operator<=>(const Match&) const noexcept = default;

  constexpr bool IsMinMatch() const noexcept {
    return !doc_limits::eof(Min) && doc_limits::eof(Max);
  }

  doc_id_t Min;
  doc_id_t Max;
};

static constexpr Match kMatchNone{0, 0};
static constexpr Match kMatchAny{1};
static constexpr Match kMatchAll{doc_limits::eof()};

struct ByNestedOptions {
  using filter_type = ByNestedFilter;

  // Parent filter.
  filter::ptr parent;

  // Child filter.
  filter::ptr child;

  // match type
  Match match{kMatchAny};

  // Score merge type.
  sort::MergeType merge_type{sort::MergeType::kSum};

  bool operator==(const ByNestedOptions& rhs) const noexcept {
    auto equal = [](const filter* lhs, const filter* rhs) noexcept {
      return ((!lhs && !rhs) || (lhs && rhs && *lhs == *rhs));
    };

    return match == rhs.match && merge_type == rhs.merge_type &&
           equal(parent.get(), rhs.parent.get()) &&
           equal(child.get(), rhs.child.get());
  }

  size_t hash() const noexcept {
    size_t hash = std::hash<doc_id_t>{}(match.Min);
    hash = hash_combine(hash, std::hash<doc_id_t>{}(match.Max));
    if (parent) {
      hash = hash_combine(hash, parent->hash());
    }
    if (child) {
      hash = hash_combine(hash, child->hash());
    }
    return hash_combine(hash, merge_type);
  }
};

class ByNestedFilter final : public filter_with_options<ByNestedOptions> {
 public:
  static ptr make();

  using filter::prepare;

  prepared::ptr prepare(const index_reader& rdr, const Order& ord,
                        score_t boost,
                        const attribute_provider* /*ctx*/) const override;
};

}  // namespace iresearch

#endif  // IRESEARCH_NESTED_FILTER_H
