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

#ifndef IRESEARCH_BOOLEAN_FILTER_H
#define IRESEARCH_BOOLEAN_FILTER_H

#include <vector>

#include "all_filter.hpp"
#include "filter.hpp"
#include "utils/iterator.hpp"

namespace iresearch {

class FilterWithAllDocsProvider : public filter, private util::noncopyable {
 public:
  using AllDocsProvider = std::function<filter::ptr(irs::score_t)>;

  static irs::filter::ptr DefaultProvider(irs::score_t boost);

  filter::ptr MakeAllDocsFilter(score_t boost) const {
    return all_docs_(boost);
  }

  void SetProvider(AllDocsProvider&& provider);

 protected:
  explicit FilterWithAllDocsProvider(irs::type_info type) noexcept;

 private:
  AllDocsProvider all_docs_;
};

// Represents user-side boolean filter as the container for other
// filters.
class boolean_filter : public FilterWithAllDocsProvider {
 public:
  auto begin() const { return ptr_iterator{std::begin(filters_)}; }
  auto end() const { return ptr_iterator{std::end(filters_)}; }

  auto begin() { return ptr_iterator{std::begin(filters_)}; }
  auto end() { return ptr_iterator{std::end(filters_)}; }

  sort::MergeType merge_type() const noexcept { return merge_type_; }

  void merge_type(sort::MergeType merge_type) noexcept {
    merge_type_ = merge_type;
  }

  template<typename T, typename... Args>
  T& add(Args&&... args) {
    static_assert(std::is_base_of_v<filter, T>);

    return static_cast<T&>(*filters_.emplace_back(
      memory::make_unique<T>(std::forward<Args>(args)...)));
  }

  filter& add(filter::ptr&& filter) {
    assert(filter);
    return *filters_.emplace_back(std::move(filter));
  }

  virtual size_t hash() const noexcept override;

  void clear() { return filters_.clear(); }
  bool empty() const { return filters_.empty(); }
  size_t size() const { return filters_.size(); }

  virtual filter::prepared::ptr prepare(
    const index_reader& rdr, const Order& ord, score_t boost,
    const attribute_provider* ctx) const override final;

 protected:
  explicit boolean_filter(const type_info& type) noexcept;
  virtual bool equals(const filter& rhs) const noexcept override;

  virtual filter::prepared::ptr prepare(
    std::vector<const filter*>& incl, std::vector<const filter*>& excl,
    const index_reader& rdr, const Order& ord, score_t boost,
    const attribute_provider* ctx) const = 0;

 private:
  void group_filters(const filter& all_docs_no_boost,
                     std::vector<const filter*>& incl,
                     std::vector<const filter*>& excl) const;

  std::vector<filter::ptr> filters_;
  sort::MergeType merge_type_{sort::MergeType::kSum};
};

// Represents conjunction
class And final : public boolean_filter {
 public:
  And() noexcept;

  using filter::prepare;

 protected:
  virtual filter::prepared::ptr prepare(
    std::vector<const filter*>& incl, std::vector<const filter*>& excl,
    const index_reader& rdr, const Order& ord, score_t boost,
    const attribute_provider* ctx) const override;
};

// Represents disjunction
class Or final : public boolean_filter {
 public:
  Or() noexcept;

  using filter::prepare;

  // Return minimum number of subqueries which must be satisfied
  size_t min_match_count() const { return min_match_count_; }

  // Sets minimum number of subqueries which must be satisfied
  Or& min_match_count(size_t count) {
    min_match_count_ = count;
    return *this;
  }

 protected:
  virtual filter::prepared::ptr prepare(
    std::vector<const filter*>& incl, std::vector<const filter*>& excl,
    const index_reader& rdr, const Order& ord, score_t boost,
    const attribute_provider* ctx) const override;

 private:
  size_t min_match_count_;
};

// Represents negation
class Not : public FilterWithAllDocsProvider {
 public:
  Not() noexcept;

  const irs::filter* filter() const { return filter_.get(); }

  template<typename T>
  const T* filter() const {
    using type =
      typename std::enable_if_t<std::is_base_of_v<irs::filter, T>, T>;

    return static_cast<const type*>(filter_.get());
  }

  template<typename T, typename... Args>
  T& filter(Args&&... args) {
    using type =
      typename std::enable_if_t<std::is_base_of_v<irs::filter, T>, T>;

    filter_ = memory::make_unique<type>(std::forward<Args>(args)...);
    return static_cast<type&>(*filter_);
  }

  void clear() { filter_.reset(); }
  bool empty() const { return nullptr == filter_; }

  using filter::prepare;

  virtual filter::prepared::ptr prepare(
    const index_reader& rdr, const Order& ord, score_t boost,
    const attribute_provider* ctx) const override;

  virtual size_t hash() const noexcept override;

 protected:
  virtual bool equals(const irs::filter& rhs) const noexcept override;

 private:
  filter::ptr filter_;
};

}  // namespace iresearch

#endif
