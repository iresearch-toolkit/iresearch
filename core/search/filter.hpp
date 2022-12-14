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

#pragma once

#include <absl/container/node_hash_map.h>

#include <functional>

#include "index/iterators.hpp"
#include "search/sort.hpp"
#include "shared.hpp"
#include "utils/hash_utils.hpp"

namespace irs {

struct index_reader;
struct PreparedStateVisitor;

enum class ExecutionMode : uint32_t {
  kAll,  // Access all documents
  kTop   // Access only top matched documents
};

struct ExecutionContext {
  const sub_reader& segment;
  const Order& scorers;
  const attribute_provider* ctx{};
  ExecutionMode mode{ExecutionMode::kAll};
};

// Base class for all user-side filters
class filter {
 public:
  // Base class for all prepared(compiled) queries
  class prepared {
   public:
    using ptr = memory::managed_ptr<const prepared>;

    static prepared::ptr empty();

    explicit prepared(score_t boost = kNoBoost) noexcept : boost_(boost) {}
    virtual ~prepared() = default;

    doc_iterator::ptr execute(const sub_reader& segment,
                              const Order& scorers = Order::kUnordered,
                              ExecutionMode mode = ExecutionMode::kAll) const {
      return execute({.segment = segment, .scorers = scorers, .mode = mode});
    }

    virtual doc_iterator::ptr execute(const ExecutionContext& ctx) const = 0;

    virtual void visit(const sub_reader& segment, PreparedStateVisitor& visitor,
                       score_t boost) const = 0;

    score_t boost() const noexcept { return boost_; }

   protected:
    void boost(score_t boost) noexcept { boost_ *= boost; }

   private:
    score_t boost_;
  };

  using ptr = std::unique_ptr<filter>;

  explicit filter(const type_info& type) noexcept;
  virtual ~filter() = default;

  virtual size_t hash() const noexcept {
    return std::hash<type_info::type_id>()(type_);
  }

  bool operator==(const filter& rhs) const noexcept { return equals(rhs); }

  bool operator!=(const filter& rhs) const noexcept { return !(*this == rhs); }

  // boost - external boost
  virtual filter::prepared::ptr prepare(
    const index_reader& rdr, const Order& ord, score_t boost,
    const attribute_provider* ctx) const = 0;

  filter::prepared::ptr prepare(const index_reader& rdr, const Order& ord,
                                const attribute_provider* ctx) const {
    return prepare(rdr, ord, irs::kNoBoost, ctx);
  }

  filter::prepared::ptr prepare(const index_reader& rdr, const Order& ord,
                                score_t boost) const {
    return prepare(rdr, ord, boost, nullptr);
  }

  filter::prepared::ptr prepare(const index_reader& rdr,
                                const Order& ord) const {
    return prepare(rdr, ord, irs::kNoBoost);
  }

  filter::prepared::ptr prepare(const index_reader& rdr) const {
    return prepare(rdr, Order::kUnordered);
  }

  score_t boost() const noexcept { return boost_; }

  filter& boost(score_t boost) noexcept {
    boost_ = boost;
    return *this;
  }

  type_info::type_id type() const noexcept { return type_; }

 protected:
  virtual bool equals(const filter& rhs) const noexcept {
    return type_ == rhs.type_;
  }

 private:
  score_t boost_;
  type_info::type_id type_;
};

// boost::hash_combine support
inline size_t hash_value(const filter& q) noexcept { return q.hash(); }

// Convenient base class filters with options
template<typename Options>
class filter_with_options : public filter {
 public:
  using options_type = Options;
  using filter_type = typename options_type::filter_type;

  filter_with_options() : filter(irs::type<filter_type>::get()) {}

  const options_type& options() const noexcept { return options_; }
  options_type* mutable_options() noexcept { return &options_; }

  size_t hash() const noexcept override {
    return hash_combine(filter::hash(), options_.hash());
  }

 protected:
  bool equals(const filter& rhs) const noexcept override {
    return filter::equals(rhs) &&
           options_ == down_cast<filter_type>(rhs).options_;
  }

 private:
  options_type options_;
};

// Convenient base class for single field filters
template<typename Options>
class filter_base : public filter_with_options<Options> {
 public:
  using options_type = typename filter_with_options<Options>::options_type;
  using filter_type = typename options_type::filter_type;

  std::string_view field() const noexcept { return field_; }
  std::string* mutable_field() noexcept { return &field_; }

  size_t hash() const noexcept override {
    return hash_combine(hash_utils::Hash(field_),
                        filter_with_options<options_type>::hash());
  }

 protected:
  bool equals(const filter& rhs) const noexcept override {
    return filter_with_options<options_type>::equals(rhs) &&
           field_ == down_cast<filter_type>(rhs).field_;
  }

 private:
  std::string field_;
};

// Filter which returns no documents
class empty final : public filter {
 public:
  empty();

  filter::prepared::ptr prepare(const index_reader& rdr, const Order& ord,
                                score_t boost,
                                const attribute_provider* ctx) const override;
};

struct filter_visitor;
using field_visitor =
  std::function<void(const sub_reader&, const term_reader&, filter_visitor&)>;

}  // namespace irs

namespace std {

template<>
struct hash<irs::filter> {
  typedef irs::filter argument_type;
  typedef size_t result_type;

  result_type operator()(const argument_type& key) const { return key.hash(); }
};

}  // namespace std

