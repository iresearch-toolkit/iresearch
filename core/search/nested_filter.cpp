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

#include "nested_filter.hpp"

#include <tuple>

#include "analysis/token_attributes.hpp"
#include "search/cost.hpp"
#include "search/score.hpp"
#include "utils/frozen_attributes.hpp"
#include "utils/type_limits.hpp"

namespace {

using namespace irs;

class ChildToParentJoin final : public doc_iterator {
 public:
  ChildToParentJoin(doc_iterator::ptr&& parent,
                    doc_iterator::ptr&& child) noexcept
      : parent_{std::move(parent)}, child_{std::move(child)} {
    assert(parent_);
    assert(child_);

    // FIXME(gnusi): cost, score

    std::get<attribute_ptr<document>>(attrs_) =
        irs::get_mutable<document>(parent_.get());
  }

  doc_id_t value() const noexcept override {
    return std::get<attribute_ptr<document>>(attrs_).ptr->value;
  }

  attribute* get_mutable(irs::type_info::type_id id) override {
    return irs::get_mutable(attrs_, id);
  }

  doc_id_t seek(doc_id_t target) override {
    const auto child = child_->seek(target);
    return parent_->seek(child);
  }

  bool next() override { return !doc_limits::eof(seek(value() + 1)); }

 private:
  using attributes = std::tuple<attribute_ptr<document>, cost, score>;

  doc_iterator::ptr parent_;
  doc_iterator::ptr child_;
  attributes attrs_;
};

class ByNesterQuery final : public filter::prepared {
 public:
  ByNesterQuery(prepared::ptr&& parent, prepared::ptr&& child) noexcept
      : parent_{std::move(parent)}, child_{std::move(child)} {
    assert(parent_);
    assert(child_);
  }

  doc_iterator::ptr execute(const sub_reader& rdr, const Order& ord,
                            ExecutionMode mode,
                            const attribute_provider* ctx) const override;

 private:
  prepared::ptr parent_;
  prepared::ptr child_;
};

doc_iterator::ptr ByNesterQuery::execute(const sub_reader& rdr,
                                         const Order& ord, ExecutionMode mode,
                                         const attribute_provider* ctx) const {
  auto parent = parent_->execute(rdr, ord, mode, ctx);

  if (IRS_UNLIKELY(!parent || doc_limits::eof(parent->value()))) {
    return doc_iterator::empty();
  }

  auto child = child_->execute(rdr, ord, mode, ctx);

  if (IRS_UNLIKELY(!child || doc_limits::eof(child->value()))) {
    return doc_iterator::empty();
  }

  return memory::make_managed<ChildToParentJoin>(std::move(parent),
                                                 std::move(child));
}

}  // namespace

namespace iresearch {

/*static*/ filter::ptr make() { return memory::make_unique<ByNestedFilter>(); }

filter::prepared::ptr ByNestedFilter::prepare(
    const index_reader& rdr, const Order& ord, score_t boost,
    const attribute_provider* ctx) const {
  const auto* parent = options().parent.get();
  const auto* child = options().child.get();

  if (!parent || !child) {
    return filter::prepared::empty();
  }

  auto prepared_parent = parent->prepare(rdr, Order::kUnordered, kNoBoost, ctx);
  auto prepared_child = child->prepare(rdr, ord, boost, ctx);

  if (!prepared_parent || !prepared_child) {
    return filter::prepared::empty();
  }

  return memory::make_managed<ByNesterQuery>(std::move(prepared_parent),
                                             std::move(prepared_child));
}

}  // namespace iresearch
