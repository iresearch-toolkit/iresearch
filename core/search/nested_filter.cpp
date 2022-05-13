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

#include "analysis/token_attributes.hpp"
#include "utils/type_limits.hpp"

namespace {

using namespace irs;

class ChildToParentJoin final : public doc_iterator {
 public:
  ChildToParentJoin(doc_iterator::ptr&& parent,
                    doc_iterator::ptr&& child) noexcept
      : parent_{std::move(parent)},
        child_{std::move(child)},
        parent_doc_{irs::get<document>(*parent_)} {
    assert(parent_);
    assert(child_);
  }

  doc_id_t value() const noexcept override { return parent_doc_->value; }

  attribute* get_mutable(irs::type_info::type_id id) override {
    if (irs::type<document>::id() == id) {
      return const_cast<document*>(parent_doc_);
    }

    return child_->get_mutable(id);
  }

  doc_id_t seek(doc_id_t target) override;
  bool next() override;

 private:
  doc_iterator::ptr parent_;
  doc_iterator::ptr child_;
  const document* parent_doc_;
};

doc_id_t ChildToParentJoin::seek(doc_id_t target) { return doc_limits::eof(); }

bool ChildToParentJoin::next() { return false; }

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
