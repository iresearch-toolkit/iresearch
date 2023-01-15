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

#include "filter.hpp"

#include "utils/singleton.hpp"

namespace irs {
namespace {

// Represents a query returning empty result set
struct EmptyQuery final : public filter::prepared {
 public:
  doc_iterator::ptr execute(const ExecutionContext&) const final {
    return irs::doc_iterator::empty();
  }

  void visit(const SubReader&, PreparedStateVisitor&, score_t) const final {
    // No terms to visit
  }
};

const EmptyQuery kEmptyQuery;

}  // namespace

filter::filter(const type_info& type) noexcept
  : boost_{irs::kNoBoost}, type_{type.id()} {}

filter::prepared::ptr filter::prepared::empty() {
  return memory::to_managed<const filter::prepared, false>(&kEmptyQuery);
}

empty::empty() : filter(irs::type<empty>::get()) {}

filter::prepared::ptr empty::prepare(const IndexReader&, const Order&, score_t,
                                     const attribute_provider*) const {
  return memory::to_managed<const filter::prepared, false>(&kEmptyQuery);
}

}  // namespace irs
