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
////////////////////////////////////////////////////////////////////////////////

#include "multiterm_query.hpp"

#include "shared.hpp"
#include "bitset_doc_iterator.hpp"
#include "disjunction.hpp"

namespace {

using namespace irs;

class bitset_iterator final : public bitset_doc_iterator {
 public:
  explicit bitset_iterator(bitset&& set, const irs::order::prepared& ord)
    : bitset_doc_iterator(set.begin(), set.end()),
      score_(ord),
      set_(std::move(set)) {
  }

  virtual attribute* get_mutable(type_info::type_id id) noexcept override {
    return id == type<score>::id()
      ? &score_
      : bitset_doc_iterator::get_mutable(id);
  }

 private:
  score score_;
  bitset set_;
};

template<typename DocIterator>
bool fill(bitset& bs, DocIterator& it) {
  auto* doc = irs::get<irs::document>(it);

  if (!doc) {
    return false;
  }

  bool has_docs = false;
  if (it.next()) {
    has_docs = true;

    do {
      bs.set(doc->value);
    } while (it.next());
  }

  return has_docs;
}

}

namespace iresearch {

doc_iterator::ptr multiterm_query::execute(
    const sub_reader& segment,
    const order::prepared& ord,
    const attribute_provider* /*ctx*/) const {
  using scored_disjunction_t = scored_disjunction_iterator<doc_iterator::ptr>;
  using disjunction_t = disjunction_iterator<doc_iterator::ptr>;

  // get term state for the specified reader
  auto state = states_.find(segment);

  if (!state) {
    // invalid state
    return doc_iterator::empty();
  }

  // get terms iterator
  auto terms = state->reader->iterator();

  if (IRS_UNLIKELY(!terms)) {
    return doc_iterator::empty();
  }

  doc_iterator::ptr unscored_docs;
  cost::cost_t unscored_docs_estimation = state->unscored_states_estimation;

  if (!state->unscored_terms.empty()) {
    bitset set(segment.docs_count() + irs::doc_limits::min());

    bool has_bit_set = false;
    for (auto& cookie : state->unscored_terms) {
      assert(cookie);
      if (!terms->seek(bytes_ref::NIL, *cookie)) {
        return doc_iterator::empty(); // internal error
      }

      auto docs = terms->postings(flags::empty_instance());

      if (IRS_LIKELY(docs)) {
        has_bit_set |= fill(set, *docs);
      }
    }

    if (has_bit_set) {
      // ensure first bit isn't set,
      // since we don't want to emit doc_limits::invalid()
      assert(set.any() && !set.test(0));

      unscored_docs = memory::make_managed<::bitset_iterator>(std::move(set), ord);
      unscored_docs_estimation = set.count();
    }
  }

  disjunction_t::doc_iterators_t itrs(state->scored_states.size() + size_t(nullptr != unscored_docs));
  auto it = itrs.begin();

  // get required features for order
  auto& features = ord.features();
  auto& stats = this->stats();

  // add an iterator for each of the scored states
  const bool no_score = ord.empty();
  for (auto& entry : state->scored_states) {
    assert(entry.cookie);
    if (!terms->seek(bytes_ref::NIL, *entry.cookie)) {
      return doc_iterator::empty(); // internal error
    }

    auto docs = terms->postings(features);

    if (IRS_UNLIKELY(!docs)) {
      continue;
    }

    if (!no_score) {
      auto* score = irs::get_mutable<irs::score>(docs.get());

      if (score) {
        assert(entry.stat_offset < stats.size());
        auto* stat = stats[entry.stat_offset].c_str();

        order::prepared::scorers scorers(
          ord, segment, *state->reader, stat,
          score->realloc(ord), *docs, entry.boost*boost());

        irs::reset(*score, std::move(scorers));
      }
    }

    *it = std::move(docs);
    ++it;
  }

  if (unscored_docs) {
    *it = std::move(unscored_docs);
    ++it;
  }

  if (IRS_UNLIKELY(it != itrs.end())) {
    itrs.erase(it, itrs.end());
  }

  if (ord.empty()) {
    return make_disjunction<disjunction_t>(
      std::move(itrs), ord, merge_type_,
      state->scored_states_estimation + unscored_docs_estimation);
  }

  return make_disjunction<scored_disjunction_t>(
    std::move(itrs), ord, merge_type_,
    state->scored_states_estimation + unscored_docs_estimation);
}

} // ROOT
