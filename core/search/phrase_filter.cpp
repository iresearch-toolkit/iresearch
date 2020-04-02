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

#include <boost/functional/hash.hpp>

#include "phrase_filter.hpp"
#include "phrase_iterator.hpp"
#include "term_query.hpp"
#include "wildcard_filter.hpp"
#include "index/field_meta.hpp"

NS_LOCAL

using namespace irs;

typedef seek_term_iterator::cookie_ptr term_state_t;
typedef std::vector<term_state_t> terms_states_t;

//////////////////////////////////////////////////////////////////////////////
/// @class fixed_phrase_state
/// @brief cached per reader phrase state
//////////////////////////////////////////////////////////////////////////////
struct fixed_phrase_state {
  fixed_phrase_state() = default;
  fixed_phrase_state(fixed_phrase_state&& rhs) = default;
  fixed_phrase_state& operator=(const fixed_phrase_state&) = delete;

  std::vector<term_state_t> terms;
  const term_reader* reader{};
}; // fixed_phrase_state

//////////////////////////////////////////////////////////////////////////////
/// @class variadic_phrase_state
/// @brief cached per reader phrase state
//////////////////////////////////////////////////////////////////////////////
struct variadic_phrase_state : fixed_phrase_state {
  variadic_phrase_state() = default;
  variadic_phrase_state(variadic_phrase_state&& rhs) = default;
  variadic_phrase_state& operator=(const variadic_phrase_state&) = delete;

  std::vector<size_t> num_terms; // number of terms per phrase part
}; // variadic_phrase_state

NS_END

NS_ROOT

//////////////////////////////////////////////////////////////////////////////
/// @class phrase_term_visitor
/// @brief filter visitor for phrase queries
//////////////////////////////////////////////////////////////////////////////
class phrase_term_visitor final : public filter_visitor {
 public:
  phrase_term_visitor(
      const sub_reader& segment,
      const term_reader& reader,
      terms_states_t& phrase_terms,
      term_collectors* collectors = nullptr)
    : segment_(segment),
      reader_(reader),
      phrase_terms_(&phrase_terms),
      collectors_(collectors) {
  }

  virtual void prepare(const seek_term_iterator& terms) noexcept override {
    terms_ = &terms;
    found_ = true;
  }

  virtual void visit() override {
    assert(terms_);
    if (variadic_) {
      auto term_offset = collectors_->push_back();
      collectors_->collect(segment_, reader_, term_offset, terms_->attributes()); // collect statistics
    } else {
      collectors_->collect(segment_, reader_, term_offset_, terms_->attributes()); // collect statistics
    }

    // estimate phrase & term
    assert(phrase_terms_);
    phrase_terms_->emplace_back(terms_->cookie());
  }

  void reset() noexcept {
    found_ = false;
    terms_ = nullptr;
  }

  void reset(size_t term_offset) noexcept {
    reset();
    term_offset_ = term_offset;
  }

  void reset(term_collectors& collectors,
             size_t term_offset) noexcept {
    reset(term_offset);
    collectors_ = &collectors;
  }

  bool found() const noexcept { return found_; }

  bool variadic_ = false;

 private:
  bool found_ = false;
  size_t term_offset_ = 0;
  const sub_reader& segment_;
  const term_reader& reader_;
  terms_states_t* phrase_terms_;
  term_collectors* collectors_ = nullptr;
  const seek_term_iterator* terms_ = nullptr;
};

NS_END

NS_ROOT

//////////////////////////////////////////////////////////////////////////////
/// @class phrase_query
/// @brief prepared phrase query implementation
//////////////////////////////////////////////////////////////////////////////
template<typename State>
class phrase_query : public filter::prepared {
 public:
  typedef states_cache<State> states_t;
  typedef std::vector<position::value_t> positions_t;

  phrase_query(
      states_t&& states,
      positions_t&& positions,
      bstring&& stats,
      boost_t boost) noexcept
    : prepared(boost),
      states_(std::move(states)),
      positions_(std::move(positions)),
      stats_(std::move(stats)) {
  }

 protected:
  states_t states_;
  positions_t positions_;
  bstring stats_;
}; // phrase_query

class fixed_phrase_query : public phrase_query<fixed_phrase_state> {
 public:
  fixed_phrase_query(
      states_t&& states, positions_t&& positions,
      bstring&& stats, boost_t boost) noexcept
    : phrase_query<fixed_phrase_state>(
        std::move(states),  std::move(positions),
        std::move(stats), boost) {
  }

  using filter::prepared::execute;

  doc_iterator::ptr execute(
      const sub_reader& rdr,
      const order::prepared& ord,
      const attribute_view& /*ctx*/) const {
    // get phrase state for the specified reader
    auto phrase_state = states_.find(rdr);

    if (!phrase_state) {
      // invalid state
      return doc_iterator::empty();
    }

    // get features required for query & order
    auto features = ord.features() | by_phrase::required();

    typedef conjunction<doc_iterator::ptr> conjunction_t;
    typedef phrase_iterator<conjunction_t, fixed_phrase_frequency> phrase_iterator_t;

    conjunction_t::doc_iterators_t itrs;
    itrs.reserve(phrase_state->terms.size());

    phrase_iterator_t::positions_t positions;
    positions.reserve(phrase_state->terms.size());

    // find term using cached state
    auto terms = phrase_state->reader->iterator();
    auto position = positions_.begin();

    for (const auto& term_state : phrase_state->terms) {
      // use bytes_ref::blank here since we do not need just to "jump"
      // to cached state, and we are not interested in term value itself
      if (!terms->seek(bytes_ref::NIL, *term_state)) {
        return doc_iterator::empty();
      }

      auto docs = terms->postings(features); // postings
      auto& pos = docs->attributes().get<irs::position>(); // needed postings attributes

      if (!pos) {
        // positions not found
        return doc_iterator::empty();
      }

      positions.emplace_back(std::ref(*pos), *position);

      // add base iterator
      itrs.emplace_back(std::move(docs));

      ++position;
    }

    return memory::make_shared<phrase_iterator_t>(
      std::move(itrs),
      std::move(positions),
      rdr,
      *phrase_state->reader,
      stats_.c_str(),
      ord,
      boost()
    );
  }
}; // fixed_phrase_query

class variadic_phrase_query : public phrase_query<variadic_phrase_state> {
 public:
  variadic_phrase_query(
      states_t&& states, positions_t&& positions,
      bstring&& stats, boost_t boost) noexcept
    : phrase_query<variadic_phrase_state>(
        std::move(states), std::move(positions),
        std::move(stats), boost) {
  }

  using filter::prepared::execute;

  doc_iterator::ptr execute(
      const sub_reader& rdr,
      const order::prepared& ord,
      const attribute_view& /*ctx*/) const override {
    // get phrase state for the specified reader
    auto phrase_state = states_.find(rdr);

    if (!phrase_state) {
      // invalid state
      return doc_iterator::empty();
    }

    // get features required for query & order
    const auto features = ord.features() | by_phrase::required();

    typedef conjunction<doc_iterator::ptr> conjunction_t;
    typedef phrase_iterator<conjunction_t, variadic_phrase_frequency> phrase_iterator_t;

    conjunction_t::doc_iterators_t conj_itrs;
    conj_itrs.reserve(phrase_state->terms.size());

    typedef position_score_iterator_adapter<doc_iterator::ptr> adapter_t;
    typedef disjunction<doc_iterator::ptr, adapter_t, true> disjunction_t;

    const auto phrase_size = phrase_state->num_terms.size();

    phrase_iterator_t::positions_t positions;
    positions.resize(phrase_size);

    // find term using cached state
    auto terms = phrase_state->reader->iterator();
    auto position = positions_.begin();

    auto term_state = phrase_state->terms.begin();
    for (size_t i = 0; i < phrase_size; ++i) {
      const auto num_terms = phrase_state->num_terms[i];
      auto& pos = positions[i];
      pos.second = *position;

      disjunction_t::doc_iterators_t disj_itrs;
      disj_itrs.reserve(num_terms);
      for (const auto end = term_state + num_terms; term_state != end; ++term_state) {
        assert(*term_state);
        // use bytes_ref::NIL here since we do not need just to "jump"
        // to cached state, and we are not interested in term value itself
        if (!terms->seek(bytes_ref::NIL, **term_state)) {
          continue;
        }

        auto docs = terms->postings(features); // postings
        auto& pos = docs->attributes().get<irs::position>(); // needed postings attributes

        if (!pos) {
          // positions not found
          continue;
        }

        // add base iterator
        disj_itrs.emplace_back(std::move(docs));
      }

      if (disj_itrs.empty()) {
        return doc_iterator::empty();
      }

      auto disj = make_disjunction<disjunction_t>(std::move(disj_itrs));
      typedef irs::compound_doc_iterator<adapter_t> compound_doc_iterator_t;
      #ifdef IRESEARCH_DEBUG
        pos.first = dynamic_cast<compound_doc_iterator_t*>(disj.get());
        assert(pos.first);
      #else
        pos.first = static_cast<compound_doc_iterator_t*>(disj.get());
      #endif
      conj_itrs.emplace_back(std::move(disj));
      ++position;
    }
    assert(term_state == phrase_state->terms.end());

    return memory::make_shared<phrase_iterator_t>(
      std::move(conj_itrs),
      std::move(positions),
      rdr,
      *phrase_state->reader,
      stats_.c_str(),
      ord,
      boost()
    );
  }
}; // variadic_phrase_query

// -----------------------------------------------------------------------------
// --SECTION--                                          by_phrase implementation
// -----------------------------------------------------------------------------

/* static */ const flags& by_phrase::required() {
  static flags req{ frequency::type(), position::type() };
  return req;
}

DEFINE_FILTER_TYPE(by_phrase)
DEFINE_FACTORY_DEFAULT(by_phrase)

by_phrase::by_phrase(): filter(by_phrase::type()) {
}

bool by_phrase::equals(const filter& rhs) const noexcept {
  const by_phrase& trhs = static_cast<const by_phrase&>(rhs);
  return filter::equals(rhs) && fld_ == trhs.fld_ && phrase_ == trhs.phrase_;
}

by_phrase::phrase_part::phrase_part() : type(PhrasePartType::TERM), st() {
}

by_phrase::phrase_part::phrase_part(const phrase_part& other) {
  allocate(other);
}

by_phrase::phrase_part::phrase_part(phrase_part&& other) noexcept {
  allocate(std::move(other));
}

by_phrase::phrase_part& by_phrase::phrase_part::operator=(const phrase_part& other) {
  if (&other == this) {
    return *this;
  }
  recreate(other);
  return *this;
}

by_phrase::phrase_part& by_phrase::phrase_part::operator=(phrase_part&& other) noexcept {
  if (&other == this) {
    return *this;
  }
  recreate(std::move(other));
  return *this;
}

bool by_phrase::phrase_part::operator==(const phrase_part& other) const noexcept {
  if (type != other.type) {
    return false;
  }
  switch (type) {
    case PhrasePartType::TERM:
      return st == other.st;
    case PhrasePartType::PREFIX:
      return pt == other.pt;
    case PhrasePartType::WILDCARD:
      return wt == other.wt;
    case PhrasePartType::LEVENSHTEIN:
      return lt == other.lt;
    case PhrasePartType::SET:
      return ct == other.ct;
    case PhrasePartType::RANGE:
      return rt == other.rt;
    default:
      assert(false);
  }
  return false;
}

bool by_phrase::phrase_part::collect(
    const term_reader& reader,
    phrase_term_visitor& ptv) const {
  auto found = false;
  switch (type) {
    case PhrasePartType::TERM:
      term_query::visit(reader, st.term, ptv);
      found = ptv.found();
      break;
    case PhrasePartType::PREFIX:
      by_prefix::visit(reader, pt.term, ptv);
      found = ptv.found();
      break;
    case PhrasePartType::WILDCARD:
      by_wildcard::visit(reader, wt.term, ptv);
      found = ptv.found();
      break;
    case PhrasePartType::LEVENSHTEIN:
      by_edit_distance::visit(
        reader, lt.term, lt.max_distance, lt.provider,
        lt.with_transpositions, ptv);
      found = ptv.found();
      break;
    case PhrasePartType::SET:
      for (const auto& term : ct.terms) {
        term_query::visit(reader, term, ptv);
        found |= ptv.found();
        ptv.reset();
      }
      break;
    case PhrasePartType::RANGE:
      by_range::visit(reader, rt.rng, ptv);
      found = ptv.found();
      break;
    default:
      assert(false);
  }
  return found;
}

void by_phrase::phrase_part::allocate(const phrase_part& other) {
  type = other.type;
  switch (type) {
    case PhrasePartType::TERM:
      new (&st) simple_term(other.st);
      break;
    case PhrasePartType::PREFIX:
      new (&pt) prefix_term(other.pt);
      break;
    case PhrasePartType::WILDCARD:
      new (&wt) wildcard_term(other.wt);
      break;
    case PhrasePartType::LEVENSHTEIN:
      new (&lt) levenshtein_term(other.lt);
      break;
    case PhrasePartType::SET:
      new (&ct) set_term(other.ct);
      break;
    case PhrasePartType::RANGE:
      new (&rt) range_term(other.rt);
      break;
    default:
      assert(false);
  }
}

void by_phrase::phrase_part::allocate(phrase_part&& other) noexcept {
  type = other.type;
  switch (type) {
    case PhrasePartType::TERM:
      new (&st) simple_term(std::move(other.st));
      break;
    case PhrasePartType::PREFIX:
      new (&pt) prefix_term(std::move(other.pt));
      break;
    case PhrasePartType::WILDCARD:
      new (&wt) wildcard_term(std::move(other.wt));
      break;
    case PhrasePartType::LEVENSHTEIN:
      new (&lt) levenshtein_term(std::move(other.lt));
      break;
    case PhrasePartType::SET:
      new (&ct) set_term(std::move(other.ct));
      break;
    case PhrasePartType::RANGE:
      new (&rt) range_term(std::move(other.rt));
      break;
    default:
      assert(false);
  }
}

void by_phrase::phrase_part::destroy() noexcept {
  switch (type) {
    case PhrasePartType::TERM:
      st.~simple_term();
      break;
    case PhrasePartType::PREFIX:
      pt.~prefix_term();
      break;
    case PhrasePartType::WILDCARD:
      wt.~wildcard_term();
      break;
    case PhrasePartType::LEVENSHTEIN:
      lt.~levenshtein_term();
      break;
    case PhrasePartType::SET:
      ct.~set_term();
      break;
    case PhrasePartType::RANGE:
      rt.~range_term();
      break;
    default:
      assert(false);
  }
}

void by_phrase::phrase_part::recreate(const phrase_part& other) {
  if (type != other.type) {
    destroy();
  }
  allocate(other);
}

void by_phrase::phrase_part::recreate(phrase_part&& other) noexcept {
  if (type != other.type) {
    destroy();
  }
  allocate(std::move(other));
}

// defined in a separate unit
size_t hash_value(const by_range::range_t& rng);

size_t hash_value(const by_phrase::phrase_part& info) {
  auto seed = std::hash<int>()(static_cast<std::underlying_type<by_phrase::PhrasePartType>::type>(info.type));
  switch (info.type) {
    case by_phrase::PhrasePartType::TERM:
      ::boost::hash_combine(seed, info.st.term);
      break;
    case by_phrase::PhrasePartType::PREFIX:
      ::boost::hash_combine(seed, info.pt.scored_terms_limit);
      ::boost::hash_combine(seed, info.pt.term);
      break;
    case by_phrase::PhrasePartType::WILDCARD:
      ::boost::hash_combine(seed, info.wt.scored_terms_limit);
      ::boost::hash_combine(seed, info.wt.term);
      break;
    case by_phrase::PhrasePartType::LEVENSHTEIN:
      ::boost::hash_combine(seed, info.lt.with_transpositions);
      ::boost::hash_combine(seed, info.lt.max_distance);
      ::boost::hash_combine(seed, info.lt.scored_terms_limit);
      ::boost::hash_combine(seed, info.lt.provider);
      ::boost::hash_combine(seed, info.lt.term);
      break;
    case by_phrase::PhrasePartType::SET:
      std::for_each(
        info.ct.terms.cbegin(), info.ct.terms.cend(),
        [&seed](const bstring& term) {
          ::boost::hash_combine(seed, term);
      });
      break;
    case by_phrase::PhrasePartType::RANGE:
      ::boost::hash_combine(seed, info.rt.scored_terms_limit);
      ::boost::hash_combine(seed, info.rt.rng);
      break;
    default:
      assert(false);
  }
  return seed;
}

size_t by_phrase::hash() const noexcept {
  size_t seed = 0;
  ::boost::hash_combine(seed, filter::hash());
  ::boost::hash_combine(seed, fld_);
  std::for_each(
    phrase_.cbegin(), phrase_.cend(),
    [&seed](const by_phrase::term_t& term) {
      ::boost::hash_combine(seed, term);
  });
  return seed;
}

filter::prepared::ptr by_phrase::prepare(
    const index_reader& index,
    const order::prepared& ord,
    boost_t boost,
    const attribute_view& /*ctx*/) const {
  if (fld_.empty() || phrase_.empty()) {
    // empty field or phrase
    return filter::prepared::empty();
  }

  const auto phrase_size = phrase_.size();
  if (1 == phrase_size) {
    const auto& term_info = phrase_.begin()->second;
    boost *= this->boost();
    switch (term_info.type) {
      case PhrasePartType::TERM: // similar to `term_query`
        return term_query::make(index, ord, boost, fld_, term_info.st.term);
      case PhrasePartType::PREFIX:
        return by_prefix::prepare(index, ord, boost, fld_, term_info.pt.term,
                                  term_info.pt.scored_terms_limit);
      case PhrasePartType::WILDCARD:
        return by_wildcard::prepare(index, ord, boost, fld_, term_info.wt.term,
                                    term_info.wt.scored_terms_limit);
      case PhrasePartType::LEVENSHTEIN:
        return by_edit_distance::prepare(index, ord, boost, fld_, term_info.lt.term,
                                         term_info.lt.scored_terms_limit,
                                         term_info.lt.max_distance, term_info.lt.provider,
                                         term_info.lt.with_transpositions);
      case PhrasePartType::SET:
        // do nothing
        break;
      case PhrasePartType::RANGE:
        return by_range::prepare(index, ord, boost, fld_, term_info.rt.rng,
                                 term_info.rt.scored_terms_limit);
      default:
        assert(false);
        return filter::prepared::empty();
    }
  }

  // prepare phrase stats (collector for each term)
  if (is_simple_term_only_) {
    return fixed_prepare_collect(index, ord, boost);
  }

  return variadic_prepare_collect(index, ord, boost);
}

filter::prepared::ptr by_phrase::fixed_prepare_collect(
    const index_reader& index,
    const order::prepared& ord,
    boost_t boost) const {
  const auto phrase_size = phrase_.size();

  // stats collectors
  field_collectors field_stats(ord);
  term_collectors term_stats(ord, phrase_size);

  // per segment phrase states
  fixed_phrase_query::states_t phrase_states(index.size());

  // per segment phrase terms
  terms_states_t phrase_terms;
  phrase_terms.reserve(phrase_size);

  // iterate over the segments
  const string_ref field = fld_;

  for (const auto& segment : index) {
    // get term dictionary for field
    const term_reader* reader = segment.field(field);

    if (!reader) {
      continue;
    }

    // check required features
    if (!by_phrase::required().is_subset_of(reader->meta().features)) {
      continue;
    }

    field_stats.collect(segment, *reader); // collect field statistics once per segment
    //collectors.collect(segment, *reader); // collect field statistics once per segment

    size_t term_offset = 0;
    auto is_ord_empty = ord.empty();

    phrase_term_visitor ptv(segment, *reader, phrase_terms, &term_stats);

    for (const auto& word : phrase_) {
      assert(PhrasePartType::TERM == word.second.type);
      ptv.reset(term_offset);
      term_query::visit(*reader, word.second.st.term, ptv);
      if (!ptv.found()) {
        if (is_ord_empty) {
          break;
        }
        // continue here because we should collect
        // stats for other terms in phrase
      }
      ++term_offset;
    }

    // we have not found all needed terms
    if (phrase_terms.size() != phrase_size) {
      phrase_terms.clear();
      continue;
    }

    auto& state = phrase_states.insert(segment);
    state.terms = std::move(phrase_terms);
    state.reader = reader;

    phrase_terms.reserve(phrase_size);
  }

  // offset of the first term in a phrase
  size_t base_offset = first_pos();

  // finish stats
  fixed_phrase_query::positions_t positions(phrase_size);
  auto pos_itr = positions.begin();

  for (const auto& term : phrase_) {
    *pos_itr = position::value_t(term.first - base_offset);
    ++pos_itr;
  }

  bstring stats(ord.stats_size(), 0); // aggregated phrase stats
  auto* stats_buf = const_cast<byte_type*>(stats.data());

  ord.prepare_stats(stats_buf);
  term_stats.finish(stats_buf, field_stats, index);

  return memory::make_shared<fixed_phrase_query>(
    std::move(phrase_states),
    std::move(positions),
    std::move(stats),
    this->boost() * boost
  );
}

filter::prepared::ptr by_phrase::variadic_prepare_collect(
    const index_reader& index,
    const order::prepared& ord,
    boost_t boost) const {
  const auto phrase_size = phrase_.size();

  // stats collectors
  field_collectors field_stats(ord);

  // FIXME
  std::vector<term_collectors> phrase_part_stats;
  phrase_part_stats.reserve(phrase_size);
  for (size_t i = 0; i < phrase_size; ++i) {
    phrase_part_stats.emplace_back(ord, 0);
  }

  // per segment phrase states
  variadic_phrase_query::states_t phrase_states(index.size());

  // per segment phrase terms
  std::vector<size_t> num_terms(phrase_size); // number of terms per part
  terms_states_t phrase_terms;
  phrase_terms.reserve(phrase_size); // reserve space for at least 1 term per part

  // iterate over the segments
  const string_ref field = fld_;
  const auto is_ord_empty = ord.empty();

  for (const auto& segment : index) {
    // get term dictionary for field
    const auto* reader = segment.field(field);

    if (!reader) {
      continue;
    }

    // check required features
    if (!by_phrase::required().is_subset_of(reader->meta().features)) {
      continue;
    }

    field_stats.collect(segment, *reader); // collect field statistics once per segment

    size_t term_offset = 0;
    size_t found_words_count = 0;

    phrase_term_visitor ptv(segment, *reader, phrase_terms);
    ptv.variadic_ = true; // FIXME

    for (const auto& word : phrase_) {
      const auto terms_count = phrase_terms.size();
      ptv.reset(phrase_part_stats[term_offset], term_offset);
      if (!word.second.collect(*reader, ptv)) {
        if (is_ord_empty) {
          break;
        }
        // continue here because we should collect
        // stats for other terms in phrase
      } else {
        ++found_words_count;
      }
      num_terms[term_offset] = phrase_terms.size() - terms_count;
      ++term_offset;
    }

    // we have not found all needed terms
    if (found_words_count != phrase_size) {
      phrase_terms.clear();
      continue;
    }

    auto& state = phrase_states.insert(segment);
    state.terms = std::move(phrase_terms);
    state.num_terms = std::move(num_terms);
    state.reader = reader;
    assert(phrase_size == state.num_terms.size());

    phrase_terms.reserve(phrase_size);
    num_terms.resize(phrase_size); // reserve space for at least 1 term per part
  }

  // offset of the first term in a phrase
  size_t base_offset = first_pos();

  // finish stats
  variadic_phrase_query::positions_t positions(phrase_size);
  auto pos_itr = positions.begin();

  for (const auto& term : phrase_) {
    *pos_itr = position::value_t(term.first - base_offset);
    ++pos_itr;
  }

  bstring stats(ord.stats_size(), 0); // aggregated phrase stats
  auto* stats_buf = const_cast<byte_type*>(stats.data());

  ord.prepare_stats(stats_buf);
  for (auto& collector: phrase_part_stats) {
    collector.finish(stats_buf, field_stats, index);
  }

  return memory::make_shared<variadic_phrase_query>(
    std::move(phrase_states),
    std::move(positions),
    std::move(stats),
    this->boost() * boost
  );
}

NS_END // ROOT
