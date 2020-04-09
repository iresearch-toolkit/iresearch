////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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

#include "tests_shared.hpp"

#include "search/collectors.hpp"
#include "search/top_terms_collector.hpp"
#include "search/multiterm_query.hpp"
#include "search/filter.hpp"
#include "search/scorers.hpp"
#include "formats/empty_term_reader.hpp"
#include "utils/memory.hpp"

NS_LOCAL

struct term_meta : irs::term_meta {
  term_meta(uint32_t docs_count = 0, uint32_t freq = 0) noexcept {
    this->docs_count = docs_count;
    this->freq = freq;
  }
};

struct sort : irs::sort {
  DECLARE_SORT_TYPE();

  static irs::sort::ptr make() {
    return std::make_shared<sort>();
  }

  sort() noexcept : irs::sort(type()) { }

  struct prepared final : irs::sort::prepared {
    struct field_collector final : irs::sort::field_collector {
      uint64_t docs_with_field = 0; // number of documents containing the matched field (possibly without matching terms)
      uint64_t total_term_freq = 0; // number of terms for processed field

      virtual void collect(const irs::sub_reader& segment,
                           const irs::term_reader& field) {
        docs_with_field += field.docs_count();

        auto& freq = field.attributes().get<irs::frequency>();

        if (freq) {
          total_term_freq += freq->value;
        }
      }

      virtual void reset() noexcept override {
        docs_with_field = 0;
        total_term_freq = 0;
      }

      virtual void collect(const irs::bytes_ref& in) { }
      virtual void write(irs::data_output& out) const override { }
    };

    struct term_collector final : irs::sort::term_collector {
      uint64_t docs_with_term = 0; // number of documents containing the matched term

      virtual void collect(const irs::sub_reader& segment,
                           const irs::term_reader& field,
                           const irs::attribute_view& term_attrs) override {
        auto& meta = term_attrs.get<irs::term_meta>();

        if (meta) {
          docs_with_term += meta->docs_count;
        }
      }

      virtual void reset() noexcept override {
        docs_with_term = 0;
      }

      virtual void collect(const irs::bytes_ref& in) override { }
      virtual void write(irs::data_output& out) const override { }
    };

    virtual void collect(
        irs::byte_type* stats,
        const irs::index_reader& index,
        const irs::sort::field_collector* field,
        const irs::sort::term_collector* term) const {
    }

    virtual const irs::flags& features() const {
      return irs::flags::empty_instance();
    }

    virtual irs::sort::field_collector::ptr prepare_field_collector() const {
      return irs::memory::make_unique<field_collector>();
    }

    virtual irs::sort::term_collector::ptr prepare_term_collector() const {
      return irs::memory::make_unique<term_collector>();
    }

    virtual std::pair<irs::score_ctx_ptr, irs::score_f> prepare_scorer(
        const irs::sub_reader& segment,
        const irs::term_reader& field,
        const irs::byte_type* stats,
        const irs::attribute_view& doc_attrs,
        irs::boost_t boost) const {
      return { nullptr, nullptr };
    }

    virtual void prepare_score(irs::byte_type* score) const {  }

    virtual void prepare_stats(irs::byte_type* stats) const {  }


    virtual bool less(const irs::byte_type* lhs,
                      const irs::byte_type* rhs) const {
      return false;
    }

    virtual std::pair<size_t, size_t> score_size() const {
      return { 0, 0 };
    }

    virtual std::pair<size_t, size_t> stats_size() const {
      return { 0, 0 };
    }
  };

  irs::sort::prepared::ptr prepare() const {
    return irs::memory::make_unique<prepared>();
  }
};

DEFINE_SORT_TYPE(sort);

class seek_term_iterator final : public irs::seek_term_iterator {
 public:
  typedef const std::pair<irs::string_ref, term_meta>* iterator_type;

  seek_term_iterator(iterator_type begin, iterator_type end)
    : begin_(begin), end_(end) {
    attrs_.emplace(meta_);
  }

  virtual irs::SeekResult seek_ge(const irs::bytes_ref& value) {
    return irs::SeekResult::NOT_FOUND;
  }

  virtual bool seek(const irs::bytes_ref& value) {
    return false;
  }

  virtual bool seek(
      const irs::bytes_ref& term,
      const seek_cookie& cookie) {

    return true;
  }

  virtual seek_cookie::ptr cookie() const override {
    return irs::memory::make_unique<struct cookie>(begin_);
  }

  virtual const irs::attribute_view& attributes() const noexcept override {
    return attrs_;
  }

  virtual bool next() noexcept override {
    if (begin_ == end_) {
      return false;
    }

    value_ = irs::ref_cast<irs::byte_type>(begin_->first);
    meta_ = begin_->second;
    ++begin_;
    return true;
  }

  virtual const irs::bytes_ref& value() const noexcept override {
    return value_;
  }

  virtual void read() override { }

  virtual irs::doc_iterator::ptr postings(const irs::flags& /*features*/) const override {
    return irs::doc_iterator::empty();
  }

 private:
  struct cookie : seek_cookie {
    explicit cookie(iterator_type ptr) noexcept
      : ptr(ptr) {
    }

    iterator_type  ptr;
  };


  irs::attribute_view attrs_;
  term_meta meta_;
  irs::bytes_ref value_;
  iterator_type begin_;
  iterator_type end_;
}; // term_iterator

template<typename StatesType>
struct aggregated_stats_visitor {
  aggregated_stats_visitor(StatesType& states, const irs::order::prepared& ord)
    : collectors(ord, 1),
      states(&states) {
  }

  void operator()(irs::byte_type key,
                  const irs::bytes_ref& term) {
  }

  void operator()(const irs::sub_reader& segment,
                  const irs::term_reader& field,
                  uint32_t docs_count) {
    it = field.iterator();
    state = &states->insert(segment);
    state->reader = &field;
    state->scored_states_estimation += docs_count;
  }

  void operator()(seek_term_iterator::cookie_ptr& cookie) {
    if (!it->seek(irs::bytes_ref::NIL, *cookie)) {
      return;
    }

    collectors.collect(*segment, *field, 0, it->attributes());
    state->scored_states.emplace_back(std::move(cookie), 0, boost);
  }

  irs::term_collectors collectors;
  irs::seek_term_iterator::ptr it;
  typename StatesType::state_type* state;
  StatesType* states{};
  const irs::sub_reader* segment{};
  const irs::term_reader* field{};
  irs::boost_t boost{ irs::no_boost() };
};

//template<typename StatesType>
//struct visitor {
//  visitor(irs::bstring* stats,
//          StatesType& states,
//          const irs::order::prepared& ord, size_t terms_count)
//    : stats_(stats),
//      ord_(&ord),
//      collectors(ord, 1), // 1 term at a time
//      states(&states) {
//  }
//
//  void operator()(irs::byte_type key,
//                  const irs::bytes_ref& term) const {
//    if (stat_offset) {
//      collectors.finish(const_cast<irs::byte_type*>(stats_[stat_offset - 1].data()),
//    }
//
//    ++stat_offset;
//    collectors = irs::fixed_terms_collectors(*ord_, 1);
//  }
//
//  void operator()(const irs::sub_reader& segment,
//                  const irs::term_reader& field,
//                  uint32_t docs_count) const {
//    it = field.iterator();
//    state = states->insert(segment);
//    state->reader = &field;
//    state->scored_states_estimation += docs_count;
//  }
//
//  void operator()(seek_term_iterator::cookie_ptr& cookie) const {
//    if (!it->seek(irs::bytes_ref::NIL, *cookie)) {
//      return;
//    }
//
//    collectors.collect(segment, field, 0, it->attributes());
//    state->scored_states.emplace_back(std::move(cookie), stat_offset, boost);
//  }
//
//  irs::bstring* stats_;
//  const irs::order::prepared* ord_;
//  uint32_t stat_offset{std::numeric_limits<uint32_t>::max()};
//  irs::fixed_terms_collectors collectors;
//  irs::seek_term_iterator::ptr it;
//  mutable typename StatesType::state_type* state;
//  mutable StatesType* states{};
//  mutable const irs::sub_reader* segment{};
//  mutable const irs::term_reader* field{};
//  mutable irs::boost_t boost{ irs::no_boost() };
//};

NS_END

TEST(top_terms_collector_test, test) {
  const std::pair<irs::string_ref, term_meta> TERMS[] {
    { "F", { 1, 1 } },
    { "G", { 2, 2 } },
    { "H", { 3, 3 } },
    { "B", { 3, 3 } },
    { "C", { 3, 3 } },
    { "A", { 3, 3 } },
    { "H", { 2, 2 } },
    { "D", { 5, 5 } },
    { "E", { 5, 5 } },
    { "I", { 15, 15 } },
    { "J", { 5, 25 } },
    { "K", { 15, 35 } },
  };

  irs::order ord;
  ord.add<::sort>(true);

  auto prepared = ord.prepare();

  irs::empty_term_reader term_reader(42);
  seek_term_iterator it(std::begin(TERMS), std::end(TERMS));

  irs::top_terms_collector<irs::top_term_state<irs::byte_type>> collector(5);
  collector.prepare(irs::sub_reader::empty(), term_reader, it);

  while (it.next()) {
    collector.collect(it.value().front());
  }

  irs::states_cache<irs::multiterm_state> states(1);
  std::vector<irs::bstring> stats;

  aggregated_stats_visitor<decltype(states)> visitor(states, prepared);

  //collector.visit(visitor);
}
