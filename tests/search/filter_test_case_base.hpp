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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_FILTER_TEST_CASE_BASE
#define IRESEARCH_FILTER_TEST_CASE_BASE

#include "tests_shared.hpp"
#include "analysis/token_attributes.hpp"
#include "search/cost.hpp"
#include "search/filter_visitor.hpp"
#include "search/score.hpp"
#include "search/filter.hpp"
#include "search/tfidf.hpp"
#include "utils/singleton.hpp"
#include "utils/type_limits.hpp"
#include "index/index_tests.hpp"

namespace tests {
namespace sort {

////////////////////////////////////////////////////////////////////////////////
/// @class boost
/// @brief boost scorer assign boost value to the particular document score
////////////////////////////////////////////////////////////////////////////////
struct boost : public irs::sort {
  struct score_ctx: public irs::score_ctx {
   public:
    explicit score_ctx(irs::boost_t boost) noexcept
    : boost_(boost) {
    }

    irs::boost_t boost_;
  };

  class prepared: public irs::PreparedSortBase<void> {
   public:
    prepared() = default;

    virtual irs::IndexFeatures features() const noexcept override {
      return irs::IndexFeatures::NONE;
    }

    virtual irs::score_function prepare_scorer(
        const irs::sub_reader&,
        const irs::term_reader&,
        const irs::byte_type* /*query_attrs*/,
        irs::score_t* /*score_buf*/,
        const irs::attribute_provider& /*doc_attrs*/,
        irs::boost_t boost) const override {
      return {
        irs::memory::make_unique<boost::score_ctx>(boost),
        [](irs::score_ctx* ctx, irs::score_t* res) noexcept {
          const auto& state = *reinterpret_cast<score_ctx*>(ctx);

          *res = state.boost_;
        }
      };
    }
  }; // sort::boost::prepared

  static ptr make();
  typedef irs::boost_t score_t;
  boost() : sort(irs::type<boost>::get()) {}
  virtual sort::prepared::ptr prepare() const {
    return irs::memory::make_unique<boost::prepared>();
  }
}; // sort::boost

//////////////////////////////////////////////////////////////////////////////
/// @brief expose sort functionality through overidable lambdas
//////////////////////////////////////////////////////////////////////////////
struct custom_sort: public irs::sort {
  class prepared: public irs::PreparedSortBase<void> {
   public:
    class field_collector :  public irs::sort::field_collector {
     public:
      field_collector(const custom_sort& sort): sort_(sort) {}

      virtual void collect(
          const irs::sub_reader& segment,
          const irs::term_reader& field) override {
        if (sort_.collector_collect_field) {
          sort_.collector_collect_field(segment, field);
        }
      }

      virtual void reset() override {
        if (sort_.field_reset_) {
          sort_.field_reset_();
        }
      }

      virtual void collect(irs::bytes_ref in) override {
        // NOOP
      }

      virtual void write(irs::data_output& out) const override {
        // NOOP
      }

     private:
      const custom_sort& sort_;
    };

    class term_collector : public irs::sort::term_collector {
     public:
      term_collector(const custom_sort& sort): sort_(sort) {}

      virtual void collect(
          const irs::sub_reader& segment,
          const irs::term_reader& field,
          const irs::attribute_provider& term_attrs) override {
        if (sort_.collector_collect_term) {
          sort_.collector_collect_term(segment, field, term_attrs);
        }
      }

      virtual void reset() override {
        if (sort_.term_reset_) {
          sort_.term_reset_();
        }
      }

      virtual void collect(irs::bytes_ref in) override {
        // NOOP
      }

      virtual void write(irs::data_output& out) const override {
        // NOOP
      }

     private:
      const custom_sort& sort_;
    };

    struct scorer: public irs::score_ctx {
      scorer(
          const custom_sort& sort,
          const irs::sub_reader& segment_reader,
          const irs::term_reader& term_reader,
          const irs::byte_type* filter_node_attrs,
          const irs::attribute_provider& document_attrs)
        : document_attrs_(document_attrs),
          filter_node_attrs_(filter_node_attrs),
          segment_reader_(segment_reader),
          sort_(sort),
          term_reader_(term_reader) {
      }

      const irs::attribute_provider& document_attrs_;
      const irs::byte_type* filter_node_attrs_;
      const irs::sub_reader& segment_reader_;
      const custom_sort& sort_;
      const irs::term_reader& term_reader_;
    };

    prepared(const custom_sort& sort)
      : sort_(sort) {
    }

    virtual void collect(
        irs::byte_type* filter_attrs,
        const irs::index_reader& index,
        const irs::sort::field_collector* field,
        const irs::sort::term_collector* term) const override {
      if (sort_.collectors_collect_) {
        sort_.collectors_collect_(filter_attrs, index, field, term);
      }
    }

    virtual irs::IndexFeatures features() const override {
      return irs::IndexFeatures::NONE;
    }

    virtual irs::sort::field_collector::ptr prepare_field_collector() const override {
      if (sort_.prepare_field_collector_) {
        return sort_.prepare_field_collector_();
      }

      return irs::memory::make_unique<field_collector>(sort_);
    }

    virtual irs::score_function prepare_scorer(
        const irs::sub_reader& segment_reader,
        const irs::term_reader& term_reader,
        const irs::byte_type* filter_node_attrs,
        irs::score_t* score_buf,
        const irs::attribute_provider& document_attrs,
        irs::boost_t /*boost*/) const override {
      if (sort_.prepare_scorer) {
        return sort_.prepare_scorer(
          segment_reader, term_reader, filter_node_attrs, score_buf, document_attrs);
      }

      return {
        irs::memory::make_unique<custom_sort::prepared::scorer>(
           sort_, segment_reader, term_reader,
           filter_node_attrs, document_attrs),
        [](irs::score_ctx* ctx, irs::score_t* res) noexcept {
          const auto& state = *reinterpret_cast<scorer*>(ctx);

          if (state.sort_.scorer_score) {
            state.sort_.scorer_score(irs::get<irs::document>(state.document_attrs_)->value, res);
          }
        }
      };
    }

    virtual irs::sort::term_collector::ptr prepare_term_collector() const override {
      if (sort_.prepare_term_collector_) {
        return sort_.prepare_term_collector_();
      }

      return irs::memory::make_unique<term_collector>(sort_);
    }

   private:
    const custom_sort& sort_;
  };

  std::function<void(const irs::sub_reader&, const irs::term_reader&)> collector_collect_field;
  std::function<void(const irs::sub_reader&, const irs::term_reader&, const irs::attribute_provider&)> collector_collect_term;
  std::function<void(irs::byte_type*, const irs::index_reader&, const irs::sort::field_collector*, const irs::sort::term_collector*)> collectors_collect_;
  std::function<irs::sort::field_collector::ptr()> prepare_field_collector_;
  std::function<irs::score_function(const irs::sub_reader&, const irs::term_reader&,
                                    const irs::byte_type*, irs::score_t*,
                                    const irs::attribute_provider&)> prepare_scorer;
  std::function<irs::sort::term_collector::ptr()> prepare_term_collector_;
  std::function<void(irs::doc_id_t, irs::score_t*)> scorer_score;
  std::function<void()> term_reset_;
  std::function<void()> field_reset_;

  static ptr make();
  custom_sort(): sort(irs::type<custom_sort>::get()) {}
  virtual prepared::ptr prepare() const {
    return irs::memory::make_unique<custom_sort::prepared>(*this);
  }
};

//////////////////////////////////////////////////////////////////////////////
/// @brief order by frequency, then if equal order by doc_id_t
//////////////////////////////////////////////////////////////////////////////
struct frequency_sort: public irs::sort {
  struct stats_t {
    irs::doc_id_t count;
  };

  class prepared: public irs::PreparedSortBase<stats_t> {
   public:
    struct term_collector: public irs::sort::term_collector {
      size_t docs_count{};
      const irs::term_meta* meta_attr;

      virtual void collect(
          const irs::sub_reader& segment,
          const irs::term_reader& field,
          const irs::attribute_provider& term_attrs) override {
        meta_attr = irs::get<irs::term_meta>(term_attrs);
        ASSERT_NE(nullptr, meta_attr);
        docs_count += meta_attr->docs_count;
      }

      virtual void reset() noexcept override {
        docs_count = 0;
      }

      virtual void collect(irs::bytes_ref in) override {
        // NOOP
      }

      virtual void write(irs::data_output& out) const override {
        // NOOP
      }
    };

    struct scorer: public irs::score_ctx {
      scorer(
          const irs::doc_id_t* docs_count,
          const irs::document* doc)
        : doc(doc),
          docs_count(docs_count) {
      }

      const irs::document* doc;
      const irs::doc_id_t* docs_count;
    };

    prepared() = default;

    virtual void collect(
        irs::byte_type* stats_buf,
        const irs::index_reader& /*index*/,
        const irs::sort::field_collector* /*field*/,
        const irs::sort::term_collector* term) const override {
      auto* term_ptr = dynamic_cast<const term_collector*>(term);
      if (term_ptr) { // may be null e.g. 'all' filter
        stats_cast(stats_buf).count = term_ptr->docs_count;
        const_cast<term_collector*>(term_ptr)->docs_count = 0;
      }
    }

    virtual irs::IndexFeatures features() const override {
      return irs::IndexFeatures::NONE;
    }

    virtual irs::sort::field_collector::ptr prepare_field_collector() const override {
      return nullptr; // do not need to collect stats
    }

    virtual irs::score_function prepare_scorer(
        const irs::sub_reader&,
        const irs::term_reader&,
        const irs::byte_type* stats_buf,
        irs::score_t* /*buf*/,
        const irs::attribute_provider& doc_attrs,
        irs::boost_t /*boost*/) const override {
      auto* doc = irs::get<irs::document>(doc_attrs);
      auto& stats = stats_cast(stats_buf);
      const irs::doc_id_t* docs_count = &stats.count;
      return {
        irs::memory::make_unique<frequency_sort::prepared::scorer>(docs_count, doc),
        [](irs::score_ctx* ctx, irs::score_t* res) noexcept  {
          const auto& state = *reinterpret_cast<scorer*>(ctx);

          // docs_count may be nullptr if no collector called,
          // e.g. by range_query for bitset_doc_iterator
          if (state.docs_count) {
            *res = 1.f / (*state.docs_count);
          } else {
            *res = std::numeric_limits<irs::score_t>::infinity();
          }
        }
      };
    }

    virtual irs::sort::term_collector::ptr prepare_term_collector() const override {
      return irs::memory::make_unique<term_collector>();
    }
  };

  static ptr make();
  frequency_sort(): sort(irs::type<frequency_sort>::get()) {}
  virtual prepared::ptr prepare() const {
    return irs::memory::make_unique<frequency_sort::prepared>();
  }
}; // sort::frequency_sort

} // sort

class filter_test_case_base : public index_test_base {
 protected:
  typedef std::vector<irs::doc_id_t> docs_t;
  typedef std::vector<irs::cost::cost_t> costs_t;

  void check_query(
      const irs::filter& filter,
      const std::vector<irs::doc_id_t>& expected,
      const std::vector<irs::cost::cost_t>& expected_costs,
      const irs::index_reader& rdr) {
    std::vector<irs::doc_id_t> result;
    std::vector<irs::cost::cost_t> result_costs;
    get_query_result(
      filter.prepare(rdr, irs::Order::kUnordered),
      rdr, nullptr, result, result_costs);
    ASSERT_EQ(expected, result);
    ASSERT_EQ(expected_costs, result_costs);
  }

  void check_query(
      const irs::filter& filter,
      const std::vector<irs::doc_id_t>& expected,
      const irs::index_reader& rdr) {
    std::vector<irs::doc_id_t> result;
    std::vector<irs::cost::cost_t> result_costs;
    get_query_result(
      filter.prepare(rdr, irs::Order::kUnordered),
      rdr, nullptr, result, result_costs);
    ASSERT_EQ(expected, result);
  }

  void check_query(
      const irs::filter& filter,
      std::span<const irs::sort::ptr> order,
      const std::vector<irs::doc_id_t>& expected,
      const irs::index_reader& rdr,
      bool score_must_be_present = true,
      bool reverse = false) {
    auto prepared_order = irs::Order::Prepare(order);
    auto prepared_filter = filter.prepare(rdr, prepared_order);
    auto score_less = [reverse, size = prepared_order.buckets().size()](
        const std::pair<irs::bstring, irs::doc_id_t>& lhs,
        const std::pair<irs::bstring, irs::doc_id_t>& rhs)->bool {
      const auto& [lhs_buf, lhs_doc] = lhs;
      const auto& [rhs_buf, rhs_doc] = rhs;

      const auto* lhs_score = reinterpret_cast<const float*>(lhs_buf.c_str());
      const auto* rhs_score = reinterpret_cast<const float*>(rhs_buf.c_str());

      for (size_t i = 0; i < size; ++i) {
        const auto r = (lhs_score[i] <=> rhs_score[i]);

        if (r < 0) {
          return !reverse;
        }

        if (r > 0) {
          return reverse;
        }
      }

      return lhs_doc < rhs_doc;
    };
    std::multiset<std::pair<irs::bstring, irs::doc_id_t>, decltype(score_less)> scored_result(score_less);

    for (const auto& sub: rdr) {
      auto docs = prepared_filter->execute(sub, prepared_order);

      auto* doc = irs::get<irs::document>(*docs);
      ASSERT_TRUE(bool(doc)); // ensure all iterators contain "document" attribute

      const auto* score = irs::get<irs::score>(*docs);

      if (!score) {
        ASSERT_FALSE(score_must_be_present);
      }

      irs::bstring score_value(prepared_order.score_size(), 0);

      while (docs->next()) {
        ASSERT_EQ(docs->value(), doc->value);

        if (score && !score->is_default()) {
          score->evaluate(reinterpret_cast<irs::score_t*>(score_value.data()));

          scored_result.emplace(score_value, docs->value());
        } else {
          scored_result.emplace(
            irs::bstring(prepared_order.score_size(), 0),
            docs->value()
          );
        }
      }
      ASSERT_FALSE(docs->next());
    }

    std::vector<irs::doc_id_t> result;

    for (auto& entry: scored_result) {
      result.emplace_back(entry.second);
    }

    ASSERT_EQ(expected, result);
  }

 private:
  void get_query_result(
      const irs::filter::prepared::ptr& q,
      const irs::index_reader& rdr,
      irs::score_t* score_value,
      std::vector<irs::doc_id_t>& result,
      std::vector<irs::cost::cost_t>& result_costs) {
    for (const auto& sub : rdr) {
      auto docs = q->execute(sub);

      auto* doc = irs::get<irs::document>(*docs);
      ASSERT_TRUE(bool(doc)); // ensure all iterators contain "document" attribute

      auto* score = irs::get<irs::score>(*docs);

      result_costs.push_back(irs::cost::extract(*docs));

      while (docs->next()) {
        ASSERT_EQ(docs->value(), doc->value);

        if (score) {
          score->evaluate(score_value);
        }
        // put score attributes to iterator
        result.push_back(docs->value());
      }
      ASSERT_FALSE(docs->next());
    }
  }
};

struct empty_term_reader : irs::singleton<empty_term_reader>, irs::term_reader {
  virtual irs::seek_term_iterator::ptr iterator() const {
    return irs::seek_term_iterator::empty();
  }

  virtual irs::seek_term_iterator::ptr iterator(irs::automaton_table_matcher&) const {
    return irs::seek_term_iterator::empty();
  }

  virtual const irs::field_meta& meta() const {
    static irs::field_meta EMPTY;
    return EMPTY;
  }

  virtual irs::attribute* get_mutable(irs::type_info::type_id) noexcept {
    return nullptr;
  }

  // total number of terms
  virtual size_t size() const { return 0; }

  // total number of documents
  virtual uint64_t docs_count() const { return 0; }

  // less significant term
  virtual const irs::bytes_ref& (min)() const {
    return irs::bytes_ref::NIL;
  }

  // most significant term
  virtual const irs::bytes_ref& (max)() const {
    return irs::bytes_ref::NIL;
  }
}; // empty_term_reader

class empty_filter_visitor : public irs::filter_visitor {
 public:
  virtual void prepare(const irs::sub_reader& segment,
                       const irs::term_reader& field,
                       const irs::seek_term_iterator& terms) noexcept override {
    it_ = &terms;
    ++prepare_calls_counter_;
  }

  virtual void visit(irs::boost_t boost) noexcept override {
    ASSERT_NE(nullptr, it_);
    terms_.emplace_back(it_->value(), boost);
    ++visit_calls_counter_;
  }

  void reset() noexcept {
    prepare_calls_counter_ = 0;
    visit_calls_counter_ = 0;
    terms_.clear();
    it_ = nullptr;
  }

  size_t prepare_calls_counter() const noexcept {
    return prepare_calls_counter_;
  }

  size_t visit_calls_counter() const noexcept {
    return visit_calls_counter_;
  }

  const std::vector<std::pair<irs::bstring, irs::boost_t>>& terms() const noexcept {
    return terms_;
  }

  template<typename Char>
  std::vector<std::pair<irs::basic_string_ref<Char>, irs::boost_t>> term_refs() const {
    std::vector<std::pair<irs::basic_string_ref<Char>, irs::boost_t>> refs(terms_.size());
    auto begin = refs.begin();
    for (auto& term : terms_) {
      begin->first = irs::ref_cast<Char>(term.first);
      begin->second = term.second;
      ++begin;
    }
    return refs;
  }

  virtual void assert_boost(irs::boost_t boost) {
    ASSERT_EQ(irs::kNoBoost, boost);
  }

 private:
  const irs::seek_term_iterator* it_{};
  std::vector<std::pair<irs::bstring, irs::boost_t>> terms_;
  size_t prepare_calls_counter_ = 0;
  size_t visit_calls_counter_ = 0;
}; // empty_filter_visitor

} // tests

#endif // IRESEARCH_FILTER_TEST_CASE_BASE
