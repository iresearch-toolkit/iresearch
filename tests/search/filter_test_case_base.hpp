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

#pragma once

#include <variant>

#include "analysis/token_attributes.hpp"
#include "index/index_tests.hpp"
#include "search/cost.hpp"
#include "search/filter.hpp"
#include "search/filter_visitor.hpp"
#include "search/score.hpp"
#include "search/tfidf.hpp"
#include "tests_shared.hpp"
#include "utils/singleton.hpp"
#include "utils/type_limits.hpp"

namespace tests {
namespace sort {

////////////////////////////////////////////////////////////////////////////////
/// @class boost
/// @brief boost scorer assign boost value to the particular document score
////////////////////////////////////////////////////////////////////////////////
struct boost : public irs::sort {
  struct score_ctx : public irs::score_ctx {
   public:
    explicit score_ctx(irs::score_t boost) noexcept : boost_(boost) {}

    irs::score_t boost_;
  };

  class prepared : public irs::PreparedSortBase<void> {
   public:
    prepared() = default;

    irs::IndexFeatures features() const noexcept override {
      return irs::IndexFeatures::NONE;
    }

    virtual irs::ScoreFunction prepare_scorer(
      const irs::SubReader&, const irs::term_reader&,
      const irs::byte_type* /*query_attrs*/,
      const irs::attribute_provider& /*doc_attrs*/,
      irs::score_t boost) const override {
      return {std::make_unique<boost::score_ctx>(boost),
              [](irs::score_ctx* ctx, irs::score_t* res) noexcept {
                const auto& state = *reinterpret_cast<score_ctx*>(ctx);

                *res = state.boost_;
              }};
    }
  };  // sort::boost::prepared

  typedef irs::score_t score_t;
  boost() : sort(irs::type<boost>::get()) {}
  virtual sort::prepared::ptr prepare() const {
    return std::make_unique<boost::prepared>();
  }
};  // sort::boost

//////////////////////////////////////////////////////////////////////////////
/// @brief expose sort functionality through overidable lambdas
//////////////////////////////////////////////////////////////////////////////
struct custom_sort : public irs::sort {
  class prepared : public irs::PreparedSortBase<void> {
   public:
    class field_collector : public irs::sort::field_collector {
     public:
      field_collector(const custom_sort& sort) : sort_(sort) {}

      void collect(const irs::SubReader& segment,
                   const irs::term_reader& field) override {
        if (sort_.collector_collect_field) {
          sort_.collector_collect_field(segment, field);
        }
      }

      void reset() override {
        if (sort_.field_reset_) {
          sort_.field_reset_();
        }
      }

      void collect(irs::bytes_view in) override {
        // NOOP
      }

      void write(irs::data_output& out) const override {
        // NOOP
      }

     private:
      const custom_sort& sort_;
    };

    class term_collector : public irs::sort::term_collector {
     public:
      term_collector(const custom_sort& sort) : sort_(sort) {}

      void collect(const irs::SubReader& segment, const irs::term_reader& field,
                   const irs::attribute_provider& term_attrs) override {
        if (sort_.collector_collect_term) {
          sort_.collector_collect_term(segment, field, term_attrs);
        }
      }

      void reset() override {
        if (sort_.term_reset_) {
          sort_.term_reset_();
        }
      }

      void collect(irs::bytes_view in) override {
        // NOOP
      }

      void write(irs::data_output& out) const override {
        // NOOP
      }

     private:
      const custom_sort& sort_;
    };

    struct scorer : public irs::score_ctx {
      scorer(const custom_sort& sort, const irs::SubReader& segment_reader,
             const irs::term_reader& term_reader,
             const irs::byte_type* filter_node_attrs,
             const irs::attribute_provider& document_attrs)
        : document_attrs_(document_attrs),
          filter_node_attrs_(filter_node_attrs),
          segment_reader_(segment_reader),
          sort_(sort),
          term_reader_(term_reader) {}

      const irs::attribute_provider& document_attrs_;
      const irs::byte_type* filter_node_attrs_;
      const irs::SubReader& segment_reader_;
      const custom_sort& sort_;
      const irs::term_reader& term_reader_;
    };

    prepared(const custom_sort& sort) : sort_(sort) {}

    void collect(irs::byte_type* filter_attrs, const irs::IndexReader& index,
                 const irs::sort::field_collector* field,
                 const irs::sort::term_collector* term) const override {
      if (sort_.collectors_collect_) {
        sort_.collectors_collect_(filter_attrs, index, field, term);
      }
    }

    irs::IndexFeatures features() const override {
      return irs::IndexFeatures::NONE;
    }

    irs::sort::field_collector::ptr prepare_field_collector() const override {
      if (sort_.prepare_field_collector_) {
        return sort_.prepare_field_collector_();
      }

      return std::make_unique<field_collector>(sort_);
    }

    virtual irs::ScoreFunction prepare_scorer(
      const irs::SubReader& segment_reader, const irs::term_reader& term_reader,
      const irs::byte_type* filter_node_attrs,
      const irs::attribute_provider& document_attrs,
      irs::score_t boost) const override {
      if (sort_.prepare_scorer) {
        return sort_.prepare_scorer(segment_reader, term_reader,
                                    filter_node_attrs, document_attrs, boost);
      }

      return {std::make_unique<custom_sort::prepared::scorer>(
                sort_, segment_reader, term_reader, filter_node_attrs,
                document_attrs),
              [](irs::score_ctx* ctx, irs::score_t* res) noexcept {
                const auto& state = *reinterpret_cast<scorer*>(ctx);

                if (state.sort_.scorer_score) {
                  state.sort_.scorer_score(
                    irs::get<irs::document>(state.document_attrs_)->value, res);
                }
              }};
    }

    irs::sort::term_collector::ptr prepare_term_collector() const override {
      if (sort_.prepare_term_collector_) {
        return sort_.prepare_term_collector_();
      }

      return std::make_unique<term_collector>(sort_);
    }

   private:
    const custom_sort& sort_;
  };

  std::function<void(const irs::SubReader&, const irs::term_reader&)>
    collector_collect_field;
  std::function<void(const irs::SubReader&, const irs::term_reader&,
                     const irs::attribute_provider&)>
    collector_collect_term;
  std::function<void(irs::byte_type*, const irs::IndexReader&,
                     const irs::sort::field_collector*,
                     const irs::sort::term_collector*)>
    collectors_collect_;
  std::function<irs::sort::field_collector::ptr()> prepare_field_collector_;
  std::function<irs::ScoreFunction(
    const irs::SubReader&, const irs::term_reader&, const irs::byte_type*,
    const irs::attribute_provider&, irs::score_t)>
    prepare_scorer;
  std::function<irs::sort::term_collector::ptr()> prepare_term_collector_;
  std::function<void(irs::doc_id_t, irs::score_t*)> scorer_score;
  std::function<void()> term_reset_;
  std::function<void()> field_reset_;

  custom_sort() : sort(irs::type<custom_sort>::get()) {}
  virtual prepared::ptr prepare() const {
    return std::make_unique<custom_sort::prepared>(*this);
  }
};

//////////////////////////////////////////////////////////////////////////////
/// @brief order by frequency, then if equal order by doc_id_t
//////////////////////////////////////////////////////////////////////////////
struct frequency_sort : public irs::sort {
  struct stats_t {
    irs::doc_id_t count;
  };

  class prepared : public irs::PreparedSortBase<stats_t> {
   public:
    struct term_collector : public irs::sort::term_collector {
      size_t docs_count{};
      const irs::term_meta* meta_attr;

      void collect(const irs::SubReader& segment, const irs::term_reader& field,
                   const irs::attribute_provider& term_attrs) override {
        meta_attr = irs::get<irs::term_meta>(term_attrs);
        ASSERT_NE(nullptr, meta_attr);
        docs_count += meta_attr->docs_count;
      }

      void reset() noexcept override { docs_count = 0; }

      void collect(irs::bytes_view in) override {
        // NOOP
      }

      void write(irs::data_output& out) const override {
        // NOOP
      }
    };

    struct scorer : public irs::score_ctx {
      scorer(const irs::doc_id_t* docs_count, const irs::document* doc)
        : doc(doc), docs_count(docs_count) {}

      const irs::document* doc;
      const irs::doc_id_t* docs_count;
    };

    prepared() = default;

    void collect(irs::byte_type* stats_buf, const irs::IndexReader& /*index*/,
                 const irs::sort::field_collector* /*field*/,
                 const irs::sort::term_collector* term) const override {
      auto* term_ptr = dynamic_cast<const term_collector*>(term);
      if (term_ptr) {  // may be null e.g. 'all' filter
        stats_cast(stats_buf).count =
          static_cast<irs::doc_id_t>(term_ptr->docs_count);
        const_cast<term_collector*>(term_ptr)->docs_count = 0;
      }
    }

    irs::IndexFeatures features() const override {
      return irs::IndexFeatures::NONE;
    }

    irs::sort::field_collector::ptr prepare_field_collector() const override {
      return nullptr;  // do not need to collect stats
    }

    irs::ScoreFunction prepare_scorer(const irs::SubReader&,
                                      const irs::term_reader&,
                                      const irs::byte_type* stats_buf,
                                      const irs::attribute_provider& doc_attrs,
                                      irs::score_t /*boost*/) const override {
      auto* doc = irs::get<irs::document>(doc_attrs);
      auto& stats = stats_cast(stats_buf);
      const irs::doc_id_t* docs_count = &stats.count;
      return {
        std::make_unique<frequency_sort::prepared::scorer>(docs_count, doc),
        [](irs::score_ctx* ctx, irs::score_t* res) noexcept {
          const auto& state = *reinterpret_cast<scorer*>(ctx);

          // docs_count may be nullptr if no collector called,
          // e.g. by range_query for bitset_doc_iterator
          if (state.docs_count) {
            *res = 1.f / (*state.docs_count);
          } else {
            *res = std::numeric_limits<irs::score_t>::infinity();
          }
        }};
    }

    irs::sort::term_collector::ptr prepare_term_collector() const override {
      return std::make_unique<term_collector>();
    }
  };

  frequency_sort() : sort(irs::type<frequency_sort>::get()) {}
  virtual prepared::ptr prepare() const {
    return std::make_unique<frequency_sort::prepared>();
  }
};  // sort::frequency_sort

}  // namespace sort

class FilterTestCaseBase : public index_test_base {
 protected:
  using Docs = std::vector<irs::doc_id_t>;
  using ScoredDocs =
    std::vector<std::pair<irs::doc_id_t, std::vector<irs::score_t>>>;
  using Costs = std::vector<irs::cost::cost_t>;

  struct Seek {
    irs::doc_id_t target;
  };

  struct Skip {
    irs::doc_id_t count;
  };

  struct Next {};

  using Action = std::variant<Seek, Skip, Next>;

  struct Test {
    Action action;
    irs::doc_id_t expected;
    std::vector<irs::score_t> score{};
  };

  using Tests = std::vector<Test>;

  // Validate matched documents and query cost
  static void CheckQuery(const irs::filter& filter, const Docs& expected,
                         const Costs& expected_costs,
                         const irs::IndexReader& index,
                         std::string_view source_location = {});

  // Validate matched documents
  static void CheckQuery(const irs::filter& filter, const Docs& expected,
                         const irs::IndexReader& index,
                         std::string_view source_location = {});

  // Validate documents and its scores
  static void CheckQuery(const irs::filter& filter,
                         std::span<const irs::sort::ptr> order,
                         const ScoredDocs& expected,
                         const irs::IndexReader& index,
                         std::string_view source_location = {});

  // Validate documents and its scores with test cases
  static void CheckQuery(const irs::filter& filter,
                         std::span<const irs::sort::ptr> order,
                         const std::vector<Tests>& tests,
                         const irs::IndexReader& index,
                         std::string_view source_location = {});

  // Validate document order
  static void CheckQuery(const irs::filter& filter,
                         std::span<const irs::sort::ptr> order,
                         const std::vector<irs::doc_id_t>& expected,
                         const irs::IndexReader& index,
                         bool score_must_be_present = true,
                         bool reverse = false);

 private:
  static void GetQueryResult(const irs::filter::prepared::ptr& q,
                             const irs::IndexReader& index, Docs& result,
                             Costs& result_costs,
                             std::string_view source_location);

  static void GetQueryResult(const irs::filter::prepared::ptr& q,
                             const irs::IndexReader& index,
                             const irs::Order& ord, ScoredDocs& result,
                             Costs& result_costs,
                             std::string_view source_location);
};

struct empty_term_reader : irs::singleton<empty_term_reader>, irs::term_reader {
  virtual irs::seek_term_iterator::ptr iterator() const {
    return irs::seek_term_iterator::empty();
  }

  virtual irs::seek_term_iterator::ptr iterator(
    irs::automaton_table_matcher&) const {
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
  virtual irs::bytes_view(min)() const { return {}; }

  // most significant term
  virtual irs::bytes_view(max)() const { return {}; }
};  // empty_term_reader

class empty_filter_visitor : public irs::filter_visitor {
 public:
  void prepare(const irs::SubReader& segment, const irs::term_reader& field,
               const irs::seek_term_iterator& terms) noexcept override {
    it_ = &terms;
    ++prepare_calls_counter_;
  }

  void visit(irs::score_t boost) noexcept override {
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

  size_t visit_calls_counter() const noexcept { return visit_calls_counter_; }

  const std::vector<std::pair<irs::bstring, irs::score_t>>& terms()
    const noexcept {
    return terms_;
  }

  template<typename Char>
  std::vector<std::pair<std::basic_string_view<Char>, irs::score_t>> term_refs()
    const {
    std::vector<std::pair<std::basic_string_view<Char>, irs::score_t>> refs(
      terms_.size());
    auto begin = refs.begin();
    for (auto& term : terms_) {
      begin->first = irs::ViewCast<Char>(irs::bytes_view{term.first});
      begin->second = term.second;
      ++begin;
    }
    return refs;
  }

  virtual void assert_boost(irs::score_t boost) {
    ASSERT_EQ(irs::kNoBoost, boost);
  }

 private:
  const irs::seek_term_iterator* it_{};
  std::vector<std::pair<irs::bstring, irs::score_t>> terms_;
  size_t prepare_calls_counter_ = 0;
  size_t visit_calls_counter_ = 0;
};

}  // namespace tests
