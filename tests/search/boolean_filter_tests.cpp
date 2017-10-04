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

#include "tests_shared.hpp"
#include "search/all_filter.hpp"
#include "search/boolean_filter.hpp"
#include "search/disjunction.hpp"
#include "search/min_match_disjunction.hpp"
#include "search/exclusion.hpp"
#include "filter_test_case_base.hpp"
#include "formats/formats_10.hpp"
#include "index/iterators.hpp"
#include "store/memory_directory.hpp"
#include "formats/formats.hpp"
#include "store/fs_directory.hpp"
#include "search/term_filter.hpp"
#include "search/term_query.hpp"
#include "utils/singleton.hpp"

#include <functional>

// ----------------------------------------------------------------------------
// --SECTION--                                                   Iterator tests
// ----------------------------------------------------------------------------

namespace tests {
namespace detail {

class basic_doc_iterator: public iresearch::doc_iterator {
 public:
  typedef std::vector<iresearch::doc_id_t> docids_t;

#if defined(_MSC_VER)
  #pragma warning( disable : 4706 )
#elif defined (__GNUC__)
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wparentheses"
#endif

  basic_doc_iterator(
      const docids_t::const_iterator& first,
      const docids_t::const_iterator& last,
      const irs::attribute_store& stats = irs::attribute_store::empty_instance(),
      const iresearch::order::prepared& ord = iresearch::order::prepared::unordered())
      : first_(first), last_(last),
        stats_(&stats), ord_(&ord),
        doc_(iresearch::type_limits<iresearch::type_t::doc_id_t>::invalid()) {
    assert(ord_ && stats_);

    est_.value(std::distance(first_, last_));
    attrs_.emplace(est_);

    if (!ord.empty()) {
      scorers_ = ord_->prepare_scorers(
        empty_sub_reader::instance(),
        empty_term_reader::instance(),
        *stats_,
        attrs_
      );

      score_.prepare(ord, [this] (irs::byte_type* score) {
        scorers_.score(*ord_, score);
      });

      attrs_.emplace(score_);
    }
  }

#if defined(_MSC_VER)
  #pragma warning( default : 4706 )
#elif defined (__GNUC__)
  #pragma GCC diagnostic pop
#endif

  virtual iresearch::doc_id_t value() const override { return doc_; }

  virtual bool next() override {
    if ( first_ == last_ ) {
      doc_ = ir::type_limits<ir::type_t::doc_id_t>::eof();
      return false;
    }

    doc_ = *first_;
    ++first_;
    return true;
  }

  virtual const irs::attribute_view& attributes() const NOEXCEPT override {
    return attrs_;
  }

  virtual iresearch::doc_id_t seek(iresearch::doc_id_t doc) override {
    if (ir::type_limits<ir::type_t::doc_id_t>::eof(doc_) || doc <= doc_) {
      return doc_;
    }

    do { 
      next(); 
    } while ( doc_ < doc );

    return doc_;
  }

 private:
  irs::cost est_;
  irs::attribute_view attrs_;
  iresearch::order::prepared::scorers scorers_;
  docids_t::const_iterator first_;
  docids_t::const_iterator last_;
  const irs::attribute_store* stats_;
  const iresearch::order::prepared* ord_;
  iresearch::score score_;
  iresearch::doc_id_t doc_;
}; // basic_doc_iterator

std::vector<iresearch::doc_id_t> union_all(
    const std::vector<std::vector<iresearch::doc_id_t>>& docs
) {
  std::vector<iresearch::doc_id_t> result;
  for(auto& part : docs) {
    std::copy(part.begin(), part.end(), std::back_inserter(result));
  }
  std::sort(result.begin(), result.end());
  result.erase(std::unique(result.begin(), result.end()), result.end());
  return result;
}

template<typename DocIterator>
std::vector<DocIterator> execute_all(
    const std::vector<std::vector<iresearch::doc_id_t>>& docs
) {
  std::vector<DocIterator> itrs;
  itrs.reserve(docs.size());
  for (const auto& doc : docs) {
    itrs.emplace_back(irs::doc_iterator::make<detail::basic_doc_iterator>(
      doc.begin(), doc.end()
    ));
  }

  return itrs;
}

struct seek_doc {
  iresearch::doc_id_t target;
  iresearch::doc_id_t expected;
};

} // detail

// ----------------------------------------------------------------------------
// --SECTION--                                              Boolean query boost
// ----------------------------------------------------------------------------

namespace detail {

struct boosted: public iresearch::filter {
  struct prepared: iresearch::filter::prepared {
    explicit prepared(
        const basic_doc_iterator::docids_t& docs,
        iresearch::boost::boost_t boost)
        : docs(docs) {
      iresearch::boost::apply(this->attributes(), boost);
    }

    virtual iresearch::doc_iterator::ptr execute(
      const iresearch::sub_reader& rdr,
      const iresearch::order::prepared& ord
    ) const override {
      return iresearch::doc_iterator::make<basic_doc_iterator>(
        docs.begin(), docs.end(), this->attributes(), ord
      );
    }

    basic_doc_iterator::docids_t docs;
  }; // prepared

  DECLARE_FACTORY_DEFAULT();

  virtual iresearch::filter::prepared::ptr prepare(
      const iresearch::index_reader&,
      const iresearch::order::prepared&,
      boost_t boost) const override {
    return filter::prepared::make<boosted::prepared>(docs, this->boost()*boost);
  }

  DECLARE_FILTER_TYPE();
  boosted(): filter(boosted::type()) { }

  basic_doc_iterator::docids_t docs;
}; // boosted

DEFINE_FILTER_TYPE(boosted);
DEFINE_FACTORY_DEFAULT(boosted);

} // detail

TEST(boolean_query_boost, hierarchy) {
  // hierarchy of boosted subqueries
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::order ord;
    ord.add<tests::sort::boost>();
    auto pord = ord.prepare();

    iresearch::And root;
    root.boost(value);
    {
      auto& sub = root.add<iresearch::Or>();
      sub.boost(value);
      {
        auto& node = sub.add<detail::boosted>();
        node.docs = { 1, 2 };
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::boosted>();
        node.docs = { 1, 2, 3 };
        node.boost(value);
      }
    }

    {
      auto& sub = root.add<iresearch::Or>();
      sub.boost(value);
      {
        auto& node = sub.add<detail::boosted>();
        node.docs = { 1, 2 };
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::boosted>();
        node.docs = { 1, 2, 3 };
        node.boost(value);
      }
    }

    {
      auto& sub = root.add<detail::boosted>();
      sub.docs = { 1, 2 };
      sub.boost(value);
    }

    auto prep = root.prepare(
      empty_index_reader::instance(),
      pord
    );

    auto docs = prep->execute(
      empty_sub_reader::instance(), pord
    );

    auto& scr = docs->attributes().get<iresearch::score>();
    ASSERT_FALSE(!scr);

    /* the first hit should be scored as 2*value^3 +2*value^3+value^2 since it
     * exists in all results */
    {
      ASSERT_TRUE(docs->next());
      scr->evaluate();
      auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
      ASSERT_EQ(4*value*value*value+value*value, doc_boost);
    }

    /* the second hit should be scored as 2*value^3+value^2 since it
     * exists in all results */
    {
      ASSERT_TRUE(docs->next());
      scr->evaluate();
      auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
      ASSERT_EQ(4*value*value*value+value*value, doc_boost);
    }

    ASSERT_FALSE(docs->next());
  }

  // hierarchy of boosted subqueries (multiple Or's)
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::order ord;
    ord.add<tests::sort::boost>();
    auto pord = ord.prepare();

    iresearch::And root;
    root.boost(value);
    {
      auto& sub = root.add<iresearch::Or>();
      sub.boost(value);
      {
        auto& node = sub.add<detail::boosted>();
        node.docs = { 1, 2 };
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::boosted>();
        node.docs = { 1, 3 };
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::boosted>();
        node.docs = { 1, 2 };
      }
    }

    {
      auto& sub = root.add<iresearch::Or>();
      {
        auto& node = sub.add<detail::boosted>();
        node.docs = { 1, 2 };
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::boosted>();
        node.docs = { 1, 2, 3 };
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::boosted>();
        node.docs = { 1 };
        node.boost(value);
      }
    }

    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2, 3 };
    }

    auto prep = root.prepare(
        empty_index_reader::instance(),
        pord
    );

    auto docs = prep->execute(
      empty_sub_reader::instance(), pord
    );

    auto& scr = docs->attributes().get<iresearch::score>();
    ASSERT_FALSE(!scr);

    /* the first hit should be scored as 2*value^3+value^2+3*value^2+value
     * since it exists in all results */
    {
      ASSERT_TRUE(docs->next());
      scr->evaluate();
      auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
      ASSERT_EQ(2*value*value*value+4*value*value+value, doc_boost);
    }

    /* the second hit should be scored as value^3+value^2+2*value^2 since it
     * exists in all results */
    {
      ASSERT_TRUE(docs->next());
      scr->evaluate();
      auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
      ASSERT_EQ(value*value*value+3*value*value+value, doc_boost);
    }

    /* the third hit should be scored as value^3+value^2 since it
     * exists in all results */
    {
      ASSERT_TRUE(docs->next());
      scr->evaluate();
      auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
      ASSERT_EQ(value*value*value+value*value+value, doc_boost);
    }

    ASSERT_FALSE(docs->next());
  }

  // hierarchy of boosted subqueries (multiple And's)
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::order ord;
    ord.add<tests::sort::boost>();
    auto pord = ord.prepare();

    iresearch::Or root;
    root.boost(value);
    {
      auto& sub = root.add<iresearch::And>();
      sub.boost(value);
      {
        auto& node = sub.add<detail::boosted>();
        node.docs = { 1, 2 };
      }
      {
        auto& node = sub.add<detail::boosted>();
        node.docs = { 1, 3 };
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::boosted>();
        node.docs = { 1, 2 };
      }
    }

    {
      auto& sub = root.add<iresearch::And>();
      {
        auto& node = sub.add<detail::boosted>();
        node.docs = { 1, 2 };
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::boosted>();
        node.docs = { 1, 2, 3 };
        node.boost(value);
      }
      {
        auto& node = sub.add<detail::boosted>();
        node.docs = { 1 };
        node.boost(value);
      }
    }

    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2, 3 };
    }

    auto prep = root.prepare(
      empty_index_reader::instance(),
      pord
    );

    auto docs = prep->execute(empty_sub_reader::instance(), pord);

    auto& scr = docs->attributes().get<iresearch::score>();
    ASSERT_FALSE(!scr);

    // the first hit should be scored as value^3+2*value^2+3*value^2+value
    {
      ASSERT_TRUE(docs->next());
      scr->evaluate();
      auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
      ASSERT_EQ(value*value*value+5*value*value+value, doc_boost);
    }

    // the second hit should be scored as value
    {
      ASSERT_TRUE(docs->next());
      scr->evaluate();
      auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
      ASSERT_EQ(value, doc_boost);
    }

    // the third hit should be scored as value
    {
      ASSERT_TRUE(docs->next());
      scr->evaluate();
      auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
      ASSERT_EQ(value, doc_boost);
    }

    ASSERT_FALSE(docs->next());
  }
}

TEST(boolean_query_boost, and) {
  // empty boolean unboosted query
  {
    iresearch::And root;

    auto prep = root.prepare(
      empty_index_reader::instance(),
      iresearch::order::prepared::unordered()
    );

    auto& boost = const_cast<const irs::attribute_store&>(prep->attributes()).get<irs::boost>();
    ASSERT_TRUE(!boost);
  }

  // boosted empty boolean query
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::And root;
    root.boost(value);

    auto prep = root.prepare(
      empty_index_reader::instance(),
      iresearch::order::prepared::unordered()
    );

    auto& boost = const_cast<const irs::attribute_store&>(prep->attributes()).get<irs::boost>();
    ASSERT_FALSE(boost);
  }

  // single boosted subquery
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::order ord;
    ord.add<tests::sort::boost>();
    auto pord = ord.prepare();

    iresearch::And root;
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1 };
      node.boost(value);
    }

    auto prep = root.prepare(
      empty_index_reader::instance(),
      pord
    );

    auto docs = prep->execute(empty_sub_reader::instance(), pord);

    auto& scr = docs->attributes().get<iresearch::score>();
    ASSERT_FALSE(!scr);
    ASSERT_TRUE(docs->next());
    scr->evaluate();
    auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0) ;
    ASSERT_EQ(value, doc_boost);
    ASSERT_FALSE(docs->next());
  }

  // boosted root & single boosted subquery
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::order ord;
    ord.add<tests::sort::boost>();
    auto pord = ord.prepare();

    iresearch::And root;
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1 };
      node.boost(value);
    }
    root.boost(value);

    auto prep = root.prepare(
      empty_index_reader::instance(),
        pord
    );

    auto docs = prep->execute(empty_sub_reader::instance(), pord);

    auto& scr = docs->attributes().get<iresearch::score>();
    ASSERT_FALSE(!scr);
    ASSERT_TRUE(docs->next());
    scr->evaluate();
    auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0) ;
    ASSERT_EQ(value*value, doc_boost);
    ASSERT_FALSE(docs->next());
  }

  // boosted root & several boosted subqueries
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::order ord;
    ord.add<tests::sort::boost>();
    auto pord = ord.prepare();

    iresearch::And root;
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1 };
      node.boost(value);
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
      node.boost(value);
    }
    root.boost(value);

    auto prep = root.prepare(
      empty_index_reader::instance(),
      pord
    );

    auto docs = prep->execute(empty_sub_reader::instance(), pord);

    /* the first hit should be scored as value*value + value*value since it
     * exists in both results */
    auto& scr = docs->attributes().get<iresearch::score>();
    ASSERT_FALSE(!scr);
    ASSERT_TRUE(docs->next());
    scr->evaluate();
    auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0) ;
    ASSERT_EQ(2*value*value, doc_boost);

    ASSERT_FALSE(docs->next());
  }

  // boosted root & several boosted subqueries
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::order ord;
    ord.add<tests::sort::boost>();
    auto pord = ord.prepare();

    iresearch::And root;
    root.boost(value);
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1 };
      node.boost(value);
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
      node.boost(value);
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
      node.boost(value);
    }

    auto prep = root.prepare(
      empty_index_reader::instance(),
      pord
    );

    auto docs = prep->execute(empty_sub_reader::instance(), pord);

    auto& scr = docs->attributes().get<iresearch::score>();
    ASSERT_FALSE(!scr);
    ASSERT_TRUE(docs->next());
    scr->evaluate();
    auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0) ;
    ASSERT_EQ(3*value*value+value, doc_boost);

    ASSERT_FALSE(docs->next());
  }

   // unboosted root & several boosted subqueries
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::order ord;
    ord.add<tests::sort::boost>();
    auto pord = ord.prepare();

    iresearch::And root;
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1 };
      node.boost(value);
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
      node.boost(value);
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
      node.boost(value);
    }

    auto prep = root.prepare(
      empty_index_reader::instance(),
      pord
    );

    auto docs = prep->execute(empty_sub_reader::instance(), pord);

    auto& scr = docs->attributes().get<iresearch::score>();
    ASSERT_FALSE(!scr);
    ASSERT_TRUE(docs->next());
    scr->evaluate();
    auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0) ;
    ASSERT_EQ(3*value, doc_boost);

    ASSERT_FALSE(docs->next());
  }


  // unboosted root & several unboosted subqueries
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::order ord;
    ord.add<tests::sort::boost>();
    auto pord = ord.prepare();

    iresearch::And root;
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1 };
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
    }

    auto prep = root.prepare(
      empty_index_reader::instance(),
      pord
    );

    auto docs = prep->execute(empty_sub_reader::instance(), pord);

    auto& scr = docs->attributes().get<iresearch::score>();
    ASSERT_FALSE(!scr);
    ASSERT_TRUE(docs->next());
    scr->evaluate();
    auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0) ;
    ASSERT_EQ(iresearch::boost::boost_t(0), doc_boost);

    ASSERT_FALSE(docs->next());
  }
}

TEST(boolean_query_boost, or) {
  // single unboosted query
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::Or root;

    auto prep = root.prepare(
      empty_index_reader::instance(),
      iresearch::order::prepared::unordered()
    );

    auto& boost = const_cast<const irs::attribute_store&>(prep->attributes()).get<irs::boost>();
    ASSERT_TRUE(!boost);
  }

  // empty single boosted query
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::Or root;
    root.boost(value);

    auto prep = root.prepare(
      empty_index_reader::instance(),
      iresearch::order::prepared::unordered()
    );

    auto& boost = const_cast<const irs::attribute_store&>(prep->attributes()).get<irs::boost>();
    ASSERT_FALSE(boost);
  }

  // boosted empty single query
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::order ord;
    ord.add<tests::sort::boost>();
    auto pord = ord.prepare();

    iresearch::Or root;
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1 };
    }
    root.boost(value);

    auto prep = root.prepare(
      empty_index_reader::instance(),
      pord
    );

    auto docs = prep->execute(empty_sub_reader::instance(), pord);

    auto& scr = docs->attributes().get<iresearch::score>();
    ASSERT_FALSE(!scr);
    ASSERT_TRUE(docs->next());
    scr->evaluate();
    auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
    ASSERT_EQ(value, doc_boost);
    ASSERT_FALSE(docs->next());
  }

  // boosted single query & subquery
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::order ord;
    ord.add<tests::sort::boost>();
    auto pord = ord.prepare();

    iresearch::Or root;
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1 };
      node.boost(value);
    }
    root.boost(value);

    auto prep = root.prepare(
      empty_index_reader::instance(),
      pord
    );

    auto docs = prep->execute(empty_sub_reader::instance(), pord);

    auto& scr = docs->attributes().get<iresearch::score>();
    ASSERT_FALSE(!scr);
    ASSERT_TRUE(docs->next());
    scr->evaluate();
    auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
    ASSERT_EQ(value*value, doc_boost);
    ASSERT_FALSE(docs->next());
  }

  // boosted single query & several subqueries
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::order ord;
    ord.add<tests::sort::boost>();
    auto pord = ord.prepare();

    iresearch::Or root;
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1 };
      node.boost(value);
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
      node.boost(value);
    }
    root.boost(value);

    auto prep = root.prepare(
      empty_index_reader::instance(),
      pord
    );

    auto docs = prep->execute(empty_sub_reader::instance(), pord);

    auto& scr = docs->attributes().get<iresearch::score>();
    ASSERT_FALSE(!scr);

    /* the first hit should be scored as value*value + value*value since it
     * exists in both results */
    {
      ASSERT_TRUE(docs->next());
      scr->evaluate();
      auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
      ASSERT_EQ(2 * value * value, doc_boost);
    }

    /* the second hit should be scored as value*value since it
     * exists in second result only */
    {
      ASSERT_TRUE(docs->next());
      scr->evaluate();
      auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
      ASSERT_EQ(value * value, doc_boost);
    }

    ASSERT_FALSE(docs->next());
  }

  // boosted root & several boosted subqueries
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::order ord;
    ord.add<tests::sort::boost>();
    auto pord = ord.prepare();

    iresearch::Or root;
    root.boost(value);

    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1 };
      node.boost(value);
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
      node.boost(value);
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
      node.boost(value);
    }

    auto prep = root.prepare(
      empty_index_reader::instance(),
      pord
    );

    auto docs = prep->execute(empty_sub_reader::instance(), pord);

    auto& scr = docs->attributes().get<iresearch::score>();
    ASSERT_FALSE(!scr);

    // first hit
    {
      ASSERT_TRUE(docs->next());
      scr->evaluate();
      auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
      ASSERT_EQ(3*value*value + value, doc_boost);
    }

    // second hit
    {
      ASSERT_TRUE(docs->next());
      scr->evaluate();
      auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
      ASSERT_EQ(2*value*value + value, doc_boost);
    }

    ASSERT_FALSE(docs->next());
  }

  // unboosted root & several boosted subqueries
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::order ord;
    ord.add<tests::sort::boost>();
    auto pord = ord.prepare();

    iresearch::Or root;

    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1 };
      node.boost(value);
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
      node.boost(value);
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
      node.boost(value);
    }

    auto prep = root.prepare(
      empty_index_reader::instance(),
      pord
    );

    auto docs = prep->execute(empty_sub_reader::instance(), pord);

    auto& scr = docs->attributes().get<iresearch::score>();
    ASSERT_FALSE(!scr);

    // first hit
    {
      ASSERT_TRUE(docs->next());
      scr->evaluate();
      auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
      ASSERT_EQ(3*value, doc_boost);
    }

    // second hit
    {
      ASSERT_TRUE(docs->next());
      scr->evaluate();
      auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
      ASSERT_EQ(2*value, doc_boost);
    }

    ASSERT_FALSE(docs->next());
  }

  // unboosted root & several unboosted subqueries
  {
    const iresearch::boost::boost_t value = 5;

    iresearch::order ord;
    ord.add<tests::sort::boost>();
    auto pord = ord.prepare();

    iresearch::Or root;
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1 };
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
    }
    {
      auto& node = root.add<detail::boosted>();
      node.docs = { 1, 2 };
    }

    auto prep = root.prepare(
      empty_index_reader::instance(),
      pord
    );

    auto docs = prep->execute(empty_sub_reader::instance(), pord);

    auto& scr = docs->attributes().get<iresearch::score>();
    ASSERT_FALSE(!scr);

    // first hit
    {
      ASSERT_TRUE(docs->next());
      scr->evaluate();
      auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
      ASSERT_EQ(iresearch::boost::boost_t(0), doc_boost);
    }

    // second hit
    {
      ASSERT_TRUE(docs->next());
      scr->evaluate();
      auto doc_boost = pord.get<tests::sort::boost::score_t>(scr->c_str(), 0);
      ASSERT_EQ(iresearch::boost::boost_t(0), doc_boost);
    }

    ASSERT_FALSE(docs->next());
  }
}

// ----------------------------------------------------------------------------
// --SECTION--                                         Boolean query estimation
// ----------------------------------------------------------------------------

namespace detail {

struct unestimated: public iresearch::filter {
  struct doc_iterator : iresearch::doc_iterator {
    virtual iresearch::doc_id_t value() const override {
      // prevent iterator to filter out
      return iresearch::type_limits<iresearch::type_t::doc_id_t>::invalid();
    }
    virtual bool next() override { return false; }
    virtual iresearch::doc_id_t seek(iresearch::doc_id_t) override {
      // prevent iterator to filter out
      return iresearch::type_limits<iresearch::type_t::doc_id_t>::invalid();
    }
    virtual const irs::attribute_view& attributes() const NOEXCEPT override {
      return irs::attribute_view::empty_instance();
    }
  }; // doc_iterator

  struct prepared: public iresearch::filter::prepared {
    virtual iresearch::doc_iterator::ptr execute(
      const iresearch::sub_reader&, 
      const iresearch::order::prepared&
    ) const override {
      return iresearch::doc_iterator::make<unestimated::doc_iterator>();
    }
  }; // prepared

  virtual filter::prepared::ptr prepare(
      const iresearch::index_reader&,
      const iresearch::order::prepared&,
      boost_t ) const override {
    return filter::prepared::make<unestimated::prepared>();
  }

  DECLARE_FILTER_TYPE();
  DECLARE_FACTORY_DEFAULT();

  unestimated() : filter(unestimated::type()) {}
}; // unestimated

DEFINE_FILTER_TYPE(unestimated);
DEFINE_FACTORY_DEFAULT(unestimated);

struct estimated: public iresearch::filter {
  struct doc_iterator : iresearch::doc_iterator {
    doc_iterator(iresearch::cost::cost_t est, bool* evaluated) {
      cost.rule([est, evaluated]() {
        *evaluated = true;
        return est;
      });
      attrs.emplace(cost);
    }
    virtual iresearch::doc_id_t value() const override {
      // prevent iterator to filter out
      return iresearch::type_limits<iresearch::type_t::doc_id_t>::invalid();
    }
    virtual bool next() override { return false; }
    virtual iresearch::doc_id_t seek(iresearch::doc_id_t) override {
      // prevent iterator to filter out
      return iresearch::type_limits<iresearch::type_t::doc_id_t>::invalid();
    }
    virtual const irs::attribute_view& attributes() const NOEXCEPT override {
      return attrs;
    }

    irs::cost cost;
    irs::attribute_view attrs;
  }; // doc_iterator

  struct prepared: public iresearch::filter::prepared {
    explicit prepared(iresearch::cost::cost_t est,bool* evaluated)
      : evaluated(evaluated), est(est) {
    }

    virtual iresearch::doc_iterator::ptr execute(
      const iresearch::sub_reader&,
      const iresearch::order::prepared&
    ) const override {
      return iresearch::doc_iterator::make<estimated::doc_iterator>(
        est, evaluated
      );
    }

    bool* evaluated;
    iresearch::cost::cost_t est;
  }; // prepared
  
  virtual filter::prepared::ptr prepare(
      const iresearch::index_reader&,
      const iresearch::order::prepared&,
      boost_t ) const override {
    return filter::prepared::make<estimated::prepared>(est,&evaluated);
  }

  DECLARE_FILTER_TYPE();
  DECLARE_FACTORY_DEFAULT();

  explicit estimated()
    : filter(estimated::type()) {
  }

  mutable bool evaluated = false;
  iresearch::cost::cost_t est{};
}; // estimated

DEFINE_FILTER_TYPE(estimated);
DEFINE_FACTORY_DEFAULT(estimated);

} // detail

TEST( boolean_query_estimation, or ) {
  // estimated subqueries
  {
    iresearch::Or root;
    root.add<detail::estimated>().est = 100;
    root.add<detail::estimated>().est = 320;
    root.add<detail::estimated>().est = 10;
    root.add<detail::estimated>().est = 1;
    root.add<detail::estimated>().est = 100;

    auto prep = root.prepare(
      empty_index_reader::instance(),
      iresearch::order::prepared::unordered()
    );

    auto docs = prep->execute(empty_sub_reader::instance());

    // check that subqueries were not estimated
    for(auto it = root.begin(), end = root.end(); it != end; ++it) {
      ASSERT_FALSE(it.safe_as<detail::estimated>()->evaluated);
    }

    ASSERT_EQ(531, iresearch::cost::extract(docs->attributes()));

    // check that subqueries were estimated
    for(auto it = root.begin(), end = root.end(); it != end; ++it) {
      ASSERT_TRUE(it.safe_as<detail::estimated>()->evaluated);
    }
  }

  // unestimated subqueries
  {
    iresearch::Or root;
    root.add<detail::unestimated>();
    root.add<detail::unestimated>();
    root.add<detail::unestimated>();
    root.add<detail::unestimated>();

    auto prep = root.prepare(
      empty_index_reader::instance(),
      iresearch::order::prepared::unordered()
    );
   
    auto docs = prep->execute(empty_sub_reader::instance());
    ASSERT_EQ(0, iresearch::cost::extract(docs->attributes()));
  }

   // estimated/unestimated subqueries
  {
    iresearch::Or root;
    root.add<detail::estimated>().est = 100;
    root.add<detail::estimated>().est = 320;
    root.add<detail::unestimated>();
    root.add<detail::estimated>().est = 10;
    root.add<detail::unestimated>();
    root.add<detail::estimated>().est = 1;
    root.add<detail::estimated>().est = 100;
    root.add<detail::unestimated>();

    auto prep = root.prepare(
      empty_index_reader::instance(),
      iresearch::order::prepared::unordered()
    );

    auto docs = prep->execute(empty_sub_reader::instance());
    
    /* check that subqueries were not estimated */
    for(auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto est_query = it.safe_as<detail::estimated>();
      if (est_query) {
        ASSERT_FALSE(est_query->evaluated);
      }
    }

    ASSERT_EQ(531, iresearch::cost::extract(docs->attributes()));

    /* check that subqueries were estimated */
    for(auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto est_query = it.safe_as<detail::estimated>();
      if (est_query) {
        ASSERT_TRUE(est_query->evaluated);
      }
    }
  }

  // estimated/unestimated/negative subqueries
  {
    iresearch::Or root;
    root.add<detail::estimated>().est = 100;
    root.add<detail::estimated>().est = 320;
    root.add<iresearch::Not>().filter<detail::estimated>().est = 3;
    root.add<detail::unestimated>();
    root.add<detail::estimated>().est = 10;
    root.add<detail::unestimated>();
    root.add<detail::estimated>().est = 7;
    root.add<detail::estimated>().est = 100;
    root.add<iresearch::Not>().filter<detail::unestimated>();
    root.add<iresearch::Not>().filter<detail::estimated>().est = 0;
    root.add<detail::unestimated>();

    auto prep = root.prepare(
      empty_index_reader::instance(),
      iresearch::order::prepared::unordered()
    );

    auto docs = prep->execute(empty_sub_reader::instance());

    // check that subqueries were not estimated
    for(auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto est_query = it.safe_as<detail::estimated>();
      if (est_query) {
        ASSERT_FALSE(est_query->evaluated);
      }
    }

    ASSERT_EQ(537, iresearch::cost::extract(docs->attributes()));

    // check that subqueries were estimated
    for(auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto est_query = it.safe_as<detail::estimated>();
      if (est_query) {
        ASSERT_TRUE(est_query->evaluated);
      }
    }
  }

  // empty case
  {
    iresearch::Or root;

    auto prep = root.prepare(
      empty_index_reader::instance(),
      iresearch::order::prepared::unordered()
    );

    auto docs = prep->execute(empty_sub_reader::instance());
    ASSERT_EQ(0, iresearch::cost::extract(docs->attributes()));
  }
}

TEST( boolean_query_estimation, and ) {
  // estimated subqueries
  {
    iresearch::And root;
    root.add<detail::estimated>().est = 100;
    root.add<detail::estimated>().est = 320;
    root.add<detail::estimated>().est = 10;
    root.add<detail::estimated>().est = 1;
    root.add<detail::estimated>().est = 100;

    auto prep = root.prepare(
      empty_index_reader::instance(),
      iresearch::order::prepared::unordered()
    );
    
    auto docs = prep->execute(empty_sub_reader::instance());

    // check that subqueries were estimated
    for(auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto est_query = it.safe_as<detail::estimated>();
      if (est_query) {
        ASSERT_TRUE(est_query->evaluated);
      }
    }

    ASSERT_EQ(1, iresearch::cost::extract(docs->attributes()));
  }

  // unestimated subqueries
  {
    iresearch::And root;
    root.add<detail::unestimated>();
    root.add<detail::unestimated>();
    root.add<detail::unestimated>();
    root.add<detail::unestimated>();

    auto prep = root.prepare(
      empty_index_reader::instance(),
      iresearch::order::prepared::unordered()
    );
    
    auto docs = prep->execute(empty_sub_reader::instance());

    // check that subqueries were estimated
    for(auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto est_query = it.safe_as<detail::estimated>();
      if (est_query) {
        ASSERT_TRUE(est_query->evaluated);
      }
    }

    ASSERT_EQ(
      decltype(iresearch::cost::MAX)(iresearch::cost::MAX),
      iresearch::cost::extract(docs->attributes())
    );
  }

  // estimated/unestimated subqueries
  {
    iresearch::And root;
    root.add<detail::estimated>().est = 100;
    root.add<detail::estimated>().est = 320;
    root.add<detail::unestimated>();
    root.add<detail::estimated>().est = 10;
    root.add<detail::unestimated>();
    root.add<detail::estimated>().est = 1;
    root.add<detail::estimated>().est = 100;
    root.add<detail::unestimated>();

    auto prep = root.prepare(
      empty_index_reader::instance(),
      iresearch::order::prepared::unordered()
    );

    auto docs = prep->execute(empty_sub_reader::instance());

    // check that subqueries were estimated
    for(auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto est_query = it.safe_as<detail::estimated>();
      if (est_query) {
        ASSERT_TRUE(est_query->evaluated);
      }
    }

    ASSERT_EQ(1, iresearch::cost::extract(docs->attributes()));
  }

  // estimated/unestimated/negative subqueries
  {
    iresearch::And root;
    root.add<detail::estimated>().est = 100;
    root.add<detail::estimated>().est = 320;
    root.add<iresearch::Not>().filter<detail::estimated>().est = 3;
    root.add<detail::unestimated>();
    root.add<detail::estimated>().est = 10;
    root.add<detail::unestimated>();
    root.add<detail::estimated>().est = 7;
    root.add<detail::estimated>().est = 100;
    root.add<iresearch::Not>().filter<detail::unestimated>();
    root.add<iresearch::Not>().filter<detail::estimated>().est = 0;
    root.add<detail::unestimated>();

    auto prep = root.prepare(
      empty_index_reader::instance(),
      iresearch::order::prepared::unordered()
    );

    auto docs = prep->execute(empty_sub_reader::instance());

    // check that subqueries were estimated
    for(auto it = root.begin(), end = root.end(); it != end; ++it) {
      auto est_query = it.safe_as<detail::estimated>();
      if (est_query) {
        ASSERT_TRUE(est_query->evaluated);
      }
    }

    ASSERT_EQ(7, iresearch::cost::extract(docs->attributes()));
  }

  // empty case
  {
    iresearch::And root;
    auto prep = root.prepare(
      empty_index_reader::instance(),
      iresearch::order::prepared::unordered()
    );

    auto docs = prep->execute(empty_sub_reader::instance());
    ASSERT_EQ(0, iresearch::cost::extract(docs->attributes()));
  }
}

// ----------------------------------------------------------------------------
// --SECTION--                                           iterator0 OR iterator1 
// ----------------------------------------------------------------------------

TEST(basic_disjunction, next) {
  typedef iresearch::basic_disjunction disjunction;
  // simple case
  {
    std::vector<iresearch::doc_id_t> first{ 1, 2, 5, 7, 9, 11, 45 };
    std::vector<iresearch::doc_id_t> last{ 1, 5, 6, 12, 29 };
    std::vector<iresearch::doc_id_t> expected{ 1, 2, 5, 6, 7, 9, 11, 12, 29, 45 };
    std::vector<iresearch::doc_id_t> result;
    {
      disjunction it(
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(first.begin(), first.end()),
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(last.begin(), last.end())
      );
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }

    ASSERT_EQ( expected, result );
  }

  // basic case : single dataset
  {
    std::vector<iresearch::doc_id_t> first{ 1, 2, 5, 7, 9, 11, 45 };
    std::vector<iresearch::doc_id_t> last{};
    std::vector<iresearch::doc_id_t> result;
    {
      disjunction it(
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(first.begin(), first.end()),
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(last.begin(), last.end())
      );
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }

    ASSERT_EQ( first, result );
  }

  // basic case : single dataset
  {
    std::vector<iresearch::doc_id_t> first{};
    std::vector<iresearch::doc_id_t> last{ 1, 5, 6, 12, 29 };
    std::vector<iresearch::doc_id_t> result;
    {
      disjunction it(
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(first.begin(), first.end()),
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(last.begin(), last.end())
      );
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( last, result );
  }
 
  // basic case : same datasets
  {
    std::vector<iresearch::doc_id_t> first{ 1, 2, 5, 7, 9, 11, 45 };
    std::vector<iresearch::doc_id_t> last{ 1, 2, 5, 7, 9, 11, 45 };
    std::vector<iresearch::doc_id_t> result;
    {
      disjunction it(
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(first.begin(), first.end()),
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(last.begin(), last.end())
      );
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( first, result );
  }

  // basic case : single dataset
  {
    std::vector<iresearch::doc_id_t> first{ 24 };
    std::vector<iresearch::doc_id_t> last{};
    std::vector<iresearch::doc_id_t> result;
    {
      disjunction it(
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(first.begin(), first.end()),
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(last.begin(), last.end())
      );
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( first, result );
  }

  // empty
  {
    std::vector<iresearch::doc_id_t> first{};
    std::vector<iresearch::doc_id_t> last{};
    std::vector<iresearch::doc_id_t> expected{};
    std::vector<iresearch::doc_id_t> result;
    {
      disjunction it(
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(first.begin(), first.end()),
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(last.begin(), last.end())
      );
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( expected, result );
  }
}

TEST(basic_disjunction_test, seek) {
  typedef iresearch::basic_disjunction disjunction;
  // simple case
  {
    std::vector<iresearch::doc_id_t> first{ 1, 2, 5, 7, 9, 11, 45 };
    std::vector<iresearch::doc_id_t> last{ 1, 5, 6, 12, 29 };
    std::vector<detail::seek_doc> expected{
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
        {1, 1},
        {9, 9},
        {8, 9},
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 9},
        {12, 12},
        {8, 12},
        {13, 29},
        {45, 45},
        {57, ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };

      disjunction it(
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(first.begin(), first.end()),
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(last.begin(), last.end())
      );

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek( target.target ));
    }
  }

  // empty datasets
  {
    std::vector<iresearch::doc_id_t> first{};
    std::vector<iresearch::doc_id_t> last{};
    std::vector<detail::seek_doc> expected{
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
      {6, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };

    disjunction it(
      iresearch::doc_iterator::make<detail::basic_doc_iterator>(first.begin(), first.end()),
      iresearch::doc_iterator::make<detail::basic_doc_iterator>(last.begin(), last.end())
    );

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek( target.target ));
    }
  }

  // NO_MORE_DOCS
  {
    std::vector<iresearch::doc_id_t> first{ 1, 2, 5, 7, 9, 11, 45 };
    std::vector<iresearch::doc_id_t> last{ 1, 5, 6, 12, 29 };
    std::vector<detail::seek_doc> expected{
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
      {ir::type_limits<ir::type_t::doc_id_t>::eof(), ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {9, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {12, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {13, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {45, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {57, ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };

    disjunction it(
      iresearch::doc_iterator::make<detail::basic_doc_iterator>(first.begin(), first.end()),
      iresearch::doc_iterator::make<detail::basic_doc_iterator>(last.begin(), last.end())
    );

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek( target.target ));
    }
  }

  // INVALID_DOC
  {
    std::vector<iresearch::doc_id_t> first{ 1, 2, 5, 7, 9, 11, 45 };
    std::vector<iresearch::doc_id_t> last{ 1, 5, 6, 12, 29 };
    std::vector<detail::seek_doc> expected{
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
        {9, 9},
        {12, 12},
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 12},
        {45, 45},
        {57, ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };

    disjunction it(
      iresearch::doc_iterator::make<detail::basic_doc_iterator>(first.begin(), first.end()),
      iresearch::doc_iterator::make<detail::basic_doc_iterator>(last.begin(), last.end())
    );

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }
}

// ----------------------------------------------------------------------------
// --SECTION--                       iterator0 OR iterator1 OR iterator2 OR ... 
// ----------------------------------------------------------------------------

TEST(disjunction_test, next) {
  using disjunction = iresearch::disjunction;

  // simple case
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 2, 5, 7, 9, 11, 45 },
      { 1, 5, 6, 12, 29 },
      { 1 ,5, 6 }
    };

    std::vector<iresearch::doc_id_t> expected{ 1, 2, 5, 6, 7, 9, 11, 12, 29, 45 };
    std::vector<iresearch::doc_id_t> result;
    {
      disjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( expected, result );
  }

  // simple case
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 2, 5, 7, 9, 11, 45 },
      { 1, 5, 6, 12, 29 },
      { 1, 5, 6 },
      { 256 },
      { 11, 79, 101, 141, 1025, 1101 }
    };

    std::vector<iresearch::doc_id_t> expected{ 1, 2, 5, 6, 7, 9, 11, 12, 29, 45, 79, 101, 141, 256, 1025, 1101 };
    std::vector<iresearch::doc_id_t> result;
    {
      disjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( expected, result );
  }

  // simple case
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1 },
      { 2 },
      { 3 }
    };

    std::vector<iresearch::doc_id_t> expected{ 1, 2, 3 };
    std::vector<iresearch::doc_id_t> result;
    {
      disjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( expected, result );
  }

  // single dataset
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 2, 5, 7, 9, 11, 45 },
    };

    std::vector<iresearch::doc_id_t> result;
    {
      disjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ(docs.front(), result );
  }

  // same datasets
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 2, 5, 7, 9, 11, 45 },
      { 1, 2, 5, 7, 9, 11, 45 },
      { 1, 2, 5, 7, 9, 11, 45 }
    };

    std::vector<iresearch::doc_id_t> result;
    {
      disjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( docs[0], result );
  }

  // empty datasets
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { }, { }, { }
    };

    std::vector<iresearch::doc_id_t> expected{ };
    std::vector<iresearch::doc_id_t> result;
    {
      disjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( expected, result );
  }
}

TEST(disjunction_test, seek) {
  using disjunction = iresearch::disjunction;

  // simple case
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 2, 5, 7, 9, 11, 45 },
      { 1, 5, 6, 12, 29 },
      { 1, 5, 6 }
    };

    std::vector<detail::seek_doc> expected{
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
        {1, 1},
        {9, 9},
        {8, 9},
        {12, 12},
        {13, 29},
        {45, 45},
        {44, 45},
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 45},
        {57, ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };

    disjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // simple case
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 2, 5, 7, 9, 11, 45 },
      { 1, 5, 6, 12, 29 },
      { 1, 5, 6 },
      { 256 },
      { 11, 79, 101, 141, 1025, 1101 }
    };

    std::vector<detail::seek_doc> expected{
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
        {1, 1},
        {9, 9},
        {8, 9},
        {13, 29},
        {45, 45},
        {80, 101},
        {513, 1025},
        {2, 1025},
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 1025},
        {2001, ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };

    disjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // empty datasets
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      {}, {}, {}, {}
    };
    std::vector<detail::seek_doc> expected{
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
      {6, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };

    disjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // NO_MORE_DOCS
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 2, 5, 7, 9, 11, 45 },
      { 1, 5, 6, 12, 29 },
      { 1, 5, 6 },
      { 256 },
      { 11, 79, 101, 141, 1025, 1101 }
    };

    std::vector<detail::seek_doc> expected{
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(),  ir::type_limits<ir::type_t::doc_id_t>::invalid()},
      {ir::type_limits<ir::type_t::doc_id_t>::eof(), ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {9, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {12, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {13, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {45, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {57, ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };

    disjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // INVALID_DOC
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 2, 5, 7, 9, 11, 45 },
      { 1, 5, 6, 12, 29 },
      { 1, 5, 6 },
      { 256 },
      { 11, 79, 101, 141, 1025, 1101 }
    };

    std::vector<detail::seek_doc> expected{
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
        {9, 9},
        {12, 12},
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 12},
        {45, 45},
        {1201, ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };

    disjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }
}

// ----------------------------------------------------------------------------
// --SECTION--  Minimum match count: iterator0 OR iterator1 OR iterator2 OR ...
// ----------------------------------------------------------------------------

TEST(min_match_disjunction_test, next) {
  using disjunction = ir::min_match_disjunction;
  // single dataset
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
        { 1, 2, 5, 7, 9, 11, 45 },
    };

    {
      const size_t min_match_count = 0;
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
          detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
          min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( docs.front(), result );
    }

    {
      const size_t min_match_count = 1;
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( docs.front(), result );
    }

    {
      const size_t min_match_count = 2;
      std::vector<iresearch::doc_id_t> expected{};
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( docs.front(), result );
    }

    {
      const size_t min_match_count = 6;
      std::vector<iresearch::doc_id_t> expected{};
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( docs.front(), result );
    }

    {
      const size_t min_match_count = ir::integer_traits<size_t>::const_max;
      std::vector<iresearch::doc_id_t> expected{};
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( docs.front(), result );
    }
  }

  // simple case
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
        { 1, 2, 5, 7, 9, 11, 45 },
        { 7, 15, 26, 212, 239 },
        { 1001,4001,5001 },
        { 10, 101, 490, 713, 1201, 2801 },
    };

    {
      const size_t min_match_count = 0;
      std::vector<iresearch::doc_id_t> expected = detail::union_all(docs);
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( expected, result );
    }

    {
      const size_t min_match_count = 1;
      std::vector<iresearch::doc_id_t> expected = detail::union_all(docs);
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( expected, result );
    }

    {
      const size_t min_match_count = 2;
      std::vector<iresearch::doc_id_t> expected{ 7 };
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( expected, result );
    }

    {
      const size_t min_match_count = 3;
      std::vector<iresearch::doc_id_t> expected;
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( expected, result );
    }

    // equals to conjunction
    {
      const size_t min_match_count = 4;
      std::vector<iresearch::doc_id_t> expected;
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( expected, result );
    }

    // equals to conjunction
    {
      const size_t min_match_count = 5;
      std::vector<iresearch::doc_id_t> expected{};
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( expected, result );
    }

    // equals to conjunction
    {
      const size_t min_match_count = ir::integer_traits<size_t>::const_max;
      std::vector<iresearch::doc_id_t> expected{};
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( expected, result );
    }
  }

  // simple case
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
        { 1, 2, 5, 7, 9, 11, 45 },
        { 1, 5, 6, 12, 29 },
        { 1 ,5, 6 },
        { 1, 2, 5, 8, 13, 29 },
    };

    {
      const size_t min_match_count = 0;
      std::vector<iresearch::doc_id_t> expected{ 1, 2, 5, 6, 7, 8, 9, 11, 12, 13, 29, 45 };
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( expected, result );
    }

    {
      const size_t min_match_count = 1;
      std::vector<iresearch::doc_id_t> expected{ 1, 2, 5, 6, 7, 8, 9, 11, 12, 13, 29, 45 };
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( expected, result );
    }

    {
      const size_t min_match_count = 2;
      std::vector<iresearch::doc_id_t> expected{ 1, 2, 5, 6, 29 };
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( expected, result );
    }

    {
      const size_t min_match_count = 3;
      std::vector<iresearch::doc_id_t> expected{ 1, 5 };
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( expected, result );
    }

    // equals to conjunction
    {
      const size_t min_match_count = 4;
      std::vector<iresearch::doc_id_t> expected{ 1, 5 };
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( expected, result );
    }

    // equals to conjunction
    {
      const size_t min_match_count = 5;
      std::vector<iresearch::doc_id_t> expected{ 1, 5 };
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( expected, result );
    }

    // equals to conjunction
    {
      const size_t min_match_count = ir::integer_traits<size_t>::const_max;
      std::vector<iresearch::doc_id_t> expected{ 1, 5 };
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( expected, result );
    }
  }

  // same datasets
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
        { 1, 2, 5, 7, 9, 11, 45 },
        { 1, 2, 5, 7, 9, 11, 45 },
        { 1, 2, 5, 7, 9, 11, 45 },
        { 1, 2, 5, 7, 9, 11, 45 },
    };

    // equals to disjunction
    {
      const size_t min_match_count = 0;
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( docs.front(), result );
    }

    // equals to disjunction
    {
      const size_t min_match_count = 1;
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( docs.front(), result );
    }

    {
      const size_t min_match_count = 2;
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( docs.front(), result );
    }

    {
      const size_t min_match_count = 3;
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( docs.front(), result );
    }

    // equals to conjunction
    {
      const size_t min_match_count = 4;
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( docs.front(), result );
    }

    // equals to conjunction
    {
      const size_t min_match_count = 5;
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( docs.front(), result );
    }

    // equals to conjunction
    {
      const size_t min_match_count = ir::integer_traits<size_t>::const_max;
      std::vector<iresearch::doc_id_t> result;
      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
            min_match_count
        );
        ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
        for ( ; it.next(); ) {
          result.push_back( it.value() );
        }
        ASSERT_FALSE( it.next() );
        ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
      }
      ASSERT_EQ( docs.front(), result );
    }
  }

  // empty datasets
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
        { }, { }, { }
    };

    std::vector<iresearch::doc_id_t> expected{ };
    {
      std::vector<iresearch::doc_id_t> expected{};
      {
        std::vector<iresearch::doc_id_t> result;
        {
          disjunction it(
              detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs), 0
          );
          ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
          for ( ; it.next(); ) {
            result.push_back( it.value() );
          }
          ASSERT_FALSE( it.next() );
          ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
        }
        ASSERT_EQ( docs.front(), result );
      }

      {
        std::vector<iresearch::doc_id_t> result;
        {
          disjunction it(
              detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs), 1
          );
          ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
          for ( ; it.next(); ) {
            result.push_back( it.value() );
          }
          ASSERT_FALSE( it.next() );
          ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
        }
        ASSERT_EQ( docs.front(), result );
      }

      {
        std::vector<iresearch::doc_id_t> result;
        {
          disjunction it(
              detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs), ir::integer_traits<size_t>::const_max
          );
          ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
          for ( ; it.next(); ) {
            result.push_back( it.value() );
          }
          ASSERT_FALSE( it.next() );
          ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
        }
        ASSERT_EQ( docs.front(), result );
      }
    }
  }
}

TEST(min_match_disjunction_test, seek) {
  using disjunction = ir::min_match_disjunction;

  // simple case
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 2, 5, 7, 9, 11, 29, 45 },
      { 1, 5, 6, 12, 29 },
      { 1, 5, 6, 12 }
    };

    // equals to disjunction
    {
      const size_t min_match_count = 0;
      std::vector<detail::seek_doc> expected{
          {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
          {1, 1},
          {9, 9},
          {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 9},
          {12, 12},
          {11, 12},
          {13, 29},
          {45, 45},
          {57, ir::type_limits<ir::type_t::doc_id_t>::eof()}
      };

      disjunction it(
        detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
        min_match_count
      );
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    // equals to disjunction
    {
      const size_t min_match_count = 1;
      std::vector<detail::seek_doc> expected{
          {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
          {1, 1},
          {9, 9},
          {8, 9},
          {12, 12},
          {13, 29},
          {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 29},
          {45, 45},
          {57, ir::type_limits<ir::type_t::doc_id_t>::eof()}
      };

      disjunction it(
          detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
          min_match_count
      );
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      const size_t min_match_count = 2;
      std::vector<detail::seek_doc> expected{
          {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
          {1, 1},
          {6, 6},
          {4, 6},
          {7, 12},
          {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 12},
          {29, 29},
          {45, ir::type_limits<ir::type_t::doc_id_t>::eof()}
      };

      disjunction it(
          detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
          min_match_count
      );
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    // equals to conjunction
    {
      const size_t min_match_count = 3;
      std::vector<detail::seek_doc> expected{
          {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
          {1, 1},
          {6, ir::type_limits<ir::type_t::doc_id_t>::eof()}
      };

      disjunction it(
          detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
          min_match_count
      );
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    // equals to conjunction
    {
      const size_t min_match_count = ir::integer_traits<size_t>::const_max;
      std::vector<detail::seek_doc> expected{
          {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
          {1, 1},
          {6, ir::type_limits<ir::type_t::doc_id_t>::eof()}
      };

      disjunction it(
          detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
          min_match_count
      );
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }
  }

  // simple case
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 2, 5, 7, 9, 11, 45, 79, 101 },
      { 1, 5, 6, 12, 29 },
      { 1, 5, 6 },
      { 256 },
      { 11, 79, 101, 141, 1025, 1101 }
    };

    // equals to disjunction
    {
      const size_t min_match_count = 0;
      std::vector<detail::seek_doc> expected{
          {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
          {1, 1},
          {9, 9},
          {8, 9},
          {13, 29},
          {45, 45},
          {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 45},
          {80, 101},
          {513, 1025},
          {2001, ir::type_limits<ir::type_t::doc_id_t>::eof()}
      };

      disjunction it(
          detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
          min_match_count
      );
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    // equals to disjunction
    {
      const size_t min_match_count = 1;
      std::vector<detail::seek_doc> expected{
          {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
          {1, 1},
          {9, 9},
          {8, 9},
          {13, 29},
          {45, 45},
          {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 45},
          {80, 101},
          {513, 1025},
          {2001, ir::type_limits<ir::type_t::doc_id_t>::eof()}
      };

      disjunction it(
          detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
          min_match_count
      );
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      const size_t min_match_count = 2;
      std::vector<detail::seek_doc> expected{
          {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
          {1, 1},
          {6, 6},
          {2, 6},
          {13, 79},
          {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 79},
          {101, 101},
          {513, ir::type_limits<ir::type_t::doc_id_t>::eof()}
      };

      disjunction it(
          detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
          min_match_count
      );
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      const size_t min_match_count = 3;
      std::vector<detail::seek_doc> expected{
          {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
          {1, 1},
          {6, ir::type_limits<ir::type_t::doc_id_t>::eof()}
      };

      disjunction it(
          detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
          min_match_count
      );
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    // equals to conjunction
    {
      const size_t min_match_count = ir::integer_traits<size_t>::const_max;
      std::vector<detail::seek_doc> expected{
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
        {1, ir::type_limits<ir::type_t::doc_id_t>::eof()},
        {6, ir::type_limits<ir::type_t::doc_id_t>::eof()}
      };

      disjunction it(
          detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs),
          min_match_count
      );
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }
  }

  // empty datasets
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      {}, {}, {}, {}
    };

    std::vector<detail::seek_doc> expected{
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
      {6, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };

    {
      disjunction it(
          detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs), 0
      );
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      disjunction it(
          detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs), 1
      );
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      disjunction it(
          detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs), ir::integer_traits<size_t>::const_max
      );
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }
  }

  // NO_MORE_DOCS
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 2, 5, 7, 9, 11, 45 },
      { 1, 5, 6, 12, 29 },
      { 1, 5, 6 },
      { 256 },
      { 11, 79, 101, 141, 1025, 1101 }
    };

    std::vector<detail::seek_doc> expected{
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
      {ir::type_limits<ir::type_t::doc_id_t>::eof(), ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {9, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {12, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {13, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {45, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {57, ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };

    {
      disjunction it(
          detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs), 0
      );
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      disjunction it(
          detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs), 1
      );
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      disjunction it(
          detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs), 2
      );
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }

    {
      disjunction it(
          detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs), ir::integer_traits<size_t>::const_max
      );
      for (const auto& target : expected) {
        ASSERT_EQ(target.expected, it.seek(target.target));
      }
    }
  }

  // INVALID_DOC
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 2, 5, 7, 9, 11, 45 },
      { 1, 5, 6, 12, 29 },
      { 1, 5, 6 },
      { 256 },
      { 11, 79, 101, 141, 1025, 1101 }
    };

    // equals to disjunction
    {
      std::vector<detail::seek_doc> expected{
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
        {9, 9},
        {12, 12},
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 12},
        {45, 45},
        {44, 45},
        {1201, ir::type_limits<ir::type_t::doc_id_t>::eof()}
      };

      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs), 0
        );
        for (const auto& target : expected) {
          ASSERT_EQ(target.expected, it.seek(target.target));
        }
      }

      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs), 1
        );
        for (const auto& target : expected) {
          ASSERT_EQ(target.expected, it.seek(target.target));
        }
      }
    }

    {
      std::vector<detail::seek_doc> expected{
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
        {6, 6},
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 6},
        {12, ir::type_limits<ir::type_t::doc_id_t>::eof()}
      };

      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs), 2
        );
        for (const auto& target : expected) {
          ASSERT_EQ(target.expected, it.seek(target.target));
        }
      }
    }

    {
      std::vector<detail::seek_doc> expected{
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
        {6, ir::type_limits<ir::type_t::doc_id_t>::eof()},
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::eof()},
      };

      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs), 3
        );
        for (const auto& target : expected) {
          ASSERT_EQ(target.expected, it.seek(target.target));
        }
      }
    }

    // equals to conjuction
    {
      std::vector<detail::seek_doc> expected{
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
        {6, ir::type_limits<ir::type_t::doc_id_t>::eof()},
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::eof()},
      };

      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs), 5
        );
        for (const auto& target : expected) {
          ASSERT_EQ(target.expected, it.seek(target.target));
        }
      }

      {
        disjunction it(
            detail::execute_all<irs::min_match_disjunction::cost_iterator_adapter>(docs), ir::integer_traits<size_t>::const_max
        );
        for (const auto& target : expected) {
          ASSERT_EQ(target.expected, it.seek(target.target));
        }
      }
    }
  }
}

// ----------------------------------------------------------------------------
// --SECTION--                    iterator0 AND iterator1 AND iterator2 AND ... 
// ----------------------------------------------------------------------------

TEST(conjunction_test, next) {
  using conjunction = ir::conjunction;

  // simple case
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 5, 6 },
      { 1, 2, 5, 7, 9, 11, 45 },
      { 1, 5, 6, 12, 29 },
      { 1, 5, 79, 101, 141, 1025, 1101 }
    };

    std::vector<iresearch::doc_id_t> expected{ 1, 5 };
    std::vector<iresearch::doc_id_t> result;
    {
      conjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
      ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( expected, result );
  }

  // not optimal case, first is the longest
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32 },
      { 1, 5, 11, 21, 27, 31 }
    };

    std::vector<iresearch::doc_id_t> expected{ 1, 5, 11, 21, 27, 31 };
    std::vector<iresearch::doc_id_t> result;
    {
      conjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
      ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( expected, result );
  }

  // simple case
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 5, 11, 21, 27, 31 },
      { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32 },
    };

    std::vector<iresearch::doc_id_t> expected{ 1, 5, 11, 21, 27, 31 };
    std::vector<iresearch::doc_id_t> result;
    {
      conjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
      ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( expected, result );
  }

  // not optimal case, first is the longest
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 5, 79, 101, 141, 1025, 1101 },
      { 1, 5, 6 },
      { 1, 2, 5, 7, 9, 11, 45 },
      { 1, 5, 6, 12, 29 }
    };

    std::vector<iresearch::doc_id_t> expected{ 1, 5 };
    std::vector<iresearch::doc_id_t> result;
    {
      conjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
      ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( expected, result );
  }

  // same datasets
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 5, 79, 101, 141, 1025, 1101 },
      { 1, 5, 79, 101, 141, 1025, 1101 },
      { 1, 5, 79, 101, 141, 1025, 1101 },
      { 1, 5, 79, 101, 141, 1025, 1101 }
    };

    std::vector<iresearch::doc_id_t> result;
    {
      conjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
      ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( docs.front(), result );
  }

  // single dataset
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 5, 79, 101, 141, 1025, 1101 }
    };

    std::vector<iresearch::doc_id_t> result;
    {
      conjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
      ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
      for ( ; it.next(); ) {
        result.push_back(it.value());
      }
      ASSERT_FALSE(it.next());
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ(docs.front(), result);
  }

  // empty intersection
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 5, 6 },
      { 1, 2, 3, 7, 9, 11, 45 },
      { 3, 5, 6, 12, 29 },
      { 1, 5, 79, 101, 141, 1025, 1101 }
    };

    std::vector<iresearch::doc_id_t> expected{ };
    std::vector<iresearch::doc_id_t> result;
    {
      conjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
      ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( expected, result );
  }

  // empty datasets
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      {}, {}, {}, {}
    };

    std::vector<iresearch::doc_id_t> expected{};
    std::vector<iresearch::doc_id_t> result;
    {
      conjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
      ASSERT_EQ(ir::type_limits<ir::type_t::doc_id_t>::invalid(), it.value());
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( expected, result );
  }
}

TEST(conjunction_test, seek) {
  using conjunction = ir::conjunction;

  // simple case
  {
    // 1 6 28 45 99 256
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 5, 6, 45, 77, 99, 256, 988 },
      { 1, 2, 5, 6, 7, 9, 11, 28, 45, 99, 256 },
      { 1, 5, 6, 12, 28, 45, 99, 124, 256, 553 },
      { 1, 6, 11, 29, 45, 99, 141, 256, 1025, 1101 }
    };

    std::vector<detail::seek_doc> expected{
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
      {1, 1},
      {6, 6},
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 6},
      {29, 45},
      {46, 99},
      {68, 99},
      {256, 256},
      {257, ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };

    conjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // not optimal, first is the longest
  {
    // 1 6 28 45 99 256
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 6, 11, 29, 45, 99, 141, 256, 1025, 1101 },
      { 1, 2, 5, 6, 7, 9, 11, 28, 45, 99, 256 },
      { 1, 5, 6, 12, 29, 45, 99, 124, 256, 553 },
      { 1, 5, 6, 45, 77, 99, 256, 988 }
    };

    std::vector<detail::seek_doc> expected{
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
        {1, 1},
        {6, 6},
        {29, 45},
        {44, 45},
        {46, 99},
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 99},
        {256, 256},
        {257, ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };

    conjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // empty datasets
  {
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      {}, {}, {}, {}
    };
    std::vector<detail::seek_doc> expected{
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
      {6, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };

    conjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // NO_MORE_DOCS
  {
    // 1 6 28 45 99 256
    std::vector<std::vector<iresearch::doc_id_t>> docs{
        {1, 6, 11, 29, 45, 99, 141, 256, 1025, 1101},
        {1, 2, 5,  6,  7,  9,  11,  28,  45, 99, 256},
        {1, 5, 6,  12, 29, 45, 99,  124, 256,  553},
        {1, 5, 6,  45, 77, 99, 256, 988}
    };

    std::vector<detail::seek_doc> expected{
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
      {ir::type_limits<ir::type_t::doc_id_t>::eof(), ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {9, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {12, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {13, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {45, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {57, ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };

    conjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }

  // INVALID_DOC
  {
    // 1 6 28 45 99 256
    std::vector<std::vector<iresearch::doc_id_t>> docs{
      { 1, 6, 11, 29, 45, 99, 141, 256, 1025, 1101 },
      { 1, 2, 5, 6, 7, 9, 11, 28, 45, 99, 256 },
      { 1, 5, 6, 12, 29, 45, 99, 124, 256, 553 },
      { 1, 5, 6, 45, 77, 99, 256, 988 }
    };

    std::vector<detail::seek_doc> expected{
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
      {6, 6},
      {45, 45},
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 45},
      {99, 99},
      {257, ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };

    conjunction it(detail::execute_all<irs::score_iterator_adapter>(docs));
    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek(target.target));
    }
  }
}

// ----------------------------------------------------------------------------
// --SECTION--                                      iterator0 AND NOT iterator1
// ----------------------------------------------------------------------------

TEST(exclusion_test, next) {
  // simple case
  {
    std::vector<iresearch::doc_id_t> included{ 1, 2, 5, 7, 9, 11, 45 };
    std::vector<iresearch::doc_id_t> excluded{ 1, 5, 6, 12, 29 };
    std::vector<iresearch::doc_id_t> expected{ 2, 7, 9, 11, 45 };
    std::vector<iresearch::doc_id_t> result;
    {
      iresearch::exclusion it(
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(included.begin(), included.end()),
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(excluded.begin(), excluded.end())
      );
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( expected, result );
  }

  // basic case: single dataset
  {
    std::vector<iresearch::doc_id_t> included{ 1, 2, 5, 7, 9, 11, 45 };
    std::vector<iresearch::doc_id_t> excluded{};
    std::vector<iresearch::doc_id_t> result;
    {
      iresearch::exclusion it(
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(included.begin(), included.end()),
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(excluded.begin(), excluded.end())
      );
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ(included, result);
  }

  // basic case: single dataset
  {
    std::vector<iresearch::doc_id_t> included{};
    std::vector<iresearch::doc_id_t> excluded{ 1, 5, 6, 12, 29 };
    std::vector<iresearch::doc_id_t> result;
    {
      iresearch::exclusion it(
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(included.begin(), included.end()),
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(excluded.begin(), excluded.end())
      );
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ(included, result);
  }

  // basic case: same datasets
  {
    std::vector<iresearch::doc_id_t> included{ 1, 2, 5, 7, 9, 11, 45 };
    std::vector<iresearch::doc_id_t> excluded{ 1, 2, 5, 7, 9, 11, 45 };
    std::vector<iresearch::doc_id_t> expected{};
    std::vector<iresearch::doc_id_t> result;
    {
      iresearch::exclusion it(
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(included.begin(), included.end()),
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(excluded.begin(), excluded.end())
      );
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( expected, result );
  }

  // basic case: single dataset
  {
    std::vector<iresearch::doc_id_t> included{ 24 };
    std::vector<iresearch::doc_id_t> excluded{};
    std::vector<iresearch::doc_id_t> result;
    {
      iresearch::exclusion it(
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(included.begin(), included.end()),
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(excluded.begin(), excluded.end())
      );
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( included, result );
  }

  // empty
  {
    std::vector<iresearch::doc_id_t> included{};
    std::vector<iresearch::doc_id_t> excluded{};
    std::vector<iresearch::doc_id_t> expected{};
    std::vector<iresearch::doc_id_t> result;
    {
      iresearch::exclusion it(
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(included.begin(), included.end()),
        iresearch::doc_iterator::make<detail::basic_doc_iterator>(excluded.begin(), excluded.end())
      );
      ASSERT_FALSE(iresearch::type_limits<iresearch::type_t::doc_id_t>::valid(it.value()));
      for ( ; it.next(); ) {
        result.push_back( it.value() );
      }
      ASSERT_FALSE( it.next() );
      ASSERT_TRUE(ir::type_limits<ir::type_t::doc_id_t>::eof(it.value()));
    }
    ASSERT_EQ( expected, result );
  }
}

TEST(exclusion_test, seek) {
  // simple case
  {
    // 2, 7, 9, 11, 45
    std::vector<iresearch::doc_id_t> included{ 1, 2, 5, 7, 9, 11, 29, 45 };
    std::vector<iresearch::doc_id_t> excluded{ 1, 5, 6, 12, 29 };
    std::vector<detail::seek_doc> expected{
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
        {1, 2},
        {5, 7},
        {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 7},
        {9, 9},
        {45, 45},
        {43, 45},
        {57, ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };
    iresearch::exclusion it(
      iresearch::doc_iterator::make<detail::basic_doc_iterator>(included.begin(), included.end()),
      iresearch::doc_iterator::make<detail::basic_doc_iterator>(excluded.begin(), excluded.end())
    );

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek( target.target ));
    }
  }

  // empty datasets
  {
    std::vector<iresearch::doc_id_t> included{};
    std::vector<iresearch::doc_id_t> excluded{};
    std::vector<detail::seek_doc> expected{
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
      {6, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };
    iresearch::exclusion it(
      iresearch::doc_iterator::make<detail::basic_doc_iterator>(included.begin(), included.end()),
      iresearch::doc_iterator::make<detail::basic_doc_iterator>(excluded.begin(), excluded.end())
    );

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek( target.target ));
    }
  }

  // NO_MORE_DOCS
  {
    // 2, 7, 9, 11, 45
    std::vector<iresearch::doc_id_t> included{ 1, 2, 5, 7, 9, 11, 29, 45 };
    std::vector<iresearch::doc_id_t> excluded{ 1, 5, 6, 12, 29 };
    std::vector<detail::seek_doc> expected{
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
      {ir::type_limits<ir::type_t::doc_id_t>::eof(), ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {9, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {12, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {13, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {45, ir::type_limits<ir::type_t::doc_id_t>::eof()},
      {57, ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };
    iresearch::exclusion it(
      iresearch::doc_iterator::make<detail::basic_doc_iterator>(included.begin(), included.end()),
      iresearch::doc_iterator::make<detail::basic_doc_iterator>(excluded.begin(), excluded.end())
    );

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek( target.target ));
    }
  }

  // INVALID_DOC
  {
    // 2, 7, 9, 11, 45
    std::vector<iresearch::doc_id_t> included{ 1, 2, 5, 7, 9, 11, 29, 45 };
    std::vector<iresearch::doc_id_t> excluded{ 1, 5, 6, 12, 29 };
    std::vector<detail::seek_doc> expected{
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), ir::type_limits<ir::type_t::doc_id_t>::invalid()},
      {7, 7},
      {11, 11},
      {ir::type_limits<ir::type_t::doc_id_t>::invalid(), 11},
      {45, 45},
      {57, ir::type_limits<ir::type_t::doc_id_t>::eof()}
    };
    iresearch::exclusion it(
      iresearch::doc_iterator::make<detail::basic_doc_iterator>(included.begin(), included.end()),
      iresearch::doc_iterator::make<detail::basic_doc_iterator>(excluded.begin(), excluded.end())
    );

    for (const auto& target : expected) {
      ASSERT_EQ(target.expected, it.seek( target.target ));
    }
  }
}

// ----------------------------------------------------------------------------
// --SECTION--                                                Boolean test case 
// ----------------------------------------------------------------------------

class boolean_filter_test_case : public filter_test_case_base {
protected:
  void mixed_sequential() {
    {
      // add segment
      {
        tests::json_doc_generator gen( 
          resource("simple_sequential.json"),
          &tests::generic_json_field_factory);
        add_segment( gen );
      }

      auto rdr = open_reader();

      // (same=xyz AND duplicated=abcd) OR (same=xyz AND duplicated=vczc)
      {
        iresearch::Or root;

        // same=xyz AND duplicated=abcd
        {
          iresearch::And& child = root.add<iresearch::And>();
          child.add<iresearch::by_term>().field("same").term("xyz");
          child.add<iresearch::by_term>().field("duplicated").term("abcd");
        }

        // same=xyz AND duplicated=vczc
        {
          iresearch::And& child = root.add<iresearch::And>();
          child.add<iresearch::by_term>().field("same").term("xyz");
          child.add<iresearch::by_term>().field("duplicated").term("vczc");
        }

        check_query(root, docs_t{ 1, 2, 3, 5, 8, 11, 14, 17, 19, 21, 24, 27, 31 }, rdr);
      }

      // ((same=xyz AND duplicated=abcd) OR (same=xyz AND duplicated=vczc)) AND name=X
      {
        iresearch::And root;
        root.add<iresearch::by_term>().field("name").term("X");

        // ( same = xyz AND duplicated = abcd ) OR( same = xyz AND duplicated = vczc )
        {
          iresearch::Or& child = root.add<iresearch::Or>();

          // same=xyz AND duplicated=abcd
          {
            iresearch::And& subchild = child.add<iresearch::And>();
            subchild.add<iresearch::by_term>().field("same").term("xyz");
            subchild.add<iresearch::by_term>().field("duplicated").term("abcd");
          }

          // same=xyz AND duplicated=vczc
          {
            iresearch::And& subchild = child.add<iresearch::And>();
            subchild.add<iresearch::by_term>().field("same").term("xyz");
            subchild.add<iresearch::by_term>().field("duplicated").term("vczc");
          }
        }

        check_query(root, docs_t{ 24 }, rdr);
      }

      // ((same=xyz AND duplicated=abcd) OR (name=A or name=C or NAME=P or name=U or name=X)) OR (same=xyz AND (duplicated=vczc OR (name=A OR name=C OR NAME=P OR name=U OR name=X)) )
      // 1, 2, 3, 4, 5, 8, 11, 14, 16, 17, 19, 21, 24, 27, 31
      {
        iresearch::Or root;

        // (same=xyz AND duplicated=abcd) OR (name=A or name=C or NAME=P or name=U or name=X)
        // 1, 3, 5,11, 16, 21, 24, 27, 31
        {
          iresearch::Or& child = root.add<iresearch::Or>();

          // ( same = xyz AND duplicated = abcd )
          {
            iresearch::And& subchild = root.add<iresearch::And>();
            subchild.add<iresearch::by_term>().field("same").term("xyz");
            subchild.add<iresearch::by_term>().field("duplicated").term("abcd");
          }
         
          child.add<iresearch::by_term>().field("name").term("A");
          child.add<iresearch::by_term>().field("name").term("C");
          child.add<iresearch::by_term>().field("name").term("P");
          child.add<iresearch::by_term>().field("name").term("X");
        }

        // (same=xyz AND (duplicated=vczc OR (name=A OR name=C OR NAME=P OR name=U OR name=X))
        // 1, 2, 3, 8, 14, 16, 17, 19, 21, 24
        {
          iresearch::And& child = root.add<iresearch::And>();
          child.add<iresearch::by_term>().field("same").term("xyz");

          // (duplicated=vczc OR (name=A OR name=C OR NAME=P OR name=U OR name=X)
          {
            iresearch::Or& subchild = child.add<iresearch::Or>();
            subchild.add<iresearch::by_term>().field("duplicated").term("vczc");

            // name=A OR name=C OR NAME=P OR name=U OR name=X
            {
              iresearch::Or& subsubchild = subchild.add<iresearch::Or>();
              subchild.add<iresearch::by_term>().field("name").term("A");
              subchild.add<iresearch::by_term>().field("name").term("C");
              subchild.add<iresearch::by_term>().field("name").term("P");
              subchild.add<iresearch::by_term>().field("name").term("X");
            }
          }
        }

        check_query(root, docs_t{ 1, 2, 3, 5, 8, 11, 14, 16, 17, 19, 21, 24, 27, 31 }, rdr);
      }

      // (same=xyz AND duplicated=abcd) OR (same=xyz AND duplicated=vczc) AND *
      {
        iresearch::Or root;

        // *
        root.add<iresearch::all>();

        // same=xyz AND duplicated=abcd
        {
          iresearch::And& child = root.add<iresearch::And>();
          child.add<iresearch::by_term>().field("same").term("xyz");
          child.add<iresearch::by_term>().field("duplicated").term("abcd");
        }

        // same=xyz AND duplicated=vczc
        {
          iresearch::And& child = root.add<iresearch::And>();
          child.add<iresearch::by_term>().field("same").term("xyz");
          child.add<iresearch::by_term>().field("duplicated").term("vczc");
        }

        check_query( 
          root, 
          docs_t{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32 },
          rdr
        );
      }

      // (same=xyz AND duplicated=abcd) OR (same=xyz AND duplicated=vczc) OR NOT *
      {
        iresearch::Or root;

        // NOT *
        root.add<iresearch::Not>().filter<iresearch::all>();

        // same=xyz AND duplicated=abcd
        {
          iresearch::And& child = root.add<iresearch::And>();
          child.add<iresearch::by_term>().field("same").term("xyz");
          child.add<iresearch::by_term>().field("duplicated").term("abcd");
        }

        // same=xyz AND duplicated=vczc
        {
          iresearch::And& child = root.add<iresearch::And>();
          child.add<iresearch::by_term>().field("same").term("xyz");
          child.add<iresearch::by_term>().field("duplicated").term("vczc");
        }

        check_query(root, docs_t{}, rdr);
      }
    }
  }

  void not_standalone_sequential() {
    // add segment
    {
      tests::json_doc_generator gen(
        resource("simple_sequential.json"),
        &tests::generic_json_field_factory);
      add_segment( gen );
    }

    auto rdr = open_reader();

    // empty query
    {
      check_query(iresearch::Not(), docs_t{}, rdr);
    }

    // single not statement - empty result
    {
      iresearch::Not not_node;
      not_node.filter<iresearch::by_term>().field("same").term("xyz"),

      check_query(not_node, docs_t{}, rdr);
    }

    // single not statement - all docs
    {
      iresearch::Not not_node;
      not_node.filter<iresearch::by_term>().field("same").term("invalid_term"),

      check_query(not_node, docs_t{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32 }, rdr);
    }

    // (NOT (NOT name=A))
    {
      iresearch::Not not_node;
      not_node.filter<iresearch::Not>().filter<iresearch::by_term>().field("name").term("A");
      check_query(not_node, docs_t{ 1 }, rdr);
    }

    // (NOT (NOT (NOT (NOT (NOT name=A)))))
    {
      iresearch::Not not_node;
      not_node.filter<iresearch::Not>().filter<iresearch::Not>().filter<iresearch::Not>().filter<iresearch::Not>().filter<iresearch::by_term>().field("name").term("A");

      check_query(not_node, docs_t{ 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32 }, rdr);
    }
  }

  void not_sequential() {
    // add segment
    {
      tests::json_doc_generator gen( 
        resource("simple_sequential.json"),
        &tests::generic_json_field_factory);
      add_segment( gen );
    }

    auto rdr = open_reader();

    // empty query
    {
      check_query(iresearch::Not(), docs_t{}, rdr);
    }

    // single not statement - empty result
    {
      iresearch::Not not_node;
      not_node.filter<iresearch::by_term>().field("same").term("xyz"),

      check_query(not_node, docs_t{}, rdr);
    }

    // duplicated=abcd AND (NOT ( NOT name=A ))
    {
      iresearch::And root;
      root.add<iresearch::by_term>().field("duplicated").term("abcd");
      root.add<iresearch::Not>().filter<iresearch::Not>().filter<iresearch::by_term>().field("name").term("A");
      check_query(root, docs_t{ 1 }, rdr);
    }

    // duplicated=abcd AND (NOT ( NOT (NOT (NOT ( NOT name=A )))))
    {
      iresearch::And root;
      root.add<iresearch::by_term>().field("duplicated").term("abcd");
      root.add<iresearch::Not>().filter<iresearch::Not>().filter<iresearch::Not>().filter<iresearch::Not>().filter<iresearch::Not>().filter<iresearch::by_term>().field("name").term("A");
      check_query(root, docs_t{ 5, 11, 21, 27, 31 }, rdr);
    }

    // * AND NOT *
    {
      {
        iresearch::And root;
        root.add<iresearch::all>();
        root.add<iresearch::Not>().filter<iresearch::all>();
        check_query(root, docs_t{ }, rdr);
      }

      {
        iresearch::Or root;
        root.add<iresearch::all>();
        root.add<iresearch::Not>().filter<iresearch::all>();
        check_query(root, docs_t{ }, rdr);
      }
    }

    // duplicated=abcd AND NOT name=A
    {
      {
        iresearch::And root;
        root.add<iresearch::by_term>().field("duplicated").term("abcd");
        root.add<iresearch::Not>().filter<iresearch::by_term>().field("name").term("A");
        check_query(root, docs_t{ 5, 11, 21, 27, 31 }, rdr);
      }

      {
        iresearch::Or root;
        root.add<iresearch::by_term>().field("duplicated").term("abcd");
        root.add<iresearch::Not>().filter<iresearch::by_term>().field("name").term("A");
        check_query(root, docs_t{ 5, 11, 21, 27, 31 }, rdr);
      }
    }

    // duplicated=abcd AND NOT name=A AND NOT name=A
    {
      {
        iresearch::And root;
        root.add<iresearch::by_term>().field("duplicated").term("abcd");
        root.add<iresearch::Not>().filter<iresearch::by_term>().field("name").term("A");
        root.add<iresearch::Not>().filter<iresearch::by_term>().field("name").term("A");
        check_query(root, docs_t{ 5, 11, 21, 27, 31 }, rdr);
      }

      {
        iresearch::Or root;
        root.add<iresearch::by_term>().field("duplicated").term("abcd");
        root.add<iresearch::Not>().filter<iresearch::by_term>().field("name").term("A");
        root.add<iresearch::Not>().filter<iresearch::by_term>().field("name").term("A");
        check_query(root, docs_t{ 5, 11, 21, 27, 31 }, rdr);
      }
    }

    // duplicated=abcd AND NOT name=A AND NOT name=E
    {
      {
        iresearch::And root;
        root.add<iresearch::by_term>().field("duplicated").term("abcd");
        root.add<iresearch::Not>().filter<iresearch::by_term>().field("name").term("A");
        root.add<iresearch::Not>().filter<iresearch::by_term>().field("name").term("E");
        check_query(root, docs_t{ 11, 21, 27, 31 }, rdr);
      }

      {
        iresearch::Or root;
        root.add<iresearch::by_term>().field("duplicated").term("abcd");
        root.add<iresearch::Not>().filter<iresearch::by_term>().field("name").term("A");
        root.add<iresearch::Not>().filter<iresearch::by_term>().field("name").term("E");
        check_query(root, docs_t{ 11, 21, 27, 31 }, rdr);
      }
    }
  }

  void and_schemas() {
    // write segments
    {
      auto writer = open_writer(iresearch::OM_CREATE);

      std::vector<doc_generator_base::ptr> gens;

      gens.emplace_back(new tests::json_doc_generator(
        resource("AdventureWorks2014.json"),
        &tests::generic_json_field_factory));
      gens.emplace_back(new tests::json_doc_generator(
        resource("AdventureWorks2014Edges.json"),
        &tests::generic_json_field_factory));
      gens.emplace_back(new tests::json_doc_generator(
        resource("Northwnd.json"),
        &tests::generic_json_field_factory));
      gens.emplace_back(new tests::json_doc_generator(
        resource("NorthwndEdges.json"),
        &tests::generic_json_field_factory));

      add_segments(*writer, gens);
    }

    auto rdr = open_reader();

    // Name = Product AND source=AdventureWor3ks2014
    {
      ir::And root;
      root.add<iresearch::by_term>().field("Name").term("Product");
      root.add<iresearch::by_term>().field("source").term("AdventureWor3ks2014");
      check_query(root, docs_t{}, rdr);
    }
  }

  void and_sequential() {
    // add segment
    {
      tests::json_doc_generator gen(
        resource("simple_sequential.json"),
        &tests::generic_json_field_factory);
      add_segment( gen );
    }

    auto rdr = open_reader();

    // empty query
    {
      check_query(iresearch::And(), docs_t{}, rdr);
    }

    // name=V
    {
      iresearch::And root;
      root.add<iresearch::by_term>().field("name").term("V"); // 22

      check_query(root, docs_t{ 22 }, rdr);
    }

    // duplicated=abcd AND same=xyz
    {
      iresearch::And root;
      root.add<iresearch::by_term>().field("duplicated").term("abcd"); // 1,5,11,21,27,31
      root.add<iresearch::by_term>().field("same").term("xyz"); // 1..32
      check_query(root, docs_t{ 1, 5, 11, 21, 27, 31 }, rdr);
    }

    // duplicated=abcd AND same=xyz AND name=A
    {
      iresearch::And root;
      root.add<iresearch::by_term>().field("duplicated").term("abcd"); // 1,5,11,21,27,31
      root.add<iresearch::by_term>().field("same").term("xyz"); // 1..32
      root.add<iresearch::by_term>().field("name").term("A"); // 1
      check_query(root, docs_t{ 1 }, rdr);
    }

    // duplicated=abcd AND same=xyz AND name=B
    {
      iresearch::And root;
      root.add<iresearch::by_term>().field("duplicated").term("abcd"); // 1,5,11,21,27,31
      root.add<iresearch::by_term>().field("same").term("xyz"); // 1..32
      root.add<iresearch::by_term>().field("name").term("B"); // 2
      check_query(root, docs_t{}, rdr);
    }
  }

  void not_standalone_sequential_ordered() {
    // add segment
    {
      tests::json_doc_generator gen(
        resource("simple_sequential.json"),
        &tests::generic_json_field_factory);
      add_segment(gen);
    }

    auto rdr = open_reader();

    // reverse order
    {
      const std::string column_name = "duplicated";

      std::vector<irs::doc_id_t> expected = { 32, 30, 29, 28, 26, 25, 24, 23, 22, 20, 19, 18, 17, 16, 15, 14, 13, 12, 10, 9, 8, 7, 6, 4, 3, 2 };

      irs::Not not_node;
      not_node.filter<irs::by_term>().field(column_name).term("abcd");

      irs::order order;
      size_t collector_collect_count = 0;
      size_t collector_finish_count = 0;
      size_t scorer_score_count = 0;
      auto& sort = order.add<sort::custom_sort>();

      sort.collector_collect = [&collector_collect_count](const irs::sub_reader&, const irs::term_reader&, const irs::attribute_view&)->void {
        ++collector_collect_count;
      };
      sort.collector_finish = [&collector_finish_count](irs::attribute_store&, const irs::index_reader&)->void {
        ++collector_finish_count;
      };
      sort.scorer_add = [](irs::doc_id_t& dst, const irs::doc_id_t& src)->void { ASSERT_TRUE(&dst); ASSERT_TRUE(&src); dst = src; };
      sort.scorer_less = [](const irs::doc_id_t& lhs, const irs::doc_id_t& rhs)->bool { return (lhs > rhs); }; // reverse order
      sort.scorer_score = [&scorer_score_count](irs::doc_id_t& score)->void { ASSERT_TRUE(&score); ++scorer_score_count; };

      auto prepared_order = order.prepare();
      auto prepared_filter = not_node.prepare(*rdr, prepared_order);
      auto score_less = [&prepared_order](
        const iresearch::bytes_ref& lhs, const iresearch::bytes_ref& rhs
      )->bool {
        return prepared_order.less(lhs.c_str(), rhs.c_str());
      };
      std::multimap<iresearch::bstring, iresearch::doc_id_t, decltype(score_less)> scored_result(score_less);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto filter_itr = prepared_filter->execute(segment, prepared_order);
      ASSERT_EQ(32, irs::cost::extract(filter_itr->attributes()));

      size_t docs_count = 0;
      auto& score = filter_itr->attributes().get<irs::score>();

      // ensure that we avoid COW for pre c++11 std::basic_string
      const irs::bytes_ref score_value = score->value();

      while (filter_itr->next()) {
        score->evaluate();
        ASSERT_FALSE(!score);
        scored_result.emplace(score_value, filter_itr->value());
        ++docs_count;
      }

      ASSERT_EQ(expected.size(), docs_count);

      ASSERT_EQ(0, collector_collect_count); // should not be executed
      ASSERT_EQ(1, collector_finish_count); // from "all" query
      ASSERT_EQ(expected.size(), scorer_score_count);

      std::vector<irs::doc_id_t> actual;

      for (auto& entry: scored_result) {
        actual.emplace_back(entry.second);
      }

      ASSERT_EQ(expected, actual);
    }
  }

  void not_sequential_ordered() {
    // add segment
    {
      tests::json_doc_generator gen(
        resource("simple_sequential.json"),
        &tests::generic_json_field_factory);
      add_segment(gen);
    }

    auto rdr = open_reader();

    // reverse order
    {
      const std::string column_name = "duplicated";

      std::vector<irs::doc_id_t> expected = { 32, 30, 29, 28, 26, 25, 24, 23, 22, 20, 19, 18, 17, 16, 15, 14, 13, 12, 10, 9, 8, 7, 6, 4, 3, 2 };

      irs::And root;
      root.add<irs::Not>().filter<irs::by_term>().field(column_name).term("abcd");

      irs::order order;
      size_t collector_collect_count = 0;
      size_t collector_finish_count = 0;
      size_t scorer_score_count = 0;
      auto& sort = order.add<sort::custom_sort>();

      sort.collector_collect = [&collector_collect_count](const irs::sub_reader&, const irs::term_reader&, const irs::attribute_view&)->void {
        ++collector_collect_count;
      };
      sort.collector_finish = [&collector_finish_count](irs::attribute_store&, const irs::index_reader&)->void {
        ++collector_finish_count;
      };
      sort.scorer_add = [](irs::doc_id_t& dst, const irs::doc_id_t& src)->void { ASSERT_TRUE(&dst); ASSERT_TRUE(&src); dst = src; };
      sort.scorer_less = [](const irs::doc_id_t& lhs, const irs::doc_id_t& rhs)->bool { return (lhs > rhs); }; // reverse order
      sort.scorer_score = [&scorer_score_count](irs::doc_id_t& score)->void { ASSERT_TRUE(&score); ++scorer_score_count; };

      auto prepared_order = order.prepare();
      auto prepared_filter = root.prepare(*rdr, prepared_order);
      auto score_less = [&prepared_order](
        const iresearch::bytes_ref& lhs, const iresearch::bytes_ref& rhs
      )->bool {
        return prepared_order.less(lhs.c_str(), rhs.c_str());
      };
      std::multimap<iresearch::bstring, iresearch::doc_id_t, decltype(score_less)> scored_result(score_less);

      ASSERT_EQ(1, rdr->size());
      auto& segment = (*rdr)[0];

      auto filter_itr = prepared_filter->execute(segment, prepared_order);
      ASSERT_EQ(32, irs::cost::extract(filter_itr->attributes()));

      size_t docs_count = 0;
      auto& score = filter_itr->attributes().get<irs::score>();

      // ensure that we avoid COW for pre c++11 std::basic_string
      const irs::bytes_ref score_value = score->value();

      while (filter_itr->next()) {
        score->evaluate();
        ASSERT_FALSE(!score);
        scored_result.emplace(score_value, filter_itr->value());
        ++docs_count;
      }

      ASSERT_EQ(expected.size(), docs_count);

      ASSERT_EQ(0, collector_collect_count); // should not be executed
      ASSERT_EQ(1, collector_finish_count); // from "all" query
      ASSERT_EQ(expected.size(), scorer_score_count);

      std::vector<irs::doc_id_t> actual;

      for (auto& entry: scored_result) {
        actual.emplace_back(entry.second);
      }

      ASSERT_EQ(expected, actual);
    }
  }

  void or_sequential_multiple_segments() {
    // populate index
    {
      tests::json_doc_generator gen(
        resource("simple_sequential.json"),
        &tests::generic_json_field_factory);

      tests::document const *doc1 = gen.next();
      tests::document const *doc2 = gen.next();
      tests::document const *doc3 = gen.next();
      tests::document const *doc4 = gen.next();
      tests::document const *doc5 = gen.next();
      tests::document const *doc6 = gen.next();
      tests::document const *doc7 = gen.next();
      tests::document const *doc8 = gen.next();
      tests::document const *doc9 = gen.next();

      auto writer = open_writer();

      ASSERT_TRUE(insert(*writer,
        doc1->indexed.begin(), doc1->indexed.end(),
        doc1->stored.begin(), doc1->stored.end()
      )); // A
      ASSERT_TRUE(insert(*writer,
        doc2->indexed.begin(), doc2->indexed.end(),
        doc2->stored.begin(), doc2->stored.end()
      )); // B
      ASSERT_TRUE(insert(*writer,
        doc3->indexed.begin(), doc3->indexed.end(),
        doc3->stored.begin(), doc3->stored.end()
      )); // C
      ASSERT_TRUE(insert(*writer,
        doc4->indexed.begin(), doc4->indexed.end(),
        doc4->stored.begin(), doc4->stored.end()
      )); // D
      writer->commit();
      ASSERT_TRUE(insert(*writer,
        doc5->indexed.begin(), doc5->indexed.end(),
        doc5->stored.begin(), doc5->stored.end()
      )); // E
      ASSERT_TRUE(insert(*writer,
        doc6->indexed.begin(), doc6->indexed.end(),
        doc6->stored.begin(), doc6->stored.end()
      )); // F
      ASSERT_TRUE(insert(*writer,
        doc7->indexed.begin(), doc7->indexed.end(),
        doc7->stored.begin(), doc7->stored.end()
      )); // G
      writer->commit();
      ASSERT_TRUE(insert(*writer,
        doc8->indexed.begin(), doc8->indexed.end(),
        doc8->stored.begin(), doc8->stored.end()
      )); // H
      ASSERT_TRUE(insert(*writer,
        doc9->indexed.begin(), doc9->indexed.end(),
        doc9->stored.begin(), doc9->stored.end()
      )); // I
      writer->commit();
    }

    auto rdr = open_reader();
    {
      iresearch::Or root;
      root.add<iresearch::by_term>().field("name").term("B");
      root.add<iresearch::by_term>().field("name").term("F");
      root.add<iresearch::by_term>().field("name").term("I");

      auto prep = root.prepare(rdr);
      auto segment = rdr.begin();
      {
        auto docs = prep->execute(*segment);
        ASSERT_TRUE(docs->next());
        ASSERT_EQ(2, docs->value());
        ASSERT_FALSE(docs->next());
      }

      ++segment;
      {
        auto docs = prep->execute(*segment);
        ASSERT_TRUE(docs->next());
        ASSERT_EQ(2, docs->value());
        ASSERT_FALSE(docs->next());
      }

      ++segment;
      {
        auto docs = prep->execute(*segment);
        ASSERT_TRUE(docs->next());
        ASSERT_EQ(2, docs->value());
        ASSERT_FALSE(docs->next());
      }
    }
  }

  void or_sequential() {
    // add segment
    {
      tests::json_doc_generator gen(
        resource("simple_sequential.json"),
        &tests::generic_json_field_factory);
      add_segment( gen );
    }

    auto rdr = open_reader();

    // empty query
    {
      check_query(iresearch::Or(), docs_t{}, rdr);
    }

    // name=V
    {
      iresearch::Or root;
      root.add<iresearch::by_term>().field("name").term("V"); // 22

      check_query(root, docs_t{ 22 }, rdr);
    }

    // name=W OR name=Z
    {
      iresearch::Or root;
      root.add<iresearch::by_term>().field("name").term("W"); // 23
      root.add<iresearch::by_term>().field("name").term("C"); // 3

      check_query(root, docs_t{ 3, 23 }, rdr);
    }

    // name=A OR name=Q OR name=Z
    {
      iresearch::Or root;
      root.add<iresearch::by_term>().field("name").term("A"); // 1
      root.add<iresearch::by_term>().field("name").term("Q"); // 17
      root.add<iresearch::by_term>().field("name").term("Z"); // 26

      check_query(root, docs_t{ 1, 17, 26 }, rdr);
    }

    // name=A OR name=Q OR same!=xyz
    {
      irs::Or root;
      root.add<irs::by_term>().field("name").term("A"); // 1
      root.add<irs::by_term>().field("name").term("Q"); // 17
      root.add<irs::Or>().add<irs::Not>().filter<irs::by_term>().field("same").term("xyz"); // none (not within an OR must be wrapped inside a single-branch OR)

      check_query(root, docs_t{ 1, 17 }, rdr);
    }

    // (name=A OR name=Q) OR same!=xyz
    {
      irs::Or root;
      root.add<irs::by_term>().field("name").term("A"); // 1
      root.add<irs::by_term>().field("name").term("Q"); // 17
      root.add<irs::Or>().add<irs::Not>().filter<irs::by_term>().field("same").term("xyz"); // none (not within an OR must be wrapped inside a single-branch OR)

      check_query(root, docs_t{ 1, 17 }, rdr);
    }

    // name=A OR name=Q OR name=Z OR same=invalid_term OR invalid_field=V
    {
      iresearch::Or root;
      root.add<iresearch::by_term>().field("name").term("A"); // 1
      root.add<iresearch::by_term>().field("name").term("Q"); // 17
      root.add<iresearch::by_term>().field("name").term("Z"); // 26
      root.add<iresearch::by_term>().field("same").term("invalid_term");
      root.add<iresearch::by_term>().field("invalid_field").term("V");

      check_query(root, docs_t{ 1, 17, 26 }, rdr);
    }

    // search : all terms
    {
      iresearch::Or root;
      root.add<iresearch::by_term>().field("name").term("A"); // 1
      root.add<iresearch::by_term>().field("name").term("Q"); // 17
      root.add<iresearch::by_term>().field("name").term("Z"); // 26
      root.add<iresearch::by_term>().field("same").term("xyz"); // 1..32
      root.add<iresearch::by_term>().field("same").term("invalid_term");

      check_query(
        root,
        docs_t{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32 },
        rdr
      );
    }

    // search : empty result
    check_query(
      ir::by_term().field( "same" ).term( "invalid_term" ),
      docs_t{},
      rdr
    );
  }
};

// ----------------------------------------------------------------------------
// --SECTION--                                                   Not base tests
// ----------------------------------------------------------------------------

TEST(Not_test, ctor) {
  irs::Not q;
  ASSERT_EQ(irs::Not::type(), q.type());
  ASSERT_EQ(nullptr, q.filter());
  ASSERT_EQ(irs::boost::no_boost(), q.boost());
}

TEST(Not_test, equal) {
  {
    irs::Not lhs, rhs;
    ASSERT_EQ(lhs, rhs);
    ASSERT_EQ(lhs.hash(), rhs.hash());
  }

  {
    irs::Not lhs;
    lhs.filter<irs::by_term>().field("abc").term("def");

    irs::Not rhs;
    rhs.filter<irs::by_term>().field("abc").term("def");
    ASSERT_EQ(lhs, rhs);
    ASSERT_EQ(lhs.hash(), rhs.hash());
  }

  {
    irs::Not lhs;
    lhs.filter<irs::by_term>().field("abc").term("def");

    irs::Not rhs;
    rhs.filter<irs::by_term>().field("abcd").term("def");
    ASSERT_NE(lhs, rhs);
  }
}

// ----------------------------------------------------------------------------
// --SECTION--                                                   And base tests 
// ----------------------------------------------------------------------------

TEST(And_test, ctor) {
  ir::And q;
  ASSERT_EQ(ir::And::type(), q.type());
  ASSERT_TRUE(q.empty());
  ASSERT_EQ(0, q.size());
  ASSERT_EQ(ir::boost::no_boost(), q.boost());
}

TEST(And_test, add_clear) {
  ir::And q;
  q.add<ir::by_term>();
  q.add<ir::by_term>();
  ASSERT_FALSE(q.empty());
  ASSERT_EQ(2, q.size());
  q.clear();
  ASSERT_TRUE(q.empty());
  ASSERT_EQ(0, q.size());
}

TEST(And_test, equal) {
  ir::And lhs;
  lhs.add<ir::by_term>().field("field").term("term");
  lhs.add<ir::by_term>().field("field1").term("term1");
  {
    ir::And& subq = lhs.add<ir::And>();
    subq.add<ir::by_term>().field("field123").term("dfterm");
    subq.add<ir::by_term>().field("fieasfdld1").term("term1");
  }

  {
    ir::And rhs;
    rhs.add<ir::by_term>().field("field").term("term");
    rhs.add<ir::by_term>().field("field1").term("term1");
    {
      ir::And& subq = rhs.add<ir::And>();
      subq.add<ir::by_term>().field("field123").term("dfterm");
      subq.add<ir::by_term>().field("fieasfdld1").term("term1");
    }

    ASSERT_EQ(lhs, rhs);
    ASSERT_EQ(lhs.hash(), rhs.hash());
  }

  {
    ir::And rhs;
    rhs.add<ir::by_term>().field("field").term("term");
    rhs.add<ir::by_term>().field("field1").term("term1");
    {
      ir::And& subq = rhs.add<ir::And>();
      subq.add<ir::by_term>().field("field123").term("dfterm");
      subq.add<ir::by_term>().field("fieasfdld1").term("term1");
      subq.add<ir::by_term>().field("fieasfdld1").term("term1");
    }

    ASSERT_NE(lhs, rhs);
  }
}

#ifndef IRESEARCH_DLL

TEST(And_test, optimize_double_negation) {
  irs::And root;
  auto& term = root.add<irs::Not>().filter<irs::Not>().filter<irs::by_term>();
  term.field("test_field").term("test_term");

  auto prepared = root.prepare(empty_index_reader::instance());
  ASSERT_NE(nullptr, dynamic_cast<irs::term_query*>(prepared.get()));
}

TEST(And_test, optimize_single_node) {
  // simple hierarchy
  {
    irs::And root;
    auto& term = root.add<irs::by_term>();
    term.field("test_field").term("test_term");

    auto prepared = root.prepare(empty_index_reader::instance());
    ASSERT_NE(nullptr, dynamic_cast<irs::term_query*>(prepared.get()));
  }

  // complex hierarchy
  {
    irs::And root;
    auto& term = root.add<irs::And>().add<irs::And>().add<irs::by_term>();
    term.field("test_field").term("test_term");

    auto prepared = root.prepare(empty_index_reader::instance());
    ASSERT_NE(nullptr, dynamic_cast<irs::term_query*>(prepared.get()));
  }
}

#endif // IRESEARCH_DLL

// ----------------------------------------------------------------------------
// --SECTION--                                                    Or base tests 
// ----------------------------------------------------------------------------

TEST(Or_test, ctor) {
  ir::Or q;
  ASSERT_EQ(ir::Or::type(), q.type());
  ASSERT_TRUE(q.empty());
  ASSERT_EQ(0, q.size());
  ASSERT_EQ(1, q.min_match_count());
  ASSERT_EQ(ir::boost::no_boost(), q.boost());
}

TEST(Or_test, add_clear) {
  ir::Or q;
  q.add<ir::by_term>();
  q.add<ir::by_term>();
  ASSERT_FALSE(q.empty());
  ASSERT_EQ(2, q.size());
  q.clear();
  ASSERT_TRUE(q.empty());
  ASSERT_EQ(0, q.size());
}

TEST(Or_test, equal) {
  ir::Or lhs;
  lhs.add<ir::by_term>().field("field").term("term");
  lhs.add<ir::by_term>().field("field1").term("term1");
  {
    ir::And& subq = lhs.add<ir::And>();
    subq.add<ir::by_term>().field("field123").term("dfterm");
    subq.add<ir::by_term>().field("fieasfdld1").term("term1");
  }

  {
    ir::Or rhs;
    rhs.add<ir::by_term>().field("field").term("term");
    rhs.add<ir::by_term>().field("field1").term("term1");
    {
      ir::And& subq = rhs.add<ir::And>();
      subq.add<ir::by_term>().field("field123").term("dfterm");
      subq.add<ir::by_term>().field("fieasfdld1").term("term1");
    }

    ASSERT_EQ(lhs, rhs);
    ASSERT_EQ(lhs.hash(), rhs.hash());
  }

  {
    ir::Or rhs;
    rhs.add<ir::by_term>().field("field").term("term");
    rhs.add<ir::by_term>().field("field1").term("term1");
    {
      ir::And& subq = rhs.add<ir::And>();
      subq.add<ir::by_term>().field("field123").term("dfterm");
      subq.add<ir::by_term>().field("fieasfdld1").term("term1");
      subq.add<ir::by_term>().field("fieasfdld1").term("term1");
    }

    ASSERT_NE(lhs, rhs);
  }
}

#ifndef IRESEARCH_DLL

TEST(Or_test, optimize_double_negation) {
  irs::Or root;
  auto& term = root.add<irs::Not>().filter<irs::Not>().filter<irs::by_term>();
  term.field("test_field").term("test_term");

  auto prepared = root.prepare(empty_index_reader::instance());
  ASSERT_NE(nullptr, dynamic_cast<irs::term_query*>(prepared.get()));
}

TEST(Or_test, optimize_single_node) {
  // simple hierarchy
  {
    irs::Or root;
    auto& term = root.add<irs::by_term>();
    term.field("test_field").term("test_term");

    auto prepared = root.prepare(empty_index_reader::instance());
    ASSERT_NE(nullptr, dynamic_cast<irs::term_query*>(prepared.get()));
  }

  // complex hierarchy
  {
    irs::Or root;
    auto& term = root.add<irs::Or>().add<irs::Or>().add<irs::by_term>();
    term.field("test_field").term("test_term");

    auto prepared = root.prepare(empty_index_reader::instance());
    ASSERT_NE(nullptr, dynamic_cast<irs::term_query*>(prepared.get()));
  }
}

#endif // IRESEARCH_DLL

// ----------------------------------------------------------------------------
// --SECTION--                           memory_directory + iresearch_format_10
// ----------------------------------------------------------------------------

class memory_boolean_test_case : public tests::boolean_filter_test_case {
protected:
  virtual ir::directory* get_directory() override {
    return new ir::memory_directory();
  }

  virtual ir::format::ptr get_codec() override {
    return ir::formats::get("1_0");
  }
};

TEST_F(memory_boolean_test_case, or) {
  or_sequential_multiple_segments();
  or_sequential();
}

TEST_F( memory_boolean_test_case, and) {
  and_schemas();
  and_sequential();
}

TEST_F(memory_boolean_test_case, not) {
  not_standalone_sequential();
  not_standalone_sequential_ordered();
  not_sequential();
  not_sequential_ordered();
}

TEST_F(memory_boolean_test_case, mixed) {
  mixed_sequential();
}

// ----------------------------------------------------------------------------
// --SECTION--                               fs_directory + iresearch_format_10
// ----------------------------------------------------------------------------

class fs_boolean_filter_test_case : public tests::boolean_filter_test_case {
protected:
  virtual ir::directory* get_directory() override {
    const fs::path dir = fs::path(test_dir()).append("index");
    return new iresearch::fs_directory(dir.string());
  }

  virtual ir::format::ptr get_codec() override {
    return ir::formats::get("1_0");
  }
};

TEST_F(fs_boolean_filter_test_case, or) {
  or_sequential_multiple_segments();
  or_sequential();
}

TEST_F(fs_boolean_filter_test_case, and ) {
  and_sequential();
  and_schemas();
}

TEST_F(fs_boolean_filter_test_case, not) {
  not_standalone_sequential();
  not_standalone_sequential_ordered();
  not_sequential();
  not_sequential_ordered();
}

TEST_F(fs_boolean_filter_test_case, mixed) {
  mixed_sequential();
}

} // tests

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------