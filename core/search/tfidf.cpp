//
// IResearch search engine 
// 
// Copyright © 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#include "tfidf.hpp"

#include "scorers.hpp"
#include "analysis/token_attributes.hpp"
#include "index/index_reader.hpp"

NS_ROOT
NS_BEGIN(tfidf)

struct idf : basic_attribute<float_t> {
  DECLARE_ATTRIBUTE_TYPE();
  DECLARE_FACTORY_DEFAULT();
  idf() : basic_attribute( idf::type(), 1.f ) {}

  virtual void clear() { value = 1.f; }
};

DEFINE_ATTRIBUTE_TYPE(iresearch::tfidf::idf);
DEFINE_FACTORY_DEFAULT(idf);

typedef tfidf_sort::score_t score_t;

class collector final : public iresearch::sort::collector {
 public:
  virtual void collect(
      const sub_reader& /* sub_reader */,
      const term_reader& /* term_reader */,
      const attributes& term_attrs ) {
    const iresearch::term_meta* meta = term_attrs.get<iresearch::term_meta>();
    if (meta) {
      docs_count += meta->docs_count;
    }
  }

  virtual void after_collect(
    const iresearch::index_reader& idx_reader, iresearch::attributes& query_attrs
  ) override {
    query_attrs.add<tfidf::idf>()->value = 1 + static_cast<float_t>(
      std::log(idx_reader.docs_count() / double_t(docs_count + 1))
    );
  }

 private:
  uint64_t docs_count = 0; // document frequency
}; // collector

class scorer final : public iresearch::sort::scorer_base<tfidf::score_t> {
 public:
  DECLARE_FACTORY(scorer);

  scorer(
      iresearch::boost::boost_t boost,
      const tfidf::idf* idf,
      const frequency* freq)
    : idf_(boost * (idf ? idf->value : 1)), 
      freq_(freq) {
  }

  virtual void score(score_t& score_buf) override {
    const float_t tf = float_t(std::sqrt(freq_ ? freq_->value : 0));
    score_buf = idf_ * tf;
  }

 private:
  float_t idf_; // precomputed : boost * idf
  const frequency* freq_;
};

class sort final: iresearch::sort::prepared_base<tfidf::score_t> {
 public:
  DECLARE_FACTORY(prepared);

  sort(bool reverse):
    iresearch::sort::prepared_base<tfidf::score_t>(flags{ &frequency::type() }) {
    static const std::function<bool(score_t, score_t)> greater = std::greater<score_t>();
    static const std::function<bool(score_t, score_t)> less = std::less<score_t>();
    less_ = reverse ? &greater : &less;
  }

  virtual collector::ptr prepare_collector() const override {
    return iresearch::sort::collector::make<tfidf::collector>();
  }

  virtual scorer::ptr prepare_scorer(
    const attributes& query_attrs, const attributes& doc_attrs
  ) const override {
    return tfidf::scorer::make<tfidf::scorer>(
      boost::extract(query_attrs),
      query_attrs.get<tfidf::idf>(),
      doc_attrs.get<frequency>()
    );
  }

  virtual void add(score_t& dst, const score_t& src) const override {
    dst += src;
  }

  virtual bool less(const score_t& lhs, const score_t& rhs) const override {
    return (*less_)(lhs, rhs);
  }

 private:
  const std::function<bool(score_t, score_t)>* less_;
}; // sort

NS_END // tfidf 

DEFINE_SORT_TYPE_NAMED(iresearch::tfidf_sort, "tfidf");
REGISTER_SCORER(iresearch::tfidf_sort);
DEFINE_FACTORY_SINGLETON(tfidf_sort);

tfidf_sort::tfidf_sort() : sort( tfidf_sort::type() ) {}

sort::prepared::ptr tfidf_sort::prepare() const {
  return tfidf::sort::make<tfidf::sort>(reverse());
}

NS_END // ROOT
