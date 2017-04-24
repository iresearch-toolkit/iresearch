//
// IResearch search engine 
// 
// Copyright (c) 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#if !defined(_MSC_VER)
  #pragma GCC diagnostic ignored "-Wunused-local-typedefs"
#else
  #pragma warning(disable: 4512)
#endif

  #include <boost/property_tree/json_parser.hpp>

#if !defined(_MSC_VER)
  #pragma GCC diagnostic pop
#else
  #pragma warning(default: 4512)
#endif

#include "tfidf.hpp"

#include "scorers.hpp"
#include "analysis/token_attributes.hpp"
#include "index/index_reader.hpp"
#include "index/field_meta.hpp"

NS_ROOT
NS_BEGIN(tfidf)

// empty frequency
const frequency EMPTY_FREQ;

const flags& features(bool normalize) {
  if (normalize) {
    // set of features required for tf-idf model without normalization
    static const flags FEATURES{ frequency::type() };
    return FEATURES;
  }

  // set of features required for tf-idf model with normalization
  static const flags NORM_FEATURES{ frequency::type(), norm::type() };
  return NORM_FEATURES;
}

struct idf : basic_attribute<float_t> {
  DECLARE_ATTRIBUTE_TYPE();
  DECLARE_FACTORY_DEFAULT();
  idf() : basic_attribute(idf::type(), 1.f) { }

  virtual void clear() { value = 1.f; }
};

DEFINE_ATTRIBUTE_TYPE(iresearch::tfidf::idf);
DEFINE_FACTORY_DEFAULT(idf);

typedef tfidf_sort::score_t score_t;

class collector final : public iresearch::sort::collector {
 public:
  collector(bool normalize)
    : normalize_(normalize) {
  }
  
  virtual void term(const attributes& term_attrs) {
    const iresearch::term_meta* meta = term_attrs.get<iresearch::term_meta>();
    if (meta) {
      docs_count += meta->docs_count;
    }
  }

  virtual void finish(
      const iresearch::index_reader& index, 
      iresearch::attributes& query_attrs) override {
    query_attrs.add<tfidf::idf>()->value = 1 + static_cast<float_t>(
      std::log(index.docs_count() / double_t(docs_count + 1))
    );

    if (normalize_) {
      // add norm attribute if requested
      query_attrs.add<norm>();
    }
  }

 private:
  uint64_t docs_count = 0; // document frequency
  bool normalize_;
}; // collector

class scorer : public iresearch::sort::scorer_base<tfidf::score_t> {
 public:
  DECLARE_FACTORY(scorer);

  scorer(
      iresearch::boost::boost_t boost,
      const tfidf::idf* idf,
      const frequency* freq)
    : idf_(boost * (idf ? idf->value : 1.f)), 
      freq_(freq ? freq : &EMPTY_FREQ) {
    assert(freq_);
  }

  virtual void score(score_t& score_buf) override {
    score_buf = tfidf();
  }

 protected:
  FORCE_INLINE float_t tfidf() const {
   return idf_ * float_t(std::sqrt(freq_->value));
  }

 private:
  float_t idf_; // precomputed : boost * idf
  const frequency* freq_;
}; // scorer

class norm_scorer final : public scorer {
 public:
  DECLARE_FACTORY(norm_scorer);

  norm_scorer(
      const iresearch::norm* norm,
      iresearch::boost::boost_t boost,
      const tfidf::idf* idf,
      const frequency* freq)
    : scorer(boost, idf, freq),
      norm_(norm) {
    assert(norm_);
  }

  virtual void score(score_t& score_buf) override {
    score_buf = tfidf() * norm_->read();
  }

 private:
  const iresearch::norm* norm_;
}; // norm_scorer

class sort final: iresearch::sort::prepared_base<tfidf::score_t> {
 public:
  DECLARE_FACTORY(prepared);

  sort(bool normalize, bool reverse)
    : normalize_(normalize) {
    static const std::function<bool(score_t, score_t)> greater = std::greater<score_t>();
    static const std::function<bool(score_t, score_t)> less = std::less<score_t>();
    less_ = reverse ? &greater : &less;
  }

  virtual const flags& features() const override {
    return tfidf::features(normalize_); 
  }

  virtual collector::ptr prepare_collector() const override {
    return iresearch::sort::collector::make<tfidf::collector>(normalize_);
  }

  virtual scorer::ptr prepare_scorer(
      const sub_reader& segment,
      const term_reader& field,
      const attributes& query_attrs, 
      const attributes& doc_attrs) const override {
    iresearch::norm* norm = query_attrs.get<iresearch::norm>();

    if (norm && norm->reset(segment, field.meta().norm, *doc_attrs.get<document>())) {
      return tfidf::scorer::make<tfidf::norm_scorer>(
        norm,
        boost::extract(query_attrs),
        query_attrs.get<tfidf::idf>(),
        doc_attrs.get<frequency>()
      );
    }

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
  bool normalize_;
}; // sort

NS_END // tfidf 

DEFINE_SORT_TYPE_NAMED(iresearch::tfidf_sort, "tfidf");
REGISTER_SCORER(iresearch::tfidf_sort);

DEFINE_FACTORY_DEFAULT(irs::tfidf_sort);

/*static*/ sort::ptr tfidf_sort::make(const string_ref& args) {
  static PTR_NAMED(tfidf_sort, ptr);

  if (args.empty()) {
    return ptr; // no jSON object to initialize with
  }

  // try to parse 'args' as a jSON config
  try {
    std::stringstream args_stream(std::string(args.c_str(), args.size()));
    ::boost::property_tree::ptree pt;

    {
      static std::mutex mutex;
      SCOPED_LOCK(mutex); // ::boost::property_tree::read_json(...) is not thread-safe, was seen to SEGFAULT
      ::boost::property_tree::read_json(args_stream, pt);
    }

    auto reverse = pt.get_optional<bool>("reverse");

    if (reverse) {
      ptr->reverse(reverse.value());
    }

    return ptr;
  } catch(...) {
    IR_FRMT_ERROR("Caught error while constructing tfidf_sort from jSON arguments: %s", args.c_str());
    IR_EXCEPTION();
  }

  return nullptr;
}

tfidf_sort::tfidf_sort(bool normalize) 
  : sort(tfidf_sort::type()),
    normalize_(normalize) {
}

sort::prepared::ptr tfidf_sort::prepare() const {
  return tfidf::sort::make<tfidf::sort>(normalize_, reverse());
}

NS_END // ROOT
