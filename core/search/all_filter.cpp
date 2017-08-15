////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "index/field_meta.hpp"
#include "search/score_doc_iterators.hpp"

#include "all_filter.hpp"

NS_LOCAL

class empty_term_reader: public irs::term_reader {
 public:
  empty_term_reader(uint64_t docs_count): docs_count_(docs_count) {
  }

  virtual irs::seek_term_iterator::ptr iterator() const {
    return nullptr;
  }

  virtual const irs::field_meta& meta() const {
    return irs::field_meta::EMPTY;
  }

  virtual const irs::attribute_store& attributes() const NOEXCEPT {
    return irs::attribute_store::empty_instance();
  }

  // total number of terms
  virtual size_t size() const {
    return 0;
  }

  // total number of documents
  virtual uint64_t docs_count() const {
    return docs_count_;
  }

  // least significant term
  virtual const irs::bytes_ref& (min)() const {
    return irs::bytes_ref::nil;
  }

  // most significant term
  virtual const irs::bytes_ref& (max)() const {
    return irs::bytes_ref::nil;
  }

 private:
  uint64_t docs_count_;
};

NS_END

NS_ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                                               all
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @class all_query
/// @brief compiled all_filter that returns all documents
////////////////////////////////////////////////////////////////////////////////
class all_query: public filter::prepared {
 public:
  class all_iterator: public score_doc_iterator_base {
   public:
    all_iterator(
        const sub_reader& reader,
        const attribute_store& prepared_filter_attrs,
        const order::prepared& order,
        uint64_t docs_count
    ): score_doc_iterator_base(order),
       max_doc_(doc_id_t(type_limits<type_t::doc_id_t>::min() + docs_count - 1)),
       doc_(type_limits<type_t::doc_id_t>::invalid()) {
      attrs_.emplace<cost>()->value(max_doc_); // set estimation value
      attrs_.emplace<document>()->value = &doc_; // make doc_id accessible via attribute
      doc_ = type_limits<type_t::doc_id_t>::invalid();

      // set scorers
      scorers_ = ord_->prepare_scorers(
        reader,
        empty_term_reader(docs_count),
        prepared_filter_attrs,
        this->attributes() // doc_iterator attributes
      );
    }

    virtual doc_id_t value() const {
      return doc_;
    }

    virtual bool next() override {
      return !type_limits<type_t::doc_id_t>::eof(seek(doc_ + 1));
    }

    virtual doc_id_t seek(doc_id_t target) override {
      doc_ = target <= max_doc_ ? target : type_limits<type_t::doc_id_t>::eof();

      return doc_;
    }

    virtual void score() override {
      scorers_.score(*ord_, scr_->leak());// FIXME TODO hint tidf to not need to score
    }

   private:
    doc_id_t max_doc_; // largest valid doc_id
    doc_id_t doc_;
    order::prepared::scorers scorers_;
  };

  explicit all_query(attribute_store&& attrs)
    : filter::prepared(std::move(attrs)) {
  }

  virtual score_doc_iterator::ptr execute(
      const sub_reader& rdr,
      const order::prepared& order
  ) const override {
    return score_doc_iterator::make<all_iterator>(
      rdr,
      this->attributes(), // prepared_filter attributes
      order,
      rdr.docs_count()
    );
  }
};

DEFINE_FILTER_TYPE(irs::all);
DEFINE_FACTORY_DEFAULT(irs::all);

all::all(): filter(all::type()) {
}

filter::prepared::ptr all::prepare(
    const index_reader& reader,
    const order::prepared& order,
    boost_t filter_boost
) const {
  attribute_store attrs;

  // skip filed-level/term-level statistics because there are no fields/terms
  order.prepare_stats().finish(reader, attrs);

  irs::boost::apply(attrs, boost() * filter_boost); // apply boost

  return filter::prepared::make<all_query>(std::move(attrs));
}

NS_END // ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------