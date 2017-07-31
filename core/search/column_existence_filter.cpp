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

#include "column_existence_filter.hpp"
#include "index/index_reader.hpp"
#include "search/score_doc_iterators.hpp"

#include <boost/functional/hash.hpp>

NS_LOCAL

class colum_existence_iterator final : public irs::score_doc_iterator_base {
 public:
  explicit colum_existence_iterator(
      irs::columnstore_reader::column_iterator::ptr&& it) NOEXCEPT
    : score_doc_iterator_base(irs::order::prepared::unordered()),
      it_(std::move(it)),
      value_(it->value().first) {
  }

  virtual void score() override { }

  virtual irs::doc_id_t value() const override {
    return value_;
  }

  virtual bool next() override {
    return it_->next();
  }

  virtual irs::doc_id_t seek(irs::doc_id_t target) override {
    it_->seek(target);
    return value_;
  }

 private:
  irs::columnstore_reader::column_iterator::ptr it_;
  const irs::doc_id_t& value_;
}; // colum_existence_iterator

class column_existence_query final : public irs::filter::prepared {
 public:
  explicit column_existence_query(const std::string& field)
    : field_(field) {
  }

  virtual irs::score_doc_iterator::ptr execute(
      const irs::sub_reader& rdr,
      const irs::order::prepared&) const override {
    return irs::score_doc_iterator::make<colum_existence_iterator>(
      rdr.values_iterator(field_)
    );
  }

 private:
  std::string field_;
}; // column_existence_query

NS_END

NS_ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                by_column_existence implementation
// -----------------------------------------------------------------------------

DEFINE_FILTER_TYPE(by_column_existence);
DEFINE_FACTORY_DEFAULT(by_column_existence);

by_column_existence::by_column_existence()
  : filter(by_column_existence::type()) {
}

bool by_column_existence::equals(const filter& rhs) const {
  const by_column_existence& trhs = static_cast<const by_column_existence&>(rhs);
  return filter::equals(rhs) && field_ == trhs.field_;
}

size_t by_column_existence::hash() const {
  size_t seed = 0;
  ::boost::hash_combine(seed, filter::hash());
  ::boost::hash_combine(seed, field_);
  return seed;
}

filter::prepared::ptr by_column_existence::prepare(
    const index_reader&,
    const order::prepared&,
    boost_t) const {
  return memory::make_unique<column_existence_query>(field_);
}

NS_END // ROOT

