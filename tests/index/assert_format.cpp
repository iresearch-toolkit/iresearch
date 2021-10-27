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

#include "assert_format.hpp"

#include <unordered_set>
#include <algorithm>
#include <iostream>
#include <cassert>

#include "analysis/token_attributes.hpp"
#include "analysis/token_stream.hpp"

#include "index/comparer.hpp"
#include "index/field_meta.hpp"
#include "index/directory_reader.hpp"

#include "search/term_filter.hpp"
#include "search/boolean_filter.hpp"
#include "search/tfidf.hpp"
#include "search/cost.hpp"
#include "search/score.hpp"

#include "utils/bit_utils.hpp"
#include "utils/automaton_utils.hpp"
#include "utils/fstext/fst_table_matcher.hpp"

#include "store/data_output.hpp"

#include "tests_shared.hpp"

namespace tests {

void assert_term(
    const irs::term_iterator& expected_term,
    const irs::term_iterator& actual_term,
    irs::IndexFeatures requested_features);

// -----------------------------------------------------------------------------
// --SECTION--                                           position implementation
// -----------------------------------------------------------------------------

position::position(
    uint32_t pos, uint32_t start,
    uint32_t end,
    const irs::bytes_ref& pay)
  : pos(pos), start(start),
    end(end), payload(pay) {
}

// -----------------------------------------------------------------------------
// --SECTION--                                            posting implementation
// -----------------------------------------------------------------------------

posting::posting(irs::doc_id_t id)
  : id_(id) {
}

void posting::add(uint32_t pos, uint32_t offs_start, const irs::attribute_provider& attrs) {
  auto* offs = irs::get<irs::offset>(attrs);
  auto* pay = irs::get<irs::payload>(attrs);

  uint32_t start = std::numeric_limits<uint32_t>::max();
  uint32_t end = std::numeric_limits<uint32_t>::max();
  if (offs) {
    start = offs_start + offs->start;
    end = offs_start + offs->end;
  }

  positions_.emplace(pos, start, end, pay ? pay->value : irs::bytes_ref::NIL);
}

posting& term::add(irs::doc_id_t id) {
  return const_cast<posting&>(*postings.emplace(id).first);
}

// -----------------------------------------------------------------------------
// --SECTION--                                               term implementation
// -----------------------------------------------------------------------------

term::term(irs::bytes_ref data): value(data) {}

bool term::operator<( const term& rhs ) const {
  return irs::memcmp_less(
    value.c_str(), value.size(),
    rhs.value.c_str(), rhs.value.size());
}

field::field(
    const irs::string_ref& name,
    irs::IndexFeatures index_features,
    const irs::features_t& features)
  : field_meta(name, index_features),
    stats{} {
  for (const auto feature : features) {
    this->features[feature] = irs::field_limits::invalid();
  }
}

term& field::insert(const irs::bytes_ref& t) {
  auto res = terms.emplace(t);
  return const_cast<term&>(*res.first);
}

term* field::find(const irs::bytes_ref& t) {
  auto it = terms.find(term(t));
  return terms.end() == it ? nullptr : const_cast<term*>(&*it);
}

size_t field::remove(const irs::bytes_ref& t) {
  return terms.erase(term(t));
}

// -----------------------------------------------------------------------------
// --SECTION--                                      column_values implementation
// -----------------------------------------------------------------------------

void column_values::insert(irs::doc_id_t key, irs::bytes_ref value) {
  const auto res = values_.emplace(key, value);
  ASSERT_TRUE(res.second);
}

void column_values::sort(const std::map<irs::doc_id_t, irs::doc_id_t>& docs) {
  std::map<irs::doc_id_t, irs::bstring> resorted_values;

  for (auto& value : values_) {
    resorted_values.emplace(
      docs.at(value.first),
      std::move(value.second));
  }

  values_ = std::move(resorted_values);
}

// -----------------------------------------------------------------------------
// --SECTION--                                      index_segment implementation
// -----------------------------------------------------------------------------

void index_segment::compute_features() {
  irs::columnstore_writer::values_writer_f writer =
      [this](irs::doc_id_t) -> column_output&{ return out_; };

  for (auto* field : doc_fields_) {
    for (auto& entry : field->feature_infos) {
      buf_.clear();

      entry.handler(field->stats, count_, writer);

      columns_[entry.id].insert(count_, buf_);
    }
  }
}

void index_segment::insert_sorted(const ifield& f) {
  irs::bstring buf;
  irs::bytes_output out(buf);
  if (f.write(out)) {
    const irs::bytes_ref value = buf;
    const auto doc_id = irs::doc_id_t((irs::doc_limits::min)() + count_);
    sort_.emplace_back(std::make_pair(irs::bstring(value.c_str(), value.size()), doc_id));
  }
}

void index_segment::insert_stored(const ifield& f) {
  buf_.clear();
  irs::bytes_output out{buf_};
  if (!f.write(out)) {
    return;
  }

  const size_t id = columns_.size();
  EXPECT_LE(id, std::numeric_limits<irs::field_id>::max());

  auto res = columns_meta_.emplace(static_cast<std::string>(f.name()), id);

  if (res.second) {
    columns_.emplace_back();
    res.first->second = id;
  }

  const auto column_id = res.first->second;
  EXPECT_LT(column_id, columns_.size()) ;

  columns_[column_id].insert(count_, buf_);
}

void index_segment::insert(const ifield& f) {
  const irs::string_ref field_name = f.name();

  const auto res = fields_.emplace(
    field_name,
    field{field_name, f.index_features(), f.features()});

  field& field = res.first->second;

  if (res.second) {
    auto& new_field = res.first->second;
    id_to_field_.emplace_back(&new_field);
    for (auto& feature : new_field.features) {
      auto handler = field_features_.find(feature.first);

      if (handler != field_features_.end()) {
        const size_t id = columns_.size();
        ASSERT_LE(id, std::numeric_limits<irs::field_id>::max());
        columns_.emplace_back();

        feature.second = irs::field_id{id};

        new_field.feature_infos.emplace_back(field::feature_info{
          irs::field_id{id}, handler->second});
      }
    }

    const_cast<irs::string_ref&>(res.first->first) = field.name;
  }

  doc_fields_.insert(&field);

  auto& stream = f.get_tokens();

  auto* term = irs::get<irs::term_attribute>(stream);
  assert(term);
  auto* inc = irs::get<irs::increment>(stream);
  assert(inc);
  auto* offs = irs::get<irs::offset>(stream);

  bool empty = true;
  auto doc_id = irs::doc_id_t((irs::doc_limits::min)() + count_);

  while (stream.next()) {
    tests::term& trm = field.insert(term->value);
    tests::posting& pst = trm.add(doc_id);
    field.stats.pos += inc->value;
    ++field.stats.len;
    pst.add(field.stats.pos, field.stats.offs, stream);
    empty = false;
  }

  if (!empty) { 
    field.docs.emplace(doc_id);
  }

  if (offs) {
    field.stats.offs += offs->end;
  }
}

void index_segment::sort(const irs::comparer& comparator) {
  if (sort_.empty()) {
    return;
  }

  std::sort(
    sort_.begin(), sort_.end(),
    [&comparator](
        const std::pair<irs::bstring, irs::doc_id_t>& lhs,
        const std::pair<irs::bstring, irs::doc_id_t>& rhs) {
      return comparator(lhs.first, rhs.first); });

  irs::doc_id_t new_doc_id = irs::doc_limits::min();
  std::map<irs::doc_id_t, irs::doc_id_t> order;
  for (auto& entry : sort_) {
    order[entry.second] = new_doc_id++;
  }

  for (auto& field : fields_) {
    field.second.sort(order);
  }

  for (auto& column : columns_) {
    column.sort(order);
  }
}
////////////////////////////////////////////////////////////////////////////////
/// @class term_reader
////////////////////////////////////////////////////////////////////////////////
class term_reader : public irs::term_reader {
 public:
  explicit term_reader(const tests::field& data);

  virtual irs::seek_term_iterator::ptr iterator(irs::SeekMode) const override;
  virtual irs::seek_term_iterator::ptr iterator(irs::automaton_table_matcher& a) const override;
  virtual const irs::field_meta& meta() const override { return data_; }
  virtual size_t size() const override { return data_.terms.size(); }
  virtual uint64_t docs_count() const override { return data_.docs.size(); }
  virtual const irs::bytes_ref& (min)() const override { return min_; }
  virtual const irs::bytes_ref& (max)() const override { return max_; }
  virtual size_t bit_union(
    const cookie_provider& provider,
    size_t* bitset) const override;
  virtual irs::doc_iterator::ptr postings(
      const irs::seek_cookie& cookie,
      irs::IndexFeatures features) const override;
  virtual irs::attribute* get_mutable(irs::type_info::type_id type) noexcept override {
    if (irs::type<irs::frequency>::id() == type) {
      return pfreq_;
    }
    return nullptr;
  }

 private:
  const tests::field& data_;
  irs::frequency freq_;
  irs::frequency* pfreq_{};
  irs::bytes_ref min_;
  irs::bytes_ref max_;
};

////////////////////////////////////////////////////////////////////////////////
/// @struct index_meta_writer
////////////////////////////////////////////////////////////////////////////////
struct index_meta_writer: public irs::index_meta_writer {
  virtual std::string filename(const irs::index_meta& meta) const override;
  virtual bool prepare(irs::directory& dir, irs::index_meta& meta) override;
  virtual bool commit() override;
  virtual void rollback() noexcept override;
};

std::string index_meta_writer::filename(
    const irs::index_meta& /*meta*/) const {
  return {};
}

bool index_meta_writer::prepare(
    irs::directory& /*dir*/,
    irs::index_meta& /*meta*/) {
  return true;
}

bool index_meta_writer::commit() { return true; }

void index_meta_writer::rollback() noexcept { }

////////////////////////////////////////////////////////////////////////////////
/// @struct index_meta_reader
////////////////////////////////////////////////////////////////////////////////
struct index_meta_reader : public irs::index_meta_reader {
  virtual bool last_segments_file(const irs::directory& dir, std::string& out) const override;
  virtual void read(
    const irs::directory& dir,
    irs::index_meta& meta,
    const irs::string_ref& filename = irs::string_ref::NIL) override;
};

bool index_meta_reader::last_segments_file(
    const irs::directory&,
    std::string&) const {
  return false;
}

void index_meta_reader::read(
    const irs::directory& /*dir*/,
    irs::index_meta& /*meta*/,
    const irs::string_ref& /*filename*/ /*= string_ref::NIL*/) {
}

////////////////////////////////////////////////////////////////////////////////
/// @struct segment_meta_writer
////////////////////////////////////////////////////////////////////////////////
struct segment_meta_writer : public irs::segment_meta_writer {
  virtual void write(
    irs::directory& dir,
    std::string& filename,
    const irs::segment_meta& meta) override;
};

void segment_meta_writer::write(
    irs::directory& /*dir*/,
    std::string& /*filename*/,
    const irs::segment_meta& /*meta*/) {
}

////////////////////////////////////////////////////////////////////////////////
/// @struct segment_meta_reader
////////////////////////////////////////////////////////////////////////////////
struct segment_meta_reader : public irs::segment_meta_reader {
  virtual void read(
    const irs::directory& dir,
    irs::segment_meta& meta,
    const irs::string_ref& filename = irs::string_ref::NIL) override;
};

void segment_meta_reader::read( 
    const irs::directory& /*dir*/,
    irs::segment_meta& /*meta*/,
    const irs::string_ref& /*filename*/ /*= string_ref::NIL*/) {
}

////////////////////////////////////////////////////////////////////////////////
/// @class document_mask_writer
////////////////////////////////////////////////////////////////////////////////
class document_mask_writer final : public irs::document_mask_writer {
 public:
  explicit document_mask_writer(const index_segment& data);
  virtual std::string filename(const irs::segment_meta& meta) const override;

  void write(
    irs::directory& dir,
    const irs::segment_meta& meta,
    const irs::document_mask& docs_mask) override;

 private:
  const index_segment& data_;
};

document_mask_writer::document_mask_writer(const index_segment& data):
  data_(data) {
}

std::string document_mask_writer::filename(
    const irs::segment_meta& /*meta*/) const {
  return { };
}

void document_mask_writer::write(
    irs::directory& /*dir*/,
    const irs::segment_meta& /*meta*/,
    const irs::document_mask& docs_mask) {
  EXPECT_EQ(data_.doc_mask().size(), docs_mask.size());
  for (auto doc_id : docs_mask) {
    EXPECT_EQ(true, data_.doc_mask().find(doc_id) != data_.doc_mask().end());
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @class document_mask_reader
////////////////////////////////////////////////////////////////////////////////
class document_mask_reader final : public irs::document_mask_reader {
 public:
  explicit document_mask_reader(const index_segment& segment) noexcept
    : segment_{&segment} {
  }

  virtual bool read(
      const irs::directory& /*dir*/,
      const irs::segment_meta& /*meta*/,
      irs::document_mask& docs_mask) override {
    docs_mask = segment_->doc_mask();
    return true;
  }

 private:
  const index_segment* segment_;
};

////////////////////////////////////////////////////////////////////////////////
/// @class field_writer
////////////////////////////////////////////////////////////////////////////////
class field_writer : public irs::field_writer {
 public:
  explicit field_writer(const index_segment& data);

  virtual void prepare(const irs::flush_state& state) override;
  virtual void write(const std::string& name,
                     irs::IndexFeatures index_features,
                     const irs::feature_map_t& custom_features,
                     irs::term_iterator& actual_term) override;
  virtual void end() override;

 private:
  field_reader readers_;
  irs::IndexFeatures index_features_{irs::IndexFeatures::NONE};
};

field_writer::field_writer(const index_segment& data)
  : readers_(data) {
}

void field_writer::prepare(const irs::flush_state& state) {
  EXPECT_EQ(state.doc_count, readers_.data().doc_count());
}

void field_writer::write(
    const std::string& name,
    irs::IndexFeatures expected_index_features,
    const irs::feature_map_t& expected_features,
    irs::term_iterator& actual_term) {
  // features to check
  const auto fld_it = readers_.data().fields().find(name);
  ASSERT_NE(readers_.data().fields().end(), fld_it);

  auto* fld = &fld_it->second;
  ASSERT_EQ(fld->name, name);
  ASSERT_EQ(fld->features, expected_features);
  ASSERT_EQ(fld->index_features, expected_index_features);
  
  const auto index_features = index_features_ & fld->index_features;

  const irs::term_reader* expected_term_reader = readers_.field(fld->name);
  ASSERT_NE(nullptr, expected_term_reader);

  irs::bytes_ref actual_min{ irs::bytes_ref::NIL };
  irs::bytes_ref actual_max{ irs::bytes_ref::NIL };
  irs::bstring actual_min_buf;
  irs::bstring actual_max_buf;
  size_t actual_size = 0;

  for (auto expected_term = expected_term_reader->iterator(irs::SeekMode::NORMAL);
       actual_term.next(); ++actual_size) {
    ASSERT_TRUE(expected_term->next());

    assert_term(*expected_term, actual_term, index_features);

    if (actual_min.null()) {
      actual_min_buf = actual_term.value();
      actual_min = actual_min_buf;
    }

    actual_max_buf = actual_term.value();
    actual_max = actual_max_buf;
  }

  // check term reader
  ASSERT_EQ(expected_term_reader->size(), actual_size);
  ASSERT_EQ((expected_term_reader->min)(), actual_min);
  ASSERT_EQ((expected_term_reader->max)(), actual_max);
}

void field_writer::end() { }

////////////////////////////////////////////////////////////////////////////////
/// @class doc_iterator
////////////////////////////////////////////////////////////////////////////////
class doc_iterator : public irs::doc_iterator {
 public:
  doc_iterator(irs::IndexFeatures features, const tests::term& data);

  irs::doc_id_t value() const override {
    return doc_.value;
  }

  irs::attribute* get_mutable(irs::type_info::type_id type) noexcept override {
    const auto it = attrs_.find(type);
    return it == attrs_.end() ? nullptr : it->second;
  }

  virtual bool next() override {
    if (next_ == data_.postings.end()) {
      doc_.value = irs::doc_limits::eof();
      return false;
    }

    prev_ = next_, ++next_;
    doc_.value = prev_->id();
    freq_.value = static_cast<uint32_t>(prev_->positions().size());
    pos_.clear();

    return true;
  }

  virtual irs::doc_id_t seek(irs::doc_id_t id) override {
    auto it = data_.postings.find(posting{id});

    if (it == data_.postings.end()) {
      prev_ = next_ = it;
      return irs::doc_limits::eof();
    }

    prev_ = it;
    next_ = ++it;
    doc_.value = prev_->id();
    pos_.clear();

    return doc_.value;
  }

 private:
  class pos_iterator final : public irs::position {
   public:
    pos_iterator(const doc_iterator& owner, irs::IndexFeatures features)
      : owner_(owner) {
      if (irs::IndexFeatures::NONE != (features & irs::IndexFeatures::OFFS)) {
        poffs_ = &offs_;
      }

      if (irs::IndexFeatures::NONE != (features & irs::IndexFeatures::PAY)) {
        ppay_ = &pay_;
      }
    }

    attribute* get_mutable(irs::type_info::type_id type) noexcept override {
      if (irs::type<irs::offset>::id() == type) {
        return poffs_;
      }

      if (irs::type<irs::payload>::id() == type) {
        return ppay_;
      }

      return nullptr;
    }

    void clear() {
      next_ = owner_.prev_->positions().begin();
      value_ = irs::type_limits<irs::type_t::pos_t>::invalid();
      offs_.clear();
      pay_.value = irs::bytes_ref::NIL;
    }

    bool next() override {
      if (next_ == owner_.prev_->positions().end()) {
        value_ = irs::type_limits<irs::type_t::pos_t>::eof();
        return false;
      }

      value_ = next_->pos;
      offs_.start = next_->start;
      offs_.end = next_->end;
      pay_.value = next_->payload;
      ++next_;

      return true;
    }

    virtual void reset() override {
      assert(false); // unsupported
    }

   private:
    std::set<tests::position>::const_iterator next_;
    irs::offset offs_;
    irs::payload pay_;
    irs::offset* poffs_{};
    irs::payload* ppay_{};
    const doc_iterator& owner_;
  };

  const tests::term& data_;
  std::map<irs::type_info::type_id, irs::attribute*> attrs_;
  irs::document doc_;
  irs::frequency freq_;
  irs::cost cost_;
  irs::score score_;
  pos_iterator pos_;
  std::set<posting>::const_iterator prev_;
  std::set<posting>::const_iterator next_;
};

doc_iterator::doc_iterator(irs::IndexFeatures features, const tests::term& data)
  : data_(data),
    pos_(*this, features) {
  next_ = data_.postings.begin();

  cost_.reset(data_.postings.size());
  attrs_[irs::type<irs::cost>::id()] = &cost_;

  attrs_[irs::type<irs::document>::id()] = &doc_;
  attrs_[irs::type<irs::score>::id()] = &score_;

  if (irs::IndexFeatures::NONE != (features & irs::IndexFeatures::FREQ)) {
    attrs_[irs::type<irs::frequency>::id()] = &freq_;
  }

  if (irs::IndexFeatures::NONE != (features & irs::IndexFeatures::POS)) {
    attrs_[irs::type<irs::position>::id()] = &pos_;
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @class term_iterator
////////////////////////////////////////////////////////////////////////////////
class term_iterator final : public irs::seek_term_iterator {
 public:
  struct term_cookie final : irs::seek_cookie {
    explicit term_cookie(irs::bytes_ref term) noexcept
      : term(term) { }

    virtual irs::attribute* get_mutable(irs::type_info::type_id) override {
      return nullptr;
    }

    irs::bytes_ref term;
  };

  explicit term_iterator(const tests::field& data) noexcept
    : data_(data) {
    next_ = data_.terms.begin();
  }

  irs::attribute* get_mutable(irs::type_info::type_id) noexcept override {
    return nullptr;
  }

  const irs::bytes_ref& value() const override {
    return value_;
  }

  bool next() override {
    if ( next_ == data_.terms.end() ) {
      value_ = irs::bytes_ref::NIL;
      return false;
    }

    prev_ = next_, ++next_;
    value_ = prev_->value;
    return true;
  }

  virtual void read() override  { }

  virtual bool seek(const irs::bytes_ref& value) override {
    auto it = data_.terms.find(term{value});

    if (it == data_.terms.end()) {
      prev_ = next_ = it;
      value_ = irs::bytes_ref::NIL;
      return false;
    }

    prev_ = it;
    next_ = ++it;
    value_ = prev_->value;
    return true;
  }

  virtual irs::SeekResult seek_ge(const irs::bytes_ref& value) override {
    auto it = data_.terms.lower_bound(term{value});
    if (it == data_.terms.end()) {
      prev_ = next_ = it;
      value_ = irs::bytes_ref::NIL;
      return irs::SeekResult::END;
    }

    if (it->value == value) {
      prev_ = it;
      next_ = ++it;
      value_ = prev_->value;
      return irs::SeekResult::FOUND;
    }

    prev_ = ++it;
    next_ = ++it;
    value_ = prev_->value;
    return irs::SeekResult::NOT_FOUND;
  }

  virtual doc_iterator::ptr postings(irs::IndexFeatures features) const override {
    return irs::memory::make_managed<doc_iterator>(data_.index_features & features, *prev_);
  }

  virtual bool seek(
      const irs::bytes_ref& /*term*/,
      const irs::seek_cookie& cookie) override {
    auto& state = dynamic_cast<const term_cookie&>(cookie);
    return seek(state.term);
  }

  virtual irs::seek_cookie::ptr cookie() const override {
    return irs::memory::make_unique<term_cookie>(value_);
  }

 private:
  const tests::field& data_;
  std::set<tests::term>::const_iterator prev_;
  std::set<tests::term>::const_iterator next_;
  irs::bytes_ref value_;
};

term_reader::term_reader(const tests::field& data)
  : data_(data),
    min_(data_.terms.begin()->value),
    max_(data_.terms.rbegin()->value) {
  if (irs::IndexFeatures::NONE != (meta().index_features & irs::IndexFeatures::FREQ)) {
    for (auto& term : data.terms) {
      for (auto& p : term.postings) {
        freq_.value += static_cast<uint32_t>(p.positions().size());
      }
    }
    pfreq_ = &freq_;
  }
}

irs::seek_term_iterator::ptr term_reader::iterator(irs::SeekMode) const {
  return irs::memory::make_managed<term_iterator>(data_);
}

irs::seek_term_iterator::ptr term_reader::iterator(irs::automaton_table_matcher& matcher) const {
  return irs::memory::make_managed<irs::automaton_term_iterator>(
    matcher.GetFst(), iterator(irs::SeekMode::NORMAL));
}

size_t term_reader::bit_union(
    const cookie_provider& provider,
    size_t* bitset) const {
  constexpr auto BITS{irs::bits_required<uint64_t>()};

  auto term = this->iterator(irs::SeekMode::NORMAL);

  size_t count{0};
  while (auto* cookie = provider()) {
    term->seek(irs::bytes_ref::NIL, *cookie);

    auto docs = term->postings(irs::IndexFeatures::NONE);

    if (docs) {
      auto* doc = irs::get<irs::document>(*docs);

      while (docs->next()) {
        const irs::doc_id_t value = doc->value;
        irs::set_bit(bitset[value / BITS], value % BITS);
        ++count;
      }
    }
  }

  return count;
}

irs::doc_iterator::ptr term_reader::postings(
    const irs::seek_cookie& cookie,
    irs::IndexFeatures features) const {
  auto it = this->iterator(irs::SeekMode::NORMAL);
  if (!it->seek(irs::bytes_ref::NIL, cookie)) {
    return irs::doc_iterator::empty();
  }
  return it->postings(features);
}

field_reader::field_reader(const index_segment& data)
  : data_(data) {
  readers_.reserve(data.fields().size());

  for (const auto& pair : data_.fields()) {
    readers_.emplace_back(irs::memory::make_unique<term_reader>(pair.second));
  }
}

field_reader::field_reader(field_reader&& other) noexcept
  : readers_(std::move(other.readers_)), data_(std::move(other.data_)) {
}

void field_reader::prepare(
    const irs::directory&,
    const irs::segment_meta&,
    const irs::document_mask&) {
}

irs::field_iterator::ptr field_reader::iterator() const {
  return nullptr;
}

const irs::term_reader* field_reader::field(const irs::string_ref& field) const {
  const auto it = std::lower_bound(
    readers_.begin(), readers_.end(), field,
    [] (const irs::term_reader::ptr& lhs, const irs::string_ref& rhs) {
      return lhs->meta().name < rhs;
  });

  return it == readers_.end() || field < (**it).meta().name
    ? nullptr
    : it->get();
}
  
size_t field_reader::size() const {
  return data_.size();
}

////////////////////////////////////////////////////////////////////////////////
/// @class column_meta_writer
////////////////////////////////////////////////////////////////////////////////
class column_meta_writer final : public irs::column_meta_writer {
 public:
  virtual void prepare(irs::directory& /*dir*/,
                       const irs::segment_meta& /*meta*/) override {
    // NOOP
  }

  virtual void write(const std::string& /*name*/,
                     irs::field_id /*id*/) override {
    // NOOP
  }

  virtual void flush() override {
    // NOOP
  }
};

////////////////////////////////////////////////////////////////////////////////
/// @class column_meta_reader
////////////////////////////////////////////////////////////////////////////////
class column_meta_reader final : public irs::column_meta_reader {
 public:
  explicit column_meta_reader(const index_segment& segment) noexcept
    : segment_{&segment},
      begin_{segment.columns_meta().begin()},
      end_{segment.columns_meta().end()} {
  }

  virtual bool prepare(
      const irs::directory& /*dir*/,
      const irs::segment_meta& /*meta*/,
      /*out*/ size_t& count,
      /*out*/ irs::field_id& max_id) override {
    count = 0;
    max_id = irs::field_limits::invalid();
    for (auto& column : segment_->columns_meta()) {
      max_id = std::max(column.second, max_id);
      ++count;
    }
    return true;
  }

  virtual bool read(irs::column_meta& column) override {
    if (begin_ == end_) {
      return false;
    }

    std::tie(column.name, column.id) = *begin_;
    ++begin_;
    return true;
  }

 private:
  const index_segment* segment_;
  decltype(segment_->columns_meta().begin()) begin_;
  decltype(segment_->columns_meta().end()) end_;
};

////////////////////////////////////////////////////////////////////////////////
/// @class columnstore_writer
////////////////////////////////////////////////////////////////////////////////
class columnstore_writer final : public irs::columnstore_writer {
 public:
  virtual void prepare(
      irs::directory& /*dir*/,
      const irs::segment_meta& /*meta*/) {
    // NOOP
  }

  virtual column_t push_column(const irs::column_info& /*info*/) {
    // NOOP
    EXPECT_FALSE(true);
    return {};
  }

  virtual void rollback() noexcept {
    // NOOP
  }

  virtual bool commit(const irs::flush_state& /*state*/) {
    // NOOP
    EXPECT_FALSE(true);
    return false;
  }
};

////////////////////////////////////////////////////////////////////////////////
/// @class column_iterator
////////////////////////////////////////////////////////////////////////////////
class column_iterator final : public irs::resettable_doc_iterator {
 public:
  explicit column_iterator(const column_values& values) noexcept
    : values_{&values},
      cur_{values_->begin()},
      next_{cur_},
      end_{values_->end()} {
    std::get<irs::cost>(attrs_).reset(values_->size());
  }

  virtual bool next() override {
    if (cur_ == end_) {
      return false;
    }

    cur_ = next_;
    ++next_;

    std::get<irs::document>(attrs_).value = cur_->first;
    std::get<irs::payload>(attrs_).value = cur_->second;

    return true;
  }

  virtual irs::doc_id_t seek(irs::doc_id_t target) override {
    return irs::seek(*this, target);
  }

  virtual irs::doc_id_t value() const noexcept override {
    return std::get<irs::document>(attrs_).value;
  }

  virtual irs::attribute* get_mutable(
      irs::type_info::type_id type) noexcept override {
    return irs::get_mutable(attrs_, type);
  }

  virtual void reset() noexcept override {
    cur_ = values_->begin();
    next_ = cur_;
  }

 private:
  using attributes = std::tuple<irs::document, irs::cost, irs::payload>;

  attributes attrs_;
  const column_values* values_;
  decltype(values_->begin()) cur_;
  decltype(values_->begin()) next_;
  decltype(values_->end()) end_;
};

////////////////////////////////////////////////////////////////////////////////
/// @class column_reader
////////////////////////////////////////////////////////////////////////////////
class column_reader final : public irs::columnstore_reader::column_reader {
 public:
  explicit column_reader(const column_values& values) noexcept
    : values_{&values} {
  }

  virtual irs::columnstore_reader::values_reader_f values() const override {
    auto moved_it = make_move_on_copy(this->iterator());
    auto* document = irs::get<irs::document>(*moved_it.value());
    if (!document || irs::doc_limits::eof(document->value)) {
      return irs::columnstore_reader::empty_reader();
    }
    const irs::bytes_ref* payload_value = &irs::bytes_ref::NIL;
    auto* payload = irs::get<irs::payload>(*moved_it.value());
    if (payload) {
      payload_value = &payload->value;
    }

    return [it = moved_it, payload_value, document](irs::doc_id_t doc, irs::bytes_ref& value) {
      if (doc > irs::doc_limits::invalid() && doc < irs::doc_limits::eof()) {
        if (doc < document->value) {
#ifdef IRESEARCH_DEBUG
          auto& impl = dynamic_cast<irs::resettable_doc_iterator&>(*it.value());
#else
          auto& impl = static_cast<irs::resettable_doc_iterator&>(*it.value());
#endif
          impl.reset();
        }

        if (doc == it.value()->seek(doc)) {
          value = *payload_value;
          return true;
        }
      }

      return false;
    };
  }

  virtual doc_iterator::ptr iterator() const override {
    return irs::memory::make_managed<column_iterator>(*values_);

  }

  virtual bool visit(const irs::columnstore_reader::values_visitor_f& visitor) const override {
    auto it = this->iterator();

    irs::payload dummy;
    auto* doc = irs::get<irs::document>(*it);
    if (!doc) {
      return false;
    }
    auto* payload = irs::get<irs::payload>(*it);
    if (!payload) {
      payload = &dummy;
    }

    while (it->next()) {
      if (!visitor(doc->value, payload->value)) {
        return false;
      }
    }

    return true;
  }

  virtual irs::doc_id_t size() const override {
    return values_->size();

  }

 private:
  const column_values* values_;
};

////////////////////////////////////////////////////////////////////////////////
/// @class columnstore_reader
////////////////////////////////////////////////////////////////////////////////
class columnstore_reader final : public irs::columnstore_reader {
 public:
  explicit columnstore_reader(const index_segment& segment) noexcept {
    columns_.reserve(segment.size());
    for (auto& column : segment.columns()) {
      columns_.emplace_back(column);
    }
  }

  virtual bool prepare(
      const irs::directory& /*dir*/,
      const irs::segment_meta& /*meta*/) {
    return !columns_.empty();
  }

  virtual const column_reader* column(irs::field_id field) const {
    if (field < columns_.size()) {
      return &columns_[field];
    }

    return nullptr;
  }

  virtual size_t size() const {
    return columns_.size();
  }

 private:
  std::vector<tests::column_reader> columns_;
};

////////////////////////////////////////////////////////////////////////////////
/// @class format
////////////////////////////////////////////////////////////////////////////////
class format : public irs::format {
 public:
  static ptr make();
  format();
  format(const index_segment& data);

  virtual irs::index_meta_writer::ptr get_index_meta_writer() const override;
  virtual irs::index_meta_reader::ptr get_index_meta_reader() const override;

  virtual irs::segment_meta_writer::ptr get_segment_meta_writer() const override;
  virtual irs::segment_meta_reader::ptr get_segment_meta_reader() const override;

  virtual irs::document_mask_writer::ptr get_document_mask_writer() const override;
  virtual irs::document_mask_reader::ptr get_document_mask_reader() const override;

  virtual irs::field_writer::ptr get_field_writer(bool consolidation) const override;
  virtual irs::field_reader::ptr get_field_reader() const override;

  virtual irs::column_meta_writer::ptr get_column_meta_writer() const override;
  virtual irs::column_meta_reader::ptr get_column_meta_reader() const override;

  virtual irs::columnstore_writer::ptr get_columnstore_writer(bool consolidation) const override;
  virtual irs::columnstore_reader::ptr get_columnstore_reader() const override;

 private:
  static const index_segment DEFAULT_SEGMENT;
  const index_segment& data_;
};

/*static*/ decltype(format::DEFAULT_SEGMENT) format::DEFAULT_SEGMENT{{}};

format::format(): format(DEFAULT_SEGMENT) {}

format::format(const index_segment& data):
  irs::format(format::type()), data_(data) {
}

irs::index_meta_writer::ptr format::get_index_meta_writer() const {
  return irs::memory::make_unique<index_meta_writer>();
}

irs::index_meta_reader::ptr format::get_index_meta_reader() const {
  // can reuse stateless reader
  static index_meta_reader reader;

  return irs::memory::to_managed<irs::index_meta_reader, false>(&reader);
}

irs::segment_meta_writer::ptr format::get_segment_meta_writer() const {
  // can reuse stateless writer
  static segment_meta_writer writer;

  return irs::memory::to_managed<irs::segment_meta_writer, false>(&writer);
}

irs::segment_meta_reader::ptr format::get_segment_meta_reader() const {
  // can reuse stateless reader
  static segment_meta_reader reader;

  return irs::memory::to_managed<irs::segment_meta_reader, false>(&reader);
}

irs::document_mask_reader::ptr format::get_document_mask_reader() const {
  return irs::memory::make_managed<tests::document_mask_reader>(data_);
}

irs::document_mask_writer::ptr format::get_document_mask_writer() const {
  return irs::memory::make_managed<tests::document_mask_writer>(data_);
}

irs::field_writer::ptr format::get_field_writer(bool /*consolidation*/) const {
  return irs::memory::make_unique<tests::field_writer>(data_);
}

irs::field_reader::ptr format::get_field_reader() const {
  return irs::memory::make_unique<tests::field_reader>(data_);
}

irs::column_meta_writer::ptr format::get_column_meta_writer() const {
  return irs::memory::make_unique<tests::column_meta_writer>();
}

irs::column_meta_reader::ptr format::get_column_meta_reader() const {
  return irs::memory::make_unique<tests::column_meta_reader>(data_);
}

irs::columnstore_writer::ptr format::get_columnstore_writer(bool /*consolidation*/) const {
  return irs::memory::make_unique<tests::columnstore_writer>();
}

irs::columnstore_reader::ptr format::get_columnstore_reader() const {
  return irs::memory::make_unique<tests::columnstore_reader>(data_);
}

REGISTER_FORMAT(tests::format);

/*static*/ irs::format::ptr format::make() {
  static const auto instance = irs::memory::make_shared<format>();
  return instance;
}

void assert_term(
    const irs::term_iterator& expected_term,
    const irs::term_iterator& actual_term,
    irs::IndexFeatures requested_features) {
  ASSERT_EQ(expected_term.value(), actual_term.value());

  const irs::doc_iterator::ptr expected_docs = expected_term.postings(requested_features);
  const irs::doc_iterator::ptr actual_docs = actual_term.postings(requested_features);

  ASSERT_TRUE(!irs::doc_limits::valid(expected_docs->value()));
  ASSERT_TRUE(!irs::doc_limits::valid(actual_docs->value()));
  // check docs
  for (; expected_docs->next();) {
    ASSERT_TRUE(actual_docs->next());
    ASSERT_EQ(expected_docs->value(), actual_docs->value());

    // check document attributes
    {
      auto* expected_freq = irs::get<irs::frequency>(*expected_docs);
      auto* actual_freq = irs::get<irs::frequency>(*actual_docs);

      if (expected_freq) {
        ASSERT_FALSE(!actual_freq);
        ASSERT_EQ(expected_freq->value, actual_freq->value);
      }

      auto* expected_pos = irs::get_mutable<irs::position>(expected_docs.get());
      auto* actual_pos = irs::get_mutable<irs::position>(actual_docs.get());

      if (expected_pos) {
        ASSERT_FALSE(!actual_pos);

        auto* expected_offs = irs::get<irs::offset>(*expected_pos);
        auto* actual_offs = irs::get<irs::offset>(*actual_pos);
        if (expected_offs) ASSERT_FALSE(!actual_offs);

        auto* expected_pay = irs::get<irs::payload>(*expected_pos);
        auto* actual_pay = irs::get<irs::payload>(*actual_pos);
        if (expected_pay) ASSERT_FALSE(!actual_pay);
        ASSERT_TRUE(!irs::pos_limits::valid(expected_pos->value()));
        ASSERT_TRUE(!irs::pos_limits::valid(actual_pos->value()));
        for (; expected_pos->next();) {
          ASSERT_TRUE(actual_pos->next());
          ASSERT_EQ(expected_pos->value(), actual_pos->value());

          if (expected_offs) {
            ASSERT_EQ(expected_offs->start, actual_offs->start);
            ASSERT_EQ(expected_offs->end, actual_offs->end);
          }

          if (expected_pay) {
            ASSERT_EQ(expected_pay->value, actual_pay->value);
          }
        }
        ASSERT_FALSE(actual_pos->next());
        ASSERT_TRUE(irs::pos_limits::eof(expected_pos->value()));
        ASSERT_TRUE(irs::pos_limits::eof(actual_pos->value()));
      }
    }
  }
  ASSERT_FALSE(actual_docs->next());
  ASSERT_TRUE(irs::doc_limits::eof(expected_docs->value()));
  ASSERT_TRUE(irs::doc_limits::eof(actual_docs->value()));
}

void assert_terms_next(
    const irs::term_reader& expected_term_reader,
    const irs::term_reader& actual_term_reader,
    irs::IndexFeatures features,
    irs::automaton_table_matcher* matcher) {
  irs::bytes_ref actual_min{ irs::bytes_ref::NIL };
  irs::bytes_ref actual_max{ irs::bytes_ref::NIL };
  irs::bstring actual_min_buf;
  irs::bstring actual_max_buf;
  size_t actual_size = 0;

  auto expected_term = matcher ? expected_term_reader.iterator(*matcher)
                               : expected_term_reader.iterator(irs::SeekMode::NORMAL);
  auto actual_term = matcher ? actual_term_reader.iterator(*matcher)
                             : actual_term_reader.iterator(irs::SeekMode::NORMAL);
  for (; expected_term->next(); ++actual_size) {
    ASSERT_TRUE(actual_term->next());

    assert_term(*expected_term, *actual_term, features);

    if (actual_min.null()) {
      actual_min_buf = actual_term->value();
      actual_min = actual_min_buf;
    }

    actual_max_buf = actual_term->value();
    actual_max = actual_max_buf;
  }
  //ASSERT_FALSE(actual_term->next()); // FIXME
  //ASSERT_FALSE(actual_term->next());

  // check term reader
  if (!matcher) {
    ASSERT_EQ(expected_term_reader.size(), actual_size);
    ASSERT_EQ((expected_term_reader.min)(), actual_min);
    ASSERT_EQ((expected_term_reader.max)(), actual_max);
  }
}

void assert_terms_seek(
    const irs::term_reader& expected_term_reader,
    const irs::term_reader& actual_term_reader,
    irs::IndexFeatures features,
    irs::automaton_table_matcher* matcher,
    size_t lookahead  = 10) {
  auto expected_term = matcher
    ? expected_term_reader.iterator(*matcher)
    : expected_term_reader.iterator(irs::SeekMode::NORMAL);
  ASSERT_NE(nullptr, expected_term);

  auto actual_term_with_state = matcher
    ? actual_term_reader.iterator(*matcher)
    : actual_term_reader.iterator(irs::SeekMode::NORMAL);
  ASSERT_NE(nullptr, actual_term_with_state);

  auto actual_term_with_state_random_only
    = actual_term_reader.iterator(irs::SeekMode::RANDOM_ONLY);
  ASSERT_NE(nullptr, actual_term_with_state_random_only);

  for (; expected_term->next();) {
    // seek with state
    {
      ASSERT_TRUE(actual_term_with_state->seek(expected_term->value()));
      assert_term(*expected_term, *actual_term_with_state, features);
    }

    // seek without state random only
    {
      auto actual_term = actual_term_reader.iterator(irs::SeekMode::RANDOM_ONLY);
      ASSERT_TRUE(actual_term->seek(expected_term->value()));

      assert_term(*expected_term, *actual_term, features);
    }

    // seek with state random only
    {
      ASSERT_TRUE(actual_term_with_state_random_only->seek(expected_term->value()));

      assert_term(*expected_term, *actual_term_with_state_random_only, features);
    }

    // seek without state, iterate forward
    irs::seek_cookie::ptr cookie;
    {
      auto actual_term = actual_term_reader.iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      assert_term(*expected_term, *actual_term, features);
      actual_term->read();
      cookie = actual_term->cookie();

      // iterate forward
      {
        auto copy_expected_term = expected_term_reader.iterator(irs::SeekMode::NORMAL);
        ASSERT_TRUE(copy_expected_term->seek(expected_term->value()));
        ASSERT_EQ(expected_term->value(), copy_expected_term->value());
        for(size_t i = 0; i < lookahead; ++i) {
          const bool copy_expected_next = copy_expected_term->next();
          const bool actual_next = actual_term->next();
          ASSERT_EQ(copy_expected_next, actual_next);
          if (!copy_expected_next) {
            break;
          }
          assert_term(*copy_expected_term, *actual_term, features);
        }
      }
      
      // seek back to initial term
      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      assert_term(*expected_term, *actual_term, features);
    }

    // seek greater or equal without state, iterate forward
    {
      auto actual_term = actual_term_reader.iterator(irs::SeekMode::NORMAL);
      ASSERT_EQ(irs::SeekResult::FOUND, actual_term->seek_ge(expected_term->value()));
      assert_term(*expected_term, *actual_term, features);

      // iterate forward
      {
        auto copy_expected_term = expected_term_reader.iterator(irs::SeekMode::NORMAL);
        ASSERT_TRUE(copy_expected_term->seek(expected_term->value()));
        ASSERT_EQ(expected_term->value(), copy_expected_term->value());
        for(size_t i = 0; i < lookahead; ++i) {
          const bool copy_expected_next = copy_expected_term->next();
          const bool actual_next = actual_term->next();
          ASSERT_EQ(copy_expected_next, actual_next);
          if (!copy_expected_next) {
            break;
          }
          assert_term(*copy_expected_term, *actual_term, features);
        }
      }
      
      // seek back to initial term
      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      assert_term(*expected_term, *actual_term, features);
    }

    // seek to cookie without state, iterate to the end
    {
      auto actual_term = actual_term_reader.iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(actual_term->seek(expected_term->value(), *cookie));
      ASSERT_EQ(expected_term->value(), actual_term->value());
      assert_term(*expected_term, *actual_term, features);

      // iterate forward
      {
        auto copy_expected_term = expected_term_reader.iterator(irs::SeekMode::NORMAL);
        ASSERT_TRUE(copy_expected_term->seek(expected_term->value()));
        ASSERT_EQ(expected_term->value(), copy_expected_term->value());
        for(size_t i = 0; i < lookahead; ++i) {
          const bool copy_expected_next = copy_expected_term->next();
          const bool actual_next = actual_term->next();
          ASSERT_EQ(copy_expected_next, actual_next);
          if (!copy_expected_next) {
            break;
          }
          assert_term(*copy_expected_term, *actual_term, features);
        }
      }

      // seek to the same term
      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      assert_term(*expected_term, *actual_term, features);

      // seek to the same term
      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      assert_term(*expected_term, *actual_term, features);

      // seek greater equal to the same term
      ASSERT_EQ(irs::SeekResult::FOUND, actual_term->seek_ge(expected_term->value()));
      assert_term(*expected_term, *actual_term, features);
    }
  }
}

void assert_index(
    const index_t& expected_index,
    const irs::index_reader& actual_index_reader,
    irs::IndexFeatures features,
    size_t skip /*= 0*/,
    irs::automaton_table_matcher* matcher /*=nullptr*/) {
  // check number of segments
  ASSERT_EQ(expected_index.size(), actual_index_reader.size());
  size_t i = 0;
  for (auto& actual_sub_reader : actual_index_reader) {
    // skip segment if validation not required
    if (skip) {
      ++i;
      --skip;
      continue;
    }

    // setting up test reader/writer
    const tests::index_segment& expected_segment = expected_index[i];
    tests::field_reader expected_reader(expected_segment);

    ASSERT_EQ(expected_reader.size(), actual_sub_reader.size());

    // get field name iterators
    auto& expected_fields = expected_segment.fields();
    auto expected_fields_begin = expected_fields.begin();
    auto expected_fields_end = expected_fields.end();

    // iterate over fields
    auto actual_fields = actual_sub_reader.fields();
    for (; actual_fields->next(); ++expected_fields_begin) {
      // check field name
      ASSERT_EQ(expected_fields_begin->first, actual_fields->value().meta().name);

      // check field terms
      auto expected_term_reader = expected_reader.field(expected_fields_begin->second.name);
      ASSERT_NE(nullptr, expected_term_reader);
      auto actual_term_reader = actual_sub_reader.field(actual_fields->value().meta().name);
      ASSERT_NE(nullptr, actual_term_reader);

      const auto expected_field_it = expected_segment.fields().find(expected_fields_begin->first);
      ASSERT_NE(expected_fields_end, expected_field_it);

      // check term reader
      ASSERT_EQ((expected_term_reader->min)(), (actual_term_reader->min)());
      ASSERT_EQ((expected_term_reader->max)(), (actual_term_reader->max)());
      ASSERT_EQ(expected_term_reader->size(), actual_term_reader->size());
      ASSERT_EQ(expected_term_reader->docs_count(), actual_term_reader->docs_count());
      ASSERT_EQ(expected_term_reader->meta(), actual_term_reader->meta());

      auto* expected_freq = irs::get<irs::frequency>(*expected_term_reader);
      auto* actual_freq = irs::get<irs::frequency>(*actual_term_reader);
      if (expected_freq) {
        ASSERT_NE(nullptr, actual_freq);
        ASSERT_EQ(expected_freq->value, actual_freq->value);
      } else {
        ASSERT_EQ(nullptr, actual_freq);
      }

      assert_terms_next(*expected_term_reader, *actual_term_reader, features, matcher);
      assert_terms_seek(*expected_term_reader, *actual_term_reader, features, matcher);
    }
    ASSERT_FALSE(actual_fields->next());
    ASSERT_EQ(expected_fields_begin, expected_fields_end);

    // iterate over columns
//    auto& expected_columns = expected_segment.columns_meta();
//    auto expected_columns_begin = expected_columns.begin();
//    auto expected_columns_end = expected_columns.end();
//    auto actual_columns = actual_sub_reader.columns();
//
//    for (; actual_columns->next(); ++expected_columns_begin) {
//      ASSERT_EQ(
//        (irs::column_meta{expected_columns_begin->first,
//                          expected_columns_begin->second}),
//        actual_columns->value());
//
//      const auto* actual_cs = ex
//
//      assert_column(irs::columnstore_reader)
//    }
//    ASSERT_FALSE(actual_columns->next());
//    ASSERT_EQ(expected_columns_begin, expected_columns_end);


    ++i;
    ASSERT_EQ(expected_fields_end, expected_fields_begin);
  }
}

void assert_index(
    const irs::directory& dir,
    irs::format::ptr codec,
    const index_t& expected_index,
    irs::IndexFeatures features,
    size_t skip /*= 0*/,
    irs::automaton_table_matcher* matcher /*= nullptr*/) {
  auto actual_index_reader = irs::directory_reader::open(dir, codec);

  assert_index(expected_index, actual_index_reader, features, skip, matcher);
}

} // tests

namespace iresearch {

// use base irs::position type for ancestors
template<>
struct type<tests::doc_iterator::pos_iterator> : type<irs::position> { };

}
