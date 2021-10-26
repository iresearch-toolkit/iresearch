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

#ifndef IRESEARCH_ASSERT_FORMAT_H
#define IRESEARCH_ASSERT_FORMAT_H

#include <set>

#include "doc_generator.hpp"
#include "index/field_meta.hpp"
#include "index/comparer.hpp"
#include "formats/formats.hpp"

namespace tests {

////////////////////////////////////////////////////////////////////////////////
/// @struct position
////////////////////////////////////////////////////////////////////////////////
struct position {
  position(uint32_t pos, uint32_t start,
           uint32_t end, const irs::bytes_ref& pay);

  bool operator<(const position& rhs) const {
    return pos < rhs.pos;
  }

  uint32_t pos;
  uint32_t start;
  uint32_t end;
  irs::bstring payload;
};

////////////////////////////////////////////////////////////////////////////////
/// @class posting
////////////////////////////////////////////////////////////////////////////////
class posting {
 public:
  explicit posting(irs::doc_id_t id);
  posting(irs::doc_id_t id, std::set<position>&& positions)
    : positions_(std::move(positions)), id_(id) {
  }
  posting(posting&& rhs) noexcept = default;
  posting& operator=(posting&& rhs) noexcept = default;

  void add(uint32_t pos, uint32_t offs_start, const irs::attribute_provider& attrs);

  bool operator<(const posting& rhs) const {
    return id_ < rhs.id_;
  }

  const std::set<position>& positions() const { return positions_; }
  irs::doc_id_t id() const { return id_; }
  size_t size() const { return positions_.size(); }

 private:
  friend struct term;

  std::set<position> positions_;
  irs::doc_id_t id_;
};

////////////////////////////////////////////////////////////////////////////////
/// @struct term
////////////////////////////////////////////////////////////////////////////////
struct term {
  explicit term(irs::bytes_ref data);

  posting& add(irs::doc_id_t id);

  bool operator<(const term& rhs) const;

  uint64_t docs_count() const { return postings.size(); }

  void sort(const std::map<irs::doc_id_t, irs::doc_id_t>& docs) {
    std::set<posting> resorted_postings;

    for (auto& posting : postings) {
      resorted_postings.emplace(
        docs.at(posting.id_),
        std::move(const_cast<tests::posting&>(posting).positions_));
    }

    postings = std::move(resorted_postings);
  }

  std::set<posting> postings;
  irs::bstring value;
};


////////////////////////////////////////////////////////////////////////////////
/// @class field
////////////////////////////////////////////////////////////////////////////////
class field : public irs::field_meta {
 public:
  struct feature_info {
    irs::field_id id;
    irs::feature_handler_f handler;
  };

  struct field_stats : irs::field_stats {
    uint32_t pos{};
    uint32_t offs{};
  };

  field(const irs::string_ref& name,
        irs::IndexFeatures index_features,
        const irs::features_t& features);
  field(field&& rhs) = default;
  field& operator=(field&& rhs) = default;

  term& insert(const irs::bytes_ref& term);
  term* find(const irs::bytes_ref& term);
  size_t remove(const irs::bytes_ref& t);
  void sort(const std::map<irs::doc_id_t, irs::doc_id_t>& docs) {
    for (auto& term : terms) {
      const_cast<tests::term&>(term).sort(docs);
    }
  }

  std::vector<feature_info> feature_infos;
  std::set<term> terms;
  std::unordered_set<irs::doc_id_t> docs;
  field_stats stats;
};

////////////////////////////////////////////////////////////////////////////////
/// @class column_values
////////////////////////////////////////////////////////////////////////////////
class column_values {
 public:
  void insert(irs::doc_id_t key, irs::bytes_ref value);

  auto begin() const { return values_.begin(); }
  auto end() const { return values_.end(); }
  auto size() const { return values_.size(); }

  void sort(const std::map<irs::doc_id_t, irs::doc_id_t>& docs);

 private:
  std::map<irs::doc_id_t, irs::bstring> values_;
};

////////////////////////////////////////////////////////////////////////////////
/// @class index_segment
////////////////////////////////////////////////////////////////////////////////
class index_segment: irs::util::noncopyable {
 public:
  using field_map_t = std::map<irs::string_ref, field>;
  using columns_t = std::deque<column_values>; // pointers remain valid
  using columns_meta_t = std::map<std::string, irs::field_id>;

  explicit index_segment(const irs::field_features_t& features)
    : field_features_{features} {
  }
  index_segment(index_segment&& rhs) noexcept = default;
  index_segment& operator=(index_segment&& rhs) noexcept = default;

  size_t doc_count() const noexcept { return count_; }
  size_t size() const noexcept { return fields_.size(); }
  auto& doc_mask() const noexcept { return doc_mask_; }
  auto& fields() const noexcept { return fields_; }
  auto& columns() noexcept { return columns_; }
  auto& columns() const noexcept { return columns_; }
  auto& columns_meta() const noexcept { return columns_meta_; }
  auto& columns_meta() noexcept { return columns_meta_; }

  template<typename Iterator>
  void insert(Iterator begin, Iterator end, ifield::ptr sorted = nullptr);

  void sort(const irs::comparer& comparator);

  void clear() noexcept {
    fields_.clear();
    count_ = 0;
  }

 private:
  class column_output final : public irs::column_output {
   public:
    explicit column_output(irs::bstring& buf) noexcept
      : buf_{&buf} {
    }

    virtual void write_byte(irs::byte_type b) override {
      (*buf_) += b;
    }

    virtual void write_bytes(const irs::byte_type* b, size_t size) override {
      buf_->append(b, size);
    }

    virtual void reset() override {
      buf_->clear();
    }

    irs::bstring* buf_;
  };

  index_segment(const index_segment& rhs) noexcept = delete;
  index_segment& operator=(const index_segment& rhs) noexcept = delete;

  void insert(const ifield& field);
  void insert_stored(const ifield& field);
  void insert_sorted(const ifield& field);
  void compute_features();

  irs::field_features_t field_features_;
  columns_meta_t columns_meta_;
  std::vector<std::pair<irs::bstring, irs::doc_id_t>> sort_;
  std::vector<const field*> id_to_field_;
  std::set<field*> doc_fields_;
  field_map_t fields_;
  columns_t columns_;
  size_t count_{};
  irs::document_mask doc_mask_;
  irs::bstring buf_;
  column_output out_{buf_};
};

template<typename Iterator>
void index_segment::insert(
    Iterator begin, Iterator end,
    ifield::ptr sorted /*= nullptr*/) {
  // reset field per-document state
  doc_fields_.clear();
  for (auto it = begin; it != end; ++it) {
    auto field = fields_.find(it->name());

    if (field != fields_.end()) {
      field->second.stats = {};
    }
  }

  for (; begin != end; ++begin) {
    insert(*begin);
    insert_stored(*begin);
  }

  if (sorted) {
    insert_sorted(*sorted);
  }

  compute_features();

  ++count_;
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

////////////////////////////////////////////////////////////////////////////////
/// @struct segment_meta_writer
////////////////////////////////////////////////////////////////////////////////
struct segment_meta_writer : public irs::segment_meta_writer {
  virtual void write(
    irs::directory& dir,
    std::string& filename,
    const irs::segment_meta& meta) override;
};

////////////////////////////////////////////////////////////////////////////////
/// @struct segment_meta_reader
////////////////////////////////////////////////////////////////////////////////
struct segment_meta_reader : public irs::segment_meta_reader {
  virtual void read(
    const irs::directory& dir,
    irs::segment_meta& meta,
    const irs::string_ref& filename = irs::string_ref::NIL) override;
};

////////////////////////////////////////////////////////////////////////////////
/// @class document_mask_writer
////////////////////////////////////////////////////////////////////////////////
class document_mask_writer: public irs::document_mask_writer {
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

////////////////////////////////////////////////////////////////////////////////
/// @class document_mask_reader
////////////////////////////////////////////////////////////////////////////////
class document_mask_reader : public irs::document_mask_reader {
 public:
  explicit document_mask_reader(const index_segment& data);

  virtual bool read(
      const irs::directory& /*dir*/,
      const irs::segment_meta& /*meta*/,
      irs::document_mask& docs_mask) {
    docs_mask = data_.doc_mask();
    return true;
  }

 private:
  const index_segment& data_;
};

////////////////////////////////////////////////////////////////////////////////
/// @class field_reader
////////////////////////////////////////////////////////////////////////////////
class field_reader : public irs::field_reader {
 public:
  field_reader( const index_segment& data );
  field_reader(field_reader&& other) noexcept;

  virtual void prepare(const irs::directory& dir,
                       const irs::segment_meta& meta,
                       const irs::document_mask& mask) override;
  virtual const irs::term_reader* field(const irs::string_ref& field) const override;
  virtual irs::field_iterator::ptr iterator() const override;
  virtual size_t size() const override;
  
  const index_segment& data() const {
    return data_;
  }

 private:
  std::vector<irs::term_reader::ptr> readers_;
  const index_segment& data_;
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

  virtual document_mask_writer::ptr get_document_mask_writer() const override;
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

typedef std::vector<index_segment> index_t;

void assert_term(
  const irs::term_iterator& expected_term,
  const irs::term_iterator& actual_term,
  irs::IndexFeatures features);

void assert_terms_seek(
  const irs::term_reader& expected_term_reader,
  const irs::term_reader& actual_term_reader,
  irs::IndexFeatures features,
  irs::automaton_table_matcher* acceptor,
  size_t lookahead = 10); // number of steps to iterate after the seek

void assert_index(
  const index_t& expected_index,
  const irs::index_reader& actual_index,
  irs::IndexFeatures features,
  size_t skip = 0, // do not validate the first 'skip' segments
  irs::automaton_table_matcher* matcher = nullptr);

void assert_index(
  const irs::directory& dir,
  irs::format::ptr codec,
  const index_t& index,
  irs::IndexFeatures features,
  size_t skip = 0, // no not validate the first 'skip' segments
  irs::automaton_table_matcher* matcher = nullptr);

} // tests

#endif
