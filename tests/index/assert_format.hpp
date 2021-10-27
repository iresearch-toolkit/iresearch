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

#include "doc_generator.hpp"
#include "index/field_meta.hpp"
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
  index_segment(index_segment&& rhs) = default;
  index_segment& operator=(index_segment&& rhs) = default;

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

    column_output(column_output&&) = default;
    column_output& operator=(column_output&&) = default;

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
/// @class field_reader
////////////////////////////////////////////////////////////////////////////////
class field_reader : public irs::field_reader {
 public:
  field_reader(const index_segment& data);
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

using index_t = std::vector<index_segment>;

void assert_columnstore(
  const irs::directory& dir,
  irs::format::ptr codec,
  const index_t& expected_index,
  size_t skip = 0); // do not validate the first 'skip' segments

void assert_columnstore(
  irs::index_reader::ptr actual_index,
  const index_t& expected_index,
  size_t skip = 0); // do not validate the first 'skip' segments

void assert_index(
  irs::index_reader::ptr actual_index,
  const index_t& expected_index,
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
