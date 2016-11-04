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

#include "shared.hpp"
#include "segment_reader.hpp"

#include "index/index_meta.hpp"

#include "formats/format_utils.hpp"
#include "utils/index_utils.hpp"
#include "utils/type_limits.hpp"

#include <unordered_map>

NS_LOCAL

class masked_docs_iterator 
    : public iresearch::segment_reader::docs_iterator_t,
      private iresearch::util::noncopyable {
 public:
  masked_docs_iterator(
    iresearch::doc_id_t begin,
    iresearch::doc_id_t end,
    const iresearch::document_mask& docs_mask
  ) :
    current_(iresearch::type_limits<iresearch::type_t::doc_id_t>::invalid()),
    docs_mask_(docs_mask),
    end_(end),
    next_(begin) {
  }

  virtual ~masked_docs_iterator() {}

  virtual bool next() override {
    while (next_ < end_) {
      current_ = next_++;

      if (docs_mask_.find(current_) == docs_mask_.end()) {
        return true;
      }
    }

    current_ = iresearch::type_limits<iresearch::type_t::doc_id_t>::eof();

    return false;
  }

  virtual iresearch::doc_id_t value() const override {
    return current_;
  }

 private:
  iresearch::doc_id_t current_;
  const iresearch::document_mask& docs_mask_;
  const iresearch::doc_id_t end_; // past last valid doc_id
  iresearch::doc_id_t next_;
};

void read_fields_meta(
    iresearch::fields_meta& meta, 
    const iresearch::format& codec, 
    const iresearch::directory& dir,
    const std::string& name) {
  iresearch::fields_meta::items_t fields;
  iresearch::flags features;

  auto visitor = [&fields, &features] (iresearch::field_meta& value)->bool {
    features |= value.features;
    fields[value.id] = std::move(value);
    return true;
  };

  auto reader = codec.get_field_meta_reader();
  reader->prepare(dir, name);
  fields.resize(reader->begin());
  iresearch::read_all<iresearch::field_meta>(visitor, *reader, fields.size());
  reader->end();

  meta = iresearch::fields_meta(std::move(fields), std::move(features));
}

void read_columns_meta(
    iresearch::columns_meta& meta, 
    const iresearch::format& codec, 
    const iresearch::directory& dir,
    const std::string& name) {
  iresearch::columns_meta::items_t columns;

  auto visitor = [&columns] (iresearch::column_meta& value)->bool {
    columns[value.id] = std::move(value);
    return true;
  };

  auto reader = codec.get_column_meta_reader();
  reader->prepare(dir, name);
  for (iresearch::column_meta meta; reader->read(meta);) {
    columns.push_back(std::move(meta));
  }

  meta = iresearch::columns_meta(std::move(columns));
}

iresearch::sub_reader::value_visitor_f INVALID_VISITOR =
  [] (iresearch::doc_id_t) { return false; };

NS_END // NS_LOCAL

NS_ROOT

const term_reader* segment_reader::terms(const string_ref& field) const {
  auto* meta = fields_.find(field);
  if (!meta) {
    return nullptr;
  }

  return fr_->terms(meta->id);
}

bool segment_reader::document(
    doc_id_t doc, 
    const stored_fields_reader::visitor_f& visitor) const {
  assert(type_limits<type_t::doc_id_t>::valid(doc));
  doc -= type_limits<type_t::doc_id_t>::min();
  return sfr_->visit(doc, visitor);
}

bool segment_reader::document(
    doc_id_t doc, 
    const document_visitor_f& visitor) const {
  assert(type_limits<type_t::doc_id_t>::valid(doc));
  doc -= type_limits<type_t::doc_id_t>::min();

  doc_header_.clear();
  auto stored_fields_visitor = [this, &visitor] (data_input& in) {
    // read document header
    auto header_reader = [this] (field_id id, bool) {
      doc_header_.push_back(id);
    };
    stored::visit_header(in, header_reader);

    // read document body
    for (auto field_id : doc_header_) {
      const field_meta* field = fields_.find(field_id);
      assert(field);

      if (!visitor(*field, in)) {
        return false;
      }
    }

    return true;
  };

  return sfr_->visit(doc, stored_fields_visitor);
}

sub_reader::value_visitor_f segment_reader::values(
    field_id field,
    const columnstore_reader::value_reader_f& value_reader) const {
  if (!csr_) {
    return INVALID_VISITOR;
  }

  auto column = csr_->values(field);

  return [&value_reader, column](doc_id_t doc)->bool {
    return column(doc, value_reader);
  };
}

sub_reader::value_visitor_f segment_reader::values(
    const string_ref& field,
    const columnstore_reader::value_reader_f& value_reader) const {
  auto meta = columns_.find(field);

  if (!meta) {
    return INVALID_VISITOR;
  }

  return values(meta->id, value_reader);
}
  
bool segment_reader::column(
    field_id field,
    const columnstore_reader::raw_reader_f& reader) const {
  if (!csr_) {
    return false;
  }

  return csr_->visit(field, reader);
}

segment_reader::docs_iterator_t::ptr segment_reader::docs_iterator() const {
  // the implementation generates doc_ids sequentially
  return segment_reader::docs_iterator_t::ptr(new masked_docs_iterator(
    type_limits<type_t::doc_id_t>::min(),
    doc_id_t(type_limits<type_t::doc_id_t>::min() + docs_count_),
    docs_mask_
  ));
}

segment_reader::ptr segment_reader::open(
    const directory& dir, 
    const segment_meta& seg) {
  segment_reader::ptr rdr = segment_reader::ptr(new segment_reader());
  rdr->dir_state_.dir = &dir;
  rdr->dir_state_.version = integer_traits<decltype(seg.version)>::const_max; // version forcing refresh(...)
  rdr->docs_count_ = seg.docs_count;
  rdr->refresh(seg);

  auto& codec = *seg.codec;

  // initialize fields meta
  read_fields_meta(rdr->fields_, codec, dir, seg.name);

  reader_state rs;
  rs.codec = &codec;
  rs.dir = &dir;
  rs.docs_mask = &rdr->docs_mask_;
  rs.fields = &rdr->fields_;
  rs.meta = &seg;

  // initialize field reader
  auto& fr = rdr->fr_;
  fr = codec.get_field_reader();
  fr->prepare(rs);

  // initialize stored fields reader
  auto& sfr = rdr->sfr_;
  sfr = codec.get_stored_fields_reader();
  sfr->prepare(rs);

  // initialize columns
  columnstore_reader::ptr csr = codec.get_columnstore_reader();
  if (csr->prepare(rs)) {
    rdr->csr_ = std::move(csr);
    read_columns_meta(rdr->columns_, codec, dir, seg.name);
  }

  return rdr;
}

void segment_reader::refresh(const segment_meta& meta) {
  if (dir_state_.version == meta.version) {
    return; // nothing to refresh
  }

  // initialize document mask
  docs_mask_.clear();
  index_utils::read_document_mask(docs_mask_, *(dir_state_.dir), meta);
  dir_state_.version = meta.version;
}

NS_END