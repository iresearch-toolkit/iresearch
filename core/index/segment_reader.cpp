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

bool read_columns_meta(
    iresearch::columns_meta& meta, 
    const iresearch::format& codec, 
    const iresearch::directory& dir,
    const std::string& name) {
  auto reader = codec.get_column_meta_reader();

  iresearch::field_id count = 0;
  if (!reader->prepare(dir, name, count)) {
    return false;
  }

  iresearch::columns_meta::items_t columns;
  columns.reserve(count);
  for (iresearch::column_meta meta; reader->read(meta);) {
    columns.emplace_back(std::move(meta));
  }

  meta = iresearch::columns_meta(std::move(columns));
  return true;
}

NS_END // NS_LOCAL

NS_ROOT


field_iterator::ptr segment_reader::fields() const {
  return fr_->iterator();
}

const term_reader* segment_reader::field(const string_ref& field) const {
  return fr_->field(field);
}

sub_reader::value_visitor_f segment_reader::values(
    field_id field,
    const columnstore_reader::value_reader_f& value_reader) const {
  if (!csr_) {
    return noop();
  }

  auto column = csr_->values(field);

  return [&value_reader, column](doc_id_t doc)->bool {
    return column(doc, value_reader);
  };
}
  
bool segment_reader::visit(
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
  auto& codec = *seg.codec;
  
  segment_reader::ptr rdr = segment_reader::ptr(new segment_reader());
  rdr->dir_state_.dir = &dir;
  rdr->dir_state_.version = integer_traits<decltype(seg.version)>::const_max; // version forcing refresh(...)
  rdr->docs_count_ = seg.docs_count;
  rdr->refresh(seg);

  reader_state rs;
  rs.codec = &codec;
  rs.dir = &dir;
  rs.docs_mask = &rdr->docs_mask_;
  rs.meta = &seg;

  // initialize field reader
  auto& fr = rdr->fr_;
  fr = codec.get_field_reader();
  if (!fr->prepare(rs)) {
    return nullptr;
  }
  
  // initialize columns
  columnstore_reader::ptr csr = codec.get_columnstore_reader();
  if (csr->prepare(rs)) {
    rdr->csr_ = std::move(csr);
  }
    
  read_columns_meta(rdr->columns_, codec, dir, seg.name);
    
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
