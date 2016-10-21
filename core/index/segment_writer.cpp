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
#include "segment_writer.hpp"
#include "store/store_utils.hpp"
#include "index_meta.hpp"
#include "analysis/token_stream.hpp"
#include "utils/timer_utils.hpp"
#include "utils/type_limits.hpp"
#include "utils/version_utils.hpp"

NS_ROOT

bool segment_writer::doc_header::write(data_output& out) const {
  return stored::write_header(out, doc_fields.begin(), doc_fields.end());
}

segment_writer::ptr segment_writer::make(
    directory& dir, 
    format::ptr codec) {
  return ptr(new segment_writer(dir, codec, nullptr, nullptr));
}

segment_writer::segment_writer(
    directory& dir,
    format::ptr codec,
    stored_fields_writer::ptr&& sf_writer,
    columns_writer::ptr&& col_writer) NOEXCEPT
  : sf_writer_(std::move(sf_writer)),
    col_writer_(std::move(col_writer)),
    codec_(codec),
    dir_(dir),
    initialized_(sf_writer_ && col_writer_) {
}

bool segment_writer::index_field(
    field_data& slot,
    token_stream* tokens,
    const flags& features,
    float_t boost) {
  REGISTER_TIMER_DETAILED();

  if (slot.invert(tokens, features, boost, num_docs_cached_ + type_limits<type_t::doc_id_t>::min())) {
    fields_ += features; // accumulate segment features
    return true;
  }

  return false;
}

bool segment_writer::store_field(
    field_data& slot,
    const serializer* serializer) {
  REGISTER_TIMER_DETAILED();
  if (serializer) {
    // store field id
    header_.doc_fields.push_back(slot.meta().id);

    // write user fields
    return sf_writer_->write(*serializer); 
  }

  return false;
}

bool segment_writer::store_attribute(
    const string_ref& name,
    const serializer* serializer) {
  REGISTER_TIMER_DETAILED();
  if (serializer) {
    return col_writer_->write(num_docs_cached_, name, *serializer);
  }

  return false;
}

void segment_writer::finish(const update_context& ctx) {
  REGISTER_TIMER_DETAILED();
  sf_writer_->end(&header_);
  col_writer_->end();
  docs_context_[type_limits<type_t::doc_id_t>::min() + num_docs_cached_++] = ctx;
  header_.doc_fields.clear();
}

void segment_writer::flush(std::string& filename, segment_meta& meta) {
  REGISTER_TIMER_DETAILED();

  // flush stored fields
  sf_writer_->finish();
  sf_writer_->reset();

  // flush columns indices
  col_writer_->flush();
  col_writer_->reset();

  // flush fields metadata & inverted data
  {
    flush_state state;
    state.codec = codec_.get();
    state.dir = &dir_;
    state.doc_count = num_docs_cached_;
    state.name = seg_name_;
    state.ver = IRESEARCH_VERSION;

    fields_.flush(state);
  }

  meta.docs_count = num_docs_cached_;
  dir_.swap_tracked(meta.files);
  
  // flush segment metadata
  {
    segment_meta_writer::ptr writer = codec_->get_segment_meta_writer();
    writer->write(dir_, meta);

    filename = writer->filename(meta);
  }
}

void segment_writer::reset() {
  initialized_ = false;

  tracking_directory::file_set empty_set;

  dir_.swap_tracked(empty_set);
  docs_context_.clear();
  fields_.reset();
  num_docs_cached_ = 0;
}

void segment_writer::reset(std::string seg_name) {
  reset();

  seg_name_ = std::move(seg_name);

  if (!sf_writer_) {
    sf_writer_ = codec_->get_stored_fields_writer();
  }

  if (!col_writer_) {
    col_writer_ = codec_->get_columns_writer();
  }

  const string_ref ref_seg_name = seg_name_;
  sf_writer_->prepare(dir_, ref_seg_name);
  col_writer_->prepare(dir_, ref_seg_name);

  initialized_ = true;
}

NS_END