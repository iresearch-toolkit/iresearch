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

#include "shared.hpp"
#include "segment_writer.hpp"
#include "store/store_utils.hpp"
#include "index_meta.hpp"
#include "analysis/token_stream.hpp"
#include "analysis/token_attributes.hpp"
#include "utils/log.hpp"
#include "utils/map_utils.hpp"
#include "utils/timer_utils.hpp"
#include "utils/type_limits.hpp"
#include "utils/version_utils.hpp"

#include <math.h>
#include <set>

NS_ROOT

segment_writer::column::column(
    const string_ref& name, 
    columnstore_writer& columnstore) {
  this->name.assign(name.c_str(), name.size());
  this->handle = columnstore.push_column();
}

segment_writer::ptr segment_writer::make(directory& dir, format::ptr codec) {
  PTR_NAMED(segment_writer, ptr, dir, codec);
  return ptr;
}

segment_writer::segment_writer(directory& dir, format::ptr codec) NOEXCEPT
  : codec_(codec), dir_(dir), initialized_(false) {
}

bool segment_writer::remove(doc_id_t doc_id) {
  return doc_id < (type_limits<type_t::doc_id_t>::min() + num_docs_cached_)
    && docs_mask_.insert(doc_id).second;
}

bool segment_writer::index_field(
    doc_id_t doc_id,
    const string_ref& name,
    token_stream& tokens,
    const flags& features,
    float_t boost) {
  REGISTER_TIMER_DETAILED();

  auto& slot = fields_.get(name);
  auto& slot_features = slot.meta().features;

  // invert only if new field features are a subset of slot features
  if ((slot.empty() || features.is_subset_of(slot_features)) &&
      slot.invert(tokens, slot.empty() ? features : slot_features, boost, doc_id)) {
    if (features.check<norm>()) {
      norm_fields_.insert(&slot);
    }

    fields_ += features; // accumulate segment features
    return true;
  }

  return false;
}

columnstore_writer::column_output& segment_writer::stream(
  doc_id_t doc_id,
  const string_ref& name
) {
  REGISTER_TIMER_DETAILED();
  static struct {
    hashed_string_ref operator()(
      const hashed_string_ref& key, const column& value
    ) const NOEXCEPT {
      // reuse hash but point ref at value
      return hashed_string_ref(key.hash(), value.name);
    }
  } generator;

  // replace original reference to 'name' provided by the caller
  // with a reference to the cached copy in 'value'
  return map_utils::try_emplace_update_key(
    columns_,                                     // container
    generator,                                    // key generator
    make_hashed_ref(name, string_ref_hash_t()),   // key
    name, *col_writer_                            // value
  ).first->second.handle.second(doc_id);
}

void segment_writer::finish(doc_id_t doc_id, const update_context& ctx) {
  REGISTER_TIMER_DETAILED();

  // write document normalization factors (for each field marked for normalization))
  float_t value;
  for (auto* field : norm_fields_) {
    value = field->boost() / float_t(std::sqrt(double_t(field->size())));
    if (value != norm::DEFAULT()) {
      auto& stream = field->norms(*col_writer_);
      write_zvfloat(stream, value);
    }
  }
  norm_fields_.clear(); // clear normalized fields

  docs_context_[doc_id] = ctx;
}

bool segment_writer::flush(std::string& filename, segment_meta& meta) {
  REGISTER_TIMER_DETAILED();

  // flush columnstore
  col_writer_->flush();

  // flush columns indices
  if (!columns_.empty()) {
    static struct less_t {
      bool operator()(const column* lhs, const column* rhs) {
        return lhs->name < rhs->name;
      };
    } less;
    std::set<const column*, decltype(less)> columns(less);

    // ensure columns are sorted
    for (auto& entry : columns_) {
      columns.emplace(&entry.second);
    }
    
    // flush columns meta
    col_meta_writer_->prepare(dir_, seg_name_);
    for (auto& column: columns) {
      col_meta_writer_->write(column->name, column->handle.first);
    }
    col_meta_writer_->flush();
    columns_.clear();
  }

  // flush fields metadata & inverted data
  {
    flush_state state;
    state.dir = &dir_;
    state.doc_count = num_docs_cached_;
    state.name = seg_name_;
    state.ver = IRESEARCH_VERSION;

    fields_.flush(*field_writer_, state);
  }

  meta.docs_count = num_docs_cached_;
  meta.files.clear(); // prepare empy set to be swaped into dir_

  if (!dir_.swap_tracked(meta.files)) {
    IR_FRMT_ERROR("Failed to swap list of tracked files in: %s", __FUNCTION__);

    return false;
  }

  // flush segment metadata
  {
    segment_meta_writer::ptr writer = codec_->get_segment_meta_writer();
    writer->write(dir_, meta);

    filename = writer->filename(meta);
  }

  return true;
}

void segment_writer::reset() {
  initialized_ = false;

  tracking_directory::file_set empty;

  if (!dir_.swap_tracked(empty)) {
    // on failre next segment might have extra files which will fail to get refs
    IR_FRMT_ERROR("Failed to swap list of tracked files in: %s", __FUNCTION__);
  }

  docs_context_.clear();
  docs_mask_.clear();
  fields_.reset();
  num_docs_cached_ = 0;
}

void segment_writer::reset(std::string seg_name) {
  reset();

  seg_name_ = std::move(seg_name);

  if (!field_writer_) {
    field_writer_ = codec_->get_field_writer();
  }

  if (!col_meta_writer_) {
    col_meta_writer_ = codec_->get_column_meta_writer();
  }

  if (!col_writer_) {
    col_writer_ = codec_->get_columnstore_writer();
  }

  col_writer_->prepare(dir_, seg_name_);

  initialized_ = true;
}

NS_END