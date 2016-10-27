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

#ifndef IRESEARCH_TL_DOC_WRITER_H
#define IRESEARCH_TL_DOC_WRITER_H

#include "field_data.hpp"
#include "doc_header.hpp"
#include "document/serializer.hpp"
#include "analysis/token_stream.hpp"
#include "formats/formats.hpp"
#include "utils/directory_utils.hpp"
#include "utils/noncopyable.hpp"

NS_ROOT

struct segment_meta;

class IRESEARCH_API segment_writer: util::noncopyable {
 public:
  DECLARE_PTR(segment_writer);
  DECLARE_FACTORY_DEFAULT(directory& dir, format::ptr codec);

  struct update_context {
    size_t generation;
    size_t update_id;
  };

  typedef std::unordered_map<doc_id_t, update_context> update_contexts;

  template<typename FieldIterator, typename AttributeIterator>
  void update(
      FieldIterator begin, FieldIterator end, 
      AttributeIterator abegin, AttributeIterator aend,
      const update_context& ctx) {
    for (; begin != end; ++begin) {
      insert_field(*begin);
    }

    for (; abegin != aend; ++abegin) {
      insert_attribute(*abegin);
    }

    finish(ctx);
  }

  void flush(std::string& filename, segment_meta& meta);

  const std::string& name() const { return seg_name_; }
  format::ptr codec() const { return codec_; }

  uint32_t docs_cached() const { return num_docs_cached_; }
  const update_contexts& docs_context() const { return docs_context_; }
  bool initialized() const { return initialized_; }
  void reset();
  void reset(std::string seg_name);

 private:
  struct doc_header : iresearch::serializer {
    bool write(data_output& out) const override;

    std::vector<field_id> doc_fields; // per document field ids
  }; // doc_header 

  segment_writer(directory& dir, format::ptr codec) NOEXCEPT;

  bool index_field(field_data& slot, token_stream* tokens, const flags& features, float_t boost);
  bool store_field(field_data& slot, const serializer* serializer);
  bool store_attribute(const string_ref& name, const serializer* serializer);
 
  // adds document attribute
  template<typename Attribute>
  void insert_attribute(const Attribute& attr) {
    REGISTER_TIMER_DETAILED();

    store_attribute(
      static_cast<const string_ref&>(attr.name()),
      static_cast<const serializer*>(attr.serializer())
    );
  }


  // adds document field
  template<typename Field>
  void insert_field(const Field& field) {
    REGISTER_TIMER_DETAILED();

    auto& slot = fields_.get(
      static_cast<const string_ref&>(field.name())
    );
    
    index_field(
      slot, 
      static_cast<token_stream*>(field.get_tokens()), 
      static_cast<const flags&>(field.features()),
      static_cast<float_t>(field.boost())
    );

    store_field(
      slot, 
      static_cast<const serializer*>(field.serializer())
    );
  }

  void finish(const update_context& ctx); // finish document

  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  doc_header header_; // 
  update_contexts docs_context_;
  fields_data fields_;
  std::unordered_map<hashed_string_ref, columns_writer::values_writer_f> columns_;
  std::string seg_name_;
  field_meta_writer::ptr field_meta_writer_;
  field_writer::ptr field_writer_;
  stored_fields_writer::ptr sf_writer_;
  columns_writer::ptr col_writer_;
  format::ptr codec_;
  tracking_directory dir_;
  std::atomic<uint32_t> num_docs_cached_{0};
  bool initialized_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
}; // segment_writer

NS_END

#endif