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

#include <deque>
#include <unordered_map>

#include "merge_writer.hpp"
#include "index/doc_header.hpp"
#include "index/field_meta.hpp"
#include "index/index_meta.hpp"
#include "index/segment_reader.hpp"
#include "utils/directory_utils.hpp"
#include "utils/type_limits.hpp"
#include "utils/version_utils.hpp"
#include "document/serializer.hpp"
#include "store/store_utils.hpp"

#include <array>

namespace {
  // mapping of old doc_id to new doc_id (reader doc_ids are sequential 0 based)
  // masked doc_ids have value of MASKED_DOC_ID
  typedef std::vector<iresearch::doc_id_t> doc_id_map_t;

  // mapping of old field_id to new field_id
  typedef std::vector<iresearch::field_id> field_id_map_t;

  typedef std::unordered_map<iresearch::string_ref, const iresearch::field_meta*> field_meta_map_t;

  const iresearch::doc_id_t MASKED_DOC_ID = iresearch::integer_traits<iresearch::doc_id_t>::const_max; // masked doc_id (ignore)

  // ...........................................................................
  // compound view of multiple attributes as a single object
  // ...........................................................................
  class compound_attributes: public iresearch::attributes {
    using iresearch::attributes::add;
    public:
    void add(const iresearch::attributes& attributes);
    void set(const iresearch::attributes& attributes);
  };

  // ..........................................................................
  // iterator over doc_ids for a term over all readers
  // ...........................................................................
  struct compound_doc_iterator: public iresearch::doc_iterator {
    typedef std::pair<iresearch::doc_iterator::ptr, const doc_id_map_t*> doc_iterator_t;

    DECLARE_PTR(compound_doc_iterator);
    compound_attributes attrs;
    std::vector<doc_iterator_t> iterators;
    iresearch::doc_id_t current_id = iresearch::type_limits<iresearch::type_t::doc_id_t>::invalid();
    size_t current_itr = 0;

    virtual ~compound_doc_iterator() {}
    void add(iresearch::doc_iterator::ptr&& postings, const doc_id_map_t& doc_id_map);
    virtual const iresearch::attributes& attributes() const override;
    virtual bool next() override;
    virtual iresearch::doc_id_t seek(iresearch::doc_id_t target) override;
    virtual iresearch::doc_id_t value() const override;
  };

  // ...........................................................................
  // iterator over field_ids over all readers
  // ...........................................................................
  class compound_field_iterator {
   public:
    compound_field_iterator() {}
    void add(const iresearch::sub_reader& reader, const doc_id_map_t& doc_id_map);
    const iresearch::field_meta& meta() const;
    bool next();
    iresearch::term_iterator::ptr terms();
    size_t size() const { return field_iterators_.size(); }

    // visit matched iterators
    template<typename Visitor>
    bool visit(const Visitor& visitor) const {
      for (auto& entry : field_iterator_mask_) {
        auto& itr = field_iterators_[entry.itr_id];
        if (!visitor(*itr.reader, itr.field_id_base, *entry.meta)) {
          return false;
        }
      }
      return true;
    }

   private:
    struct field_iterator_t {
      field_iterator_t(
        iresearch::iterator<const iresearch::string_ref&>::ptr&& v_itr,
        const iresearch::sub_reader& v_reader,
        const doc_id_map_t& v_doc_id_map,
        size_t field_id_base
      ): itr(std::move(v_itr)), reader(&v_reader), doc_id_map(&v_doc_id_map), field_id_base(field_id_base) {}
      field_iterator_t(field_iterator_t&& other):
        itr(std::move(other.itr)), reader(other.reader), doc_id_map(other.doc_id_map), field_id_base(other.field_id_base) {}

      iresearch::iterator<const iresearch::string_ref&>::ptr itr;
      const iresearch::sub_reader* reader;
      const doc_id_map_t* doc_id_map;
      size_t field_id_base; // field id offset for current segment
    };
    struct term_iterator_t {
      size_t itr_id;
      const iresearch::field_meta* meta;
      const iresearch::term_reader* reader;
    };
    iresearch::string_ref current_field_;
    const iresearch::field_meta* current_meta_{};
    std::vector<term_iterator_t> field_iterator_mask_; // valid iterators for current field
    std::vector<field_iterator_t> field_iterators_; // all segment iterators
  };

  // ...........................................................................
  // iterator over documents for a term over all readers
  // ...........................................................................
  class compound_term_iterator: public iresearch::term_iterator {
   public:
    DECLARE_PTR(compound_term_iterator);
    compound_term_iterator(const iresearch::field_meta& meta): meta_(meta) {}
    virtual ~compound_term_iterator() {}
    compound_term_iterator& operator=(const compound_term_iterator&) = delete; // due to references
    const iresearch::field_meta& meta() { return meta_; }
    void add(const iresearch::term_reader& reader, const doc_id_map_t& doc_id_map);
    virtual const iresearch::attributes& attributes() const override;
    virtual bool next() override;
    virtual iresearch::doc_iterator::ptr postings(const iresearch::flags& features) const override;
    virtual void read() override;
    virtual const iresearch::bytes_ref& value() const override;
   private:
    typedef std::pair<iresearch::seek_term_iterator::ptr, const doc_id_map_t*> term_iterator_t;

    iresearch::bytes_ref current_term_;
    const iresearch::field_meta meta_;
    std::vector<size_t> term_iterator_mask_; // valid iterators for current term
    std::vector<term_iterator_t> term_iterators_; // all term iterators
  };

  void compound_attributes::add(const iresearch::attributes& attributes) {
    auto visitor = [this](
      const iresearch::attribute::type_id& type_id,
      const iresearch::attribute_ref<iresearch::attribute>&
    )->bool {
      add(type_id);
      return true;
    };

    attributes.visit(visitor); // add
  }

  void compound_attributes::set(const iresearch::attributes& attributes) {
    auto visitor_unset = [](
      const iresearch::attribute::type_id&,
      iresearch::attribute_ref<iresearch::attribute>& value
    )->bool {
      value = nullptr;
      return true;
    };
    auto visitor_update = [this](
      const iresearch::attribute::type_id& type_id,
      const iresearch::attribute_ref<iresearch::attribute>& value
    )->bool {
      add(type_id) = value;
      return true;
    };

    visit(visitor_unset); // unset
    attributes.visit(visitor_update); // set
  }

  void compound_doc_iterator::add(
    iresearch::doc_iterator::ptr&& postings, const doc_id_map_t& doc_id_map
  ) {
    if (iterators.empty()) {
      attrs.set(postings->attributes()); // add keys and set values
    }
    else {
      attrs.add(postings->attributes()); // only add missing keys
    }

    iterators.emplace_back(std::move(postings), &doc_id_map);
  }

  const iresearch::attributes& compound_doc_iterator::attributes() const {
    return attrs;
  }

  bool compound_doc_iterator::next() {
    for (
      bool update_attributes = false;
      current_itr < iterators.size();
      update_attributes = true, ++current_itr
    ) {
      auto& itr_entry = iterators[current_itr];
      auto& itr = itr_entry.first;
      auto& id_map = itr_entry.second;

      if (!itr) {
        continue;
      }

      if (update_attributes) {
        attrs.set(itr->attributes());
      }

      while (itr->next()) {
        auto doc = itr->value();

        if (doc >= id_map->size()) {
          continue; // invalid doc_id
        }

        current_id = (*id_map)[doc];

        if (current_id == MASKED_DOC_ID) {
          continue; // masked doc_id
        }

        return true;
      }

      itr.reset();
    }

    current_id = MASKED_DOC_ID;
    attrs.set(iresearch::attributes::empty_instance());

    return false;
  }

  iresearch::doc_id_t compound_doc_iterator::seek(iresearch::doc_id_t target) {
    return iresearch::seek(*this, target);
  }

  iresearch::doc_id_t compound_doc_iterator::value() const {
    return current_id;
  }

  void compound_field_iterator::add(
    const iresearch::sub_reader& reader,
    const doc_id_map_t& doc_id_map
  ) {
    field_iterator_mask_.emplace_back(term_iterator_t{field_iterators_.size(), nullptr, nullptr}); // mark as used to trigger next()

    size_t field_id_base = 0;
    if (!field_iterators_.empty()) {
      auto& back = field_iterators_.back();
      field_id_base = back.field_id_base + back.reader->fields().size();
    }

    field_iterators_.emplace_back(std::move(reader.iterator()), reader, doc_id_map, field_id_base);
  }

  const iresearch::field_meta& compound_field_iterator::meta() const {
    static const iresearch::field_meta empty;
    return current_meta_ ? *current_meta_ : empty;
  }

  bool compound_field_iterator::next() {
    // advance all used iterators
    for (auto& entry: field_iterator_mask_) {
      if (field_iterators_[entry.itr_id].itr && !field_iterators_[entry.itr_id].itr->next()) {
        field_iterators_[entry.itr_id].itr.reset();
      }
    }

    field_iterator_mask_.clear(); // reset for next pass

    for (size_t i = 0, count = field_iterators_.size(); i < count; ++i) {
      auto& field_itr = field_iterators_[i];

      if (!field_itr.itr) {
        continue; // empty iterator
      }

      auto& field_id = field_itr.itr->value();
      const auto* field_meta = field_itr.reader->fields().find(field_id);
      const auto* field_terms = field_itr.reader->terms(field_id);

      if (!field_meta || !field_terms  || /* !field_meta->indexed() || */
          (!field_iterator_mask_.empty() && field_id > current_field_)) {
        continue; // empty field or value too large
      }

      // found a smaller field
      if (field_iterator_mask_.empty() || field_id < current_field_) {
        field_iterator_mask_.clear();
        current_field_ = field_id;
        current_meta_ = field_meta;
      }

      assert(*field_meta == *current_meta_); // validated by caller
      field_iterator_mask_.emplace_back(term_iterator_t{i, field_meta, field_terms});
    }

    if (!field_iterator_mask_.empty()) {
      return true;
    }

    current_field_ = iresearch::string_ref::nil;

    return false;
  }

  iresearch::term_iterator::ptr compound_field_iterator::terms() {
    auto terms_itr = compound_term_iterator::ptr(new compound_term_iterator(meta()));

    for (auto& segment: field_iterator_mask_) {
      terms_itr->add(*(segment.reader), *(field_iterators_[segment.itr_id].doc_id_map));
    }

    return iresearch::term_iterator::ptr(terms_itr.release());
  }  void compound_term_iterator::add(
    const iresearch::term_reader& reader,
    const doc_id_map_t& doc_id_map
  ) {
    term_iterator_mask_.emplace_back(term_iterators_.size()); // mark as used to trigger next()
    term_iterators_.emplace_back(std::move(reader.iterator()), &doc_id_map);
  }

  const iresearch::attributes& compound_term_iterator::attributes() const {
    // no way to merge attributes for the same term spread over multiple iterators
    // would require API change for attributes
    throw iresearch::not_impl_error();
  }

  bool compound_term_iterator::next() {
    // advance all used iterators
    for (auto& itr_id: term_iterator_mask_) {
      auto& it = term_iterators_[itr_id].first;
      if (it && !it->next()) {
        it.reset();
      }
    }

    term_iterator_mask_.clear(); // reset for next pass

    for (size_t i = 0, count = term_iterators_.size(); i < count; ++i) {
      auto& term_itr = term_iterators_[i];

      if (!term_itr.first
          || (!term_iterator_mask_.empty() && term_itr.first->value() > current_term_)) {
        continue; // empty iterator or value too large
      }

      // found a smaller term
      if (term_iterator_mask_.empty() || term_itr.first->value() < current_term_) {
        term_iterator_mask_.clear();
        current_term_ = term_itr.first->value();
      }

      term_iterator_mask_.emplace_back(i);
    }

    if (!term_iterator_mask_.empty()) {
      return true;
    }

    current_term_ = iresearch::bytes_ref::nil;

    return false;
  }

  iresearch::doc_iterator::ptr compound_term_iterator::postings(const iresearch::flags& /*features*/) const {
    auto docs_itr = compound_doc_iterator::ptr(new compound_doc_iterator);

    for (auto& itr_id: term_iterator_mask_) {
      auto& term_itr = term_iterators_[itr_id];

      docs_itr->add(term_itr.first->postings(meta_.features), *(term_itr.second));
    }

    return iresearch::doc_iterator::ptr(docs_itr.release());
  }

  void compound_term_iterator::read() {
    for (auto& itr_id: term_iterator_mask_) {
      if (term_iterators_[itr_id].first) {
        term_iterators_[itr_id].first->read();
      }
    }
  }

  const iresearch::bytes_ref& compound_term_iterator::value() const {
    return current_term_;
  }

  // ...........................................................................
  // compute doc_id_map and docs_count
  // ...........................................................................
  iresearch::doc_id_t compute_doc_ids(
    doc_id_map_t& doc_id_map,
    const iresearch::sub_reader& reader,
    iresearch::doc_id_t next_id
  ) {
    // assume not a lot of space wasted if type_limits<type_t::doc_id_t>::min() > 0
    doc_id_map.resize(reader.docs_max() + iresearch::type_limits<iresearch::type_t::doc_id_t>::min(), MASKED_DOC_ID);

    for (auto docs_itr = reader.docs_iterator(); docs_itr->next(); ++next_id) {
      auto src_doc_id = docs_itr->value();

      assert(src_doc_id >= iresearch::type_limits<iresearch::type_t::doc_id_t>::min());
      assert(src_doc_id < reader.docs_max() + iresearch::type_limits<iresearch::type_t::doc_id_t>::min());
      doc_id_map[src_doc_id] = next_id; // set to next valid doc_id
    }

    return next_id;
  }

  // ...........................................................................
  // compute fields_type and fields_count
  // ...........................................................................
  bool compute_filed_meta(
    field_meta_map_t& field_meta_map,
    iresearch::flags& fields_features,
    const iresearch::sub_reader& reader
  ) {
    auto& fields = reader.fields();
    for (auto itr = reader.iterator(); itr->next();) {
      auto& field_id = itr->value();
      const auto* field_meta = fields.find(field_id);
      auto field_meta_map_itr = field_meta_map.emplace(field_id, field_meta);
      auto* tracked_field_meta = field_meta_map_itr.first->second;

      // validate field_meta equivalence
      if (!field_meta_map_itr.second &&
          (field_meta || tracked_field_meta) &&
          (!field_meta || !tracked_field_meta || *field_meta != *tracked_field_meta)) {
        return false; // field_meta is not equal, so cannot merge segments
      }

      fields_features |= field_meta->features;
    }

    return true;
  }

  // ...........................................................................
  // write field meta and field term data
  // ...........................................................................
  bool write(
    iresearch::directory& dir,
    iresearch::format& codec,
    const iresearch::string_ref& segment_name,
    compound_field_iterator& field_itr,
    const field_meta_map_t& field_meta_map,
    const iresearch::flags& fields_features,
    size_t docs_count,
    field_id_map_t& field_id_map // should have enough space
  ) {
      if (field_meta_map.size() > iresearch::integer_traits<uint32_t>::const_max) {
      return false; // number of fields exceeds fmw->begin(...) type
    }

    auto fields_count = static_cast<uint32_t>(field_meta_map.size()); // max boundary ensured by check above

    iresearch::flush_state flush_state;
    flush_state.dir = &dir;
    flush_state.doc_count = docs_count;
    flush_state.fields_count = fields_count;
    flush_state.features = &fields_features;
    flush_state.name = segment_name;
    flush_state.ver = IRESEARCH_VERSION;

    auto fmw = codec.get_field_meta_writer();
    fmw->prepare(flush_state);

    auto fw = codec.get_field_writer(true);
    fw->prepare(flush_state);

    size_t mapped_field_id = 0;
    auto remapper = 
      [&field_id_map, &mapped_field_id] (
        const iresearch::sub_reader& /* segment */,
        size_t field_id_base, 
        const iresearch::field_meta& field) {
      field_id_map[field_id_base + field.id] = mapped_field_id;
      return true;
    };

    for (; field_itr.next(); ++mapped_field_id) {
      auto& field_meta = field_itr.meta();
      auto& field_features = field_meta.features;

      field_itr.visit(remapper); // remap field id's
      fmw->write(iresearch::field_id(mapped_field_id), field_meta.name, field_features);
      fw->write(iresearch::field_id(mapped_field_id), field_features, *(field_itr.terms()));
    }

    fw->end();
    fmw->end();

    fw.reset();
    fmw.reset();

    return true;
  }

  // ...........................................................................
  // in order to copy documents from one segment to another, 
  // we treat document as the stored field, 
  // then we just put it into oridnary stored_fields_writer
  // ...........................................................................
  struct document final : iresearch::serializer {
    bool write(iresearch::data_output& out) const {
      // write header - remap field id's
      auto header_remapper = [this, &out] (iresearch::field_id id, bool next) {
        const auto mapped_id = static_cast<iresearch::field_id>((*field_id_map)[field_id_base + id]);
        out.write_vint(iresearch::shift_pack_32(mapped_id, next));
      };
      iresearch::stored::visit_header(*in, header_remapper);

      // write body
      for (size_t read = in->read_bytes(buf.data(), buf.size()); read;
           read = in->read_bytes(buf.data(), buf.size())) {
        out.write_bytes(buf.data(), read);
      }

      return false; // break the loop
    }

    iresearch::data_input* in;
    const field_id_map_t* field_id_map;
    size_t field_id_base{};
    mutable std::array<iresearch::byte_type, 1024> buf;
  };

  // ...........................................................................
  // write stored field data
  // ReaderIterator - std::pair<const iresearch::sub_reader*, doc_id_map_t>
  // ...........................................................................
  template <typename ReaderIterator>
  void write(
    iresearch::directory& dir,
    iresearch::format& codec,
    const iresearch::string_ref& segment_name,
    const field_id_map_t& field_id_map,
    ReaderIterator begin,
    ReaderIterator end
  ) {
    iresearch::stored_fields_writer::ptr sfw = codec.get_stored_fields_writer();

    document serializer;
    serializer.field_id_map = &field_id_map;
    iresearch::stored_fields_reader::visitor_f copier = [&serializer, &sfw](iresearch::data_input& in) {
      serializer.in = &in; 
      return sfw->write(serializer);
    };

    sfw->prepare(dir, segment_name);

    // copy stored fields for all documents (doc_id must be sequential 0 based)
    for (auto itr = begin; itr != end; ++itr) {
      auto& reader = *(itr->first);
      auto& doc_id_map = itr->second;

      // the implementation generates doc_ids sequentially
      for (size_t i = 0, count = doc_id_map.size(); i < count; ++i) {
        if (doc_id_map[i] != MASKED_DOC_ID) {
          auto doc = static_cast<iresearch::doc_id_t>(i); // can't have more docs then highest doc_id (offset == doc_id as per compute_doc_ids(...))
          reader.document(doc, copier); // in order to get access to the beginning of the document, we use low-level visitor API here 
          sfw->end(nullptr); // do not write header here
        }
      }

      serializer.field_id_base += reader.fields().size();
    }

    sfw->finish();
  }
}

NS_ROOT

merge_writer::merge_writer(
  directory& dir,
  format::ptr codec,
  const string_ref& name
) NOEXCEPT: codec_(codec), dir_(dir), name_(name) {
}

void merge_writer::add(const sub_reader& reader) {
  readers_.emplace_back(&reader);
}

bool merge_writer::flush(std::string& filename, segment_meta& meta) {
  // reader with map of old doc_id to new doc_id
  typedef std::pair<const iresearch::sub_reader*, doc_id_map_t> reader_t;

  assert(codec_);

  auto& codec = *(codec_.get());
  size_t fragmented_fields_count = 0; // total number of undergragmented fields (from all segment readers)
  std::unordered_map<iresearch::string_ref, const iresearch::field_meta*> field_metas;
  compound_field_iterator fields_itr;
  iresearch::flags fields_features;
  doc_id_t next_id = type_limits<type_t::doc_id_t>::min(); // next valid doc_id
  std::deque<reader_t> readers; // a container that does not copy when growing (iterators store pointers)

  // collect field meta and field term data
  for (auto& reader: readers_) {
    readers.emplace_back(reader, doc_id_map_t());

    if (!compute_filed_meta(field_metas, fields_features, *reader)) {
      return false;
    }

    auto& doc_id_map = readers.back().second;

    next_id = compute_doc_ids(doc_id_map, *reader, next_id);
    fields_itr.add(*reader, doc_id_map);
    fragmented_fields_count += reader->fields().size();
  }

  auto docs_count = next_id - type_limits<type_t::doc_id_t>::min(); // total number of doc_ids

  //...........................................................................
  // write merged segment data
  //...........................................................................

  tracking_directory track_dir(dir_); // track writer created files

  // write field meta and field term data
  field_id_map_t field_id_map(
    fragmented_fields_count,
    iresearch::type_limits<iresearch::type_t::field_id_t>::invalid()
  );

  if (!write(track_dir, codec, name_, fields_itr, field_metas, fields_features, docs_count, field_id_map)) {
    return false; // flush failure
  }

  // write stored field data
  write(track_dir, codec, name_, field_id_map, readers.begin(), readers.end());

  // ...........................................................................
  // write segment meta
  // ...........................................................................
  meta.codec = codec_;
  meta.docs_count = docs_count;
  meta.name = name_;
  track_dir.swap_tracked(meta.files);

  iresearch::segment_meta_writer::ptr writer = codec.get_segment_meta_writer();

  writer->write(dir_, meta);
  filename = writer->filename(meta);

  // ...........................................................................
  // finish/cleanup
  // ...........................................................................
  readers_.clear();

  return true;
}

NS_END