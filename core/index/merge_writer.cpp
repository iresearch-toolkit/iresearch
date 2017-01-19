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

#include <deque>
#include <unordered_map>

#include "merge_writer.hpp"
#include "index/doc_header.hpp"
#include "index/field_meta.hpp"
#include "index/index_meta.hpp"
#include "index/segment_reader.hpp"
#include "utils/directory_utils.hpp"
#include "utils/log.hpp"
#include "utils/type_limits.hpp"
#include "utils/version_utils.hpp"
#include "store/store_utils.hpp"

#include <array>

NS_LOCAL

// mapping of old doc_id to new doc_id (reader doc_ids are sequential 0 based)
// masked doc_ids have value of MASKED_DOC_ID
typedef std::vector<iresearch::doc_id_t> doc_id_map_t;

// mapping of old field_id to new field_id
typedef std::vector<iresearch::field_id> id_map_t;

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
  virtual const iresearch::attributes& attributes() const NOEXCEPT override;
  virtual bool next() override;
  virtual iresearch::doc_id_t seek(iresearch::doc_id_t target) override;
  virtual iresearch::doc_id_t value() const override;
};

template<typename Iterator>
class compound_iterator {
 public:
  typedef typename std::remove_reference<decltype(Iterator()->value())>::type value_type;

  size_t size() const { return iterators_.size(); }

  void add(const iresearch::sub_reader& reader, 
           const doc_id_map_t& doc_id_map) {
    iterator_mask_.emplace_back(iterators_.size());
    iterators_.emplace_back(reader.columns(), reader, doc_id_map);
  }
  
  // visit matched iterators
  template<typename Visitor>
  bool visit(const Visitor& visitor) const {
    for (auto id : iterator_mask_) {
      auto& it = iterators_[id];
      if (!visitor(*it.reader, *it.doc_id_map, it.it->value())) {
        return false;
      }
    }
    return true;
  }

  const value_type& operator*() const {
    static value_type empty;
    return current_value_ ? *current_value_ : empty;
  }

  bool next() {
    // advance all used iterators
    for (auto id : iterator_mask_) {
      auto& it = iterators_[id].it;
      if (it && !it->next()) {
        it = nullptr;
      }
    }

    iterator_mask_.clear(); // reset for next pass

    for (size_t i = 0, size = iterators_.size(); i < size; ++i) {
      auto& it = iterators_[i].it;
      if (!it) {
        continue; // empty iterator
      }

      const auto& value = it->value();
      const iresearch::string_ref key = value.name;

      if (!iterator_mask_.empty() && current_key_ < key) {
        continue; // empty field or value too large
      }

      // found a smaller field
      if (iterator_mask_.empty() || key < current_key_) {
        iterator_mask_.clear();
        current_key_ = key;
        current_value_ = &value;
      }

      assert(value == *current_value_); // validated by caller
      iterator_mask_.push_back(i);
    }

    if (!iterator_mask_.empty()) {
      return true;
    }

    current_key_ = iresearch::string_ref::nil;

    return false;
  }

 private:
  struct iterator_t {
    iterator_t(
        Iterator&& it,
        const iresearch::sub_reader& reader,
        const doc_id_map_t& doc_id_map)
      : it(std::move(it)),
        reader(&reader), 
        doc_id_map(&doc_id_map) {
      }

    iterator_t(iterator_t&& other) NOEXCEPT
      : it(std::move(other.it)),
        reader(std::move(other.reader)),
        doc_id_map(std::move(other.doc_id_map)) {
    }

    Iterator it;
    const iresearch::sub_reader* reader;
    const doc_id_map_t* doc_id_map;
  };

  const value_type* current_value_{};
  iresearch::string_ref current_key_;
  std::vector<size_t> iterator_mask_; // valid iterators for current step 
  std::vector<iterator_t> iterators_; // all segment iterators
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
      if (!visitor(*itr.reader, *itr.doc_id_map, *entry.meta)) {
        return false;
      }
    }
    return true;
  }

 private:
  struct field_iterator_t {
    field_iterator_t(
        iresearch::field_iterator::ptr&& v_itr,
        const iresearch::sub_reader& v_reader,
        const doc_id_map_t& v_doc_id_map)
      : itr(std::move(v_itr)),
        reader(&v_reader), 
        doc_id_map(&v_doc_id_map) {
      }
    field_iterator_t(field_iterator_t&& other) NOEXCEPT
      : itr(std::move(other.itr)),
        reader(std::move(other.reader)),
        doc_id_map(std::move(other.doc_id_map)) {
    }

    iresearch::field_iterator::ptr itr;
    const iresearch::sub_reader* reader;
    const doc_id_map_t* doc_id_map;
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

typedef compound_iterator<iresearch::column_iterator::ptr> compound_column_iterator_t;

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
  virtual const iresearch::attributes& attributes() const NOEXCEPT override;
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

const iresearch::attributes& compound_doc_iterator::attributes() const NOEXCEPT {
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
    const doc_id_map_t& doc_id_map) {   
  field_iterator_mask_.emplace_back(term_iterator_t{
    field_iterators_.size(),
    nullptr,
    nullptr
  }); // mark as used to trigger next()

  field_iterators_.emplace_back(
    reader.fields(),
    reader, 
    doc_id_map
  );
}

const iresearch::field_meta& compound_field_iterator::meta() const {
  static const iresearch::field_meta empty;
  return current_meta_ ? *current_meta_ : empty;
}

bool compound_field_iterator::next() {
  // advance all used iterators
  for (auto& entry : field_iterator_mask_) {
    auto& it = field_iterators_[entry.itr_id];
    if (it.itr && !it.itr->next()) {
      it.itr = nullptr;
    }
  }

  field_iterator_mask_.clear(); // reset for next pass

  for (size_t i = 0, count = field_iterators_.size(); i < count; ++i) {
    auto& field_itr = field_iterators_[i];

    if (!field_itr.itr) {
      continue; // empty iterator
    }

    const auto& field_meta = field_itr.itr->value().meta();
    const auto* field_terms = field_itr.reader->field(field_meta.name);
    const iresearch::string_ref field_id = field_meta.name;

    if (!field_terms  || 
        (!field_iterator_mask_.empty() && field_id > current_field_)) {
      continue; // empty field or value too large
    }

    // found a smaller field
    if (field_iterator_mask_.empty() || field_id < current_field_) {
      field_iterator_mask_.clear();
      current_field_ = field_id;
      current_meta_ = &field_meta;
    }

    assert(field_meta.features.is_subset_of(current_meta_->features)); // validated by caller
    field_iterator_mask_.emplace_back(term_iterator_t{i, &field_meta, field_terms});
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
}  

void compound_term_iterator::add(
    const iresearch::term_reader& reader,
    const doc_id_map_t& doc_id_map) {
  term_iterator_mask_.emplace_back(term_iterators_.size()); // mark as used to trigger next()
  term_iterators_.emplace_back(std::move(reader.iterator()), &doc_id_map);
}

const iresearch::attributes& compound_term_iterator::attributes() const NOEXCEPT {
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
  doc_id_map.resize(reader.docs_count() + iresearch::type_limits<iresearch::type_t::doc_id_t>::min(), MASKED_DOC_ID);

  for (auto docs_itr = reader.docs_iterator(); docs_itr->next(); ++next_id) {
    auto src_doc_id = docs_itr->value();

    assert(src_doc_id >= iresearch::type_limits<iresearch::type_t::doc_id_t>::min());
    assert(src_doc_id < reader.docs_count() + iresearch::type_limits<iresearch::type_t::doc_id_t>::min());
    doc_id_map[src_doc_id] = next_id; // set to next valid doc_id
  }

  return next_id;
}

// ...........................................................................
// compute fields_type and fields_count
// ...........................................................................
bool compute_field_meta(
  field_meta_map_t& field_meta_map,
  iresearch::flags& fields_features,
  const iresearch::sub_reader& reader
) {
  for (auto it = reader.fields(); it->next();) {
    const auto& field_meta = it->value().meta();
    auto field_meta_map_itr = field_meta_map.emplace(field_meta.name, &field_meta);
    auto* tracked_field_meta = field_meta_map_itr.first->second;

    // validate field_meta equivalence
    if (!field_meta_map_itr.second &&
        !field_meta.features.is_subset_of(tracked_field_meta->features)) {
      return false; // field_meta is not equal, so cannot merge segments
    }

    fields_features |= field_meta.features;
  }

  return true;
}

// ...........................................................................
// Helper class responsible for writing a data from different sources 
// into single columnstore
//
// Using by 
//  'write' function to merge field norms values from different segments
//  'write_columns' function to merge columns values from different segmnets
// ...........................................................................
class columnstore {
 public:
  columnstore(
      iresearch::directory& dir,
      iresearch::format& codec,
      const iresearch::string_ref& segment_name) {
    auto writer = codec.get_columnstore_writer();
    if (!writer->prepare(dir, segment_name)) {
      return; // flush failure
    }

    writer_ = std::move(writer);
  }

  ~columnstore() {
    try {
      writer_->flush();
    } catch (...) {
      // NOOP
    }
  }

  // inserts live values from the specified 'column' and 'reader' into column
  bool insert(
      const iresearch::sub_reader& reader, 
      iresearch::field_id column, 
      const doc_id_map_t& doc_id_map) {
    return reader.visit(
        column, 
        [this, &doc_id_map](iresearch::doc_id_t doc, iresearch::data_input& in) {
          const auto mapped_doc = doc_id_map[doc];
          if (MASKED_DOC_ID == mapped_doc) {
            // skip deleted document
            return true;
          }

          empty_ = false;

          auto& out = column_.second(mapped_doc);
          for (size_t read = in.read_bytes(buf_, sizeof buf_); read;
               read = in.read_bytes(buf_, sizeof buf_)) {
            out.write_bytes(buf_, read);
          }
          return true;
        });
  }

  void reset() {
    if (!empty_) {
      column_ = writer_->push_column();
      empty_ = true;
    }
  }

  // returs 
  //   'true' if object has been initialized sucessfully, 
  //   'false' otherwise
  operator bool() const { return static_cast<bool>(writer_); }

  // returns 'true' if no data has been written to columnstore
  bool empty() const { return empty_; }

  // returns current column identifier
  iresearch::field_id id() const { return column_.first; }

 private:
  iresearch::byte_type buf_[1024]; // temporary buffer for copying
  iresearch::columnstore_writer::ptr writer_;
  iresearch::columnstore_writer::column_t column_{};
  bool empty_{ false };
}; // columnstore
  
bool write_columns(
    columnstore& cs,
    iresearch::directory& dir,
    iresearch::format& codec,
    const iresearch::string_ref& segment_name,
    compound_column_iterator_t& column_itr) {
  assert(cs);
  
  auto visitor = [&cs](
      const iresearch::sub_reader& segment,
      const doc_id_map_t& doc_id_map,
      const iresearch::column_meta& column) {
    return cs.insert(segment, column.id, doc_id_map);
  };
  
  auto cmw = codec.get_column_meta_writer();
  if (!cmw->prepare(dir, segment_name)) {
    // failed to prepare writer
    return false;
  }
  
  while (column_itr.next()) {
    cs.reset();  

    // visit matched columns from merging segments and
    // write all survived values to the new segment 
    column_itr.visit(visitor); 

    if (!cs.empty()) {
      cmw->write((*column_itr).name, cs.id());
    } 
  }
  cmw->flush();

  return true;
}

// ...........................................................................
// write field term data
// ...........................................................................
bool write(
    columnstore& cs,
    iresearch::directory& dir,
    iresearch::format& codec,
    const iresearch::string_ref& segment_name,
    compound_field_iterator& field_itr,
    const field_meta_map_t& field_meta_map,
    const iresearch::flags& fields_features,
    size_t docs_count) {
  REGISTER_TIMER_DETAILED();
  assert(cs);

  iresearch::flush_state flush_state;
  flush_state.dir = &dir;
  flush_state.doc_count = docs_count;
  flush_state.fields_count = field_meta_map.size();
  flush_state.features = &fields_features;
  flush_state.name = segment_name;
  flush_state.ver = IRESEARCH_VERSION;

  auto fw = codec.get_field_writer(true);
  fw->prepare(flush_state);

  auto merge_norms = [&cs] (
      const iresearch::sub_reader& segment,
      const doc_id_map_t& doc_id_map,
      const iresearch::field_meta& field) {
    // merge field norms if present
    if (iresearch::type_limits<iresearch::type_t::field_id_t>::valid(field.norm)) {
      cs.insert(segment, field.norm, doc_id_map);
    }

    return true;
  };

  while(field_itr.next()) {
    cs.reset();

    auto& field_meta = field_itr.meta();
    auto& field_features = field_meta.features;

    // remap merge norms
    field_itr.visit(merge_norms); 
   
    // write field terms
    fw->write(
      field_meta.name, 
      cs.empty() ? iresearch::type_limits<iresearch::type_t::field_id_t>::invalid() : cs.id(),
      field_features, 
      *(field_itr.terms())
    );
  }

  fw->end();

  fw.reset();

  return true;
}

NS_END // LOCAL

NS_ROOT

merge_writer::merge_writer(
    directory& dir,
    format::ptr codec,
    const string_ref& name) NOEXCEPT
  : codec_(codec), dir_(dir), name_(name) {
}

void merge_writer::add(const sub_reader& reader) {
  readers_.emplace_back(&reader);
}

bool merge_writer::flush(std::string& filename, segment_meta& meta) {
  REGISTER_TIMER_DETAILED();
  // reader with map of old doc_id to new doc_id
  typedef std::pair<const iresearch::sub_reader*, doc_id_map_t> reader_t;

  assert(codec_);

  auto& codec = *(codec_.get());
  std::unordered_map<iresearch::string_ref, const iresearch::field_meta*> field_metas;
  compound_field_iterator fields_itr;
  compound_column_iterator_t columns_itr;
  iresearch::flags fields_features;
  doc_id_t next_id = type_limits<type_t::doc_id_t>::min(); // next valid doc_id
  std::deque<reader_t> readers; // a container that does not copy when growing (iterators store pointers)

  // collect field meta and field term data
  for (auto& reader: readers_) {
    readers.emplace_back(reader, doc_id_map_t());

    if (!compute_field_meta(field_metas, fields_features, *reader)) {
      return false;
    }

    auto& doc_id_map = readers.back().second;

    next_id = compute_doc_ids(doc_id_map, *reader, next_id);
    fields_itr.add(*reader, doc_id_map);
    columns_itr.add(*reader, doc_id_map);
  }

  auto docs_count = next_id - type_limits<type_t::doc_id_t>::min(); // total number of doc_ids

  //...........................................................................
  // write merged segment data
  //...........................................................................

  tracking_directory track_dir(dir_); // track writer created files

  columnstore cs(track_dir, codec, name_);

  if (!cs) {
    return false; // flush failure
  }

  // write columns
  if (!write_columns(cs, track_dir, codec, name_, columns_itr)) {
    return false; // flush failure
  }

  // write field meta and field term data
  if (!write(cs, track_dir, codec, name_, fields_itr, field_metas, fields_features, docs_count)) {
    return false; // flush failure
  }

  // ...........................................................................
  // write segment meta
  // ...........................................................................
  meta.codec = codec_;
  meta.docs_count = docs_count;
  meta.name = name_;

  if (!track_dir.swap_tracked(meta.files)) {
    IR_ERROR() << "Failed to swap list of tracked files in: " << __FUNCTION__ << ":" << __LINE__;
    return false;
  }

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