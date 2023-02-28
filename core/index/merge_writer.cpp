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

#include "merge_writer.hpp"

#if defined(IRESEARCH_DEBUG) && !defined(__clang__)
#include <ranges>
#endif

#include "analysis/token_attributes.hpp"
#include "index/comparer.hpp"
#include "index/field_meta.hpp"
#include "index/heap_iterator.hpp"
#include "index/index_meta.hpp"
#include "index/norm.hpp"
#include "index/segment_reader.hpp"
#include "store/store_utils.hpp"
#include "utils/directory_utils.hpp"
#include "utils/log.hpp"
#include "utils/memory.hpp"
#include "utils/timer_utils.hpp"
#include "utils/type_limits.hpp"
#include "utils/version_utils.hpp"

#include <absl/container/flat_hash_map.h>

namespace irs {
namespace {

bool IsSubsetOf(const feature_map_t& lhs, const feature_map_t& rhs) noexcept {
  for (auto& entry : lhs) {
    if (!rhs.count(entry.first)) {
      return false;
    }
  }
  return true;
}

void AccumulateFeatures(feature_set_t& accum, const feature_map_t& features) {
  for (auto& entry : features) {
    accum.emplace(entry.first);
  }
}

// mapping of old doc_id to new doc_id (reader doc_ids are sequential 0 based)
// masked doc_ids have value of MASKED_DOC_ID
using doc_id_map_t = std::vector<doc_id_t>;

// document mapping function
using doc_map_f = std::function<doc_id_t(doc_id_t)>;

using field_meta_map_t =
  absl::flat_hash_map<std::string_view, const field_meta*>;

class NoopDirectory : public directory {
 public:
  static NoopDirectory& instance() {
    static NoopDirectory INSTANCE;
    return INSTANCE;
  }

  directory_attributes& attributes() noexcept final { return attrs_; }

  index_output::ptr create(std::string_view) noexcept final { return nullptr; }

  bool exists(bool&, std::string_view) const noexcept final { return false; }

  bool length(uint64_t&, std::string_view) const noexcept final {
    return false;
  }

  index_lock::ptr make_lock(std::string_view) noexcept final { return nullptr; }

  bool mtime(std::time_t&, std::string_view) const noexcept final {
    return false;
  }

  index_input::ptr open(std::string_view, IOAdvice) const noexcept final {
    return nullptr;
  }

  bool remove(std::string_view) noexcept final { return false; }

  bool rename(std::string_view, std::string_view) noexcept final {
    return false;
  }

  bool sync(std::span<const std::string_view>) noexcept final { return false; }

  bool visit(const directory::visitor_f&) const final { return false; }

 private:
  NoopDirectory() = default;

  directory_attributes attrs_{0, nullptr};
};

class ProgressTracker {
 public:
  explicit ProgressTracker(const MergeWriter::FlushProgress& progress,
                           size_t count) noexcept
    : progress_(&progress), count_(count) {
    IRS_ASSERT(progress);
  }

  bool operator()() {
    if (hits_++ >= count_) {
      hits_ = 0;
      valid_ = (*progress_)();
    }

    return valid_;
  }

  explicit operator bool() const noexcept { return valid_; }

  void reset() noexcept {
    hits_ = 0;
    valid_ = true;
  }

 private:
  const MergeWriter::FlushProgress* progress_;
  const size_t count_;  // call progress callback each `count_` hits
  size_t hits_{0};      // current number of hits
  bool valid_{true};
};

class RemappingDocIterator : public doc_iterator {
 public:
  RemappingDocIterator(doc_iterator::ptr&& it, const doc_map_f& mapper) noexcept
    : it_{std::move(it)}, mapper_{&mapper}, src_{irs::get<document>(*it_)} {
    IRS_ASSERT(it_ && src_);
  }

  bool next() final;

  doc_id_t value() const noexcept final { return doc_.value; }

  doc_id_t seek(doc_id_t target) final {
    irs::seek(*this, target);
    return value();
  }

  attribute* get_mutable(irs::type_info::type_id type) noexcept final {
    return irs::type<irs::document>::id() == type ? &doc_
                                                  : it_->get_mutable(type);
  }

 private:
  doc_iterator::ptr it_;
  const doc_map_f* mapper_;
  const irs::document* src_;
  irs::document doc_;
};

bool RemappingDocIterator::next() {
  while (it_->next()) {
    doc_.value = (*mapper_)(src_->value);

    if (doc_limits::eof(doc_.value)) {
      continue;  // masked doc_id
    }

    return true;
  }

  return false;
}

// Iterator over doc_ids for a term over all readers
class CompoundDocIterator : public doc_iterator {
 public:
  typedef std::pair<doc_iterator::ptr, std::reference_wrapper<const doc_map_f>>
    doc_iterator_t;
  typedef std::vector<doc_iterator_t> iterators_t;

  static constexpr const size_t kProgressStepDocs = size_t(1) << 14;

  explicit CompoundDocIterator(
    const MergeWriter::FlushProgress& progress) noexcept
    : progress_(progress, kProgressStepDocs) {}

  template<typename Func>
  bool reset(Func&& func) {
    if (!func(iterators_)) {
      return false;
    }

    doc_.value = doc_limits::invalid();
    current_itr_ = 0;

    return true;
  }

  size_t size() const noexcept { return iterators_.size(); }

  bool aborted() const noexcept { return !static_cast<bool>(progress_); }

  attribute* get_mutable(irs::type_info::type_id type) noexcept final {
    if (irs::type<irs::document>::id() == type) {
      return &doc_;
    }

    return irs::type<attribute_provider_change>::id() == type
             ? &attribute_change_
             : nullptr;
  }

  bool next() final;

  doc_id_t seek(doc_id_t target) final {
    irs::seek(*this, target);
    return value();
  }

  doc_id_t value() const noexcept final { return doc_.value; }

 private:
  friend class SortingCompoundDocIterator;

  attribute_provider_change attribute_change_;
  std::vector<doc_iterator_t> iterators_;
  size_t current_itr_{0};
  ProgressTracker progress_;
  document doc_;
};

bool CompoundDocIterator::next() {
  progress_();

  if (aborted()) {
    doc_.value = doc_limits::eof();
    iterators_.clear();
    return false;
  }

  for (bool notify = !doc_limits::valid(doc_.value);
       current_itr_ < iterators_.size(); notify = true, ++current_itr_) {
    auto& itr_entry = iterators_[current_itr_];
    auto& itr = itr_entry.first;
    auto& id_map = itr_entry.second.get();

    if (!itr) {
      continue;
    }

    if (notify) {
      attribute_change_(*itr);
    }

    while (itr->next()) {
      doc_.value = id_map(itr->value());

      if (doc_limits::eof(doc_.value)) {
        continue;  // masked doc_id
      }

      return true;
    }

    itr.reset();
  }

  doc_.value = doc_limits::eof();

  return false;
}

// Iterator over sorted doc_ids for a term over all readers
class SortingCompoundDocIterator : public doc_iterator {
 public:
  explicit SortingCompoundDocIterator(CompoundDocIterator& doc_it) noexcept
    : doc_it_{&doc_it}, heap_it_{min_heap_context{doc_it.iterators_}} {}

  template<typename Func>
  bool reset(Func&& func) {
    if (!doc_it_->reset(std::forward<Func>(func))) {
      return false;
    }

    heap_it_.reset(doc_it_->iterators_.size());
    lead_ = nullptr;

    return true;
  }

  attribute* get_mutable(irs::type_info::type_id type) noexcept final {
    return doc_it_->get_mutable(type);
  }

  bool next() final;

  doc_id_t seek(doc_id_t target) final {
    irs::seek(*this, target);
    return value();
  }

  doc_id_t value() const noexcept final { return doc_it_->value(); }

 private:
  class min_heap_context {
   public:
    explicit min_heap_context(CompoundDocIterator::iterators_t& itrs) noexcept
      : itrs_{&itrs} {}

    // advance
    bool operator()(const size_t i) const {
      IRS_ASSERT(i < itrs_->size());
      auto& doc_it = (*itrs_)[i];
      const auto& map = doc_it.second.get();
      while (doc_it.first->next()) {
        if (!doc_limits::eof(map(doc_it.first->value()))) {
          return true;
        }
      }
      return false;
    }

    // compare
    bool operator()(const size_t lhs, const size_t rhs) const {
      return remap(lhs) > remap(rhs);
    }

   private:
    doc_id_t remap(const size_t i) const {
      IRS_ASSERT(i < itrs_->size());
      auto& doc_it = (*itrs_)[i];
      return doc_it.second.get()(doc_it.first->value());
    }

    CompoundDocIterator::iterators_t* itrs_;
  };

  CompoundDocIterator* doc_it_;
  ExternalHeapIterator<min_heap_context> heap_it_;
  CompoundDocIterator::doc_iterator_t* lead_{};
};

bool SortingCompoundDocIterator::next() {
  auto& iterators = doc_it_->iterators_;
  auto& current_id = doc_it_->doc_;

  doc_it_->progress_();

  if (doc_it_->aborted()) {
    current_id.value = doc_limits::eof();
    iterators.clear();
    return false;
  }

  while (heap_it_.next()) {
    auto& new_lead = iterators[heap_it_.value()];
    auto& it = new_lead.first;
    auto& doc_map = new_lead.second.get();

    if (&new_lead != lead_) {
      // update attributes
      doc_it_->attribute_change_(*it);
      lead_ = &new_lead;
    }

    current_id.value = doc_map(it->value());

    if (doc_limits::eof(current_id.value)) {
      continue;
    }

    return true;
  }

  current_id.value = doc_limits::eof();

  return false;
}

class DocIteratorContainer {
 public:
  explicit DocIteratorContainer(size_t size) { itrs_.reserve(size); }

  auto begin() { return std::begin(itrs_); }
  auto end() { return std::end(itrs_); }

  template<typename Func>
  bool reset(Func&& func) {
    return func(itrs_);
  }

 private:
  std::vector<RemappingDocIterator> itrs_;
};

class CompoundColumnIterator final {
 public:
  explicit CompoundColumnIterator(size_t size) {
    iterators_.reserve(size);
    iterator_mask_.reserve(size);
  }

  size_t size() const { return iterators_.size(); }

  void add(const SubReader& reader, const doc_map_f& doc_map) {
    auto it = reader.columns();
    IRS_ASSERT(it);

    if (IRS_LIKELY(it)) {
      iterator_mask_.emplace_back(iterators_.size());
      iterators_.emplace_back(std::move(it), reader, doc_map);
    }
  }

  // visit matched iterators
  template<typename Visitor>
  bool visit(const Visitor& visitor) const {
    for (auto id : iterator_mask_) {
      auto& it = iterators_[id];
      if (!visitor(*it.reader, *it.doc_map, it.it->value())) {
        return false;
      }
    }
    return true;
  }

  const column_reader& value() const {
    if (IRS_LIKELY(current_value_)) {
      return *current_value_;
    }

    return column_iterator::empty()->value();
  }

  bool next() {
    // advance all used iterators
    for (auto id : iterator_mask_) {
      auto& it = iterators_[id].it;

      if (it) {
        // Skip annonymous columns
        bool exhausted;
        do {
          exhausted = !it->next();
        } while (!exhausted && IsNull(it->value().name()));

        if (exhausted) {
          it = nullptr;
        }
      }
    }

    iterator_mask_.clear();  // reset for next pass

    for (size_t i = 0, size = iterators_.size(); i < size; ++i) {
      auto& it = iterators_[i].it;
      if (!it) {
        continue;  // empty iterator
      }

      const auto& value = it->value();
      const std::string_view key = value.name();
      IRS_ASSERT(!IsNull(key));

      if (!iterator_mask_.empty() && current_key_ < key) {
        continue;  // empty field or value too large
      }

      // found a smaller field
      if (iterator_mask_.empty() || key < current_key_) {
        iterator_mask_.clear();
        current_key_ = key;
        current_value_ = &value;
      }

      IRS_ASSERT(value.name() ==
                 current_value_->name());  // validated by caller
      iterator_mask_.push_back(i);
    }

    if (!iterator_mask_.empty()) {
      return true;
    }

    current_key_ = {};

    return false;
  }

 private:
  struct Iterator : util::noncopyable {
    Iterator(column_iterator::ptr&& it, const SubReader& reader,
             const doc_map_f& doc_map)
      : it(std::move(it)), reader(&reader), doc_map(&doc_map) {}

    Iterator(Iterator&&) = default;
    Iterator& operator=(Iterator&&) = delete;

    column_iterator::ptr it;
    const SubReader* reader;
    const doc_map_f* doc_map;
  };

  static_assert(std::is_nothrow_move_constructible_v<Iterator>);

  const column_reader* current_value_{};
  std::string_view current_key_;
  std::vector<size_t> iterator_mask_;  // valid iterators for current step
  std::vector<Iterator> iterators_;    // all segment iterators
};

// Iterator over documents for a term over all readers
class CompoundTermIterator : public term_iterator {
 public:
  static constexpr const size_t kProgressStepTerms = size_t(1) << 7;

  explicit CompoundTermIterator(const MergeWriter::FlushProgress& progress,
                                const Comparer* comparator)
    : doc_itr_{progress},
      has_comparer_{nullptr != comparator},
      progress_{progress, kProgressStepTerms} {}

  bool aborted() const {
    return !static_cast<bool>(progress_) || doc_itr_.aborted();
  }

  void reset(const field_meta& meta) noexcept {
    meta_ = &meta;
    term_iterator_mask_.clear();
    term_iterators_.clear();
    current_term_ = {};
  }

  const field_meta& meta() const noexcept { return *meta_; }
  void add(const term_reader& reader, const doc_map_f& doc_map);
  attribute* get_mutable(irs::type_info::type_id) noexcept final {
    // no way to merge attributes for the same term spread over multiple
    // iterators would require API change for attributes
    IRS_ASSERT(false);
    return nullptr;
  }
  bool next() final;
  doc_iterator::ptr postings(IndexFeatures features) const final;
  void read() final {
    for (auto& itr_id : term_iterator_mask_) {
      if (term_iterators_[itr_id].first) {
        term_iterators_[itr_id].first->read();
      }
    }
  }
  bytes_view value() const final { return current_term_; }

 private:
  struct TermIterator {
    seek_term_iterator::ptr first;
    const doc_map_f* second;

    TermIterator(seek_term_iterator::ptr&& term_itr, const doc_map_f* doc_map)
      : first(std::move(term_itr)), second(doc_map) {}

    TermIterator(TermIterator&& other) noexcept
      : first(std::move(other.first)), second(std::move(other.second)) {}
  };

  CompoundTermIterator(const CompoundTermIterator&) =
    delete;  // due to references
  CompoundTermIterator& operator=(const CompoundTermIterator&) =
    delete;  // due to references

  bytes_view current_term_;
  const field_meta* meta_{};
  std::vector<size_t> term_iterator_mask_;  // valid iterators for current term
  std::vector<TermIterator> term_iterators_;  // all term iterators
  mutable CompoundDocIterator doc_itr_;
  mutable SortingCompoundDocIterator sorting_doc_itr_{doc_itr_};
  bool has_comparer_;
  ProgressTracker progress_;
};

void CompoundTermIterator::add(const term_reader& reader,
                               const doc_map_f& doc_id_map) {
  auto it = reader.iterator(SeekMode::NORMAL);
  IRS_ASSERT(it);

  if (IRS_LIKELY(it)) {
    // mark as used to trigger next()
    term_iterator_mask_.emplace_back(term_iterators_.size());
    term_iterators_.emplace_back(std::move(it), &doc_id_map);
  }
}

bool CompoundTermIterator::next() {
  progress_();

  if (aborted()) {
    term_iterators_.clear();
    term_iterator_mask_.clear();
    return false;
  }

  // advance all used iterators
  for (auto& itr_id : term_iterator_mask_) {
    auto& it = term_iterators_[itr_id].first;
    if (it && !it->next()) {
      it.reset();
    }
  }

  term_iterator_mask_.clear();  // reset for next pass

  for (size_t i = 0, count = term_iterators_.size(); i < count; ++i) {
    auto& term_itr = term_iterators_[i];

    if (!term_itr.first || (!term_iterator_mask_.empty() &&
                            term_itr.first->value() > current_term_)) {
      continue;  // empty iterator or value too large
    }

    // found a smaller term
    if (term_iterator_mask_.empty() ||
        term_itr.first->value() < current_term_) {
      term_iterator_mask_.clear();
      current_term_ = term_itr.first->value();
    }

    term_iterator_mask_.emplace_back(i);
  }

  if (!term_iterator_mask_.empty()) {
    return true;
  }

  current_term_ = {};

  return false;
}

doc_iterator::ptr CompoundTermIterator::postings(
  IndexFeatures /*features*/) const {
  auto add_iterators = [this](CompoundDocIterator::iterators_t& itrs) {
    itrs.clear();
    itrs.reserve(term_iterator_mask_.size());

    for (auto& itr_id : term_iterator_mask_) {
      auto& term_itr = term_iterators_[itr_id];

      auto it = term_itr.first->postings(meta().index_features);
      IRS_ASSERT(it);

      if (IRS_LIKELY(it)) {
        itrs.emplace_back(std::move(it), *term_itr.second);
      }
    }

    return true;
  };

  if (has_comparer_) {
    sorting_doc_itr_.reset(add_iterators);
    // TODO(MBkkt) Why?
    if (doc_itr_.size() > 1) {
      return memory::to_managed<doc_iterator>(sorting_doc_itr_);
    }
  } else {
    doc_itr_.reset(add_iterators);
  }
  return memory::to_managed<doc_iterator>(doc_itr_);
}

// Iterator over field_ids over all readers
class CompoundFiledIterator final : public basic_term_reader {
 public:
  static constexpr const size_t kProgressStepFields = size_t(1);

  explicit CompoundFiledIterator(size_t size,
                                 const MergeWriter::FlushProgress& progress,
                                 const Comparer* comparator = nullptr)
    : term_itr_(progress, comparator),
      progress_(progress, kProgressStepFields) {
    field_iterators_.reserve(size);
    field_iterator_mask_.reserve(size);
  }

  void add(const SubReader& reader, const doc_map_f& doc_id_map);
  bool next();
  size_t size() const noexcept { return field_iterators_.size(); }

  // visit matched iterators
  template<typename Visitor>
  bool visit(const Visitor& visitor) const {
    for (auto& entry : field_iterator_mask_) {
      auto& itr = field_iterators_[entry.itr_id];
      if (!visitor(*itr.reader, *itr.doc_map, *entry.meta)) {
        return false;
      }
    }
    return true;
  }

  const field_meta& meta() const noexcept final {
    IRS_ASSERT(current_meta_);
    return *current_meta_;
  }

  bytes_view(min)() const noexcept final { return min_; }

  bytes_view(max)() const noexcept final { return max_; }

  attribute* get_mutable(irs::type_info::type_id) noexcept final {
    return nullptr;
  }

  term_iterator::ptr iterator() const final;

  bool aborted() const {
    return !static_cast<bool>(progress_) || term_itr_.aborted();
  }

 private:
  struct FieldIterator : util::noncopyable {
    FieldIterator(field_iterator::ptr&& itr, const SubReader& reader,
                  const doc_map_f& doc_map)
      : itr(std::move(itr)), reader(&reader), doc_map(&doc_map) {}

    FieldIterator(FieldIterator&&) = default;
    FieldIterator& operator=(FieldIterator&&) = delete;

    field_iterator::ptr itr;
    const SubReader* reader;
    const doc_map_f* doc_map;
  };

  static_assert(std::is_nothrow_move_constructible_v<FieldIterator>);

  struct TermIterator {
    size_t itr_id;
    const field_meta* meta;
    const term_reader* reader;
  };

  std::string_view current_field_;
  const field_meta* current_meta_{&field_meta::kEmpty};
  bytes_view min_{};
  bytes_view max_{};
  // valid iterators for current field
  std::vector<TermIterator> field_iterator_mask_;
  std::vector<FieldIterator> field_iterators_;  // all segment iterators
  mutable CompoundTermIterator term_itr_;
  ProgressTracker progress_;
};

void CompoundFiledIterator::add(const SubReader& reader,
                                const doc_map_f& doc_id_map) {
  auto it = reader.fields();
  IRS_ASSERT(it);

  if (IRS_LIKELY(it)) {
    field_iterator_mask_.emplace_back(
      TermIterator{field_iterators_.size(), nullptr,
                   nullptr});  // mark as used to trigger next()
    field_iterators_.emplace_back(std::move(it), reader, doc_id_map);
  }
}

bool CompoundFiledIterator::next() {
  progress_();

  if (aborted()) {
    field_iterators_.clear();
    field_iterator_mask_.clear();
    current_field_ = {};
    max_ = min_ = {};
    return false;
  }

  // advance all used iterators
  for (auto& entry : field_iterator_mask_) {
    auto& it = field_iterators_[entry.itr_id];
    if (it.itr && !it.itr->next()) {
      it.itr = nullptr;
    }
  }

  // reset for next pass
  field_iterator_mask_.clear();
  max_ = min_ = {};

  for (size_t i = 0, count = field_iterators_.size(); i < count; ++i) {
    auto& field_itr = field_iterators_[i];

    if (!field_itr.itr) {
      continue;  // empty iterator
    }

    const auto& field_meta = field_itr.itr->value().meta();
    const auto* field_terms = field_itr.reader->field(field_meta.name);
    const std::string_view field_id = field_meta.name;

    if (!field_terms ||
        (!field_iterator_mask_.empty() && field_id > current_field_)) {
      continue;  // empty field or value too large
    }

    // found a smaller field
    if (field_iterator_mask_.empty() || field_id < current_field_) {
      field_iterator_mask_.clear();
      current_field_ = field_id;
      current_meta_ = &field_meta;
    }

    // validated by caller
    IRS_ASSERT(IsSubsetOf(field_meta.features, meta().features));
    IRS_ASSERT(field_meta.index_features <= meta().index_features);

    field_iterator_mask_.emplace_back(
      TermIterator{i, &field_meta, field_terms});

    // update min and max terms
    min_ = std::min(min_, field_terms->min());
    max_ = std::max(max_, field_terms->max());
  }

  if (!field_iterator_mask_.empty()) {
    return true;
  }

  current_field_ = {};

  return false;
}

term_iterator::ptr CompoundFiledIterator::iterator() const {
  term_itr_.reset(meta());

  for (const auto& segment : field_iterator_mask_) {
    term_itr_.add(*(segment.reader),
                  *(field_iterators_[segment.itr_id].doc_map));
  }

  return memory::to_managed<term_iterator>(term_itr_);
}

// Computes fields_type
bool compute_field_meta(field_meta_map_t& field_meta_map,
                        IndexFeatures& index_features,
                        feature_set_t& fields_features,
                        const SubReader& reader) {
  REGISTER_TIMER_DETAILED();

  for (auto it = reader.fields(); it->next();) {
    const auto& field_meta = it->value().meta();
    const auto [field_meta_it, is_new] =
      field_meta_map.emplace(field_meta.name, &field_meta);

    // validate field_meta equivalence
    if (!is_new &&
        (!IsSubsetOf(field_meta.index_features,
                     field_meta_it->second->index_features) ||
         !IsSubsetOf(field_meta.features, field_meta_it->second->features))) {
      return false;  // field_meta is not equal, so cannot merge segments
    }

    AccumulateFeatures(fields_features, field_meta.features);
    index_features |= field_meta.index_features;
  }

  return true;
}

// Helper class responsible for writing a data from different sources
// into single columnstore.
class Columnstore {
 public:
  static constexpr size_t kProgressStepColumn = size_t{1} << 13;

  Columnstore(columnstore_writer::ptr&& writer,
              const MergeWriter::FlushProgress& progress)
    : progress_{progress, kProgressStepColumn}, writer_{std::move(writer)} {}

  Columnstore(directory& dir, const SegmentMeta& meta,
              const MergeWriter::FlushProgress& progress)
    : progress_{progress, kProgressStepColumn} {
    auto writer = meta.codec->get_columnstore_writer(true);
    writer->prepare(dir, meta);

    writer_ = std::move(writer);
  }

  // Inserts live values from the specified iterators into a column.
  // Returns column id of the inserted column on success,
  //  field_limits::invalid() in case if no data were inserted,
  //  empty value is case if operation was interrupted.
  template<typename Writer>
  std::optional<field_id> insert(
    DocIteratorContainer& itrs, const ColumnInfo& info,
    columnstore_writer::column_finalizer_f&& finalizer, Writer&& writer);

  // Inserts live values from the specified 'iterator' into a column.
  // Returns column id of the inserted column on success,
  //  field_limits::invalid() in case if no data were inserted,
  //  empty value is case if operation was interrupted.
  template<typename Writer>
  std::optional<field_id> insert(
    SortingCompoundDocIterator& it, const ColumnInfo& info,
    columnstore_writer::column_finalizer_f&& finalizer, Writer&& writer);

  // Returns `true` if anything was actually flushed
  bool flush(const flush_state& state) { return writer_->commit(state); }

  bool valid() const noexcept { return static_cast<bool>(writer_); }

 private:
  ProgressTracker progress_;
  columnstore_writer::ptr writer_;
};

template<typename Writer>
std::optional<field_id> Columnstore::insert(
  DocIteratorContainer& itrs, const ColumnInfo& info,
  columnstore_writer::column_finalizer_f&& finalizer, Writer&& writer) {
  auto next_iterator = [end = std::end(itrs)](auto begin) {
    return std::find_if(begin, end, [](auto& it) { return it.next(); });
  };

  auto begin = next_iterator(std::begin(itrs));

  if (begin == std::end(itrs)) {
    // Empty column
    return std::make_optional(field_limits::invalid());
  }

  auto column = writer_->push_column(info, std::move(finalizer));

  auto write_column = [&column, &writer, this](auto& it) -> bool {
    auto* payload = irs::get<irs::payload>(it);

    do {
      if (!progress_()) {
        // Stop was requested
        return false;
      }

      auto& out = column.second(it.value());

      if (payload) {
        writer(out, payload->value);
      }
    } while (it.next());

    return true;
  };

  do {
    if (!write_column(*begin)) {
      // Stop was requested
      return std::nullopt;
    }

    begin = next_iterator(++begin);
  } while (begin != std::end(itrs));

  return std::make_optional(column.first);
}

template<typename Writer>
std::optional<field_id> Columnstore::insert(
  SortingCompoundDocIterator& it, const ColumnInfo& info,
  columnstore_writer::column_finalizer_f&& finalizer, Writer&& writer) {
  const payload* payload = nullptr;

  auto* callback = irs::get<attribute_provider_change>(it);

  if (callback) {
    callback->subscribe([&payload](const attribute_provider& attrs) {
      payload = irs::get<irs::payload>(attrs);
    });
  } else {
    payload = irs::get<irs::payload>(it);
  }

  if (it.next()) {
    auto column = writer_->push_column(info, std::move(finalizer));

    do {
      if (!progress_()) {
        // Stop was requested
        return std::nullopt;
      }

      auto& out = column.second(it.value());

      if (payload) {
        writer(out, payload->value);
      }
    } while (it.next());

    return std::make_optional(column.first);
  } else {
    // Empty column
    return std::make_optional(field_limits::invalid());
  }
}

struct PrimarySortIteratorAdapter {
  explicit PrimarySortIteratorAdapter(doc_iterator::ptr it,
                                      doc_iterator::ptr live_docs) noexcept
    : it{std::move(it)},
      doc{irs::get<document>(*this->it)},
      payload{irs::get<irs::payload>(*this->it)},
      live_docs{std::move(live_docs)},
      live_doc{this->live_docs ? irs::get<document>(*this->live_docs)
                               : nullptr} {
    IRS_ASSERT(valid());
  }

  [[nodiscard]] bool valid() const noexcept {
    return it && doc && payload && (!live_docs || live_doc);
  }

  doc_iterator::ptr it;
  const document* doc;
  const irs::payload* payload;
  doc_iterator::ptr live_docs;
  const document* live_doc;
  doc_id_t min{};
};

class MinHeapContext {
 public:
  MinHeapContext(std::span<PrimarySortIteratorAdapter> itrs,
                 const Comparer& compare) noexcept
    : itrs_{itrs}, compare_{&compare} {}

  // advance
  bool operator()(const size_t i) const {
    IRS_ASSERT(i < itrs_.size());
    auto& it = itrs_[i];
    it.min = it.doc->value + 1;
    return it.it->next();
  }

  // compare
  bool operator()(const size_t lhs_idx, const size_t rhs_idx) const {
    IRS_ASSERT(lhs_idx != rhs_idx);
    IRS_ASSERT(lhs_idx < itrs_.size());
    IRS_ASSERT(rhs_idx < itrs_.size());

    const auto& lhs = itrs_[lhs_idx];
    const auto& rhs = itrs_[rhs_idx];

    const bytes_view lhs_value = lhs.payload->value;
    const bytes_view rhs_value = rhs.payload->value;

    if (const auto r = compare_->Compare(lhs_value, rhs_value); r) {
      return r > 0;
    }

    // tie breaker to avoid splitting document blocks, can use index as we
    // always merge different segments
    return rhs_idx > lhs_idx;
  }

 private:
  std::span<PrimarySortIteratorAdapter> itrs_;
  const Comparer* compare_;
};

template<typename Iterator>
bool write_columns(Columnstore& cs, Iterator& columns,
                   const ColumnInfoProvider& column_info,
                   CompoundColumnIterator& column_itr,
                   const MergeWriter::FlushProgress& progress) {
  REGISTER_TIMER_DETAILED();
  IRS_ASSERT(cs.valid());
  IRS_ASSERT(progress);

  auto add_iterators = [&column_itr](auto& itrs) {
    auto add_iterators = [&itrs](const SubReader& /*segment*/,
                                 const doc_map_f& doc_map,
                                 const irs::column_reader& column) {
      auto it = column.iterator(ColumnHint::kConsolidation);

      if (IRS_LIKELY(it && irs::get<document>(*it))) {
        itrs.emplace_back(std::move(it), doc_map);
      } else {
        IRS_ASSERT(false);
        IR_FRMT_ERROR(
          "Got an invalid iterator during consolidationg of the columnstore, "
          "skipping it");
      }
      return true;
    };

    itrs.clear();
    return column_itr.visit(add_iterators);
  };

  while (column_itr.next()) {
    // visit matched columns from merging segments and
    // write all survived values to the new segment
    if (!progress() || !columns.reset(add_iterators)) {
      return false;  // failed to visit all values
    }

    const std::string_view column_name = column_itr.value().name();

    const auto res = cs.insert(
      columns, column_info(column_name),
      [column_name](bstring&) { return column_name; },
      [](data_output& out, bytes_view payload) {
        if (!payload.empty()) {
          out.write_bytes(payload.data(), payload.size());
        }
      });

    if (!res.has_value()) {
      return false;  // failed to insert all values
    }
  }

  return true;
}

// Write field term data
template<typename Iterator>
bool write_fields(Columnstore& cs, Iterator& feature_itr,
                  const flush_state& flush_state, const SegmentMeta& meta,
                  const FeatureInfoProvider& column_info,
                  CompoundFiledIterator& field_itr,
                  const MergeWriter::FlushProgress& progress) {
  REGISTER_TIMER_DETAILED();
  IRS_ASSERT(cs.valid());

  feature_map_t features;
  irs::type_info::type_id feature{};
  std::vector<bytes_view> hdrs;
  hdrs.reserve(field_itr.size());

  auto add_iterators = [&field_itr, &hdrs, &feature](auto& itrs) {
    auto add_iterators = [&itrs, &hdrs, &feature](const SubReader& segment,
                                                  const doc_map_f& doc_map,
                                                  const field_meta& field) {
      const auto column = field.features.find(feature);

      if (column == field.features.end() ||
          !field_limits::valid(column->second)) {
        // field has no feature
        return true;
      }

      auto* reader = segment.column(column->second);

      // Tail columns can be removed if empty.
      if (reader) {
        auto it = reader->iterator(ColumnHint::kConsolidation);
        IRS_ASSERT(it);

        if (IRS_LIKELY(it)) {
          hdrs.emplace_back(reader->payload());

          if (IRS_LIKELY(irs::get<document>(*it))) {
            itrs.emplace_back(std::move(it), doc_map);
          } else {
            IRS_ASSERT(false);
            IR_FRMT_ERROR(
              "Failed to get document attribute from the iterator, skipping "
              "it");
          }
        }
      }

      return true;
    };

    hdrs.clear();
    itrs.clear();
    return field_itr.visit(add_iterators);
  };

  auto field_writer = meta.codec->get_field_writer(true);
  field_writer->prepare(flush_state);

  while (field_itr.next()) {
    features.clear();
    auto& field_meta = field_itr.meta();

    auto begin = field_meta.features.begin();
    auto end = field_meta.features.end();

    for (; begin != end; ++begin) {
      if (!progress()) {
        return false;
      }

      std::tie(feature, std::ignore) = *begin;

      if (!feature_itr.reset(add_iterators)) {
        return false;
      }

      const auto [info, factory] = column_info(feature);

      std::optional<field_id> res;
      auto feature_writer =
        factory ? (*factory)({hdrs.data(), hdrs.size()}) : FeatureWriter::ptr{};

      if (feature_writer) {
        auto value_writer = [writer = feature_writer.get()](
                              data_output& out, bytes_view payload) {
          writer->write(out, payload);
        };

        res = cs.insert(
          feature_itr, info,
          [feature_writer = std::move(feature_writer)](bstring& out) {
            feature_writer->finish(out);
            return std::string_view{};
          },
          std::move(value_writer));
      } else if (!factory) {  // Otherwise factory has failed to instantiate
                              // writer
        res = cs.insert(
          feature_itr, info, [](bstring&) { return std::string_view{}; },
          [](data_output& out, bytes_view payload) {
            if (!payload.empty()) {
              out.write_bytes(payload.data(), payload.size());
            }
          });
      }

      if (!res.has_value()) {
        return false;  // Failed to insert all values
      }

      features[feature] = res.value();
    }

    // write field terms
    auto terms = field_itr.iterator();

    field_writer->write(field_meta.name, field_meta.index_features, features,
                        *terms);
  }

  field_writer->end();
  field_writer.reset();

  return !field_itr.aborted();
}

// Computes doc_id_map and docs_count
doc_id_t compute_doc_ids(doc_id_map_t& doc_id_map, const SubReader& reader,
                         doc_id_t next_id) noexcept {
  REGISTER_TIMER_DETAILED();
  // assume not a lot of space wasted if doc_limits::min() > 0
  try {
    doc_id_map.resize(reader.docs_count() + doc_limits::min(),
                      doc_limits::eof());
  } catch (...) {
    IR_FRMT_ERROR(
      "Failed to resize merge_writer::doc_id_map to accommodate "
      "element: " IR_UINT64_T_SPECIFIER,
      reader.docs_count() + doc_limits::min());

    return doc_limits::invalid();
  }

  for (auto docs_itr = reader.docs_iterator(); docs_itr->next(); ++next_id) {
    auto src_doc_id = docs_itr->value();

    IRS_ASSERT(src_doc_id >= doc_limits::min());
    IRS_ASSERT(src_doc_id < reader.docs_count() + doc_limits::min());
    doc_id_map[src_doc_id] = next_id;  // set to next valid doc_id
  }

  return next_id;
}

#if defined(IRESEARCH_DEBUG) && !defined(__clang__)
void EnsureSorted(const auto& readers) {
  for (const auto& reader : readers) {
    const auto& doc_map = reader.doc_id_map;
    IRS_ASSERT(doc_map.size() >= doc_limits::min());

    auto view = doc_map | std::views::filter([](doc_id_t doc) noexcept {
                  return !doc_limits::eof(doc);
                });

    IRS_ASSERT(std::ranges::is_sorted(view));
  }
}
#endif

const MergeWriter::FlushProgress kProgressNoop = []() { return true; };

}  // namespace

MergeWriter::ReaderCtx::ReaderCtx(const SubReader* reader) noexcept
  : reader{reader}, doc_map{[](doc_id_t) noexcept {
      return doc_limits::eof();
    }} {
  IRS_ASSERT(this->reader);
}

MergeWriter::MergeWriter() noexcept : dir_(NoopDirectory::instance()) {}

MergeWriter::operator bool() const noexcept {
  return &dir_ != &NoopDirectory::instance();
}

bool MergeWriter::FlushUnsorted(TrackingDirectory& dir, SegmentMeta& segment,
                                const FlushProgress& progress) {
  REGISTER_TIMER_DETAILED();
  IRS_ASSERT(progress);
  IRS_ASSERT(!comparator_);
  IRS_ASSERT(column_info_ && *column_info_);
  IRS_ASSERT(feature_info_ && *feature_info_);

  const size_t size = readers_.size();

  field_meta_map_t field_meta_map;
  CompoundFiledIterator fields_itr{size, progress};
  CompoundColumnIterator columns_itr{size};
  feature_set_t fields_features;
  IndexFeatures index_features{IndexFeatures::NONE};

  DocIteratorContainer remapping_itrs{size};

  doc_id_t base_id = doc_limits::min();  // next valid doc_id

  // collect field meta and field term data
  for (auto& reader_ctx : readers_) {
    // ensured by merge_writer::add(...)
    IRS_ASSERT(reader_ctx.reader);

    auto& reader = *reader_ctx.reader;
    const auto docs_count = reader.docs_count();

    if (reader.live_docs_count() == docs_count) {  // segment has no deletes
      const auto reader_base = base_id - doc_limits::min();
      base_id += docs_count;

      reader_ctx.doc_map = [reader_base](doc_id_t doc) noexcept {
        return reader_base + doc;
      };
    } else {  // segment has some deleted docs
      auto& doc_id_map = reader_ctx.doc_id_map;
      base_id = compute_doc_ids(doc_id_map, reader, base_id);

      reader_ctx.doc_map = [&doc_id_map](doc_id_t doc) noexcept {
        return doc >= doc_id_map.size() ? doc_limits::eof() : doc_id_map[doc];
      };
    }

    if (!doc_limits::valid(base_id)) {
      return false;  // failed to compute next doc_id
    }

    if (!compute_field_meta(field_meta_map, index_features, fields_features,
                            reader)) {
      return false;
    }

    fields_itr.add(reader, reader_ctx.doc_map);
    columns_itr.add(reader, reader_ctx.doc_map);
  }

  // total number of doc_ids
  segment.docs_count = base_id - doc_limits::min();
  // all merged documents are live
  segment.live_docs_count = segment.docs_count;

  if (!progress()) {
    return false;  // progress callback requested termination
  }

  // write merged segment data
  REGISTER_TIMER_DETAILED();
  Columnstore cs(dir, segment, progress);

  if (!cs.valid()) {
    return false;  // flush failure
  }

  if (!progress()) {
    return false;  // progress callback requested termination
  }

  if (!write_columns(cs, remapping_itrs, *column_info_, columns_itr,
                     progress)) {
    return false;  // flush failure
  }

  if (!progress()) {
    return false;  // progress callback requested termination
  }

  const flush_state state{.dir = &dir,
                          .features = &fields_features,
                          .name = segment.name,
                          .scorers = scorers_,
                          .doc_count = segment.docs_count,
                          .index_features = index_features};

  // write field meta and field term data
  if (!write_fields(cs, remapping_itrs, state, segment, *feature_info_,
                    fields_itr, progress)) {
    return false;  // flush failure
  }

  if (!progress()) {
    return false;  // progress callback requested termination
  }

  segment.column_store = cs.flush(state);

  return true;
}

bool MergeWriter::FlushSorted(TrackingDirectory& dir, SegmentMeta& segment,
                              const FlushProgress& progress) {
  REGISTER_TIMER_DETAILED();
  IRS_ASSERT(progress);
  IRS_ASSERT(comparator_);
  IRS_ASSERT(column_info_ && *column_info_);
  IRS_ASSERT(feature_info_ && *feature_info_);

  const size_t size = readers_.size();

  field_meta_map_t field_meta_map;
  CompoundColumnIterator columns_itr{size};
  CompoundFiledIterator fields_itr{size, progress, comparator_};
  feature_set_t fields_features;
  IndexFeatures index_features{IndexFeatures::NONE};

  std::vector<PrimarySortIteratorAdapter> itrs;
  itrs.reserve(size);

  auto emplace_iterator = [&itrs](const SubReader& segment) {
    if (!segment.sort()) {
      // sort column is not present, give up
      return false;
    }

    auto sort =
      segment.mask(segment.sort()->iterator(irs::ColumnHint::kConsolidation));

    if (IRS_UNLIKELY(!sort)) {
      return false;
    }

    doc_iterator::ptr live_docs;
    if (segment.docs_count() != segment.live_docs_count()) {
      live_docs = segment.docs_iterator();
    }

    auto& it = itrs.emplace_back(std::move(sort), std::move(live_docs));

    if (IRS_UNLIKELY(!it.valid())) {
      return false;
    }

    return true;
  };

  segment.docs_count = 0;

  // Init doc map for each reader
  for (auto& reader_ctx : readers_) {
    // ensured by merge_writer::add(...)
    IRS_ASSERT(reader_ctx.reader);

    auto& reader = *reader_ctx.reader;

    if (reader.docs_count() > doc_limits::eof() - doc_limits::min()) {
      // can't merge segment holding more than 'doc_limits::eof()-1' docs
      return false;
    }

    if (!emplace_iterator(reader)) {
      // sort column is not present, give up
      return false;
    }

    if (!compute_field_meta(field_meta_map, index_features, fields_features,
                            reader)) {
      return false;
    }

    fields_itr.add(reader, reader_ctx.doc_map);
    columns_itr.add(reader, reader_ctx.doc_map);

    // count total number of documents in consolidated segment
    if (!math::sum_check_overflow(segment.docs_count, reader.live_docs_count(),
                                  segment.docs_count)) {
      return false;
    }

    // prepare doc maps
    auto& doc_id_map = reader_ctx.doc_id_map;

    try {
      doc_id_map.resize(reader.docs_count() + doc_limits::min(),
                        doc_limits::eof());
    } catch (...) {
      IR_FRMT_ERROR(
        "Failed to resize merge_writer::doc_id_map to accommodate "
        "element: " IR_UINT64_T_SPECIFIER,
        reader.docs_count() + doc_limits::min());

      return false;
    }

    reader_ctx.doc_map = [doc_id_map =
                            std::span{doc_id_map}](size_t doc) noexcept {
      IRS_ASSERT(doc_id_map[0] == doc_limits::eof());
      return doc_id_map[doc * static_cast<size_t>(doc < doc_id_map.size())];
    };
  }

  if (segment.docs_count >= doc_limits::eof()) {
    // can't merge segments holding more than 'doc_limits::eof()-1' docs
    return false;
  }

  if (!progress()) {
    return false;  // progress callback requested termination
  }

  // Write new sorted column and fill doc maps for each reader
  auto writer = segment.codec->get_columnstore_writer(true);
  writer->prepare(dir, segment);

  // get column info for sorted column
  const auto info = (*column_info_)({});
  auto [column_id, column_writer] = writer->push_column(info, {});

  doc_id_t next_id = doc_limits::min();

  auto fill_doc_map = [&](doc_id_map_t& doc_id_map,
                          PrimarySortIteratorAdapter& it, doc_id_t max) {
    if (auto min = it.min; min < max) {
      if (it.live_docs) {
        auto& live_docs = *it.live_docs;
        const auto* live_doc = it.live_doc;
        for (live_docs.seek(min); live_doc->value < max; live_docs.next()) {
          doc_id_map[live_doc->value] = next_id++;
          if (!progress()) {
            return false;
          }
        }
      } else {
        do {
          doc_id_map[min] = next_id++;
          ++min;
          if (!progress()) {
            return false;
          }
        } while (min < max);
      }
    }
    return true;
  };

  ExternalHeapIterator columns_it{MinHeapContext{itrs, *comparator_}};

  for (columns_it.reset(itrs.size()); columns_it.next();) {
    const auto index = columns_it.value();
    auto& it = itrs[index];
    IRS_ASSERT(it.valid());

    const auto max = it.doc->value;
    auto& doc_id_map = readers_[index].doc_id_map;

    // fill doc id map
    if (!fill_doc_map(doc_id_map, it, max)) {
      return false;  // progress callback requested termination
    }
    doc_id_map[max] = next_id;

    // write value into new column if present
    auto& stream = column_writer(next_id);
    const auto payload = it.payload->value;
    stream.write_bytes(payload.data(), payload.size());

    ++next_id;

    if (!progress()) {
      return false;  // progress callback requested termination
    }
  }

  // Handle empty values greater than the last document in sort column
  for (auto it = itrs.begin(); auto& reader : readers_) {
    if (!fill_doc_map(reader.doc_id_map, *it,
                      doc_limits::min() + reader.reader->docs_count())) {
      return false;  // progress callback requested termination
    }
    ++it;
  }

#if defined(IRESEARCH_DEBUG) && !defined(__clang__)
  EnsureSorted(readers_);
#endif

  Columnstore cs(std::move(writer), progress);
  CompoundDocIterator doc_it(progress);
  SortingCompoundDocIterator sorting_doc_it(doc_it);

  if (!cs.valid()) {
    return false;  // flush failure
  }

  if (!progress()) {
    return false;  // progress callback requested termination
  }

  if (!write_columns(cs, sorting_doc_it, *column_info_, columns_itr,
                     progress)) {
    return false;  // flush failure
  }

  if (!progress()) {
    return false;  // progress callback requested termination
  }

  const flush_state state{.dir = &dir,
                          .features = &fields_features,
                          .name = segment.name,
                          .scorers = scorers_,
                          .doc_count = segment.docs_count,
                          .index_features = index_features};

  // write field meta and field term data
  if (!write_fields(cs, sorting_doc_it, state, segment, *feature_info_,
                    fields_itr, progress)) {
    return false;  // flush failure
  }

  if (!progress()) {
    return false;  // progress callback requested termination
  }

  segment.column_store = cs.flush(state);  // flush columnstore
  segment.sort = column_id;                // set sort column identifier
  // all merged documents are live
  segment.live_docs_count = segment.docs_count;

  return true;
}

bool MergeWriter::Flush(SegmentMeta& segment,
                        const FlushProgress& progress /*= {}*/) {
  REGISTER_TIMER_DETAILED();
  IRS_ASSERT(segment.codec);  // Must be set outside

  bool result = false;  // Flush result

  Finally segment_invalidator = [&result, &segment]() noexcept {
    if (IRS_UNLIKELY(!result)) {
      // Invalidate segment
      segment.files.clear();
      segment.column_store = false;
      static_cast<SegmentInfo&>(segment) = SegmentInfo{};
    }
  };

  const auto& progress_callback = progress ? progress : kProgressNoop;

  TrackingDirectory track_dir{dir_};  // Track writer created files

  result = comparator_ ? FlushSorted(track_dir, segment, progress_callback)
                       : FlushUnsorted(track_dir, segment, progress_callback);

  segment.files = track_dir.FlushTracked(segment.byte_size);

  return result;
}

}  // namespace irs
