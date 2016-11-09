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

#ifndef IRESEARCH_FORMAT_TEST_CASE_BASE
#define IRESEARCH_FORMAT_TEST_CASE_BASE

#include "tests_shared.hpp"

#include "search/cost.hpp"

#include "index/doc_generator.hpp"
#include "index/index_tests.hpp"
#include "iql/query_builder.hpp"

#include "analysis/token_attributes.hpp"
#include "store/memory_directory.hpp"
#include "utils/version_utils.hpp"

namespace ir = iresearch;

// ----------------------------------------------------------------------------
// --SECTION--                                                   Base test case
// ----------------------------------------------------------------------------

namespace tests {
    
class format_test_case_base : public index_test_base {
 public:  
  class postings;

  class position : public ir::position::impl {
   public:
    position(const ir::flags& features) { 
      if (features.check<ir::offset>()) {
        offs_ = attrs_.add<ir::offset>();
      }

      if (features.check<ir::payload>()) {
        pay_ = attrs_.add<ir::payload>();
      }
    }

    uint32_t value() const {
      return begin_;
    }

    bool next() override {
      if (begin_ == end_) {
        begin_ = ir::position::NO_MORE;
        return false;
      }

      ++begin_;

      if (pay_) {
        pay_data_ = std::to_string(begin_);
        pay_->value = ir::ref_cast<ir::byte_type>(ir::string_ref(pay_data_));
      }

      if (offs_) {
        offs_->start = begin_;
        offs_->end = offs_->start + pay_data_.size();
      }
      return true;
    }

    void clear() override {
      if (pay_) pay_->clear();
      if (offs_) offs_->clear();
    }

   private:
    friend class postings;

    uint32_t begin_{ ir::position::INVALID };
    uint32_t end_;
    ir::offset* offs_{};
    ir::payload* pay_{};
    std::string pay_data_;
  };

  class postings : public ir::doc_iterator {
   public:
    typedef std::vector<ir::doc_id_t> docs_t;
    typedef std::vector<ir::cost::cost_t> costs_t;

    postings(const docs_t::const_iterator& begin, const docs_t::const_iterator& end, 
             const ir::flags& features = ir::flags::empty_instance())
      : next_(begin), end_(end) {
      if (features.check<ir::frequency>()) {
        ir::frequency* freq = attrs_.add<ir::frequency>();
        freq->value = 10;

        if (features.check<ir::position>()) {
          ir::position* pos = attrs_.add<ir::position>();
          pos->prepare(pos_ = new position(features));
        }
      }
    }

    bool next() {      
      if (next_ == end_) {
        doc_ = ir::type_limits<ir::type_t::doc_id_t>::eof();
        return false;
      }

      doc_ = *next_;
      if (pos_) {
        pos_->begin_ = doc_;
        pos_->end_ = pos_->begin_ + 10;
        pos_->clear();
      }

      ++next_;
      return true;
    }

    ir::doc_id_t value() const {
      return doc_;
    }

    ir::doc_id_t seek(ir::doc_id_t target) {
      return ir::seek(*this, target);
    }

    const ir::attributes& attributes() const {
      return attrs_;
    }

   private:
    ir::attributes attrs_;
    docs_t::const_iterator next_;
    docs_t::const_iterator end_;
    position* pos_{};
    ir::doc_id_t doc_{ ir::type_limits<ir::type_t::doc_id_t>::invalid() };
  }; // postings 

  template<typename Iterator>
  class terms : public ir::term_iterator {
  public:
    terms(const Iterator& begin, const Iterator& end)
      : next_(begin), end_(end) {
      docs_.push_back(ir::type_limits<ir::type_t::doc_id_t>::min());
    }

    terms(const Iterator& begin, const Iterator& end,
          std::vector<ir::doc_id_t>::const_iterator doc_begin, 
          std::vector<ir::doc_id_t>::const_iterator doc_end) 
      : docs_(doc_begin, doc_end), next_(begin), end_(end) {
    }

    bool next() {
      if (next_ == end_) {
        return false;
      }

      val_ = ir::ref_cast<ir::byte_type>(*next_);
      ++next_;
      return true;
    }

    const ir::bytes_ref& value() const {
      return val_;
    }

    ir::doc_iterator::ptr postings(const ir::flags& features) const {
      return ir::doc_iterator::make<format_test_case_base::postings>(
        docs_.begin(), docs_.end()
      );
    }

    void read() { }

    const ir::attributes& attributes() const {
      return ir::attributes::empty_instance();
    }

  private:
    ir::bytes_ref val_;
    std::vector<ir::doc_id_t> docs_;
    Iterator next_;
    Iterator end_;
  }; // terms

  void assert_no_directory_artifacts(
    const iresearch::directory& dir,
    iresearch::format& codec,
    const std::unordered_set<std::string>& expect_additional = std::unordered_set<std::string> ()
  ) {
    iresearch::directory::files dir_files;

    ASSERT_TRUE(dir.list(dir_files));

    // ignore lock file present in fs_directory
    for (auto itr = dir_files.begin(); itr != dir_files.end();) {
      itr = iresearch::index_writer::WRITE_LOCK_NAME == *itr
          ? dir_files.erase(itr) : ++itr;
    }

    iresearch::index_meta index_meta;
    auto reader = codec.get_index_meta_reader();
    iresearch::index_meta::file_set index_files(expect_additional.begin(), expect_additional.end());
    auto* segment_file = reader->last_segments_file(dir_files);

    if (segment_file) {
      reader->read(dir, index_meta, *segment_file);

      index_meta.visit_files([&index_files] (const std::string& file) {
        index_files.emplace(file);
        return true;
      });

      index_files.insert(*segment_file);
    }

    for (auto& file: dir_files) {
      ASSERT_TRUE(index_files.erase(file) == 1);
    }

    ASSERT_TRUE(index_files.empty());
  }

  void directory_artifact_cleaner() {
    tests::json_doc_generator gen(
      resource("simple_sequential.json"),
      &tests::generic_json_field_factory);
    tests::document const* doc1 = gen.next();
    tests::document const* doc2 = gen.next();
    tests::document const* doc3 = gen.next();
    tests::document const* doc4 = gen.next();
    auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());
    auto query_doc2 = iresearch::iql::query_builder().build("name==B", std::locale::classic());
    auto query_doc3 = iresearch::iql::query_builder().build("name==C", std::locale::classic());
    auto query_doc4 = iresearch::iql::query_builder().build("name==D", std::locale::classic());

    iresearch::directory::files files;
    iresearch::directory::ptr dir(get_directory());
    ASSERT_TRUE(dir->list(files));
    ASSERT_TRUE(files.empty());

    // register ref counter
    iresearch::directory_cleaner::init(*dir);

    // cleanup on refcount decrement (old files not in use)
    {
      // create writer to directory
      auto writer = iresearch::index_writer::make(*dir, codec(), iresearch::OPEN_MODE::OM_CREATE);

      // initialize directory
      {
        writer->commit();
        iresearch::directory_cleaner::clean(*dir); // clean unused files
        assert_no_directory_artifacts(*dir, *codec());
      }

      // add first segment
      {
        writer->add(doc1->begin(), doc1->end());
        writer->add(doc2->begin(), doc2->end());
        writer->commit();
        iresearch::directory_cleaner::clean(*dir); // clean unused files
        assert_no_directory_artifacts(*dir, *codec());
      }

      // add second segment (creating new index_meta file, remove old)
      {
        writer->add(doc3->begin(), doc3->end());
        writer->commit();
        iresearch::directory_cleaner::clean(*dir); // clean unused files
        assert_no_directory_artifacts(*dir, *codec());
      }

      // delete record from first segment (creating new index_meta file + doc_mask file, remove old)
      {
        writer->remove(*(query_doc1.filter));
        writer->commit();
        iresearch::directory_cleaner::clean(*dir); // clean unused files
        assert_no_directory_artifacts(*dir, *codec());
      }

      // delete all record from first segment (creating new index_meta file, remove old meta + unused segment)
      {
        writer->remove(*(query_doc2.filter));
        writer->commit();
        iresearch::directory_cleaner::clean(*dir); // clean unused files
        assert_no_directory_artifacts(*dir, *codec());
      }

      // delete all records from second segment (creating new index_meta file, remove old meta + unused segment)
      {
        writer->remove(*(query_doc2.filter));
        writer->commit();
        iresearch::directory_cleaner::clean(*dir); // clean unused files
        assert_no_directory_artifacts(*dir, *codec());
      }
    }

    dir->list(files);

    // reset directory
    for (auto& file: files) {
      dir->remove(file);
    }

    files.clear();
    ASSERT_TRUE(dir->list(files));
    ASSERT_TRUE(files.empty());

    // cleanup on refcount decrement (old files still in use)
    {
      // create writer to directory
      auto writer = iresearch::index_writer::make(*dir, codec(), iresearch::OPEN_MODE::OM_CREATE);

      // initialize directory
      {
        writer->commit();
        iresearch::directory_cleaner::clean(*dir); // clean unused files
        assert_no_directory_artifacts(*dir, *codec());
      }

      // add first segment
      {
        writer->add(doc1->begin(), doc1->end());
        writer->add(doc2->begin(), doc2->end());
        writer->add(doc3->begin(), doc3->end());
        writer->commit();
        iresearch::directory_cleaner::clean(*dir); // clean unused files
        assert_no_directory_artifacts(*dir, *codec());
      }

      // delete record from first segment (creating new doc_mask file)
      {
        writer->remove(*(query_doc1.filter));
        writer->commit();
        iresearch::directory_cleaner::clean(*dir); // clean unused files
        assert_no_directory_artifacts(*dir, *codec());
      }

      // create reader to directory
      auto reader = iresearch::directory_reader::open(*dir, codec());
      std::unordered_set<std::string> reader_files;
      {
        iresearch::directory::files files;

        dir->list(files);

        iresearch::index_meta index_meta;
        auto meta_reader = codec()->get_index_meta_reader();
        auto* segments_file = meta_reader->last_segments_file(files);

        ASSERT_TRUE(nullptr != segments_file);
        meta_reader->read(*dir, index_meta, *segments_file);

        index_meta.visit_files([&reader_files] (std::string& file) {
          reader_files.emplace(std::move(file));
          return true;
        });

        reader_files.emplace(*segments_file);
      }

      // add second segment (creating new index_meta file, not-removing old)
      {
        writer->add(doc4->begin(), doc4->end());
        writer->commit();
        iresearch::directory_cleaner::clean(*dir); // clean unused files
        assert_no_directory_artifacts(*dir, *codec(), reader_files);
      }

      // delete record from first segment (creating new doc_mask file, not-remove old)
      {
        writer->remove(*(query_doc2.filter));
        writer->commit();
        iresearch::directory_cleaner::clean(*dir); // clean unused files
        assert_no_directory_artifacts(*dir, *codec(), reader_files);
      }

      // delete all record from first segment (creating new index_meta file, remove old meta but leave first segment)
      {
        writer->remove(*(query_doc3.filter));
        writer->commit();
        iresearch::directory_cleaner::clean(*dir); // clean unused files
        assert_no_directory_artifacts(*dir, *codec(), reader_files);
      }

      // delete all records from second segment (creating new index_meta file, remove old meta + unused segment)
      {
        writer->remove(*(query_doc4.filter));
        writer->commit();
        iresearch::directory_cleaner::clean(*dir); // clean unused files
        assert_no_directory_artifacts(*dir, *codec(), reader_files);
      }

      // close reader (remove old meta + old doc_mask + first segment)
      {
        reader.reset();
        iresearch::directory_cleaner::clean(*dir); // clean unused files
        assert_no_directory_artifacts(*dir, *codec());
      }
    }

    dir->list(files);

    // reset directory
    for (auto& file: files) {
      dir->remove(file);
    }

    files.clear();
    ASSERT_TRUE(dir->list(files));
    ASSERT_TRUE(files.empty());

    // cleanup on writer startup
    {
      // fill directory
      {
        auto writer = iresearch::index_writer::make(*dir, codec(), iresearch::OPEN_MODE::OM_CREATE);

        writer->commit(); // initialize directory
        writer->add(doc1->begin(), doc1->end());
        writer->commit(); // add first segment
        writer->add(doc2->begin(), doc2->end());
        writer->add(doc3->begin(), doc3->end());
        writer->commit(); // add second segment
        writer->remove(*(query_doc1.filter));
        writer->commit(); // remove first segment
      }

      // add invalid files
      {
        iresearch::index_output::ptr tmp;
        tmp = dir->create("dummy.file.1");
        tmp = dir->create("dummy.file.2");
      }
      ASSERT_TRUE(dir->exists("dummy.file.1"));
      ASSERT_TRUE(dir->exists("dummy.file.2"));

      // open writer
      auto writer = iresearch::index_writer::make(*dir, codec(), iresearch::OPEN_MODE::OM_CREATE);

      // if directory has files (for fs directory) then ensure only valid meta+segments loaded
      ASSERT_FALSE(dir->exists("dummy.file.1"));
      ASSERT_FALSE(dir->exists("dummy.file.2"));
      assert_no_directory_artifacts(*dir, *codec());
    }
  }

  void fields_read_write() {
    // create sorted && unsorted terms
    typedef std::set<ir::bytes_ref> sorted_terms_t;
    typedef std::vector<ir::bytes_ref> unsorted_terms_t;
    sorted_terms_t sorted_terms;
    unsorted_terms_t unsorted_terms;

    tests::json_doc_generator gen(
      resource("fst_prefixes.json"),
      [&sorted_terms, &unsorted_terms] (tests::document& doc, const std::string& name, const tests::json::json_value& data) {
        doc.add(new tests::templates::string_field(
          ir::string_ref(name),
          ir::string_ref(data.value),
          true, true
        ));

        auto ref = ir::ref_cast<ir::byte_type>((doc.end() - 1).as<tests::templates::string_field>().value());
        sorted_terms.emplace(ref);
        unsorted_terms.emplace_back(ref);
    });

    // define field
    ir::field_meta field;
    field.id = 0;
    field.name = "field";

    // write fields
    {
      ir::flush_state state;
      state.dir = &dir();
      state.doc_count = 100;
      state.fields_count = 1;
      state.name = "segment_name";
      state.ver = IRESEARCH_VERSION;
      state.features = &field.features;

      // should use sorted terms on write
      terms<sorted_terms_t::iterator> terms(sorted_terms.begin(), sorted_terms.end());

      auto writer = codec()->get_field_writer();
      writer->prepare(state);
      writer->write(field.id, field.features, terms);
      writer->end();
    }

    // read field
    {
      ir::fields_meta fields;
      {
        std::vector<ir::field_meta> src;
        src.emplace_back(field);
        fields = ir::fields_meta(std::move(src), ir::flags());
      }

      ir::segment_meta meta;
      meta.name = "segment_name";

      ir::reader_state state;
      state.docs_mask = nullptr;
      state.dir = &dir();
      state.meta = &meta;
      state.fields = &fields;

      auto reader = codec()->get_field_reader();
      reader->prepare(state);
      ASSERT_EQ(1, reader->size());

      // check terms
      ASSERT_EQ(nullptr, reader->terms(ir::type_limits<ir::type_t::field_id_t>::invalid()));
      auto term_reader = reader->terms(field.id);
      ASSERT_NE(nullptr, term_reader);

      ASSERT_EQ(sorted_terms.size(), term_reader->size());
      ASSERT_EQ(*sorted_terms.begin(), term_reader->min());
      ASSERT_EQ(*sorted_terms.rbegin(), term_reader->max());

      // check terms using "next"
      {
        auto expected_term = sorted_terms.begin();
        auto term = term_reader->iterator();
        for (; term->next(); ++expected_term) {
          ASSERT_EQ(*expected_term, term->value());
        }
        ASSERT_EQ(sorted_terms.end(), expected_term);
        ASSERT_FALSE(term->next());
      }

      // check terms using single "seek"
       {
         auto expected_sorted_term = sorted_terms.begin();
         for (auto end = sorted_terms.end(); expected_sorted_term != end; ++expected_sorted_term) {
           auto term = term_reader->iterator();
           ASSERT_TRUE(term->seek(*expected_sorted_term));
           ASSERT_EQ(*expected_sorted_term, term->value());
         }
       }

       // check sorted terms using "seek to cookie"
       {
         auto expected_sorted_term = sorted_terms.begin();
         auto term = term_reader->iterator();
         for (auto end = sorted_terms.end(); term->next(); ++expected_sorted_term) {
           ASSERT_EQ(*expected_sorted_term, term->value());

           // get cookie
           auto cookie = term->cookie();
           ASSERT_NE(nullptr, cookie);
           {
             auto seeked_term = term_reader->iterator();
             ASSERT_TRUE(seeked_term->seek(*expected_sorted_term, *cookie));
             ASSERT_EQ(*expected_sorted_term, seeked_term->value());

             // iterate to the end with seeked_term
             auto copy_expected_sorted_term = expected_sorted_term;
             for (++copy_expected_sorted_term; seeked_term->next(); ++copy_expected_sorted_term) {
               ASSERT_EQ(*copy_expected_sorted_term, seeked_term->value());
             }
             ASSERT_EQ(sorted_terms.end(), copy_expected_sorted_term);
             ASSERT_FALSE(seeked_term->next());
           }
         }
         ASSERT_EQ(sorted_terms.end(), expected_sorted_term);
         ASSERT_FALSE(term->next());
       }

       // check unsorted terms using "seek to cookie"
       {
         auto expected_term = unsorted_terms.begin();
         auto term = term_reader->iterator();
         for (auto end = unsorted_terms.end(); term->next(); ++expected_term) {
           auto sorted_term = sorted_terms.find(*expected_term);
           ASSERT_NE(sorted_terms.end(), sorted_term);

           // get cookie
           auto cookie = term->cookie();
           ASSERT_NE(nullptr, cookie);
           {
             auto seeked_term = term_reader->iterator();
             ASSERT_TRUE(seeked_term->seek(*sorted_term, *cookie));
             ASSERT_EQ(*sorted_term, seeked_term->value());

             // iterate to the end with seeked_term
             auto copy_sorted_term = sorted_term;
             for (++copy_sorted_term; seeked_term->next(); ++copy_sorted_term) {
               ASSERT_EQ(*copy_sorted_term, seeked_term->value());
             }
             ASSERT_EQ(sorted_terms.end(), copy_sorted_term);
             ASSERT_FALSE(seeked_term->next());
           }
         }
         ASSERT_EQ(unsorted_terms.end(), expected_term);
         ASSERT_FALSE(term->next());
       }

       // check sorted terms using multiple "seek"s on single iterator
       {
         auto expected_term = sorted_terms.begin();
         auto term = term_reader->iterator();
         for (auto end = sorted_terms.end(); expected_term != end; ++expected_term) {
           ASSERT_TRUE(term->seek(*expected_term));

           /* seek to the same term */
           ASSERT_TRUE(term->seek(*expected_term));
           ASSERT_EQ(*expected_term, term->value());
         }
       }
       
       /* check sorted terms in reverse order using multiple "seek"s on single iterator */
       {
         auto expected_term = sorted_terms.rbegin();
         auto term = term_reader->iterator();
         for (auto end = sorted_terms.rend(); expected_term != end; ++expected_term) {
           ASSERT_TRUE(term->seek(*expected_term));

           /* seek to the same term */
           ASSERT_TRUE(term->seek(*expected_term));
           ASSERT_EQ(*expected_term, term->value());
         }
       }
       
       /* check unsorted terms using multiple "seek"s on single iterator */
       {
         auto expected_term = unsorted_terms.begin();
         auto term = term_reader->iterator();
         for (auto end = unsorted_terms.end(); expected_term != end; ++expected_term) {
           ASSERT_TRUE(term->seek(*expected_term));

           /* seek to the same term */
           ASSERT_TRUE(term->seek(*expected_term));
           ASSERT_EQ(*expected_term, term->value());
         }
       }

       /* seek to nil (the smallest possible term) */
       {
         /* with state */
         {
           auto term = term_reader->iterator();
           ASSERT_FALSE(term->seek(ir::bytes_ref::nil));
           ASSERT_EQ(term_reader->min(), term->value());
           ASSERT_EQ(ir::SeekResult::NOT_FOUND, term->seek_ge(ir::bytes_ref::nil));
           ASSERT_EQ(term_reader->min(), term->value());
         }

         /* without state */
         {
           auto term = term_reader->iterator();
           ASSERT_FALSE(term->seek(ir::bytes_ref::nil));
           ASSERT_EQ(term_reader->min(), term->value());
         }
         
         {
           auto term = term_reader->iterator();
           ASSERT_EQ(ir::SeekResult::NOT_FOUND, term->seek_ge(ir::bytes_ref::nil));
           ASSERT_EQ(term_reader->min(), term->value());
         }
       }
       
       /* Here is the structure of blocks:
        *   TERM aaLorem
        *   TERM abaLorem
        *   BLOCK abab ------> Integer
        *                      ...
        *                      ...
        *   TERM abcaLorem
        *   ...
        *
        * Here we seek to "abaN" and since first entry that 
        * is greater than "abaN" is BLOCK entry "abab".
        *   
        * In case of "seek" we end our scan on BLOCK entry "abab",
        * and further "next" cause the skipping of the BLOCK "abab".
        *
        * In case of "seek_next" we also end our scan on BLOCK entry "abab"
        * but furher "next" get us to the TERM "ababInteger" */
       {
         auto seek_term = ir::ref_cast<ir::byte_type>(ir::string_ref("abaN"));
         auto seek_result = ir::ref_cast<ir::byte_type>(ir::string_ref("ababInteger"));

         /* seek exactly to term */
         {
           auto term = term_reader->iterator();
           ASSERT_FALSE(term->seek(seek_term));
           /* we on the BLOCK "abab" */
           ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("abab")), term->value()); 
           ASSERT_TRUE(term->next());
           ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("abcaLorem")), term->value());
         }

         /* seek to term which is equal or greater than current */
         {
           auto term = term_reader->iterator();
           ASSERT_EQ(ir::SeekResult::NOT_FOUND, term->seek_ge(seek_term));
           ASSERT_EQ(seek_result, term->value());

           /* iterate over the rest of the terms */
           auto expected_sorted_term = sorted_terms.find(seek_result);
           ASSERT_NE(sorted_terms.end(), expected_sorted_term);
           for (++expected_sorted_term; term->next(); ++expected_sorted_term) {
             ASSERT_EQ(*expected_sorted_term, term->value());
           }
           ASSERT_FALSE(term->next());
           ASSERT_EQ(sorted_terms.end(), expected_sorted_term);
         }
       }
    }
  }

  void segment_meta_read_write() {
    iresearch::segment_meta meta;
    meta.name = "meta_name";
    meta.docs_count = 453;
    meta.version = 100;

    meta.files.emplace("file1");
    meta.files.emplace("index_file2");
    meta.files.emplace("file3");
    meta.files.emplace("stored_file4");

    // write segment meta
    {
      auto writer = codec()->get_segment_meta_writer();
      writer->write(dir(), meta);
    }

    // read segment meta
    {
      ir::segment_meta read_meta;
      read_meta.name = meta.name;
      read_meta.version = 100;

      auto reader = codec()->get_segment_meta_reader();
      reader->read(dir(), read_meta);
      ASSERT_EQ(meta.codec, read_meta.codec); // codec stays nullptr
      ASSERT_EQ(meta.name, read_meta.name);
      ASSERT_EQ(meta.docs_count, read_meta.docs_count);
      ASSERT_EQ(meta.version, read_meta.version);
      ASSERT_EQ(meta.files, read_meta.files);
    }
  }

  void document_mask_read_write() {
    const std::unordered_set<iresearch::doc_id_t> mask_set = { 1, 4, 5, 7, 10, 12 };
    iresearch::segment_meta meta("_1", nullptr);
    meta.version = 42;

    // write document_mask
    {
      auto writer = codec()->get_document_mask_writer();

      writer->prepare(dir(), meta);
      writer->begin(static_cast<uint32_t>(mask_set.size())); // only 6 values

      for (auto& mask : mask_set) {
        writer->write(mask);
      }

      writer->end();
    }

    // read document_mask
    {
      auto reader = codec()->get_document_mask_reader();
      auto expected = mask_set;

      EXPECT_EQ(true, reader->prepare(dir(), meta));

      auto count = reader->begin();

      EXPECT_EQ(expected.size(), count);

      for (; count > 0; --count) {
        iresearch::doc_id_t mask;

        reader->read(mask);
        EXPECT_EQ(true, expected.erase(mask) != 0);
      }

      EXPECT_EQ(true, expected.empty());
      reader->end();
    }
  }

  void field_meta_read_write() {
    const std::string seg_name("_1");
    std::vector<iresearch::field_meta> fields{
      {"field0", 0, iresearch::flags{ iresearch::offset::type(), iresearch::position::type() }},
      {"field1", 1, iresearch::flags{ iresearch::position::type() }},
      {"field2", 2, iresearch::flags{ iresearch::document::type() }},
      {"field3", 3, iresearch::flags{ iresearch::offset::type(), iresearch::position::type() }},
      {"field4", 4, iresearch::flags{ iresearch::term_meta::type(), iresearch::offset::type() }},
      {"field5", 5, iresearch::flags{ iresearch::increment::type(), iresearch::offset::type() }}
    };

    // write field_meta
    {
      iresearch::flags segment_features;
      for (auto& field : fields) {
        segment_features |= field.features;
      }

      iresearch::flush_state state{};
      state.name = seg_name;
      state.dir = &dir();
      state.fields_count = fields.size();
      state.features = &segment_features;

      field_meta_writer::ptr writer = codec()->get_field_meta_writer();
      writer->prepare(state);
      for (const auto& meta : fields) {
        writer->write(meta.id, meta.name, meta.features);
      }
      writer->end();
    }

    // read field_meta
    {
      iresearch::field_meta_reader::ptr reader = codec()->get_field_meta_reader();
      reader->prepare(dir(), seg_name);
      EXPECT_EQ(fields.size(), reader->begin());
      for (const auto& meta : fields) {
        iresearch::field_meta read;
        reader->read(read);
        EXPECT_EQ(meta, read);
      }
      reader->end();
    }
  }
  
  void stored_fields_read_write_reuse() {
    struct csv_doc_template : delim_doc_generator::doc_template {
      virtual void init() {
        fields_.clear();
        fields_.reserve(2);
        fields_.emplace_back(new tests::templates::string_field("id", false, true));
        fields_.emplace_back(new tests::templates::string_field("name", false, true));
      }

      virtual void value(size_t idx, const std::string& value) {
        auto& field = static_cast<tests::templates::string_field&>(*fields_[idx]);
        field.value(value);
      }
      virtual void end() {}
      virtual void reset() {}
    } doc_template; // two_columns_doc_template 

    tests::delim_doc_generator gen(resource("simple_two_column.csv"), doc_template, ',');

    iresearch::segment_meta seg_1("_1", nullptr);
    iresearch::segment_meta seg_2("_2", nullptr);
    iresearch::segment_meta seg_3("_3", nullptr);

    // write documents 
    {
      auto writer = codec()->get_stored_fields_writer();

      // write 1st segment 
      writer->prepare(dir(), seg_1.name);
      for (const document* doc; seg_1.docs_count < 30000 && (doc = gen.next());) {
        for (auto& field : *doc) {
          writer->write(field);
        }
        writer->end(0);
        ++seg_1.docs_count;
      }
      writer->finish();

      gen.reset();
      writer->reset();
      
      // write 2nd segment 
      writer->prepare(dir(), seg_2.name);
      for (const document* doc; seg_2.docs_count < 30000 && (doc = gen.next());) {
        for (auto& field : *doc) {
          writer->write(field);
        }
        writer->end(nullptr);
        ++seg_2.docs_count;
      }
      writer->finish();

      writer->reset();

      // write 3rd segment
      writer->prepare(dir(), seg_3.name);
      for (const document* doc; seg_3.docs_count < 70000 && (doc = gen.next());) {
        for (auto& field : *doc) {
          writer->write(field);
        }
        writer->end(nullptr);
        ++seg_3.docs_count;
      }
      writer->finish();
    }

    // read documents
    {
      ir::fields_meta fields;
      {
        std::vector<ir::field_meta> src;
        src.emplace_back("id", 0, ir::flags::empty_instance());
        src.emplace_back("name", 1, ir::flags::empty_instance());
        fields = ir::fields_meta(std::move(src), ir::flags());
      }

      std::string expected_id;
      std::string expected_name;
      auto check_document = [&expected_id, &expected_name] (ir::data_input& in) {
        if (ir::read_string<std::string>(in) != expected_id) {
          return false;
        }
        if (ir::read_string<std::string>(in) != expected_name) {
          return false;
        }
        return true;
      };

      // check 1st segment
      {
        iresearch::reader_state state_1{
          codec().get(), &dir(),
          nullptr, &fields,
          &seg_1
        };

        auto reader_1 = codec()->get_stored_fields_reader();
        reader_1->prepare(state_1);

        gen.reset();
        ir::doc_id_t i = 0;
        for (const document* doc; i < seg_1.docs_count && (doc = gen.next());++i) {
          expected_id = doc->get<tests::templates::string_field>(0).value();
          expected_name = doc->get<tests::templates::string_field>(1).value();
          ASSERT_TRUE(reader_1->visit(i, check_document));
        }

        // check 2nd segment (same as 1st)
        iresearch::reader_state state_2{
          codec().get(), &dir(),
          nullptr, &fields,
          &seg_2
        };

        auto reader_2 = codec()->get_stored_fields_reader();
        reader_2->prepare(state_2);

        auto read_document = [&expected_id, &expected_name] (ir::data_input& in) {
          expected_id = ir::read_string<std::string>(in);
          expected_name = ir::read_string<std::string>(in);
          return true;
        };

        // check for equality
        for (ir::doc_id_t i = 0, count = seg_2.docs_count; i < count; ++i) {
          reader_1->visit(i, read_document);
          reader_2->visit(i, check_document);
        }
      }

      // check 3rd segment
      {
        iresearch::reader_state state{
          codec().get(), &dir(),
          nullptr, &fields,
          &seg_3
        };

        auto reader = codec()->get_stored_fields_reader();
        reader->prepare(state);

        ir::doc_id_t i = 0;
        for (const document* doc; i < seg_3.docs_count && (doc = gen.next()); ++i) {
          expected_id = doc->get<tests::templates::string_field>(0).value();
          expected_name = doc->get<tests::templates::string_field>(1).value();
          ASSERT_TRUE(reader->visit(i, check_document));
        }
      }
    }
  }
  
  void columns_meta_read_write() {
    // write meta
    {
      auto writer = codec()->get_column_meta_writer();

      // write segment _1
      writer->prepare(dir(), "_1");
      writer->write("_1_column1", 1);
      writer->write("_1_column2", 2);
      writer->write("_1_column0", 0);
      writer->flush();
      
      // write segment _2
      writer->prepare(dir(), "_2");
      writer->write("_2_column2", 2);
      writer->write("_2_column1", 1);
      writer->write("_2_column0", 0);
      writer->flush();
    }

    // read meta from segment _1
    {
      auto reader = codec()->get_column_meta_reader();
      ASSERT_TRUE(reader->prepare(dir(), "_1"));
     
      iresearch::column_meta meta;
      ASSERT_TRUE(reader->read(meta));
      ASSERT_EQ("_1_column1", meta.name);
      ASSERT_EQ(1, meta.id);
      ASSERT_TRUE(reader->read(meta));
      ASSERT_EQ("_1_column2", meta.name);
      ASSERT_EQ(2, meta.id);
      ASSERT_TRUE(reader->read(meta));
      ASSERT_EQ("_1_column0", meta.name);
      ASSERT_EQ(0, meta.id);
      ASSERT_FALSE(reader->read(meta));
    }
    
    // read meta from segment _2
    {
      auto reader = codec()->get_column_meta_reader();
      ASSERT_TRUE(reader->prepare(dir(), "_2"));
     
      iresearch::column_meta meta;
      ASSERT_TRUE(reader->read(meta));
      ASSERT_EQ("_2_column2", meta.name);
      ASSERT_EQ(2, meta.id);
      ASSERT_TRUE(reader->read(meta));
      ASSERT_EQ("_2_column1", meta.name);
      ASSERT_EQ(1, meta.id);
      ASSERT_TRUE(reader->read(meta));
      ASSERT_EQ("_2_column0", meta.name);
      ASSERT_EQ(0, meta.id);
      ASSERT_FALSE(reader->read(meta));
    }
  }
  
  void columns_read_write() {
    ir::fields_data fdata;
    ir::fields_meta fields;

    struct string_serializer : iresearch::serializer {
      bool write(data_output& out) const {
        iresearch::write_string(out, value);
        return true;
      }

      std::string value;
    } string_writer;
    
    struct invalid_serializer : iresearch::serializer {
      bool write(data_output& out) const {
        return false;
      }
    } invalid_writer;

    iresearch::field_id segment0_field0_id;
    iresearch::field_id segment0_field1_id;
    iresearch::field_id segment0_field2_id;
    iresearch::field_id segment1_field0_id;
    iresearch::field_id segment1_field1_id;
    iresearch::field_id segment1_field2_id;

    iresearch::segment_meta meta0("_1", nullptr);
    meta0.version = 42;
    meta0.docs_count = 89;
    
    iresearch::segment_meta meta1("_2", nullptr);
    meta1.version = 23;
    meta1.docs_count = 115;

    // read attributes from empty directory 
    {
      iresearch::reader_state rs;
      rs.codec = codec().get();
      rs.dir = &dir();
      rs.docs_mask = nullptr;
      rs.fields = &fields;
      rs.meta = &meta1;

      auto reader = codec()->get_columnstore_reader();
      ASSERT_FALSE(reader->prepare(rs)); // no attributes found

      size_t calls_count = 0;
      iresearch::columnstore_reader::value_reader_f value_reader = [&calls_count] (iresearch::data_input& in) {
        ++calls_count;
        return true;
      };

      // try to read invalild column
      {
        auto column = reader->values(iresearch::type_limits<iresearch::type_t::field_id_t>::invalid());
        calls_count = 0;
        ASSERT_FALSE(column(0, value_reader));
        ASSERT_EQ(0, calls_count);
        calls_count = 0;
        ASSERT_FALSE(column(2, value_reader));
        ASSERT_EQ(0, calls_count);
        calls_count = 0;
        ASSERT_FALSE(column(3, value_reader));
        ASSERT_EQ(0, calls_count);
        calls_count = 0;
        ASSERT_FALSE(column(56, value_reader));
        ASSERT_EQ(0, calls_count);
      }
    }
     
    // write columns values
    auto writer = codec()->get_columnstore_writer();

    // write _1 segment
    {
      ASSERT_TRUE(writer->prepare(dir(), meta0.name));
      
      auto field0 = writer->push_column();
      segment0_field0_id = field0.first;
      auto& field0_writer = field0.second;
      ASSERT_EQ(0, segment0_field0_id);
      auto field1 = writer->push_column();
      segment0_field1_id = field1.first;
      auto& field1_writer = field1.second;
      ASSERT_EQ(1, segment0_field1_id);
      auto field2 = writer->push_column();
      segment0_field2_id = field2.first;
      auto& field2_writer = field2.second;
      ASSERT_EQ(2, segment0_field2_id);

      string_writer.value = "field0_doc0";
      ASSERT_TRUE(field0_writer(0, string_writer)); // doc==0, column==field0

      // multivalued attribute
      string_writer.value = "field1_doc0";
      ASSERT_TRUE(field1_writer(0, string_writer)); // doc==0, column==field1
      string_writer.value = "field1_doc0_1";
      ASSERT_TRUE(field1_writer(0, string_writer)); // doc==0, column==field1

      string_writer.value = "field2_doc0";
      ASSERT_FALSE(field2_writer(0, invalid_writer)); // doc==0, column==field2

      string_writer.value = "field0_doc1";
      ASSERT_FALSE(field0_writer(1, invalid_writer)); // doc==1, column==field0

      string_writer.value = "field0_doc2";
      ASSERT_TRUE(field0_writer(2, string_writer)); // doc==2, colum==field0

      string_writer.value = "field0_doc33";
      ASSERT_TRUE(field0_writer(33, string_writer)); // doc==33, colum==field0

      writer->flush();
    }

    // write _2 segment, reuse writer
    {
      ASSERT_TRUE(writer->prepare(dir(), meta1.name));
      
      auto field0 = writer->push_column();
      segment1_field0_id = field0.first;
      auto& field0_writer = field0.second;
      ASSERT_EQ(0, segment1_field0_id);
      auto field1 = writer->push_column();
      segment1_field1_id = field1.first;
      auto& field1_writer = field1.second;
      ASSERT_EQ(1, segment1_field1_id);
      auto field2 = writer->push_column();
      segment1_field2_id = field2.first;
      auto& field2_writer = field2.second;
      ASSERT_EQ(2, segment1_field2_id);

      string_writer.value = "segment_2_field3_doc0";
      ASSERT_TRUE(field2_writer(0, string_writer)); // doc==0, column==field3

      // multivalued attribute
      string_writer.value = "segment_2_field1_doc0";
      ASSERT_TRUE(field0_writer(0, string_writer)); // doc==0, column==field1

      string_writer.value = "segment_2_field2_doc0";
      ASSERT_FALSE(field1_writer(0, invalid_writer)); // doc==0, column==field2

      string_writer.value = "segment_2_field0_doc1";
      ASSERT_FALSE(field2_writer(1, invalid_writer)); // doc==1, column==field3

      string_writer.value = "segment_2_field1_doc12";
      ASSERT_TRUE(field0_writer(12, string_writer)); // doc==12, colum==field1

      string_writer.value = "segment_2_field3_doc23";
      ASSERT_TRUE(field2_writer(23, string_writer)); // doc==23, colum==field3

      writer->flush();
    }

    // read columns values from segment _1
    {
      iresearch::reader_state rs;
      rs.codec = codec().get();
      rs.dir = &dir();
      rs.docs_mask = nullptr;
      rs.fields = &fields;
      rs.meta = &meta0;

      auto reader = codec()->get_columnstore_reader();
      ASSERT_TRUE(reader->prepare(rs));
      
      // try to read invalild column
      {
        size_t calls_count = 0;
        iresearch::columnstore_reader::value_reader_f value_reader = [&calls_count] (iresearch::data_input& in) {
          ++calls_count;
          return true;
        };

        auto column = reader->values(ir::type_limits<ir::type_t::field_id_t>::invalid());
        calls_count = 0;
        ASSERT_FALSE(column(0, value_reader));
        ASSERT_EQ(0, calls_count);
        calls_count = 0;
        ASSERT_FALSE(column(2, value_reader));
        ASSERT_EQ(0, calls_count);
        calls_count = 0;
        ASSERT_FALSE(column(3, value_reader));
        ASSERT_EQ(0, calls_count);
        calls_count = 0;
        ASSERT_FALSE(column(56, value_reader));
        ASSERT_EQ(0, calls_count);
      }
      
      // visit field0 values (not cached)
      {
        std::unordered_map<std::string, iresearch::doc_id_t> expected_values = {
          {"field0_doc0", 0},
          {"field0_doc2", 2},
          {"field0_doc33", 33}
        };

        auto visitor = [&expected_values] (iresearch::doc_id_t doc, data_input& in) {
          const auto actual_value = iresearch::read_string<std::string>(in);

          auto it = expected_values.find(actual_value);
          if (it == expected_values.end()) {
            // can't find value
            return false;
          }

          if (it->second != doc) {
            // wrong document
            return false;
          }

          expected_values.erase(it);
          return true;
        };

        ASSERT_TRUE(reader->visit(segment0_field0_id, visitor));
        ASSERT_TRUE(expected_values.empty());
      }
      
      // partailly visit field0 values (not cached)
      {
        std::unordered_map<std::string, iresearch::doc_id_t> expected_values = {
          {"field0_doc0", 0},
          {"field0_doc2", 2},
          {"field0_doc33", 33}
        };

        size_t calls_count = 0;
        auto visitor = [&expected_values, &calls_count] (iresearch::doc_id_t doc, data_input& in) {
          ++calls_count;

          if (calls_count > 2) {
            // break the loop
            return false;
          }

          const auto actual_value = iresearch::read_string<std::string>(in);

          auto it = expected_values.find(actual_value);
          if (it == expected_values.end()) {
            // can't find value
            return false;
          }

          if (it->second != doc) {
            // wrong document
            return false;
          }

          expected_values.erase(it);
          return true;
        };

        ASSERT_FALSE(reader->visit(segment0_field0_id, visitor));
        ASSERT_FALSE(expected_values.empty());
        ASSERT_EQ(1, expected_values.size());
        ASSERT_NE(expected_values.end(), expected_values.find("field0_doc33"));
      }

      // check field0
      {
        std::string expected_value;
        size_t calls_count = 0;
        iresearch::columnstore_reader::value_reader_f value_reader = [&calls_count, &expected_value] (iresearch::data_input& in) {
          ++calls_count;
          if (expected_value != iresearch::read_string<std::string>(in)) {
            return false;
          }

          iresearch::byte_type b;
          if (in.read_bytes(&b, 1)) {
            // read more than we allowed 
            return false;
          }

          return true;
        };

        iresearch::columnstore_reader::value_reader_f invalid_value_reader = [&expected_value] (iresearch::data_input& in) {
          expected_value.reserve(in.read_vlong()); // partial reading
          return false;
        };

        auto column = reader->values(segment0_field0_id);

        // read (not cached)
        {
          expected_value = "field0_doc0";
          calls_count = 0;
          ASSERT_TRUE(column(0, value_reader)); // check doc==0, column==field0
          ASSERT_EQ(1, calls_count);
          expected_value = "field0_doc0";
          calls_count = 0;
          ASSERT_FALSE(column(0, invalid_value_reader)); // reader returns false
          ASSERT_EQ(0, calls_count);
          expected_value = "field0_doc0";
          calls_count = 0;
          ASSERT_FALSE(column(5, value_reader)); // doc without value in field0
          ASSERT_EQ(0, calls_count);
          expected_value = "field0_doc33";
          calls_count = 0;
          ASSERT_TRUE(column(33, value_reader)); // check doc==33, column==field0
          ASSERT_EQ(1, calls_count);
        }
        
        // read (cached)
        {
          expected_value = "field0_doc0";
          calls_count = 0;
          ASSERT_TRUE(column(0, value_reader)); // check doc==0, column==field0
          ASSERT_EQ(1, calls_count);
          expected_value = "field0_doc0";
          calls_count = 0;
          ASSERT_FALSE(column(0, invalid_value_reader)); // reader returns false
          ASSERT_EQ(0, calls_count);
          expected_value = "field0_doc0";
          calls_count = 0;
          ASSERT_FALSE(column(5, value_reader)); // doc without value in field0
          ASSERT_EQ(0, calls_count);
          expected_value = "field0_doc33";
          calls_count = 0;
          ASSERT_TRUE(column(33, value_reader)); // check doc==33, column==field0
          ASSERT_EQ(1, calls_count);
        }
      }
      
      // visit field0 values (cached)
      {
        std::unordered_map<std::string, iresearch::doc_id_t> expected_values = {
          {"field0_doc0", 0},
          {"field0_doc2", 2},
          {"field0_doc33", 33}
        };

        auto visitor = [&expected_values] (iresearch::doc_id_t doc, data_input& in) {
          const auto actual_value = iresearch::read_string<std::string>(in);

          auto it = expected_values.find(actual_value);
          if (it == expected_values.end()) {
            // can't find value
            return false;
          }

          if (it->second != doc) {
            // wrong document
            return false;
          }

          expected_values.erase(it);
          return true;
        };

        ASSERT_TRUE(reader->visit(segment0_field0_id, visitor));
        ASSERT_TRUE(expected_values.empty());
      }
      
      // check field1 (multiple values per document)
      {
        std::string expected_value1;
        std::string expected_value2;
        size_t calls_count = 0;
        iresearch::columnstore_reader::value_reader_f value_reader = [&calls_count, &expected_value1, &expected_value2] (iresearch::data_input& in) {
          ++calls_count;

          if (expected_value1 != iresearch::read_string<std::string>(in)) {
            return false;
          }
          
          if (expected_value2 != iresearch::read_string<std::string>(in)) {
            return false;
          }

          iresearch::byte_type b;
          if (in.read_bytes(&b, 1)) {
            // read more than we allowed 
            return false;
          }

          return true;
        };


        auto column = reader->values(segment0_field1_id);
        
        // read compound column value
        // check doc==0, column==field1
        expected_value1 = "field1_doc0";
        expected_value2 = "field1_doc0_1";
        calls_count = 0;
        ASSERT_TRUE(column(0, value_reader)); 
        ASSERT_EQ(1, calls_count);

        // read by invalid key
        calls_count = 0;
        ASSERT_FALSE(column(iresearch::type_limits<iresearch::type_t::doc_id_t>::eof(), value_reader)); 
        ASSERT_EQ(0, calls_count);
      }
    }

    // read columns values from segment _2
    {
      iresearch::reader_state rs;
      rs.codec = codec().get();
      rs.dir = &dir();
      rs.docs_mask = nullptr;
      rs.fields = &fields;
      rs.meta = &meta1;

      auto reader = codec()->get_columnstore_reader();
      ASSERT_TRUE(reader->prepare(rs));

      std::string expected_value;
      size_t calls_count = 0;
      iresearch::columnstore_reader::value_reader_f value_reader = [&calls_count, &expected_value] (iresearch::data_input& in) {
        ++calls_count;
        if (expected_value != iresearch::read_string<std::string>(in)) {
          return false;
        }
        
        iresearch::byte_type b;
        if (in.read_bytes(&b, 1)) {
          // read more than we allowed 
          return false;
        }

        return true;
      };
      
      iresearch::columnstore_reader::value_reader_f invalid_value_reader = [&expected_value] (iresearch::data_input& in) {
        expected_value.reserve(in.read_vlong()); // partial reading
        return false;
      };

      // try to read invalild column
      {
        auto column = reader->values(ir::type_limits<ir::type_t::field_id_t>::invalid());
        calls_count = 0;
        ASSERT_FALSE(column(0, value_reader));
        ASSERT_EQ(0, calls_count);
        calls_count = 0;
        ASSERT_FALSE(column(2, value_reader));
        ASSERT_EQ(0, calls_count);
        calls_count = 0;
        ASSERT_FALSE(column(3, value_reader));
        ASSERT_EQ(0, calls_count);
        calls_count = 0;
        ASSERT_FALSE(column(56, value_reader));
        ASSERT_EQ(0, calls_count);
      }

      // check field0
      {
        auto column = reader->values(0);
        expected_value = "segment_2_field1_doc0";
        calls_count = 0;
        ASSERT_TRUE(column(0, value_reader)); // check doc==0, column==field0
        ASSERT_EQ(1, calls_count);
        expected_value = "segment_2_field1_doc0";
        calls_count = 0;
        ASSERT_FALSE(column(0, invalid_value_reader)); // reader returns false
        ASSERT_EQ(0, calls_count);
        expected_value = "segment_2_field1_doc12";
        calls_count = 0;
        ASSERT_TRUE(column(12, value_reader)); // check doc==12, column==field1
        ASSERT_EQ(1, calls_count);
        calls_count = 0;
        ASSERT_FALSE(column(5, value_reader)); // doc without value in field0
        ASSERT_EQ(0, calls_count);
      }
    }
  }

  void stored_fields_read_write() {
    struct Value {
      enum class Type {
        String, Binary, Double
      };

      Value(const ir::string_ref& name, const ir::string_ref& value) 
        : name(name), value(value), type(Type::String) {
      }
      
      Value(const ir::string_ref& name, const ir::bytes_ref& value) 
        : name(name), value(value), type(Type::Binary) {
      }
      
      Value(const ir::string_ref& name, double_t value) 
        : name(name), value(value), type(Type::Double) {
      }
      
      ir::string_ref name;
      struct Rep {
        Rep(const ir::string_ref& value) : sValue(value) {}
        Rep(const ir::bytes_ref& value) : binValue(value) {}
        Rep(double_t value) : dblValue(value) {}
        ~Rep() { }
        
        ir::string_ref sValue;
        ir::bytes_ref binValue;
        double_t dblValue;
      } value;
      Type type;
    };

    std::deque<Value> values;
    tests::json_doc_generator gen(
      resource("simple_sequential_33.json"),
      [&values](tests::document& doc, const std::string& name, const tests::json::json_value& data) {
      if (data.quoted) {
        doc.add(new templates::string_field(
          ir::string_ref(name),
          ir::string_ref(data.value),
          true, true));
        
        auto& field = (doc.end() - 1).as<templates::string_field>();
        values.emplace_back(field.name(), field.value());
      } else if ("null" == data.value) {
        doc.add(new tests::binary_field());
        auto& field = (doc.end() - 1).as<tests::binary_field>();
        field.name(iresearch::string_ref(name));
        field.value(ir::null_token_stream::value_null());
        values.emplace_back(field.name(), field.value());
      } else if ("true" == data.value) {
        doc.add(new tests::binary_field());
        auto& field = (doc.end() - 1).as<tests::binary_field>();
        field.name(iresearch::string_ref(name));
        field.value(ir::boolean_token_stream::value_true());
        values.emplace_back(field.name(), field.value());
      } else if ("false" == data.value) {
        doc.add(new tests::binary_field());
        auto& field = (doc.end() - 1).as<tests::binary_field>();
        field.name(iresearch::string_ref(name));
        field.value(ir::boolean_token_stream::value_true());
        values.emplace_back(field.name(), field.value());
      } else {
        char* czSuffix;
        double dValue = strtod(data.value.c_str(), &czSuffix);

        // 'value' can be interpreted as a double
        if (!czSuffix[0]) {
          doc.add(new tests::double_field());
          auto& field = (doc.end() - 1).as<tests::double_field>();
          field.name(iresearch::string_ref(name));
          field.value(dValue);
          values.emplace_back(field.name(), field.value());
        }
      }
    });

    ir::fields_data fdata;
    ir::fields_meta fields;

    iresearch::segment_meta meta("_1", nullptr);
    meta.version = 42;

    struct meta_serializer : ir::serializer {
      bool write(ir::data_output& out) const {
        out.write_vint(meta->id);
        return true;
      }
      const ir::field_meta* meta;
    } serializer;

    // write stored documents
    {
      std::vector<iresearch::field_meta> fields_src;
      stored_fields_writer::ptr writer = codec()->get_stored_fields_writer();
      writer->prepare(dir(), meta.name);
      for (const document* doc; doc = gen.next();) {
        for (const auto& field : *doc) {
          const auto& field_meta = fdata.get(field.name());
          fields_src.push_back(field_meta.meta());
          {
            serializer.meta = &field_meta.meta();
            writer->write(serializer);
          }

          auto field_serializer = field.serializer();
          ASSERT_NE(nullptr, field_serializer);
          writer->write(*field_serializer);
        }
        writer->end(nullptr);
        ++meta.docs_count;
      }
      writer->finish();

      fields = ir::fields_meta(std::move(fields_src), ir::flags());
    }

    gen.reset();

    // read stored documents
    {
      iresearch::document_mask mask;
      iresearch::reader_state state{
        codec().get(), &dir(),
        &mask, &fields,
        &meta
      };

      iresearch::stored_fields_reader::ptr reader = codec()->get_stored_fields_reader();
      reader->prepare(state);

      auto visitor = [&fields, &values] (data_input& in) {
        auto& expected_field = values.front();

        // read field meta
        auto field = fields.find(in.read_vint());
        if (field->name != expected_field.name) {
          return false;
        }

        switch (expected_field.type) {
          case Value::Type::String: {
            auto value = ir::read_string<std::string>(in);
            if (expected_field.value.sValue != ir::string_ref(value)) {
              return false;
            }
          } break;
          case Value::Type::Binary: {
            auto value = ir::read_string<ir::bstring>(in);
            if (expected_field.value.binValue != value) {
              return false;
            }
          } break;
          case Value::Type::Double: {
            auto value = ir::read_zvdouble(in);
            if (expected_field.value.dblValue != value) {
              return false;
            }
          } break;
          default:
            return false;
        };

        values.pop_front();
        return true;
      };

      for (uint64_t i = 0, docs_count = meta.docs_count; i < docs_count; ++i) {
        ASSERT_TRUE(reader->visit(iresearch::doc_id_t(i), visitor));
      }
    }
  }
}; // format_test_case_base

} // tests

#endif // IRESEARCH_FORMAT_TEST_CASE_BASE
