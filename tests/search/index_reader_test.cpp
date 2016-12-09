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

#include "tests_shared.hpp"
#include "index/index_reader.hpp"
#include "formats/formats_10.hpp"
#include "index/index_writer.hpp"
#include "store/memory_directory.hpp"
#include "index/doc_generator.hpp"
#include "index/index_tests.hpp"
#include "utils/version_utils.hpp"
#include "utils/utf8_path.hpp"

namespace ir = iresearch;

NS_LOCAL

ir::format* codec0;
ir::format* codec1;

ir::format::ptr get_codec0() { return ir::format::ptr(codec0, [](ir::format*)->void{}); }
ir::format::ptr get_codec1() { return ir::format::ptr(codec1, [](ir::format*)->void{}); }

NS_END

// ----------------------------------------------------------------------------
// --SECTION--                                           Composite index reader
// ----------------------------------------------------------------------------

TEST(directory_reader_test, open_empty_directory) {
  ir::memory_directory dir;
  ir::version10::format codec;
  iresearch::format::ptr codec_ptr(&codec, [](iresearch::format*)->void{});

  /* no index */
  ASSERT_THROW(ir::directory_reader::open(dir, codec_ptr), ir::index_not_found);
}

TEST(directory_reader_test, open_empty_index) {
  ir::memory_directory dir;
  ir::version10::format codec;
  iresearch::format::ptr codec_ptr(&codec, [](iresearch::format*)->void{});

  /* create empty index */
  ir::index_writer::make(dir, codec_ptr, ir::OM_CREATE)->commit();

  /* open reader */
  auto rdr = ir::directory_reader::open(dir, codec_ptr);
  ASSERT_NE(nullptr, rdr);
  ASSERT_EQ(0, rdr->docs_max());
  ASSERT_EQ(0, rdr->docs_count());  
  ASSERT_EQ(0, rdr->size());
  ASSERT_EQ(rdr->end(), rdr->begin());
}

TEST(directory_reader_test, open_newest_index) {
  struct test_index_meta_reader: public ir::index_meta_reader {
    virtual bool last_segments_file(const ir::directory&, std::string& out) const override { 
      out = segments_file;
      return true;
    }
    virtual void read(const ir::directory& dir, ir::index_meta& meta, const ir::string_ref& filename = ir::string_ref::nil) override {
      read_file.assign(filename.c_str(), filename.size());
    };
    std::string segments_file;
    std::string read_file;
  };
  class test_format: public ir::format {
   public:
    ir::index_meta_reader::ptr index_meta_reader;
    test_format(const ir::format::type_id& type): ir::format(type) {}
    virtual ir::index_meta_writer::ptr get_index_meta_writer() const override { return nullptr; }
    virtual ir::index_meta_reader::ptr get_index_meta_reader() const override { return index_meta_reader; }
    virtual ir::segment_meta_writer::ptr get_segment_meta_writer() const override { return nullptr; }
    virtual ir::segment_meta_reader::ptr get_segment_meta_reader() const override { return nullptr; }
    virtual ir::document_mask_writer::ptr get_document_mask_writer() const override { return nullptr; }
    virtual ir::document_mask_reader::ptr get_document_mask_reader() const override { return nullptr; }
    virtual ir::field_meta_reader::ptr get_field_meta_reader() const override { return nullptr; }
    virtual ir::field_meta_writer::ptr get_field_meta_writer() const override { return nullptr; }
    virtual ir::field_writer::ptr get_field_writer(bool volatile_attributes = false) const override { return nullptr; }
    virtual ir::field_reader::ptr get_field_reader() const override { return nullptr; }
    virtual ir::stored_fields_writer::ptr get_stored_fields_writer() const override { return nullptr; }
    virtual ir::stored_fields_reader::ptr get_stored_fields_reader() const override { return nullptr; }
    virtual ir::column_meta_writer::ptr get_column_meta_writer() const override { return nullptr; }
    virtual ir::column_meta_reader::ptr get_column_meta_reader() const override { return nullptr; }
    virtual ir::columnstore_writer::ptr get_columnstore_writer() const override { return nullptr; }
    virtual ir::columnstore_reader::ptr get_columnstore_reader() const override { return nullptr; }
  };
  ir::format::type_id test_format0_type("test_format0");
  ir::format::type_id test_format1_type("test_format1");
  test_format test_codec0(test_format0_type);
  test_format test_codec1(test_format1_type);
  ir::format_registrar test_format0_registrar(test_format0_type, &get_codec0);
  ir::format_registrar test_format1_registrar(test_format1_type, &get_codec1);
  test_index_meta_reader test_reader0;
  test_index_meta_reader test_reader1;

  test_codec0.index_meta_reader = ir::index_meta_reader::ptr(&test_reader0, [](ir::index_meta_reader*){});
  test_codec1.index_meta_reader = ir::index_meta_reader::ptr(&test_reader1, [](ir::index_meta_reader*){});
  codec0 = &test_codec0;
  codec1 = &test_codec1;

  ir::memory_directory dir;
  std::string codec0_file0("0seg0");
  std::string codec0_file1("0seg1");
  std::string codec1_file0("1seg0");
  std::string codec1_file1("1seg1");

  ASSERT_FALSE(!dir.create(codec0_file0));
  ASSERT_FALSE(!dir.create(codec1_file0));
  ir::sleep_ms(1000); // wait 1 sec to ensure index file timestamps differ
  ASSERT_FALSE(!dir.create(codec0_file1));
  ASSERT_FALSE(!dir.create(codec1_file1));

  test_reader0.read_file.clear();
  test_reader1.read_file.clear();
  test_reader0.segments_file = codec0_file0;
  test_reader1.segments_file = codec1_file1;
  ir::directory_reader::open(dir);
  ASSERT_TRUE(test_reader0.read_file.empty()); // file not read from codec0
  ASSERT_EQ(codec1_file1, test_reader1.read_file);  // check file read from codec1

  test_reader0.read_file.clear();
  test_reader1.read_file.clear();
  test_reader0.segments_file = codec0_file1;
  test_reader1.segments_file = codec1_file0;
  ir::directory_reader::open(dir);
  ASSERT_EQ(codec0_file1, test_reader0.read_file); // check file read from codec0
  ASSERT_TRUE(test_reader1.read_file.empty()); // file not read from codec1

  codec0 = nullptr;
  codec1 = nullptr;
}

TEST(directory_reader_test, open) {
  tests::json_doc_generator gen(
    test_base::resource("simple_sequential.json"),
    [] (tests::document& doc, const std::string& name, const tests::json::json_value& data) {
    if (data.quoted) {
      doc.add(new tests::templates::string_field(
        ir::string_ref(name),
        ir::string_ref(data.value),
        true, true));
    }
  });

  tests::document const* doc1 = gen.next();
  tests::document const* doc2 = gen.next();
  tests::document const* doc3 = gen.next();
  tests::document const* doc4 = gen.next();
  tests::document const* doc5 = gen.next();
  tests::document const* doc6 = gen.next();
  tests::document const* doc7 = gen.next();
  tests::document const* doc8 = gen.next();
  tests::document const* doc9 = gen.next(); 

  ir::memory_directory dir;
  ir::version10::format codec;
  iresearch::format::ptr codec_ptr(&codec, [](iresearch::format*)->void{});

  // create index
  {
    // open writer
    auto writer = ir::index_writer::make(dir, codec_ptr, ir::OM_CREATE);

    // add first segment
    ASSERT_TRUE(writer->insert(doc1->begin(), doc1->end()));
    ASSERT_TRUE(writer->insert(doc2->begin(), doc2->end()));
    ASSERT_TRUE(writer->insert(doc3->begin(), doc3->end()));
    writer->commit();

    // add second segment
    ASSERT_TRUE(writer->insert(doc4->begin(), doc4->end()));
    ASSERT_TRUE(writer->insert(doc5->begin(), doc5->end()));
    ASSERT_TRUE(writer->insert(doc6->begin(), doc6->end()));
    ASSERT_TRUE(writer->insert(doc7->begin(), doc7->end()));
    writer->commit();

    // add third segment
    ASSERT_TRUE(writer->insert(doc8->begin(), doc8->end()));
    ASSERT_TRUE(writer->insert(doc9->begin(), doc9->end()));
    writer->commit();
  }

  // open reader
  auto rdr = ir::directory_reader::open(dir, codec_ptr);
  ASSERT_NE(nullptr, rdr);
  ASSERT_EQ(9, rdr->docs_max());
  ASSERT_EQ(9, rdr->docs_count());  
  ASSERT_EQ(3, rdr->size());

  // check subreaders
  auto sub = rdr->begin();

  const iresearch::string_ref expected_name = "name";
  iresearch::string_ref expected_value;
  size_t calls_count = 0;
  iresearch::index_reader::document_visitor_f visitor = [&calls_count, &expected_value, &expected_name](
    const ir::field_meta& field, ir::data_input& in
  ) {
    ++calls_count;

    if (field.name != expected_name) {
      ir::read_string<std::string>(in); // skip string
      return true;
    }

    auto value = ir::read_string<std::string>(in);
    if (value != expected_value) {
      return false;
    }

    return true;
  };

  // first segment
  {
    ASSERT_NE(rdr->end(), sub);
    ASSERT_EQ(1, sub->size());
    ASSERT_EQ(3, sub->docs_max());
    ASSERT_EQ(3, sub->docs_count());

    // read documents
    expected_value = "A"; // 'name' value in doc1 
    ASSERT_TRUE(sub->document(1, visitor)); 
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(sub->document(2, visitor)); 
    expected_value = "C"; // 'name' value in doc3
    ASSERT_TRUE(sub->document(3, visitor)); 

    // read invalid document
    calls_count = 0;
    ASSERT_FALSE(sub->document(4, visitor));
    ASSERT_EQ(0, calls_count);
  }

  // second segment
  {
    ++sub;
    ASSERT_NE(rdr->end(), sub);
    ASSERT_EQ(1, sub->size());
    ASSERT_EQ(4, sub->docs_max());
    ASSERT_EQ(4, sub->docs_count());

    // read documents
    expected_value = "D"; // 'name' value in doc4
    ASSERT_TRUE(sub->document(1, visitor)); 
    expected_value = "E"; // 'name' value in doc5
    ASSERT_TRUE(sub->document(2, visitor)); 
    expected_value = "F"; // 'name' value in doc6
    ASSERT_TRUE(sub->document(3, visitor)); 
    expected_value = "G"; // 'name' value in doc7
    ASSERT_TRUE(sub->document(4, visitor)); 

    // read invalid document
    calls_count = 0;
    ASSERT_FALSE(sub->document(5, visitor));
    ASSERT_EQ(0, calls_count);
  }

  // third segment
  {
    ++sub;
    ASSERT_NE(rdr->end(), sub);
    ASSERT_EQ(1, sub->size());
    ASSERT_EQ(2, sub->docs_max());
    ASSERT_EQ(2, sub->docs_count());

    // read documents
    expected_value = "H"; // 'name' value in doc8
    ASSERT_TRUE(sub->document(1, visitor)); 
    expected_value = "I"; // 'name' value in doc9
    ASSERT_TRUE(sub->document(2, visitor)); 

    // read invalid document
    calls_count = 0;
    ASSERT_FALSE(sub->document(3, visitor));
    ASSERT_EQ(0, calls_count);
  }

  ++sub;
  ASSERT_EQ(rdr->end(), sub);

  // read documents
  expected_value = "A"; // 'name' value in doc1
  ASSERT_TRUE(rdr->document(1, visitor));
  expected_value = "B"; // 'name' value in doc2
  ASSERT_TRUE(rdr->document(2, visitor));
  expected_value = "C"; // 'name' value in doc3
  ASSERT_TRUE(rdr->document(3, visitor));
  expected_value = "D"; // 'name' value in doc4
  ASSERT_TRUE(rdr->document(4, visitor));
  expected_value = "E"; // 'name' value in doc5
  ASSERT_TRUE(rdr->document(5, visitor));
  expected_value = "F"; // 'name' value in doc6
  ASSERT_TRUE(rdr->document(6, visitor));
  expected_value = "G"; // 'name' value in doc7
  ASSERT_TRUE(rdr->document(7, visitor));
  expected_value = "H"; // 'name' value in doc8
  ASSERT_TRUE(rdr->document(8, visitor));
  expected_value = "I"; // 'name' value in doc9
  ASSERT_TRUE(rdr->document(9, visitor));
}

// ----------------------------------------------------------------------------
// --SECTION--                                                   Segment reader 
// ----------------------------------------------------------------------------

TEST(segment_reader_test, open_invalid_segment) {
  ir::memory_directory dir;
  ir::version10::format codec;
  ir::format::ptr codec_ptr(&codec, [](ir::format*)->void{});

  /* open invalid segment */
  {
    ir::segment_meta meta;
    meta.codec = codec_ptr;
    meta.name = "invalid_segment_name";

    ASSERT_THROW(ir::segment_reader::open(dir, meta), ir::detailed_io_error);
  }
}

TEST(segment_reader_test, open) {
  tests::json_doc_generator gen(
    test_base::resource("simple_sequential.json"),
    &tests::generic_json_field_factory);
  tests::document const* doc1 = gen.next();
  tests::document const* doc2 = gen.next();
  tests::document const* doc3 = gen.next();
  tests::document const* doc4 = gen.next();
  tests::document const* doc5 = gen.next();
  tests::document const* doc6 = gen.next();
  tests::document const* doc7 = gen.next();
  tests::document const* doc8 = gen.next();
  tests::document const* doc9 = gen.next(); 

  ir::memory_directory dir;
  ir::version10::format codec;
  ir::format::ptr codec_ptr(&codec, [](ir::format*)->void{});
  {
    // open writer
    auto writer = ir::index_writer::make(dir, codec_ptr, ir::OM_CREATE);

    // add first segment
    ASSERT_TRUE(writer->insert(doc1->begin(), doc1->end()));
    ASSERT_TRUE(writer->insert(doc2->begin(), doc2->end()));
    ASSERT_TRUE(writer->insert(doc3->begin(), doc3->end()));
    ASSERT_TRUE(writer->insert(doc4->begin(), doc4->end()));
    ASSERT_TRUE(writer->insert(doc5->begin(), doc5->end()));
    writer->commit();
  }

  // check segment
  {
    ir::segment_meta meta;
    meta.codec = codec_ptr;
    meta.docs_count = 5;
    meta.name = "_1";
    meta.version = IRESEARCH_VERSION;

    auto rdr = ir::segment_reader::open(dir, meta);
    ASSERT_NE(nullptr, rdr);
    ASSERT_EQ(1, rdr->size());
    ASSERT_EQ(meta.docs_count, rdr->docs_max());
    ASSERT_EQ(meta.docs_count, rdr->docs_count());

    std::unordered_map<iresearch::string_ref, std::function<void(iresearch::data_input&)>> codecs{
      { "name", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
      { "same", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
      { "duplicated", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
      { "prefix", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
      { "seq", [](iresearch::data_input& in)->void{ iresearch::read_zvdouble(in); } },
      { "value", [](iresearch::data_input& in)->void{ iresearch::read_zvdouble(in); } },
    };

    const iresearch::string_ref expected_name = "name";
    size_t calls_count = 0;
    iresearch::string_ref expected_value;
    iresearch::index_reader::document_visitor_f visitor = [&calls_count, &codecs, &expected_value, &expected_name](
      const iresearch::field_meta& field, iresearch::data_input& in
    ) {
      ++calls_count;
      if (field.name != expected_name) {
        auto it = codecs.find(field.name);
        if (codecs.end() == it) {
          return false; // can't find codec
        }
        it->second(in); // skip field
        return true;
      }

      auto value = ir::read_string<std::string>(in);
      if (value != expected_value) {
        return false;
      }

      return true;
    };

    // read documents
    expected_value = "A"; // 'name' value in doc1
    ASSERT_TRUE(rdr->document(1, visitor));
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(rdr->document(2, visitor));
    expected_value = "C"; // 'name' value in doc3
    ASSERT_TRUE(rdr->document(3, visitor));
    expected_value = "D"; // 'name' value in doc4
    ASSERT_TRUE(rdr->document(4, visitor));
    expected_value = "E"; // 'name' value in doc5
    ASSERT_TRUE(rdr->document(5, visitor));
    
    calls_count = 0;
    ASSERT_FALSE(rdr->document(6, visitor)); // read invalid document 
    ASSERT_EQ(0, calls_count);

    // check iterators
    {
      auto it = rdr->begin();
      ASSERT_EQ(rdr.get(), &*it); /* should return self */
      ASSERT_NE(rdr->end(), it);
      ++it;
      ASSERT_EQ(rdr->end(), it);
    }

    // check field names
    {
      auto it = rdr->fields().begin();
      auto end = rdr->fields().end();
      ASSERT_NE(end, it);
      ASSERT_EQ("duplicated", it->name);
      ++it;
      ASSERT_NE(end, it);
      ASSERT_EQ("name", it->name);
      ++it;
      ASSERT_NE(end, it);
      ASSERT_EQ("prefix", it->name);
      ++it;
      ASSERT_NE(end, it);
      ASSERT_EQ("same", it->name);
      ++it;
      ASSERT_NE(end, it);
      ASSERT_EQ("seq", it->name);
      ++it;
      ASSERT_NE(end, it);
      ASSERT_EQ("value", it->name);
      ++it;
      ASSERT_EQ(end, it);
    }

    // check live docs
    {
      auto it = rdr->docs_iterator();
      ASSERT_TRUE(it->next());
      ASSERT_EQ(1, it->value());
      ASSERT_TRUE(it->next());
      ASSERT_EQ(2, it->value());
      ASSERT_TRUE(it->next());
      ASSERT_EQ(3, it->value());
      ASSERT_TRUE(it->next());
      ASSERT_EQ(4, it->value());
      ASSERT_TRUE(it->next());
      ASSERT_EQ(5, it->value());
      ASSERT_FALSE(it->next());
    }

    // check field metadata
    {
      auto& meta = rdr->fields();
      ASSERT_EQ(6, meta.size());

      // check field
      {       
        const ir::string_ref name = "name";
        auto field = meta.find(name);
        ASSERT_EQ(0, field->id);
        ASSERT_EQ(name, field->name);

        // check terms
        auto terms = rdr->terms(name);
        ASSERT_NE(nullptr, terms);

        ASSERT_EQ(5, terms->size());
        ASSERT_EQ(5, terms->docs_count());
        ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("A")), (terms->min)());
        ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("E")), (terms->max)());

        auto term = terms->iterator();

        // check term: A
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("A")), term->value());

          // check docs
          {
            auto docs = term->postings(ir::flags::empty_instance());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(1, docs->value());
            ASSERT_FALSE(docs->next());
          }
        }

        // check term: B
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("B")), term->value());

          // check docs
          {
            auto docs = term->postings(ir::flags::empty_instance());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(2, docs->value());
            ASSERT_FALSE(docs->next());
          }
        }

        // check term: C
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("C")), term->value());

          // check docs
          {
            auto docs = term->postings(ir::flags::empty_instance());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(3, docs->value());
            ASSERT_FALSE(docs->next());
          }
        }

        // check term: D
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("D")), term->value());

          // check docs
          {
            auto docs = term->postings(ir::flags::empty_instance());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(4, docs->value());
            ASSERT_FALSE(docs->next());
          }
        }

        // check term: E
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("E")), term->value());

          // check docs
          {
            auto docs = term->postings(ir::flags::empty_instance());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(5, docs->value());
            ASSERT_FALSE(docs->next());
          }
        }

        ASSERT_FALSE(term->next());
      }

      // check field
      {
        const ir::string_ref name = "seq";
        auto field = meta.find(name);
        ASSERT_EQ(1, field->id);
        ASSERT_EQ(name, field->name);

        // check terms
        auto terms = rdr->terms(name);
        ASSERT_NE(nullptr, terms);
      }

      // check field
      {
        const ir::string_ref name = "same";
        auto field = meta.find(name);
        ASSERT_EQ(2, field->id);
        ASSERT_EQ(name, field->name);

        // check terms
        auto terms = rdr->terms(name);
        ASSERT_NE(nullptr, terms);
        ASSERT_EQ(1, terms->size());
        ASSERT_EQ(5, terms->docs_count());
        ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("xyz")), (terms->min)());
        ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("xyz")), (terms->max)());

        auto term = terms->iterator();

        // check term: xyz
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("xyz")), term->value());

          /* check docs */
          {
            auto docs = term->postings(ir::flags::empty_instance());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(1, docs->value());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(2, docs->value());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(3, docs->value());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(4, docs->value());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(5, docs->value());
            ASSERT_FALSE(docs->next());
          }
        }

        ASSERT_FALSE(term->next());
      }

      // check field
      {
        const ir::string_ref name = "duplicated";
        auto field = meta.find(name);
        ASSERT_EQ(4, field->id);
        ASSERT_EQ(name, field->name);

        // check terms
        auto terms = rdr->terms(name);
        ASSERT_NE(nullptr, terms);
        ASSERT_EQ(2, terms->size());
        ASSERT_EQ(4, terms->docs_count());
        ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("abcd")), (terms->min)());
        ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("vczc")), (terms->max)());

        auto term = terms->iterator();

        // check term: abcd
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("abcd")), term->value());

          // check docs
          {
            auto docs = term->postings(ir::flags::empty_instance());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(1, docs->value());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(5, docs->value());
            ASSERT_FALSE(docs->next());
          }
        }

        // check term: vczc
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("vczc")), term->value());

          // check docs
          {
            auto docs = term->postings(ir::flags::empty_instance());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(2, docs->value());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(3, docs->value());
            ASSERT_FALSE(docs->next());
          }
        }

        ASSERT_FALSE(term->next());
      }

      // check field
      {
        const ir::string_ref name = "prefix";
        auto field = meta.find(name);
        ASSERT_EQ(5, field->id);
        ASSERT_EQ(name, field->name);

        // check terms
        auto terms = rdr->terms(name);
        ASSERT_NE(nullptr, terms);
        ASSERT_EQ(2, terms->size());
        ASSERT_EQ(2, terms->docs_count());
        ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("abcd")), (terms->min)());
        ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("abcde")), (terms->max)());

        auto term = terms->iterator();

        // check term: abcd
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("abcd")), term->value());

          // check docs
          {
            auto docs = term->postings(ir::flags::empty_instance());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(1, docs->value());
            ASSERT_FALSE(docs->next());
          }
        }

        // check term: abcde
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(ir::ref_cast<ir::byte_type>(ir::string_ref("abcde")), term->value());

          // check docs
          {
            auto docs = term->postings(ir::flags::empty_instance());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(4, docs->value());
            ASSERT_FALSE(docs->next());
          }
        }

        ASSERT_FALSE(term->next());
      }

      // invalid field
      {
        const ir::string_ref name = "invalid_field";
        ASSERT_EQ(nullptr, meta.find(name));
        ASSERT_EQ(nullptr, rdr->terms(name));
      }

      ASSERT_FALSE(meta.empty());
    }
  }
}
