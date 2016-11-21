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
#include "formats/formats_10.hpp"
#include "index/doc_generator.hpp"
#include "index/index_tests.hpp"
#include "index/index_writer.hpp"
#include "iql/query_builder.hpp"
#include "store/directory_cleaner.hpp"
#include "store/memory_directory.hpp"
#include "utils/directory_utils.hpp"

namespace tests {
  class directory_cleaner_tests: public ::testing::Test {

    virtual void SetUp() {
      // Code here will be called immediately after the constructor (right before each test).
    }

    virtual void TearDown() {
      // Code here will be called immediately after each test (right before the destructor).
    }
  };
}

using namespace tests;

// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------

TEST_F(directory_cleaner_tests, test_directory_cleaner) {
  iresearch::memory_directory dir;

  // add a dummy files
  {
    iresearch::index_output::ptr tmp;
    tmp = dir.create("dummy.file.1");
    tmp = dir.create("dummy.file.2");
  }

  // test clean before initializing directory
  {
    ASSERT_EQ(0, iresearch::directory_cleaner::clean(dir));
  }

  // start tracking refs
  auto& refs = iresearch::directory_cleaner::init(dir);

  // add a more dummy files
  {
    iresearch::index_output::ptr tmp;
    tmp = dir.create("dummy.file.3");
    tmp = dir.create("dummy.file.4");
  }

  // add tracked files
  auto ref1 = refs.add("tracked.file.1");
  auto ref2 = refs.add("tracked.file.2");
  {
    iresearch::index_output::ptr tmp;
    tmp = dir.create("tracked.file.1");
    tmp = dir.create("tracked.file.2");
  }

  // test initial directory state
  {
    std::unordered_set<std::string> expected = {
      "dummy.file.1",
      "dummy.file.2",
      "dummy.file.3",
      "dummy.file.4",
      "tracked.file.1",
      "tracked.file.2"
    };
    iresearch::directory::files files;

    dir.list(files);

    for (auto& file: files) {
      ASSERT_EQ(1, expected.erase(file));
    }

    ASSERT_TRUE(expected.empty());
  }

  // test clean without any changes (refs still active)
  {
    std::unordered_set<std::string> expected = {
      "dummy.file.1",
      "dummy.file.2",
      "dummy.file.3",
      "dummy.file.4",
      "tracked.file.1",
      "tracked.file.2"
    };
    iresearch::directory::files files;

    ASSERT_EQ(0, iresearch::directory_cleaner::clean(dir));
    dir.list(files);

    for (auto& file: files) {
      ASSERT_EQ(1, expected.erase(file));
    }

    ASSERT_TRUE(expected.empty());
  }

  // test clean without any changes (due to 'keep')
  {
    std::unordered_set<std::string> retain = {
      "tracked.file.1",
      "tracked.file.2"
    };
    auto acceptor = [&retain] (const std::string &filename)->bool {
      return retain.find(filename) == retain.end();
    };
    std::unordered_set<std::string> expected = {
      "dummy.file.1",
      "dummy.file.2",
      "dummy.file.3",
      "dummy.file.4",
      "tracked.file.1",
      "tracked.file.2"
    };
    iresearch::directory::files files;

    ref2.reset();
    ASSERT_EQ(0, iresearch::directory_cleaner::clean(dir, acceptor));
    dir.list(files);

    for (auto& file: files) {
      ASSERT_EQ(1, expected.erase(file));
    }

    ASSERT_TRUE(expected.empty());
  }

  // test clean removing tracked files without references (no 'tracked.file.2')
  {
    std::unordered_set<std::string> expected = {
      "dummy.file.1",
      "dummy.file.2",
      "dummy.file.3",
      "dummy.file.4",
      "tracked.file.1"
    };
    iresearch::directory::files files;

    ASSERT_EQ(1, iresearch::directory_cleaner::clean(dir));
    dir.list(files);

    for (auto& file: files) {
      ASSERT_EQ(1, expected.erase(file));
    }

    ASSERT_TRUE(expected.empty());
  }

  // test clean removing tracked files without references (no 'tracked.file.1')
  {
    std::unordered_set<std::string> retain = {
      "dummy.file.1",
      "dummy.file.2",
      "dummy.file.3",
      "dummy.file.4"
    };
    auto acceptor = [&retain](const std::string& filename)->bool {
      return retain.find(filename) == retain.end();
    };
    std::unordered_set<std::string> expected = {
      "dummy.file.1",
      "dummy.file.2",
      "dummy.file.3",
      "dummy.file.4"
    };
    iresearch::directory::files files;

    ref1.reset();
    ASSERT_EQ(1, iresearch::directory_cleaner::clean(dir, acceptor));
    dir.list(files);

    for (auto& file: files) {
      ASSERT_EQ(1, expected.erase(file));
    }

    ASSERT_TRUE(expected.empty());
  }

  // test empty references removed too
  ASSERT_TRUE(refs.empty());
}

TEST_F(directory_cleaner_tests, test_directory_cleaner_current_segment) {
  tests::json_doc_generator gen(
    test_base::resource("simple_sequential.json"),
    &tests::generic_json_field_factory 
  );
  tests::document const* doc1 = gen.next();
  tests::document const* doc2 = gen.next();
  auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());
  iresearch::memory_directory dir;
  iresearch::version10::format codec;
  iresearch::format::ptr codec_ptr(&codec, [](iresearch::format*)->void{});

  iresearch::directory_cleaner::init(dir);

  // writer commit tracks files that are in active segments
  {
    auto writer = iresearch::index_writer::make(dir, codec_ptr, iresearch::OPEN_MODE::OM_CREATE);

    writer->add(doc1->begin(), doc1->end());
    writer->commit();

    iresearch::directory::files files;
    std::unordered_set<std::string> file_set;

    dir.list(files);
    ASSERT_FALSE(files.empty());
    file_set.insert(files.begin(), files.end());

    writer->remove(std::move(query_doc1.filter));
    writer->add(doc2->begin(), doc2->end());
    writer->commit();

    iresearch::directory_cleaner::clean(dir, iresearch::directory_utils::remove_except_current_segments(dir, codec));
    dir.list(files);
    ASSERT_FALSE(files.empty());

    // new list should not overlap due to first segment having been removed
    for (auto& file: files) {
      ASSERT_TRUE(file_set.find(file) == file_set.end());
    }
  }

  std::unordered_set<std::string> file_set;

  // remember files used for first/single segment
  {
    std::string segments_file;

    iresearch::index_meta index_meta;
    auto meta_reader = codec.get_index_meta_reader();
    const auto index_exists = meta_reader->last_segments_file(dir, segments_file);

    ASSERT_TRUE(index_exists);
    meta_reader->read(dir, index_meta, segments_file);

    file_set.insert(segments_file);

    index_meta.visit_files([&file_set] (std::string& file) {
      file_set.emplace(std::move(file));
      return true;
    });
  }

  // no active refs keeps files from latest segments
  {
    iresearch::directory::files files;
    std::unordered_set<std::string> current_files(file_set);

    iresearch::directory_cleaner::clean(dir, iresearch::directory_utils::remove_except_current_segments(dir, codec));
    dir.list(files);
    ASSERT_FALSE(files.empty());

    // new list should be exactly the files listed in index_meta
    for (auto& file: files) {
      ASSERT_EQ(1, current_files.erase(file));
    }

    ASSERT_TRUE(current_files.empty());
  }

  // active reader refs keeps files referenced by reader
  {
    iresearch::directory::files files;
    std::unordered_set<std::string> current_files(file_set);
    auto reader = iresearch::directory_reader::open(dir, codec_ptr);

    iresearch::directory_cleaner::clean(dir);
    dir.list(files);
    ASSERT_FALSE(files.empty());

    // new list should be exactly the files listed in index_meta
    for (auto& file: files) {
      ASSERT_EQ(1, file_set.erase(file));
    }

    ASSERT_TRUE(file_set.empty());
  }

  // no refs and no current segment removes all files
  {
    iresearch::directory::files files;

    iresearch::directory_cleaner::clean(dir);
    dir.list(files);
    ASSERT_TRUE(files.empty()); // current segment should have been removed too
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
