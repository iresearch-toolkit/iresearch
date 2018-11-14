////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "tests_shared.hpp"
#include "index_tests.hpp"
#include "formats/formats.hpp"
#include "store/memory_directory.hpp"

NS_LOCAL

class failing_directory : public irs::directory {
 public:
  enum class Failure : size_t {
    CREATE = 0,
    EXISTS,
    LENGTH,
    MAKE_LOCK,
    MTIME,
    OPEN,
    RENAME,
    REMOVE,
    SYNC,
    UNKNOWN_FAILURE
  };

  explicit failing_directory(irs::directory& impl) NOEXCEPT
    : impl_(&impl) {
  }

  bool register_failure(Failure type, const std::string& name) {
    return failures_.emplace(name, type).second;
  }

  void clear_failures() {
    failures_.clear();
  }

  using irs::directory::attributes;
  virtual irs::attribute_store& attributes() NOEXCEPT override {
    return impl_->attributes();
  }
  virtual void close() NOEXCEPT override {
    impl_->close();
  }
  virtual irs::index_output::ptr create(const std::string &name) NOEXCEPT override {
    if (should_fail(Failure::CREATE, name)) {
      return nullptr;
    }

    return impl_->create(name);
  }
  virtual bool exists(bool& result, const std::string& name) const NOEXCEPT override {
    if (should_fail(Failure::EXISTS, name)) {
      return false;
    }

    return impl_->exists(result, name);
  }
  virtual bool length(uint64_t& result, const std::string& name) const NOEXCEPT override {
    if (should_fail(Failure::LENGTH, name)) {
      return false;
    }

    return impl_->length(result, name);
  }
  virtual irs::index_lock::ptr make_lock(const std::string& name) NOEXCEPT override {
    if (should_fail(Failure::MAKE_LOCK, name)) {
      return nullptr;
    }

    return impl_->make_lock(name);
  }
  virtual bool mtime(std::time_t& result, const std::string& name) const NOEXCEPT override {
    if (should_fail(Failure::MTIME, name)) {
      return false;
    }

    return impl_->mtime(result, name);
  }
  virtual irs::index_input::ptr open(const std::string& name, irs::IOAdvice advice) const NOEXCEPT override {
    if (should_fail(Failure::OPEN, name)) {
      return nullptr;
    }

    return impl_->open(name, advice);
  }
  virtual bool remove(const std::string& name) NOEXCEPT override {
    if (should_fail(Failure::REMOVE, name)) {
      return false;
    }

    return impl_->remove(name);
  }
  virtual bool rename(const std::string& src, const std::string& dst) NOEXCEPT override {
    if (should_fail(Failure::RENAME, src)) {
      return false;
    }

    return impl_->rename(src, dst);
  }
  virtual bool sync(const std::string& name) NOEXCEPT override {
    if (should_fail(Failure::SYNC, name)) {
      return false;
    }

    return impl_->sync(name);
  }
  virtual bool visit(const visitor_f& visitor) const override {
    return impl_->visit(visitor);
  }

 private:
  bool should_fail(Failure type, const std::string& name) const {
    auto it = failures_.find(std::make_pair(name, type));

    if (failures_.end() != it) {
      failures_.erase(it);
      return true;
    }

    return false;
  }

  typedef std::pair<std::string, Failure> fail_t;

  struct fail_less {
    bool operator()(const fail_t& lhs, const fail_t& rhs) const NOEXCEPT {
      if (lhs.second == rhs.second) {
        return lhs.first < rhs.first;
      }

      return lhs.second < rhs.second;
    }
  };

  irs::directory* impl_;
  mutable std::set<fail_t, fail_less> failures_;
}; // failing_directory

NS_END

TEST(index_death_test_formats_10, index_meta_write_fail_1st_phase) {
  tests::json_doc_generator gen(
    test_base::resource("simple_sequential.json"),
    [] (tests::document& doc, const std::string& name, const tests::json_doc_generator::json_value& data) {
    if (data.is_string()) {
      doc.insert(std::make_shared<tests::templates::string_field>(
        irs::string_ref(name),
        data.str
      ));
    }
  });
  const auto* doc1 = gen.next();

  irs::memory_directory impl;

  auto codec = irs::formats::get("1_0");
  ASSERT_NE(nullptr, codec);

  failing_directory dir(impl);
  dir.register_failure(failing_directory::Failure::CREATE, "pending_segments_1"); // fail first phase of transaction

  // write index
  {
    auto writer = irs::index_writer::make(dir, codec, irs::OM_CREATE);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(insert(*writer,
      doc1->indexed.begin(), doc1->indexed.end(),
      doc1->stored.begin(), doc1->stored.end()
    ));

    ASSERT_THROW(writer->begin(), irs::illegal_state); // FIXME why illegal_state

    // successful attempt
    ASSERT_TRUE(writer->begin());
    writer->commit();
  }

  // check index
  {
    const auto all_features = irs::flags{
      irs::document::type(),
      irs::frequency::type(),
      irs::position::type(),
      irs::payload::type(),
      irs::offset::type()
    };

    auto reader = irs::directory_reader::open(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().add(doc1->indexed.begin(), doc1->indexed.end());
    tests::assert_index(expected_index, *reader, all_features);

    // validate columnstore
    irs::bytes_ref actual_value;
    auto& segment = reader[0]; // assume 0 is id of first/only segment
    const auto* column = segment.column_reader("name");
    ASSERT_NE(nullptr, column);
    auto values = column->values();
    ASSERT_EQ(1, segment.docs_count()); // total count of documents
    ASSERT_EQ(1, segment.live_docs_count()); // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    ASSERT_TRUE(values(docsItr->value(), actual_value));
    ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str())); // 'name' value in doc3
    ASSERT_FALSE(docsItr->next());
  }
}

TEST(index_death_test_formats_10, index_meta_write_fail_2nd_phase) {
  tests::json_doc_generator gen(
    test_base::resource("simple_sequential.json"),
    [] (tests::document& doc, const std::string& name, const tests::json_doc_generator::json_value& data) {
    if (data.is_string()) {
      doc.insert(std::make_shared<tests::templates::string_field>(
        irs::string_ref(name),
        data.str
      ));
    }
  });
  const auto* doc1 = gen.next();

  irs::memory_directory impl;

  auto codec = irs::formats::get("1_0");
  ASSERT_NE(nullptr, codec);

  failing_directory dir(impl);
  dir.register_failure(failing_directory::Failure::RENAME, "pending_segments_1"); // fail second phase of transaction

  // write index
  {
    auto writer = irs::index_writer::make(dir, codec, irs::OM_CREATE);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(insert(*writer,
      doc1->indexed.begin(), doc1->indexed.end(),
      doc1->stored.begin(), doc1->stored.end()
    ));

    ASSERT_TRUE(writer->begin());
    ASSERT_THROW(writer->commit(), irs::illegal_state); // FIXME why illegal_state

    // second attempt
    ASSERT_TRUE(writer->begin());
    writer->commit();
  }

  // check index
  {
    const auto all_features = irs::flags{
      irs::document::type(),
      irs::frequency::type(),
      irs::position::type(),
      irs::payload::type(),
      irs::offset::type()
    };

    auto reader = irs::directory_reader::open(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().add(doc1->indexed.begin(), doc1->indexed.end());
    tests::assert_index(expected_index, *reader, all_features);

    // validate columnstore
    irs::bytes_ref actual_value;
    auto& segment = reader[0]; // assume 0 is id of first/only segment
    const auto* column = segment.column_reader("name");
    ASSERT_NE(nullptr, column);
    auto values = column->values();
    ASSERT_EQ(1, segment.docs_count()); // total count of documents
    ASSERT_EQ(1, segment.live_docs_count()); // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    ASSERT_TRUE(values(docsItr->value(), actual_value));
    ASSERT_EQ("A", irs::to_string<irs::string_ref>(actual_value.c_str())); // 'name' value in doc3
    ASSERT_FALSE(docsItr->next());
  }
}

//TEST(index_death_test_formats_10, segment_meta_write_fail) {
//  irs::memory_directory impl;
//
//  auto codec = irs::formats::get("1_0");
//  ASSERT_NE(nullptr, codec);
//
//  failing_directory dir(impl);
//  dir.register_failure(failing_directory::Failure::CREATE, "_1.0.sm"); // segment meta
//
//  auto writer = irs::index_writer::make(dir, codec, irs::OM_CREATE);
//  ASSERT_NE(nullptr, writer);
//
//  tests::json_doc_generator gen(
//    test_base::resource("simple_sequential.json"),
//    [] (tests::document& doc, const std::string& name, const tests::json_doc_generator::json_value& data) {
//    if (data.is_string()) {
//      doc.insert(std::make_shared<tests::templates::string_field>(
//        irs::string_ref(name),
//        data.str
//      ));
//    }
//  });
//
//  auto const* doc1 = gen.next();
//  ASSERT_TRUE(insert(*writer,
//    doc1->indexed.begin(), doc1->indexed.end(),
//    doc1->stored.begin(), doc1->stored.end()
//  ));
//
//  writer->commit(); // should fail
//
//
//
//}

