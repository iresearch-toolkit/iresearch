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
#include "analysis/token_attributes.hpp"
#include "document/field.hpp"
#include "iql/query_builder.hpp"
#include "formats/formats_10.hpp"
#include "search/filter.hpp"
#include "store/fs_directory.hpp"
#include "store/memory_directory.hpp"
#include "index/index_reader.hpp"
#include "utils/async_utils.hpp"
#include "utils/index_utils.hpp"

#include "index_tests.hpp"

#include <thread>

namespace ir = iresearch;

namespace tests {

struct directory_mock : public ir::directory {
  directory_mock(ir::directory& impl) : impl_(impl) {}
  using directory::attributes;
  virtual iresearch::attributes& attributes() override {
    return impl_.attributes();
  }
  virtual void close() override {
    impl_.close();
  }
  virtual ir::index_output::ptr create(const std::string& name) override {
    return impl_.create(name);
  }
  virtual bool exists(const std::string& name) const override {
    return impl_.exists(name);
  }
  virtual int64_t length(const std::string& name) const override {
    return impl_.length(name);
  }
  virtual bool list(ir::directory::files& names) const override {
    return impl_.list(names);
  }
  virtual ir::index_lock::ptr make_lock(const std::string& name) override {
    return impl_.make_lock(name);
  }
  virtual std::time_t mtime(const std::string& name) const override {
    return impl_.mtime(name);
  }
  virtual ir::index_input::ptr open(const std::string& name) const override {
    return impl_.open(name);
  }
  virtual bool remove(const std::string& name) override {
    return impl_.remove(name);
  }
  virtual void rename(const std::string& src, const std::string& dst) override { 
    impl_.rename(src, dst);
  }
  virtual void sync(const std::string& name) override {
    impl_.sync(name);
  }

 private:
  ir::directory& impl_;
}; // directory_mock 

namespace templates {

// ----------------------------------------------------------------------------
// --SECTION--                               token_stream_payload implemntation
// ----------------------------------------------------------------------------

token_stream_payload::token_stream_payload(ir::token_stream* impl)
  : impl_(impl) {
    assert(impl_);
    ir::attributes& attrs = const_cast<ir::attributes&>(impl_->attributes());
    term_ = attrs.get<ir::term_attribute>();
    assert(term_);
    pay_ = attrs.add<ir::payload>();
}

bool token_stream_payload::next() {
  if (impl_->next()) {
    pay_->value = term_->value();
    return true;
  }
  pay_->value = ir::bytes_ref::nil;
  return false;
}

// ----------------------------------------------------------------------------
// --SECTION--                                       string_field implemntation
// ----------------------------------------------------------------------------

string_field::string_field(
    const ir::string_ref& name, 
    bool indexed, bool stored) {
  this->name(name);
  this->indexed(indexed);
  this->stored(stored);
}

string_field::string_field(
    const ir::string_ref& name, 
    const ir::string_ref& value,
    bool indexed, bool stored) 
  : value_(value) {
  this->name(name);
  this->indexed(indexed);
  this->stored(stored);
}

const ir::flags& string_field::features() const {
  static ir::flags features{ ir::frequency::type(), ir::position::type() };
  return features;
}
  
// reject too long terms
void string_field::value(const ir::string_ref& str) {
  const auto size_len = ir::vencode_size_32(ir::BYTE_BLOCK_SIZE);
  const auto max_len = (std::min)(str.size(), size_t(ir::BYTE_BLOCK_SIZE - size_len));
  auto begin = str.begin();
  auto end = str.begin() + max_len;
  value_ = std::string(begin, end);
}

bool string_field::write(ir::data_output& out) const {
  if (!stored()) {
    return false;
  }

  ir::write_string(out, value_);
  return true;
}

ir::token_stream* string_field::get_tokens() const {
  REGISTER_TIMER_DETAILED();
  if (!indexed()) {
    return nullptr;
  }

  stream_.reset(value_);
  return &stream_;
}

//////////////////////////////////////////////////////////////////////////////
/// @class europarl_doc_template
/// @brief document template for europarl.subset.text
//////////////////////////////////////////////////////////////////////////////
class europarl_doc_template : public delim_doc_generator::doc_template {
 public:
  typedef templates::text_field<ir::string_ref> text_field;

  virtual void init() {
    fields_.clear();
    fields_.reserve(8);
    fields_.emplace_back(new tests::templates::string_field("title", true, true));
    fields_.emplace_back(new text_field("title_anl", true, false));
    fields_.emplace_back(new text_field("title_anl_pay", true, true));
    {
      fields_.emplace_back(new tests::long_field());
      auto& field = static_cast<tests::long_field&>(*fields_.back());
      field.name(ir::string_ref("date"));
      field.indexed(true);
      field.stored(true);
    }
    fields_.emplace_back(new tests::templates::string_field("datestr", true, true));
    fields_.emplace_back(new tests::templates::string_field("body", true, true));
    fields_.emplace_back(new text_field("body_anl", true, false));
    fields_.emplace_back(new text_field("body_anl_pay", true, true));
    {
      fields_.emplace_back(new tests::int_field());
      auto& field = static_cast<tests::int_field&>(*fields_.back());
      field.name(ir::string_ref("id"));
      field.indexed(true);
      field.stored(true);
    }
    fields_.emplace_back(new string_field("idstr", true, true));
  }

  virtual void value(size_t idx, const std::string& value) {
    static auto get_time = [](const std::string& src) {
      std::istringstream ss(src);
      std::tm tmb{};
      char c;
      ss >> tmb.tm_year >> c >> tmb.tm_mon >> c >> tmb.tm_mday;
      return std::mktime( &tmb );
    };

    switch (idx) {
      case 0: // title
        title_ = value;
        get<tests::templates::string_field>("title")->value(title_);
        get<text_field>("title_anl")->value(title_);
        get<text_field>("title_anl_pay")->value(title_);
        break;
      case 1: // dateA
        get<tests::long_field>("date")->value(get_time(value));
        get<tests::templates::string_field>("datestr")->value(value);
        break;
      case 2: // body
        body_ = value;
        get<tests::templates::string_field>("body")->value(body_);
        get<text_field>("body_anl")->value(body_);
        get<text_field>("body_anl_pay")->value(body_);
        break;
    }
  }

  virtual void end() {
    ++idval_;
    get<tests::int_field>("id")->value(idval_);
    get<tests::templates::string_field>("idstr")->value(std::to_string(idval_));
  }

  virtual void reset() {
    idval_ = 0;
  }

 private:
  std::string title_; // current title
  std::string body_; // current body
  ir::doc_id_t idval_ = 0;
}; // europarl_doc_template

} // templates

void generic_json_field_factory(
    tests::document& doc,
    const std::string& name,
    const tests::json::json_value& data) {
  if (data.quoted) {
    doc.add(new templates::string_field(
      ir::string_ref(name),
      ir::string_ref(data.value),
      true, true));
  } else if ("null" == data.value) {
    doc.add(new tests::binary_field());
    auto& field = (doc.end() - 1).as<tests::binary_field>();
    field.name(iresearch::string_ref(name));
    field.value(ir::null_token_stream::value_null());
  } else if ("true" == data.value) {
    doc.add(new tests::binary_field());
    auto& field = (doc.end() - 1).as<tests::binary_field>();
    field.name(iresearch::string_ref(name));
    field.value(ir::boolean_token_stream::value_true());
  } else if ("false" == data.value) {
    doc.add(new tests::binary_field());
    auto& field = (doc.end() - 1).as<tests::binary_field>();
    field.name(iresearch::string_ref(name));
    field.value(ir::boolean_token_stream::value_true());
  } else {
    char* czSuffix;
    double dValue = strtod(data.value.c_str(), &czSuffix);

    // 'value' can be interpreted as a double
    if (!czSuffix[0]) {
      doc.add(new tests::double_field());
      auto& field = (doc.end() - 1).as<tests::double_field>();
      field.name(iresearch::string_ref(name));
      field.value(dValue);
    }
  }
}

void payloaded_json_field_factory(
    tests::document& doc,
    const std::string& name,
    const tests::json::json_value& data) {
  typedef templates::text_field<std::string> text_field;

  if (data.quoted) {
    // analyzed && pyaloaded
    doc.add(new text_field(
      std::string(name.c_str()) + "_anl_pay",
      ir::string_ref(data.value),
      true,
      true
    ));

    // analyzed field
    doc.add(new text_field(
      std::string(name.c_str()) + "_anl",
      ir::string_ref(data.value),
      true
    ));

    // not analyzed field
    doc.add(new templates::string_field(
      ir::string_ref(name),
      ir::string_ref(data.value),
      true, true
    ));
  } else if ("null" == data.value) {
    doc.add(new tests::binary_field());
    auto& field = (doc.end() - 1).as<tests::binary_field>();
    field.name(iresearch::string_ref(name));
    field.value(ir::null_token_stream::value_null());
  } else if ("true" == data.value) {
    doc.add(new tests::binary_field());
    auto& field = (doc.end() - 1).as<tests::binary_field>();
    field.name(iresearch::string_ref(name));
    field.value(ir::boolean_token_stream::value_true());
  } else if ("false" == data.value) {
    doc.add(new tests::binary_field());
    auto& field = (doc.end() - 1).as<tests::binary_field>();
    field.name(iresearch::string_ref(name));
    field.value(ir::boolean_token_stream::value_false());
  } else {
    char* czSuffix;
    double dValue = strtod(data.value.c_str(), &czSuffix);
    if (!czSuffix[0]) {
      // 'value' can be interpreted as a double
      doc.add(new tests::double_field());
      auto& field = (doc.end() - 1).as<tests::double_field>();
      field.name(iresearch::string_ref(name));
      field.value(dValue);
    }
  }
}

class index_test_case_base : public tests::index_test_base {
 public:
  void open_writer_check_lock() {
    {
      // open writer
      auto writer = ir::index_writer::make(dir(), codec(), ir::OM_CREATE);
      // can't open another writer at the same time on the same directory
      ASSERT_THROW(ir::index_writer::make(dir(), codec(), ir::OM_CREATE), ir::lock_obtain_failed);
      writer->buffered_docs();
    }

    {
      // open writer
      auto writer = ir::index_writer::make(dir(), codec(), ir::OM_CREATE);

      writer->commit();
      iresearch::directory_cleaner::clean(dir());
      // can't open another writer at the same time on the same directory
      ASSERT_THROW(ir::index_writer::make(dir(), codec(), ir::OM_CREATE), ir::lock_obtain_failed);
      writer->buffered_docs();
    }

    {
      // open writer
      auto writer = ir::index_writer::make(dir(), codec(), ir::OM_CREATE);

      writer->buffered_docs();
    }
  } 
  
  void profile_bulk_index_dedicated_commit(size_t insert_threads, size_t commit_threads, size_t commit_interval) {
    struct csv_doc_template_t: public tests::delim_doc_generator::doc_template, private iresearch::util::noncopyable {
      using document::end;

      csv_doc_template_t() = default;
      csv_doc_template_t(csv_doc_template_t&& other) {
        fields_ = std::move(other.fields_);
      }

      virtual void init() {
        fields_.clear();
        fields_.reserve(2);
        fields_.emplace_back(new tests::templates::string_field("id", true, true));
        fields_.emplace_back(new tests::templates::string_field("label", true, true));
      }

      virtual void value(size_t idx, const std::string& value) {
        switch(idx) {
         case 0:
          get<tests::templates::string_field>("id")->value(value);
          break;
         case 1:
          get<tests::templates::string_field>("label")->value(value);
        }
      }

      fields_t& fields() {
        return fields_;
      }
    };

    iresearch::timer_utils::init_stats(true);

    std::atomic<size_t> parsed_docs_count(0);
    std::atomic<size_t> writer_commit_count(0);
    insert_threads = std::max((size_t)1, insert_threads);
    commit_threads = std::max((size_t)1, commit_threads);
    commit_interval = std::max((size_t)1, commit_interval);
    auto thread_count = insert_threads + commit_threads;
    ir::async_utils::thread_pool thread_pool(thread_count, thread_count);
    auto writer = open_writer();
    std::mutex mutex;

    {
      std::lock_guard<std::mutex> lock(mutex);

      // spawn insert threads
      for (size_t i = 0; i < insert_threads; ++i) {
        thread_pool.run([&mutex, &writer, insert_threads, i, &parsed_docs_count, this]()->void {
          {
            // wait for all threads to be registered
            std::lock_guard<std::mutex> lock(mutex);
          }

          csv_doc_template_t csv_doc_template;
          tests::delim_doc_generator gen(resource("simple_two_column.csv"), csv_doc_template, ',');
          size_t gen_skip = i;

          for(size_t count = 0;; ++count) {
            // assume docs generated in same order and skip docs not meant for this thread
            if (gen_skip--) {
              if (!gen.next()) {
                break;
              }

              continue;
            }

            gen_skip = insert_threads - 1;

            {
              REGISTER_TIMER_NAMED_DETAILED("parse");
              csv_doc_template.init();

              if (!gen.next()) {
                break;
              }
            }

            ++parsed_docs_count;

            {
              REGISTER_TIMER_NAMED_DETAILED("load");
              writer->add(csv_doc_template.begin(), csv_doc_template.end());
            }
          }
        });
      }

      // wait for insert threads
      while (thread_pool.tasks_active() < insert_threads) { }
     
      // spawn commit threads   
      for (size_t i = 0; i < commit_threads; ++i) {
        thread_pool.run([&mutex, &writer, &writer_commit_count, commit_threads, commit_interval, &thread_pool]()->void {
          {
            // wait for all threads to be registered
            std::lock_guard<std::mutex> lock(mutex);
          }
      

          for(;;) {
            std::this_thread::sleep_for(std::chrono::milliseconds(commit_interval));

            {
              REGISTER_TIMER_NAMED_DETAILED("commit");
              writer->commit();
            }

            ++writer_commit_count;

            if (thread_pool.tasks_active() <= commit_threads) {
              break;
            }
          }
        });
      }
    }

    thread_pool.stop();

    if (writer->buffered_docs()) {
      {
        REGISTER_TIMER_NAMED_DETAILED("commit");
        writer->commit();
      }
      ++writer_commit_count;
    }

    std::map<std::string, std::pair<size_t, size_t>> ordered_stats;

    iresearch::timer_utils::visit([&ordered_stats](const std::string& key, size_t count, size_t time)->bool {
      std::string key_str = key;

      #if defined(__GNUC__)
        if (key_str.compare(0, strlen("virtual "), "virtual ") == 0) {
          key_str = key_str.substr(strlen("virtual "));
        }

        size_t i;

        if (std::string::npos != (i = key_str.find(' ')) && key_str.find('(') > i) {
          key_str = key_str.substr(i + 1);
        }
      #elif defined(_MSC_VER)
        size_t i;

        if (std::string::npos != (i = key_str.find("__cdecl "))) {
          key_str = key_str.substr(i + strlen("__cdecl "));
        }
      #endif

      ordered_stats.emplace(key_str, std::make_pair(count, time));
      return true;
    });

    auto path = fs::path(test_dir()).append("profile_bulk_index.log");
    std::ofstream out(path.native());

    for (auto& entry: ordered_stats) {
      auto& key = entry.first;
      auto& count = entry.second.first;
      auto& time = entry.second.second;
      out << key << "\tcalls:" << count << ",\ttime: " << time/1000 << " us,\tavg call: " << time/1000/(double)count << " us"<< std::endl;
    }

    out.close();
    std::cout << "Path to timing log: " << fs::absolute(path).string() << std::endl;

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(true, writer_commit_count <= reader->size());
    ASSERT_EQ(true, writer_commit_count * iresearch::index_writer::THREAD_COUNT >= reader->size());

    size_t indexed_docs_count = 0;

    for (size_t i = 0, count = reader->size(); i < count; ++i) {
      indexed_docs_count += (*reader)[i].docs_count();
    }

    ASSERT_EQ(parsed_docs_count, indexed_docs_count);
  }

  void profile_bulk_index(size_t num_threads, size_t batch_size) {
    struct csv_doc_template_t: public tests::delim_doc_generator::doc_template, private iresearch::util::noncopyable {
      using document::end;

      csv_doc_template_t() = default;
      csv_doc_template_t(csv_doc_template_t&& other) {
        fields_ = std::move(other.fields_);
      }

      virtual void init() {
        fields_.clear();
        fields_.reserve(2);
        fields_.emplace_back(new tests::templates::string_field("id", true, true));
        fields_.emplace_back(new tests::templates::string_field("label", true, true));
      }

      virtual void value(size_t idx, const std::string& value) {
        switch(idx) {
         case 0:
          get<tests::templates::string_field>("id")->value(value);
          break;
         case 1:
          get<tests::templates::string_field>("label")->value(value);
        }
      }

      fields_t& fields() {
        return fields_;
      }
    };

    iresearch::timer_utils::init_stats(true);

    std::atomic<size_t> parsed_docs_count(0);
    size_t writer_batch_size = batch_size ? batch_size : std::numeric_limits<size_t>::max();
    std::atomic<size_t> writer_commit_count(0);
    auto thread_count = std::max((size_t)1, num_threads);
    ir::async_utils::thread_pool thread_pool(thread_count, thread_count);
    auto writer = open_writer();
    std::mutex mutex;
    std::mutex commit_mutex;

    {
      std::lock_guard<std::mutex> lock(mutex);

      for (size_t i = 0; i < thread_count; ++i) {
        thread_pool.run([&mutex, &commit_mutex, &writer, thread_count, i, writer_batch_size, &parsed_docs_count, &writer_commit_count, this]()->void {
          {
            // wait for all threads to be registered
            std::lock_guard<std::mutex> lock(mutex);
          }

          csv_doc_template_t csv_doc_template;
          tests::delim_doc_generator gen(resource("simple_two_column.csv"), csv_doc_template, ',');
          size_t gen_skip = i;

          for(size_t count = 0;; ++count) {
            // assume docs generated in same order and skip docs not meant for this thread
            if (gen_skip--) {
              if (!gen.next()) {
                break;
              }

              continue;
            }

            gen_skip = thread_count - 1;

            {
              REGISTER_TIMER_NAMED_DETAILED("parse");
              csv_doc_template.init();

              if (!gen.next()) {
                break;
              }
            }

            ++parsed_docs_count;

            {
              REGISTER_TIMER_NAMED_DETAILED("load");
              writer->add(csv_doc_template.begin(), csv_doc_template.end());
            }

            if (count >= writer_batch_size) {
              TRY_SCOPED_LOCK_NAMED(commit_mutex, commit_lock);

              // break commit chains by skipping commit if one is already in progress
              if (!commit_lock) {
                continue;
              }

              {
                REGISTER_TIMER_NAMED_DETAILED("commit");
                writer->commit();
              }

              count = 0;
              ++writer_commit_count;
            }
          }

          {
            REGISTER_TIMER_NAMED_DETAILED("commit");
            writer->commit();
          }

          ++writer_commit_count;
        });
      }
    }

    thread_pool.stop();

    std::map<std::string, std::pair<size_t, size_t>> ordered_stats;

    iresearch::timer_utils::visit([&ordered_stats](const std::string& key, size_t count, size_t time)->bool {
      std::string key_str = key;

      #if defined(__GNUC__)
        if (key_str.compare(0, strlen("virtual "), "virtual ") == 0) {
          key_str = key_str.substr(strlen("virtual "));
        }

        size_t i;

        if (std::string::npos != (i = key_str.find(' ')) && key_str.find('(') > i) {
          key_str = key_str.substr(i + 1);
        }
      #elif defined(_MSC_VER)
        size_t i;

        if (std::string::npos != (i = key_str.find("__cdecl "))) {
          key_str = key_str.substr(i + strlen("__cdecl "));
        }
      #endif

      ordered_stats.emplace(key_str, std::make_pair(count, time));
      return true;
    });

    auto path = fs::path(test_dir()).append("profile_bulk_index.log");
    std::ofstream out(path.native());

    for (auto& entry: ordered_stats) {
      auto& key = entry.first;
      auto& count = entry.second.first;
      auto& time = entry.second.second;
      out << key << "\tcalls:" << count << ",\ttime: " << time/1000 << " us,\tavg call: " << time/1000/(double)count << " us"<< std::endl;
    }

    out.close();
    std::cout << "Path to timing log: " << fs::absolute(path).string() << std::endl;

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(true, 1 <= reader->size()); // not all commits might produce a new segment, some might merge with concurrent commits
    ASSERT_EQ(true, writer_commit_count * iresearch::index_writer::THREAD_COUNT >= reader->size());

    size_t indexed_docs_count = 0;

    for (size_t i = 0, count = reader->size(); i < count; ++i) {
      indexed_docs_count += (*reader)[i].docs_count();
    }

    ASSERT_EQ(parsed_docs_count, indexed_docs_count);
  }

  void writer_check_open_modes() {
    // APPEND to nonexisting index, shoud fail
    ASSERT_THROW(ir::index_writer::make(dir(), codec(), ir::OM_APPEND), ir::file_not_found);
    // read index in empty directory, should fail
    ASSERT_THROW(ir::directory_reader::open(dir(), codec()), ir::index_not_found);
    
    // create empty index
    {
      auto writer = ir::index_writer::make(dir(), codec(), ir::OM_CREATE);

      writer->commit();
    }

    // read empty index, it should not fail
    {
      auto reader = ir::directory_reader::open(dir(), codec());
      ASSERT_EQ(0, reader->docs_count());
      ASSERT_EQ(0, reader->docs_max());
      ASSERT_EQ(0, reader->size());
      ASSERT_EQ(reader->begin(), reader->end());
    }

    // append to index
    {
      auto writer = ir::index_writer::make(dir(), codec(), ir::OM_APPEND);
      tests::json_doc_generator gen(
        resource("simple_sequential.json"), 
        &tests::generic_json_field_factory
      );
      tests::document const* doc1 = gen.next();
      ASSERT_EQ(0, writer->buffered_docs());
      writer->add(doc1->begin(), doc1->end());
      ASSERT_EQ(1, writer->buffered_docs());
      writer->commit();
      ASSERT_EQ(0, writer->buffered_docs());
    }
    
    // read empty index, it should not fail
    {
      auto reader = ir::directory_reader::open(dir(), codec());
      ASSERT_EQ(1, reader->docs_count());
      ASSERT_EQ(1, reader->docs_max());
      ASSERT_EQ(1, reader->size());
      ASSERT_NE(reader->begin(), reader->end());
    }
  }
  
  void writer_transaction_isolation() {
    tests::json_doc_generator gen(
      resource("simple_sequential.json"),
      [] (tests::document& doc, const std::string& name, const tests::json::json_value& data) {
        if (data.quoted) {
          doc.add(new templates::string_field(
            ir::string_ref(name),
            ir::string_ref(data.value),
            true, true));
        }
      });
      tests::document const* doc1 = gen.next();
      tests::document const* doc2 = gen.next();
      
      auto writer = ir::index_writer::make(dir(), codec(), ir::OM_CREATE);

      writer->add(doc1->begin(), doc1->end());
      ASSERT_EQ(1, writer->buffered_docs());
      writer->begin(); // start transaction #1 
      ASSERT_EQ(0, writer->buffered_docs());
      writer->add(doc2->begin(), doc2->end()); // add another document while transaction in opened
      ASSERT_EQ(1, writer->buffered_docs());
      writer->commit(); // finish transaction #1
      ASSERT_EQ(1, writer->buffered_docs()); // still have 1 buffered document not included into transaction #1

      // check index, 1 document in 1 segment 
      {
        auto reader = ir::directory_reader::open(dir(), codec());
        ASSERT_EQ(1, reader->docs_count());
        ASSERT_EQ(1, reader->docs_max());
        ASSERT_EQ(1, reader->size());
        ASSERT_NE(reader->begin(), reader->end());        
      }

      writer->commit(); // transaction #2
      ASSERT_EQ(0, writer->buffered_docs());
      // check index, 2 documents in 2 segments
      {
        auto reader = ir::directory_reader::open(dir(), codec());
        ASSERT_EQ(2, reader->docs_count());
        ASSERT_EQ(2, reader->docs_max());
        ASSERT_EQ(2, reader->size());
        ASSERT_NE(reader->begin(), reader->end());
      }

      // check documents
      {       
        std::unordered_map<iresearch::string_ref, std::function<void(iresearch::data_input&)>> codecs{
          { "name", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
          { "same", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
          { "duplicated", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
          { "prefix", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
          { "seq", [](iresearch::data_input& in)->void{ iresearch::read_zvdouble(in); } },
          { "value", [](iresearch::data_input& in)->void{ iresearch::read_zvdouble(in); } },
        };

        const iresearch::string_ref expected_name = "name";
        iresearch::string_ref expected_value;
        iresearch::index_reader::document_visitor_f visitor = [&codecs, &expected_value, &expected_name](
          const iresearch::field_meta& field, iresearch::data_input& in
        ) {
          if (field.name != expected_name) {
            auto it = codecs.find(field.name);
            if (codecs.end() == it) {
              return false; // can't find codec
            }
            it->second(in); // skip field
            return true;
          }

          auto value = iresearch::read_string<std::string>(in);
          if (value != expected_value) {
            return false;
          }

          return true;
        };

        auto reader = ir::directory_reader::open(dir(), codec());

        // segment #1
        {
          auto& segment = (*reader)[0];
          auto terms = segment.terms("same");
          ASSERT_NE(nullptr, terms);
          auto termItr = terms->iterator();
          ASSERT_TRUE(termItr->next());
          auto docsItr = termItr->postings(iresearch::flags());
          ASSERT_TRUE(docsItr->next());
          expected_value = "A";
          ASSERT_TRUE(segment.document(docsItr->value(), visitor));
          ASSERT_FALSE(docsItr->next());
        }
        
        // segment #1
        {
          auto& segment = (*reader)[1];
          auto terms = segment.terms("same");
          ASSERT_NE(nullptr, terms);
          auto termItr = terms->iterator();
          ASSERT_TRUE(termItr->next());
          auto docsItr = termItr->postings(iresearch::flags());
          ASSERT_TRUE(docsItr->next());
          expected_value = "B";
          ASSERT_TRUE(segment.document(docsItr->value(), visitor));
          ASSERT_FALSE(docsItr->next());
        }
      }
  }
  
  void writer_begin_rollback() {
      tests::json_doc_generator gen(
        resource("simple_sequential.json"), 
        &tests::generic_json_field_factory
      );
      tests::document const* doc1 = gen.next();

      auto writer = ir::index_writer::make(dir(), codec(), ir::OM_CREATE);

      writer->add(doc1->begin(), doc1->end());
      writer->rollback(); // does nothing
      ASSERT_EQ(1, writer->buffered_docs());
      ASSERT_TRUE(writer->begin());
      ASSERT_FALSE(writer->begin()); // try to begin already opened transaction 

      // index still does not exist
      ASSERT_THROW(ir::directory_reader::open(dir(), codec()), ir::index_not_found);

      writer->rollback(); // rollback transaction
      writer->rollback(); // does nothing
      ASSERT_EQ(0, writer->buffered_docs());

      writer->commit(); // commit

      // check index, it should be empty 
      {
        auto reader = ir::directory_reader::open(dir(), codec());
        ASSERT_EQ(0, reader->docs_count());
        ASSERT_EQ(0, reader->docs_max());
        ASSERT_EQ(0, reader->size());
        ASSERT_EQ(reader->begin(), reader->end());
      }      
  }

  void read_empty_doc_attributes() {
    tests::json_doc_generator gen(
      resource("simple_sequential.json"),
      &tests::generic_json_field_factory
    );
    tests::document const* doc1 = gen.next();
    tests::document const* doc2 = gen.next();
    tests::document const* doc3 = gen.next();
    tests::document const* doc4 = gen.next();

    // write documents without attributes
    {
      auto writer = ir::index_writer::make(dir(), codec(), ir::OM_CREATE);

      // fields only
      writer->add(doc1->begin(), doc1->end());
      writer->add(doc2->begin(), doc2->end());
      writer->add(doc3->begin(), doc3->end());
      writer->add(doc4->begin(), doc4->end());
      writer->commit();
    }

    auto reader = ir::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = *reader->begin();

    // read attributes
    {
      size_t calls_count = 0;
      iresearch::columns_reader::value_reader_f visitor = [&calls_count] (data_input& in) {
        ++calls_count;
        return true;
      };

      {
        auto column = segment.values("name", visitor);
        ASSERT_FALSE(column(0)); 
        ASSERT_FALSE(column(1));
        ASSERT_FALSE(column(2));
        ASSERT_FALSE(column(3));
      }
    }
  }

  void read_write_doc_attributes() {
    tests::json_doc_generator gen(
      resource("simple_sequential.json"),
      &tests::generic_json_field_factory
    );
    tests::document const* doc1 = gen.next();
    tests::document const* doc2 = gen.next();
    tests::document const* doc3 = gen.next();
    tests::document const* doc4 = gen.next();

    // write documents
    {
      auto writer = ir::index_writer::make(dir(), codec(), ir::OM_CREATE);

      // attributes only
      writer->add(doc1->end(), doc1->end(), doc1->begin(), doc1->end());
      writer->add(doc2->end(), doc2->end(), doc2->begin(), doc2->end());
      writer->add(doc3->end(), doc3->end(), doc3->begin(), doc3->end());
      writer->add(doc4->end(), doc4->end(), doc4->begin(), doc4->end());
      writer->commit();
    }

    auto reader = ir::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = *reader->begin();

    // read attribute from invalid column
    {
      size_t calls_count = 0;
      iresearch::columns_reader::value_reader_f visitor = [&calls_count] (data_input& in) {
        ++calls_count;
        return true;
      };

      auto value_reader = segment.values("invalid_column", visitor);
      ASSERT_FALSE(value_reader(0));
      ASSERT_EQ(0, calls_count);
      ASSERT_FALSE(value_reader(1));
      ASSERT_EQ(0, calls_count);
      ASSERT_FALSE(value_reader(2));
      ASSERT_EQ(0, calls_count);
      ASSERT_FALSE(value_reader(3));
      ASSERT_EQ(0, calls_count);
      ASSERT_FALSE(value_reader(4));
      ASSERT_EQ(0, calls_count);
    }

    // read attributes from 'name' column (dense)
    {
      std::string expected_value;
      size_t calls_count = 0;
      iresearch::columns_reader::value_reader_f visitor = [&calls_count, &expected_value] (data_input& in) {
        ++calls_count;
        return expected_value == iresearch::read_string<std::string>(in);
      };

      auto value_reader = segment.values("name", visitor);
      expected_value = "B"; // 'name' value in doc2
      ASSERT_TRUE(value_reader(2));
      expected_value = "D"; // 'name' value in doc4
      ASSERT_TRUE(value_reader(4));
      expected_value = "A"; // 'name' value in doc1
      ASSERT_TRUE(value_reader(1));
      expected_value = "C"; // 'name' value in doc3
      ASSERT_TRUE(value_reader(3));
      ASSERT_EQ(4, calls_count);
      ASSERT_FALSE(value_reader(5)); // invalid document id
      ASSERT_EQ(4, calls_count);
    }

    // read attributes from 'prefix' column (sparse)
    {
      std::string expected_value;
      size_t calls_count = 0;
      iresearch::columns_reader::value_reader_f visitor = [&calls_count, &expected_value] (data_input& in) {
        ++calls_count;
        return expected_value == iresearch::read_string<std::string>(in);
      };

      auto value_reader = segment.values("prefix", visitor);
      expected_value = "abcd"; // 'prefix' value in doc1
      ASSERT_TRUE(value_reader(1)); 
      ASSERT_FALSE(value_reader(2)); // doc2 does not contain 'prefix' column
      expected_value = "abcde"; // 'prefix' value in doc4
      ASSERT_TRUE(value_reader(4));
      ASSERT_FALSE(value_reader(3)); // doc3 does not contain 'prefix' column
      ASSERT_EQ(2, calls_count);
      ASSERT_FALSE(value_reader(5)); // invalid document id
      ASSERT_EQ(2, calls_count);
    }
  }

  void writer_atomicity_check() {
    struct override_sync_directory : directory_mock {
      typedef std::function<bool (const std::string&)> sync_f;

      override_sync_directory(ir::directory& impl, sync_f&& sync)
        : directory_mock(impl), sync_(std::move(sync)) {
      }

      virtual void sync(const std::string& name) override {
        if (!sync_(name)) {
          directory_mock::sync(name);
        }
      }

      sync_f sync_;
    }; // invalid_sync_directory 

    // create empty index
    {
      auto writer = ir::index_writer::make(dir(), codec(), ir::OM_CREATE);

      writer->commit();
    }

    // error while commiting index (during sync in index_meta_writer)
    {
      override_sync_directory override_dir(
        dir(), [this] (const ir::string_ref& name) {
          throw ir::io_error();
          return true;
      });

      tests::json_doc_generator gen(
        resource("simple_sequential.json"), 
        &tests::generic_json_field_factory
      );
      tests::document const* doc1 = gen.next();
      tests::document const* doc2 = gen.next();
      tests::document const* doc3 = gen.next();
      tests::document const* doc4 = gen.next();

      auto writer = ir::index_writer::make(override_dir, codec(), ir::OM_APPEND);

      writer->add(doc1->begin(), doc1->end());
      writer->add(doc2->begin(), doc2->end());
      writer->add(doc3->begin(), doc3->end());
      writer->add(doc4->begin(), doc4->end());
      ASSERT_THROW(writer->commit(), ir::illegal_state);
    }

    // error while commiting index (during sync in index_writer)
    {
      override_sync_directory override_dir(
        dir(), [this] (const ir::string_ref& name) {
        if (ir::starts_with(name, "_")) {
          throw ir::io_error();
        }
        return false;
      });

      tests::json_doc_generator gen(
        resource("simple_sequential.json"), 
        &tests::generic_json_field_factory
      );
      tests::document const* doc1 = gen.next();
      tests::document const* doc2 = gen.next();
      tests::document const* doc3 = gen.next();
      tests::document const* doc4 = gen.next();

      auto writer = ir::index_writer::make(override_dir, codec(), ir::OM_APPEND);

      writer->add(doc1->begin(), doc1->end());
      writer->add(doc2->begin(), doc2->end());
      writer->add(doc3->begin(), doc3->end());
      writer->add(doc4->begin(), doc4->end());
      ASSERT_THROW(writer->commit(), ir::io_error);
    }
    
    // check index, it should be empty 
    {
      auto reader = ir::directory_reader::open(dir(), codec());
      ASSERT_EQ(0, reader->docs_count());
      ASSERT_EQ(0, reader->docs_max());
      ASSERT_EQ(0, reader->size());
      ASSERT_EQ(reader->begin(), reader->end());
    }
  }
}; // index_test_case

class memory_test_case_base : public index_test_case_base {
protected:
  virtual ir::directory* get_directory() override {
    return new ir::memory_directory();
  }

  virtual ir::format::ptr get_codec() override {
    return ir::formats::get("1_0");
  }
}; // memory_test_case_base

class fs_test_case_base : public index_test_case_base { 
protected:
  virtual ir::directory* get_directory() override {
    const fs::path dir = fs::path( test_dir() ).append( "index" );
    return new iresearch::fs_directory(dir.string());
  }

  virtual ir::format::ptr get_codec() override {
    return ir::formats::get("1_0");
  }
}; // fs_test_case_base 

namespace cases {

template<typename Base>
class tfidf : public Base {
 public:
  using Base::assert_index;
  using Base::get_codec;
  using Base::get_directory;

  void assert_index(size_t skip = 0) const {
    assert_index(ir::flags(), skip);
    assert_index(ir::flags{ ir::document::type() }, skip);
    assert_index(ir::flags{ ir::document::type(), ir::frequency::type() }, skip);
    assert_index(ir::flags{ ir::document::type(), ir::frequency::type(), ir::position::type() }, skip);
    assert_index(ir::flags{ ir::document::type(), ir::frequency::type(), ir::position::type(), ir::offset::type() }, skip);
    assert_index(ir::flags{ ir::document::type(), ir::frequency::type(), ir::position::type(), ir::payload::type() }, skip);
    assert_index(ir::flags{ ir::document::type(), ir::frequency::type(), ir::position::type(), ir::payload::type(), ir::offset::type() }, skip);
  }
};

} // cases 
} // tests

// ----------------------------------------------------------------------------
// --SECTION--                           memory_directory + iresearch_format_10
// ----------------------------------------------------------------------------

class memory_index_test 
  : public tests::cases::tfidf<tests::memory_test_case_base> {
}; // memory_index_test

TEST_F(memory_index_test, arango_demo_docs) {
  {
    tests::json_doc_generator gen(
      resource("arango_demo.json"),
      &tests::generic_json_field_factory);
    add_segment(gen);
  }
  assert_index();
}

TEST_F(memory_index_test, read_write_doc_attributes) {
  read_write_doc_attributes();
  read_empty_doc_attributes();
}

TEST_F(memory_index_test, open_writer) {
  open_writer_check_lock();
}

TEST_F(memory_index_test, check_writer_open_modes) {
  writer_check_open_modes();
}

TEST_F(memory_index_test, writer_transaction_isolation) {
  writer_transaction_isolation();
}

TEST_F(memory_index_test, writer_atomicity_check) {
  writer_atomicity_check();
}

TEST_F(memory_index_test, writer_begin_rollback) {
  writer_begin_rollback();
}

TEST_F(memory_index_test, europarl_docs) {
  {
    tests::templates::europarl_doc_template doc;
    tests::delim_doc_generator gen(resource("europarl.subset.txt"), doc);
    add_segment(gen);
  }
  assert_index();
}

TEST_F(memory_index_test, monarch_eco_onthology) {
  {
    tests::json_doc_generator gen(
      resource("ECO_Monarch.json"), 
      &tests::payloaded_json_field_factory);
    add_segment(gen);
  }
  assert_index();
}

TEST_F(memory_index_test, concurrent_add) {
  tests::json_doc_generator gen(resource("simple_sequential.json"), &tests::generic_json_field_factory);
  std::vector<const tests::document*> docs;

  for (const tests::document* doc; (doc = gen.next()) != nullptr; docs.emplace_back(doc));

  {
    auto writer = open_writer();

    std::thread thread0([&writer, docs](){
      for (size_t i = 0, count = docs.size(); i < count; i += 2) {
        auto& doc = docs[i];
        writer->add(doc->begin(), doc->end());
      }
    });
    std::thread thread1([&writer, docs](){
      for (size_t i = 1, count = docs.size(); i < count; i += 2) {
        auto& doc = docs[i];
        writer->add(doc->begin(), doc->end());
      }
    });

    thread0.join();
    thread1.join();
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_TRUE(reader->size() == 1 || reader->size() == 2); // can be 1 if thread0 finishes before thread1 starts
    ASSERT_EQ(docs.size(), reader->docs_count());
  }
}

TEST_F(memory_index_test, concurrent_add_remove) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [] (tests::document& doc, const std::string& name, const tests::json::json_value& data) {
    if (data.quoted) {
      doc.add(new tests::templates::string_field(
        ir::string_ref(name),
        ir::string_ref(data.value),
        true, true));
      }
  });
  std::vector<const tests::document*> docs;
  std::atomic<bool> first_doc(false);

  for (const tests::document* doc; (doc = gen.next()) != nullptr; docs.emplace_back(doc));

  {
    auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());
    auto writer = open_writer();

    std::thread thread0([&writer, docs, &first_doc]() {
      auto& doc = docs[0];
      writer->add(doc->begin(), doc->end());
      first_doc = true;

      for (size_t i = 2, count = docs.size(); i < count; i += 2) { // skip first doc
        auto& doc = docs[i];
        writer->add(doc->begin(), doc->end());
      }
    });
    std::thread thread1([&writer, docs](){
      for (size_t i = 1, count = docs.size(); i < count; i += 2) {
        auto& doc = docs[i];
        writer->add(doc->begin(), doc->end());
      }
    });
    std::thread thread2([&writer,&query_doc1, &first_doc](){
      while(!first_doc); // busy-wait until first document loaded
      writer->remove(std::move(query_doc1.filter));
    });

    thread0.join();
    thread1.join();
    thread2.join();
    writer->commit();

    std::unordered_set<std::string> expected = { "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "~", "!", "@", "#", "$", "%" };
    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_TRUE(reader->size() == 1 || reader->size() == 2); // can be 1 if thread0 finishes before thread1 starts
    ASSERT_EQ(docs.size(), reader->docs_count());

    std::unordered_map<iresearch::string_ref, std::function<void(iresearch::data_input&)>> codecs{
      { "name", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
      { "same", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
      { "duplicated", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
      { "prefix", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
      { "seq", [](iresearch::data_input& in)->void{ iresearch::read_zvdouble(in); } },
      { "value", [](iresearch::data_input& in)->void{ iresearch::read_zvdouble(in); } },
    };

    const iresearch::string_ref expected_name = "name";
    iresearch::index_reader::document_visitor_f visitor = [&codecs, &expected, &expected_name](
      const iresearch::field_meta& field, iresearch::data_input& in
    ) {
      if (field.name != expected_name) {
        auto it = codecs.find(field.name);
        if (codecs.end() == it) {
          return false; // can't find codec
        }
        it->second(in); // skip field
        return true;
      }

      auto value = ir::read_string<std::string>(in);
      if (1 != expected.erase(value)) {
        return false;
      }

      return true;
    };

    for (size_t i = 0, count = reader->size(); i < count; ++i) {
      auto& segment = (*reader)[i];
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      while(docsItr->next()) {
        ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      }
    }

    ASSERT_TRUE(expected.empty());
  }
}

TEST_F(memory_index_test, doc_removal) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [] (tests::document& doc, const std::string& name, const tests::json::json_value& data) {
    if (data.quoted) {
      doc.add(new tests::templates::string_field(
        ir::string_ref(name),
        ir::string_ref(data.value),
        true, true));
      }
  });
        
  std::unordered_map<iresearch::string_ref, std::function<void(iresearch::data_input&)>> codecs{
    { "name", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
    { "same", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
    { "duplicated", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
    { "prefix", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
    { "seq", [](iresearch::data_input& in)->void{ iresearch::read_zvdouble(in); } },
    { "value", [](iresearch::data_input& in)->void{ iresearch::read_zvdouble(in); } },
  };

  const iresearch::string_ref expected_name = "name";
  iresearch::string_ref expected_value;
  iresearch::index_reader::document_visitor_f visitor = [&codecs, &expected_value, &expected_name](
    const iresearch::field_meta& field, iresearch::data_input& in
  ) {
    if (field.name != expected_name) {
      auto it = codecs.find(field.name);
      if (codecs.end() == it) {
        return false; // can't find codec
      }
      it->second(in); // skip field
      return true;
    }

    auto value = iresearch::read_string<std::string>(in);
    if (value != expected_value) {
      return false;
    }

    return true;
  };

  tests::document const* doc1 = gen.next();
  tests::document const* doc2 = gen.next();
  tests::document const* doc3 = gen.next();
  tests::document const* doc4 = gen.next();
  tests::document const* doc5 = gen.next();
  tests::document const* doc6 = gen.next();
  tests::document const* doc7 = gen.next();
  tests::document const* doc8 = gen.next();
  tests::document const* doc9 = gen.next();

  // new segment: add
  {
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "A"; // 'name' value in doc1
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // new segment: add + remove 1st (as reference)
  {
    auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->remove(*(query_doc1.filter.get()));
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // new segment: add + remove 1st (as unique_ptr)
  {
    auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->remove(std::move(query_doc1.filter));
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // new segment: add + remove 1st (as shared_ptr)
  {
    auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->remove(std::shared_ptr<iresearch::filter>(std::move(query_doc1.filter)));
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // new segment: remove + add
  {
    auto query_doc2 = iresearch::iql::query_builder().build("name==B", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->remove(std::move(query_doc2.filter)); // not present yet
    writer->add(doc2->begin(), doc2->end());
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "A"; // 'name' value in doc1
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // new segment: add + remove + readd
  {
    auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->remove(std::move(query_doc1.filter));
    writer->add(doc1->begin(), doc1->end());
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "A"; // 'name' value in doc1
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // new segment: add + remove, old segment: remove
  {
    auto query_doc2 = iresearch::iql::query_builder().build("name==B", std::locale::classic());
    auto query_doc3 = iresearch::iql::query_builder().build("name==C", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->add(doc3->begin(), doc3->end());
    writer->remove(std::move(query_doc3.filter));
    writer->commit(); // document mask with 'doc3' created
    writer->remove(std::move(query_doc2.filter));
    writer->commit(); // new document mask with 'doc2','doc3' created

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());

    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "A"; // 'name' value in doc1
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // new segment: add + add, old segment: remove + remove + add
  {
    auto query_doc1_doc2 = iresearch::iql::query_builder().build("name==A||name==B", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->commit();
    writer->remove(std::move(query_doc1_doc2.filter));
    writer->add(doc3->begin(), doc3->end());
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "C"; // 'name' value in doc3
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // new segment: add, old segment: remove
  {
    auto query_doc2 = iresearch::iql::query_builder().build("name==B", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->commit();
    writer->add(doc3->begin(), doc3->end());
    writer->remove(std::move(query_doc2.filter));
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(2, reader->size());

    {
      auto& segment = (*reader)[0]; // assume 0 is id of old segment
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "A"; // 'name' value in doc1
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }

    {
      auto& segment = (*reader)[1]; // assume 1 is id of new segment
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "C"; // 'name' value in doc3
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }
  }

  // new segment: add + remove, old segment: remove
  {
    auto query_doc1_doc3 = iresearch::iql::query_builder().build("name==A || name==C", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->commit();
    writer->add(doc3->begin(), doc3->end());
    writer->add(doc4->begin(), doc4->end());
    writer->remove(std::move(query_doc1_doc3.filter));
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(2, reader->size());

    {
      auto& segment = (*reader)[0]; // assume 0 is id of old segment
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "B"; // 'name' value in doc2
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }

    {
      auto& segment = (*reader)[1]; // assume 1 is id of new segment
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "D"; // 'name' value in doc4
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }
  }

  // new segment: add + remove, old segment: add + remove old-old segment: remove
  {
    auto query_doc2_doc6_doc9 = iresearch::iql::query_builder().build("name==B||name==F||name==I", std::locale::classic());
    auto query_doc3_doc7 = iresearch::iql::query_builder().build("name==C||name==G", std::locale::classic());
    auto query_doc4 = iresearch::iql::query_builder().build("name==D", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end()); // A
    writer->add(doc2->begin(), doc2->end()); // B
    writer->add(doc3->begin(), doc3->end()); // C
    writer->add(doc4->begin(), doc4->end()); // D
    writer->remove(std::move(query_doc4.filter));
    writer->commit();
    writer->add(doc5->begin(), doc5->end()); // E
    writer->add(doc6->begin(), doc6->end()); // F
    writer->add(doc7->begin(), doc7->end()); // G
    writer->remove(std::move(query_doc3_doc7.filter));
    writer->commit();
    writer->add(doc8->begin(), doc8->end()); // H
    writer->add(doc9->begin(), doc9->end()); // I
    writer->remove(std::move(query_doc2_doc6_doc9.filter));
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(3, reader->size());

    {
      auto& segment = (*reader)[0]; // assume 0 is id of old-old segment
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "A"; // 'name' value in doc1
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }

    {
      auto& segment = (*reader)[1]; // assume 1 is id of old segment
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "E"; // 'name' value in doc5
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }

    {
      auto& segment = (*reader)[2]; // assume 2 is id of new segment
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "H"; // 'name' value in doc8
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }
  }
}

TEST_F(memory_index_test, doc_update) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [] (tests::document& doc, const std::string& name, const tests::json::json_value& data) {
    if (data.quoted) {
      doc.add(new tests::templates::string_field(
        ir::string_ref(name),
        ir::string_ref(data.value),
        true, true));
      }
  });
  
  std::unordered_map<iresearch::string_ref, std::function<void(iresearch::data_input&)>> codecs{
    { "name", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
    { "same", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
    { "duplicated", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
    { "prefix", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
    { "seq", [](iresearch::data_input& in)->void{ iresearch::read_zvdouble(in); } },
    { "value", [](iresearch::data_input& in)->void{ iresearch::read_zvdouble(in); } },
  };

  const iresearch::string_ref expected_name = "name";
  iresearch::string_ref expected_value;
  iresearch::index_reader::document_visitor_f visitor = [&codecs, &expected_value, &expected_name](
    const iresearch::field_meta& field, iresearch::data_input& in
  ) {
    if (field.name != expected_name) {
      auto it = codecs.find(field.name);
      if (codecs.end() == it) {
        return false; // can't find codec
      }
      it->second(in); // skip field
      return true;
    }

    auto value = iresearch::read_string<std::string>(in);
    if (value != expected_value) {
      return false;
    }

    return true;
  };
 
  tests::document const* doc1 = gen.next();
  tests::document const* doc2 = gen.next();
  tests::document const* doc3 = gen.next();
  tests::document const* doc4 = gen.next();

  // new segment update (as reference)
  {
    auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->update(*(query_doc1.filter.get()), doc2->begin(), doc2->end());
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // new segment update (as unique_ptr)
  {
    auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->update(std::move(query_doc1.filter), doc2->begin(), doc2->end());
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // new segment update (as shared_ptr)
  {
    auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->update(std::shared_ptr<iresearch::filter>(std::move(query_doc1.filter)), doc2->begin(), doc2->end());
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // old segment update
  {
    auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->commit();
    writer->update(std::move(query_doc1.filter), doc3->begin(), doc3->end());
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(2, reader->size());

    {
      auto& segment = (*reader)[0]; // assume 0 is id of old segment
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "B"; // 'name' value in doc2
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }

    {
      auto& segment = (*reader)[1]; // assume 1 is id of new segment
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "C"; // 'name' value in doc3
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }
  }

  // 3x updates (same segment)
  {
    auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());
    auto query_doc2 = iresearch::iql::query_builder().build("name==B", std::locale::classic());
    auto query_doc3 = iresearch::iql::query_builder().build("name==C", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->update(std::move(query_doc1.filter), doc2->begin(), doc2->end());
    writer->update(std::move(query_doc2.filter), doc3->begin(), doc3->end());
    writer->update(std::move(query_doc3.filter), doc4->begin(), doc4->end());
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "D"; // 'name' value in doc4
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // 3x updates (different segments)
  {
    auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());
    auto query_doc2 = iresearch::iql::query_builder().build("name==B", std::locale::classic());
    auto query_doc3 = iresearch::iql::query_builder().build("name==C", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->commit();
    writer->update(std::move(query_doc1.filter), doc2->begin(), doc2->end());
    writer->commit();
    writer->update(std::move(query_doc2.filter), doc3->begin(), doc3->end());
    writer->commit();
    writer->update(std::move(query_doc3.filter), doc4->begin(), doc4->end());
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "D"; // 'name' value in doc4
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // no matching documnts
  {
    auto query_doc2 = iresearch::iql::query_builder().build("name==B", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->commit();
    writer->update(std::move(query_doc2.filter), doc2->begin(), doc2->end()); // non-existent document
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "A"; // 'name' value in doc1
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // update + delete (same segment)
  {
    auto query_doc2 = iresearch::iql::query_builder().build("name==B", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->update(*(query_doc2.filter), doc3->begin(), doc3->end());
    writer->remove(*(query_doc2.filter)); // remove no longer existent
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "A"; // 'name' value in doc1
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "C"; // 'name' value in doc3
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // update + delete (different segments)
  {
    auto query_doc2 = iresearch::iql::query_builder().build("name==B", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->commit();
    writer->update(*(query_doc2.filter), doc3->begin(), doc3->end());
    writer->commit();
    writer->remove(*(query_doc2.filter)); // remove no longer existent
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(2, reader->size());

    {
      auto& segment = (*reader)[0]; // assume 0 is id of old segment
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "A"; // 'name' value in doc1
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }

    {
      auto& segment = (*reader)[1]; // assume 1 is id of new segment
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "C"; // 'name' value in doc3
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }
  }

  // delete + update (same segment)
  {
    auto query_doc2 = iresearch::iql::query_builder().build("name==B", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->remove(*(query_doc2.filter));
    writer->update(*(query_doc2.filter), doc3->begin(), doc3->end()); // update no longer existent
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "A"; // 'name' value in doc1
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // delete + update (different segments)
  {
    auto query_doc2 = iresearch::iql::query_builder().build("name==B", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->commit();
    writer->remove(*(query_doc2.filter));
    writer->commit();
    writer->update(*(query_doc2.filter), doc3->begin(), doc3->end()); // update no longer existent
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "A"; // 'name' value in doc1
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // delete + update then update (2nd - update of modified doc) (same segment)
  {
    auto query_doc2 = iresearch::iql::query_builder().build("name==B", std::locale::classic());
    auto query_doc3 = iresearch::iql::query_builder().build("name==C", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->remove(*(query_doc2.filter));
    writer->update(*(query_doc2.filter), doc3->begin(), doc3->end());
    writer->update(*(query_doc3.filter), doc4->begin(), doc4->end());
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "A"; // 'name' value in doc1
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // delete + update then update (2nd - update of modified doc) (different segments)
  {
    auto query_doc2 = iresearch::iql::query_builder().build("name==B", std::locale::classic());
    auto query_doc3 = iresearch::iql::query_builder().build("name==C", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->commit();
    writer->remove(*(query_doc2.filter));
    writer->commit();
    writer->update(*(query_doc2.filter), doc3->begin(), doc3->end());
    writer->commit();
    writer->update(*(query_doc3.filter), doc4->begin(), doc4->end());
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "A"; // 'name' value in doc1
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }
}

TEST_F(memory_index_test, import_reader) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [] (tests::document& doc, const std::string& name, const tests::json::json_value& data) {
    if (data.quoted) {
      doc.add(new tests::templates::string_field(
        ir::string_ref(name),
        ir::string_ref(data.value),
        true, true));
      }
  });
  
  std::unordered_map<iresearch::string_ref, std::function<void(iresearch::data_input&)>> codecs{
    { "name", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
    { "same", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
    { "duplicated", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
    { "prefix", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
    { "seq", [](iresearch::data_input& in)->void{ iresearch::read_zvdouble(in); } },
    { "value", [](iresearch::data_input& in)->void{ iresearch::read_zvdouble(in); } },
  };

  const iresearch::string_ref expected_name = "name";
  iresearch::string_ref expected_value;
  iresearch::index_reader::document_visitor_f visitor = [&codecs, &expected_value, &expected_name](
    const iresearch::field_meta& field, iresearch::data_input& in
  ) {
    if (field.name != expected_name) {
      auto it = codecs.find(field.name);
      if (codecs.end() == it) {
        return false; // can't find codec
      }
      it->second(in); // skip field
      return true;
    }

    auto value = iresearch::read_string<std::string>(in);
    if (value != expected_value) {
      return false;
    }

    return true;
  };

  tests::document const* doc1 = gen.next();
  tests::document const* doc2 = gen.next();
  tests::document const* doc3 = gen.next();
  tests::document const* doc4 = gen.next();

  // add a reader with 1 full segment
  {
    iresearch::memory_directory data_dir;
    auto data_writer = iresearch::index_writer::make(data_dir, codec(), iresearch::OM_CREATE);
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    data_writer->commit();
    ASSERT_TRUE(writer->import(*(iresearch::directory_reader::open(data_dir, codec()))));
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    ASSERT_EQ(2, segment.docs_count());
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "A"; // 'name' value in doc1
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // add a reader with 1 sparse segment
  {
    auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());
    iresearch::memory_directory data_dir;
    auto data_writer = iresearch::index_writer::make(data_dir, codec(), iresearch::OM_CREATE);
    auto writer = open_writer();

    data_writer->add(doc1->begin(), doc1->end());
    data_writer->add(doc2->begin(), doc2->end());
    data_writer->remove(std::move(query_doc1.filter));
    data_writer->commit();
    ASSERT_TRUE(writer->import(*(iresearch::directory_reader::open(data_dir, codec()))));
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    ASSERT_EQ(1, segment.docs_count());
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // add a reader with 2 full segments
  {
    iresearch::memory_directory data_dir;
    auto data_writer = iresearch::index_writer::make(data_dir, codec(), iresearch::OM_CREATE);
    auto writer = open_writer();

    data_writer->add(doc1->begin(), doc1->end());
    data_writer->add(doc2->begin(), doc2->end());
    data_writer->commit();
    data_writer->add(doc3->begin(), doc3->end());
    data_writer->add(doc4->begin(), doc4->end());
    data_writer->commit();
    ASSERT_TRUE(writer->import(*(iresearch::directory_reader::open(data_dir, codec()))));
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    ASSERT_EQ(4, segment.docs_count());
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "A"; // 'name' value in doc1
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "C"; // 'name' value in doc3
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "D"; // 'name' value in doc4
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // add a reader with 2 sparse segments
  {
    auto query_doc2_doc3 = iresearch::iql::query_builder().build("name==B||name==C", std::locale::classic());
    iresearch::memory_directory data_dir;
    auto data_writer = iresearch::index_writer::make(data_dir, codec(), iresearch::OM_CREATE);
    auto writer = open_writer();

    data_writer->add(doc1->begin(), doc1->end());
    data_writer->add(doc2->begin(), doc2->end());
    data_writer->commit();
    data_writer->add(doc3->begin(), doc3->end());
    data_writer->add(doc4->begin(), doc4->end());
    data_writer->remove(std::move(query_doc2_doc3.filter));
    data_writer->commit();
    ASSERT_TRUE(writer->import(*(iresearch::directory_reader::open(data_dir, codec()))));
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    ASSERT_EQ(2, segment.docs_count());
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "A"; // 'name' value in doc1
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "D"; // 'name' value in doc4
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // add a reader with 2 mixed segments
  {
    auto query_doc4 = iresearch::iql::query_builder().build("name==D", std::locale::classic());
    iresearch::memory_directory data_dir;
    auto data_writer = iresearch::index_writer::make(data_dir, codec(), iresearch::OM_CREATE);
    auto writer = open_writer();

    data_writer->add(doc1->begin(), doc1->end());
    data_writer->add(doc2->begin(), doc2->end());
    data_writer->commit();
    data_writer->add(doc3->begin(), doc3->end());
    data_writer->add(doc4->begin(), doc4->end());
    data_writer->remove(std::move(query_doc4.filter));
    data_writer->commit();
    ASSERT_TRUE(writer->import(*(iresearch::directory_reader::open(data_dir, codec()))));
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    ASSERT_EQ(3, segment.docs_count());
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "A"; // 'name' value in doc1
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "C"; // 'name' value in doc3
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // new: add + add + delete, old: import
  {
    auto query_doc2 = iresearch::iql::query_builder().build("name==B", std::locale::classic());
    iresearch::memory_directory data_dir;
    auto data_writer = iresearch::index_writer::make(data_dir, codec(), iresearch::OM_CREATE);
    auto writer = open_writer();

    data_writer->add(doc1->begin(), doc1->end());
    data_writer->add(doc2->begin(), doc2->end());
    data_writer->commit();
    writer->add(doc3->begin(), doc3->end());
    writer->remove(std::move(query_doc2.filter)); // should not match any documents
    ASSERT_TRUE(writer->import(*(iresearch::directory_reader::open(data_dir, codec()))));
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(2, reader->size());

    {
      auto& segment = (*reader)[1]; // assume 1 is id of imported segment
      ASSERT_EQ(2, segment.docs_count());
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "A"; // 'name' value in doc1
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_TRUE(docsItr->next());
      expected_value = "B"; // 'name' value in doc2
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }

    {
      auto& segment = (*reader)[0]; // assume 0 is id of original segment
      ASSERT_EQ(1, segment.docs_count());
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "C"; // 'name' value in doc3
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }
  }
}

TEST_F(memory_index_test, profile_bulk_index_singlethread_full) {
  profile_bulk_index(0, 0);
}

TEST_F(memory_index_test, profile_bulk_index_singlethread_batched) {
  profile_bulk_index(0, 10000);
}

TEST_F(memory_index_test, profile_bulk_index_multithread_dedicated_commit) {
  profile_bulk_index_dedicated_commit(16, 1, 1000);
}

TEST_F(memory_index_test, profile_bulk_index_multithread_full) {
  profile_bulk_index(16, 0);
}

TEST_F(memory_index_test, profile_bulk_index_multithread_batched) {
  profile_bulk_index(16, 10000);
}

TEST_F(memory_index_test, refresh_reader) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [] (tests::document& doc, const std::string& name, const tests::json::json_value& data) {
    if (data.quoted) {
      doc.add(new tests::templates::string_field(
        ir::string_ref(name),
        ir::string_ref(data.value),
        true, true));
      }
  });


  std::unordered_map<iresearch::string_ref, std::function<void(iresearch::data_input&)>> codecs{
    { "name", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
    { "same", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
    { "duplicated", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
    { "prefix", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
    { "seq", [](iresearch::data_input& in)->void{ iresearch::read_zvdouble(in); } },
    { "value", [](iresearch::data_input& in)->void{ iresearch::read_zvdouble(in); } },
  };

  const iresearch::string_ref expected_name = "name";
  iresearch::string_ref expected_value;
  iresearch::index_reader::document_visitor_f visitor = [&codecs, &expected_value, &expected_name](
    const iresearch::field_meta& field, iresearch::data_input& in
  ) {
    if (field.name != expected_name) {
      auto it = codecs.find(field.name);
      if (codecs.end() == it) {
        return false; // can't find codec
      }
      it->second(in); // skip field
      return true;
    }

    auto value = iresearch::read_string<std::string>(in);
    if (value != expected_value) {
      return false;
    }

    return true;
  };
  
  tests::document const* doc1 = gen.next();
  tests::document const* doc2 = gen.next();
  tests::document const* doc3 = gen.next();
  tests::document const* doc4 = gen.next();

  // initial state (1st segment 2 docs)
  {
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->commit();
  }

  // refreshable reader
  auto reader = iresearch::directory_reader::open(dir(), codec());

  // validate state
  {
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "A"; // 'name' value in doc1
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // modify state (delete doc2)
  {
    auto writer = open_writer(ir::OPEN_MODE::OM_APPEND);
    auto query_doc2 = iresearch::iql::query_builder().build("name==B", std::locale::classic());

    writer->remove(std::move(query_doc2.filter));
    writer->commit();
  }

  // validate state pre/post refresh (existing segment changed)
  {
    {
      ASSERT_EQ(1, reader->size());
      auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "A"; // 'name' value in doc1
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_TRUE(docsItr->next());
      expected_value = "B"; // 'name' value in doc2
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }

    {
      reader->refresh();
      ASSERT_EQ(1, reader->size());
      auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "A"; // 'name' value in doc1
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }
  }

  // modify state (2nd segment 2 docs)
  {
    auto writer = open_writer(ir::OPEN_MODE::OM_APPEND);

    writer->add(doc3->begin(), doc3->end());
    writer->add(doc4->begin(), doc4->end());
    writer->commit();
  }

  // validate state pre/post refresh (new segment added)
  {
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "A"; // 'name' value in doc1
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());

    reader->refresh();
    ASSERT_EQ(2, reader->size());

    {
      auto& segment = (*reader)[0]; // assume 0 is id of first segment
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "A"; // 'name' value in doc1
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }

    {
      auto& segment = (*reader)[1]; // assume 1 is id of second segment
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "C"; // 'name' value in doc3
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_TRUE(docsItr->next());
      expected_value = "D"; // 'name' value in doc4
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }
  }

  // modify state (delete doc1)
  {
    auto writer = open_writer(ir::OPEN_MODE::OM_APPEND);
    auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());

    writer->remove(std::move(query_doc1.filter));
    writer->commit();
    writer->defragment(iresearch::index_utils::defragment_fill(0.5)); // value garanteeing merge
  }

  // validate state pre/post refresh (old segment removed)
  {
    ASSERT_EQ(2, reader->size());

    {
      auto& segment = (*reader)[0]; // assume 0 is id of first segment
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "A"; // 'name' value in doc1
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }

    {
      auto& segment = (*reader)[1]; // assume 1 is id of second segment
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(iresearch::flags());
      ASSERT_TRUE(docsItr->next());
      expected_value = "C"; // 'name' value in doc3
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_TRUE(docsItr->next());
      expected_value = "D"; // 'name' value in doc4
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      ASSERT_FALSE(docsItr->next());
    }

    reader->refresh();
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of second segment
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "C"; // 'name' value in doc3
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "D"; // 'name' value in doc4
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }
}

TEST_F(memory_index_test, reuse_segment_writer) {
  tests::json_doc_generator gen0(resource("arango_demo.json"), &tests::generic_json_field_factory);
  tests::json_doc_generator gen1(resource("simple_sequential.json"), &tests::generic_json_field_factory);
  auto writer = open_writer();

  // populate initial 2 very small segments
  {
    {
      auto& index_ref = const_cast<tests::index_t&>(index());
      index_ref.emplace_back();
      gen0.reset();
      write_segment(*writer, index_ref.back(), gen0);
      writer->commit();
    }

    {
      auto& index_ref = const_cast<tests::index_t&>(index());
      index_ref.emplace_back();
      gen1.reset();
      write_segment(*writer, index_ref.back(), gen1);
      writer->commit();
    }
  }

  // populate initial small segment
  {
    auto& index_ref = const_cast<tests::index_t&>(index());
    index_ref.emplace_back();
    gen0.reset();
    write_segment(*writer, index_ref.back(), gen0);
    gen1.reset();
    write_segment(*writer, index_ref.back(), gen1);
    writer->commit();
  }

  // populate initial large segment
  {
    auto& index_ref = const_cast<tests::index_t&>(index());
    index_ref.emplace_back();

    for(size_t i = 100; i > 0; --i) {
      gen0.reset();
      write_segment(*writer, index_ref.back(), gen0);
      gen1.reset();
      write_segment(*writer, index_ref.back(), gen1);
    }

    writer->commit();
  }

  // populate and validate small segments in hopes of triggering segment_writer reuse
  // 10 iterations, although 2 should be enough since index_wirter::flush_context_pool_.size() == 2
  for(size_t i = 10; i > 0; --i) {
    auto& index_ref = const_cast<tests::index_t&>(index());
    index_ref.emplace_back();

    // add varying sized segments
    for (size_t j = 0; j < i; ++j) {
      // add test documents
      if (i%3 == 0 || i%3 == 1) {
        gen0.reset();
        write_segment(*writer, index_ref.back(), gen0);
      }

      // add different test docs (overlap to make every 3rd segment contain docs from both sources)
      if (i%3 == 1 || i%3 == 2) {
        gen1.reset();
        write_segment(*writer, index_ref.back(), gen1);
      }
    }

    writer->commit();
  }

  assert_index();

  // merge all segments
  {
    auto merge_all = [](const iresearch::directory& dir, const iresearch::index_meta& meta)->iresearch::index_writer::defragment_acceptor_t {
      return [](const iresearch::segment_meta& meta)->bool { return true; };
    };

    writer->defragment(merge_all);
    writer->commit();
  }
}

TEST_F(memory_index_test, segment_defragment) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [] (tests::document& doc, const std::string& name, const tests::json::json_value& data) {
      if (data.quoted) {
        doc.add(new tests::templates::string_field(
          ir::string_ref(name),
          ir::string_ref(data.value),
          true, true));
      }
  });
  
  const iresearch::string_ref expected_name = "name";
  iresearch::string_ref expected_value;
  iresearch::index_reader::document_visitor_f visitor = [&expected_value, &expected_name](
    const ir::field_meta& field, data_input& in
  ) {
    if (field.name != expected_name) {
      iresearch::read_string<std::string>(in); // skip field
      return true;
    }

    auto value = ir::read_string<std::string>(in);
    if (value != expected_value) {
      return false;
    }

    return true;
  };

  tests::document const* doc1 = gen.next();
  tests::document const* doc2 = gen.next();
  tests::document const* doc3 = gen.next();
  tests::document const* doc4 = gen.next();
  tests::document const* doc5 = gen.next();
  tests::document const* doc6 = gen.next();
  auto always_merge = [](const iresearch::directory& dir, const iresearch::index_meta& meta)->iresearch::index_writer::defragment_acceptor_t {
    return [](const iresearch::segment_meta& meta)->bool { return true; };
  };
  auto all_features = ir::flags{ ir::document::type(), ir::frequency::type(), ir::position::type(), ir::payload::type(), ir::offset::type() };

  // remove empty new segment
  {
    auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->remove(std::move(query_doc1.filter));
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(0, reader->size());
  }

  // remove empty old segment
  {
    auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->commit();
    writer->remove(std::move(query_doc1.filter));
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(0, reader->size());
  }

  // remove empty old, defragment new
  {
    auto query_doc1_doc2 = iresearch::iql::query_builder().build("name==A||name==B", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->commit();
    writer->add(doc2->begin(), doc2->end());
    writer->add(doc3->begin(), doc3->end());
    writer->remove(std::move(query_doc1_doc2.filter));
    writer->commit();
    writer->defragment(always_merge);
    writer->commit();

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().add(doc3->begin(), doc3->end());
    tests::assert_index(dir(), codec(), expected, all_features);

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    const auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    ASSERT_EQ(1, segment.docs_count()); // total count of documents
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "C"; // 'name' value in doc3
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // remove empty old, defragment old
  {
    auto query_doc1_doc2 = iresearch::iql::query_builder().build("name==A||name==B", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->commit();
    writer->add(doc2->begin(), doc2->end());
    writer->add(doc3->begin(), doc3->end());
    writer->commit();
    writer->remove(std::move(query_doc1_doc2.filter));
    writer->commit();
    writer->defragment(always_merge);
    writer->commit();

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().add(doc3->begin(), doc3->end());
    tests::assert_index(dir(), codec(), expected, all_features);

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    ASSERT_EQ(1, segment.docs_count()); // total count of documents
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "C"; // 'name' value in doc3
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // do not defragment old segment with uncommited removal (i.e. do not consider uncomitted removals)
  {
    auto merge_if_masked = [](const iresearch::directory& dir, const iresearch::index_meta& meta)->iresearch::index_writer::defragment_acceptor_t {
      return [&dir](const iresearch::segment_meta& meta)->bool { 
        return meta.codec->get_document_mask_reader()->prepare(dir, meta);
      };
    };
    auto query_doc1 = iresearch::iql::query_builder().build("name==A", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->commit();
    writer->remove(std::move(query_doc1.filter));
    writer->defragment(merge_if_masked);
    writer->commit();

    {
      auto reader = iresearch::directory_reader::open(dir(), codec());
      ASSERT_EQ(1, reader->size());
      auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
      ASSERT_EQ(2, segment.docs_count()); // total count of documents
    }

    writer->defragment(merge_if_masked); // previous removal now committed and considered
    writer->commit();

    {
      auto reader = iresearch::directory_reader::open(dir(), codec());
      ASSERT_EQ(1, reader->size());
      auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
      ASSERT_EQ(1, segment.docs_count()); // total count of documents
    }
  }

  // merge new+old segment
  {
    auto query_doc1_doc3 = iresearch::iql::query_builder().build("name==A||name==C", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->commit();
    writer->add(doc3->begin(), doc3->end());
    writer->add(doc4->begin(), doc4->end());
    writer->remove(std::move(query_doc1_doc3.filter));
    writer->commit();
    writer->defragment(always_merge);
    writer->commit();

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().add(doc2->begin(), doc2->end());
    expected.back().add(doc4->begin(), doc4->end());
    tests::assert_index(dir(), codec(), expected, all_features);

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    ASSERT_EQ(2, segment.docs_count()); // total count of documents
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "D"; // 'name' value in doc4
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // merge old+old segment
  {
    auto query_doc1_doc3 = iresearch::iql::query_builder().build("name==A||name==C", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->commit();
    writer->add(doc3->begin(), doc3->end());
    writer->add(doc4->begin(), doc4->end());
    writer->commit();
    writer->remove(std::move(query_doc1_doc3.filter));
    writer->commit();
    writer->defragment(always_merge);
    writer->commit();

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().add(doc2->begin(), doc2->end());
    expected.back().add(doc4->begin(), doc4->end());
    tests::assert_index(dir(), codec(), expected, all_features);

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    ASSERT_EQ(2, segment.docs_count()); // total count of documents
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "D"; // 'name' value in doc4
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // merge old+old+old segment
  {
    auto query_doc1_doc3_doc5 = iresearch::iql::query_builder().build("name==A||name==C||name==E", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->commit();
    writer->add(doc3->begin(), doc3->end());
    writer->add(doc4->begin(), doc4->end());
    writer->commit();
    writer->add(doc5->begin(), doc5->end());
    writer->add(doc6->begin(), doc6->end());
    writer->commit();
    writer->remove(std::move(query_doc1_doc3_doc5.filter));
    writer->commit();
    writer->defragment(always_merge);
    writer->commit();

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().add(doc2->begin(), doc2->end());
    expected.back().add(doc4->begin(), doc4->end());
    expected.back().add(doc6->begin(), doc6->end());
    tests::assert_index(dir(), codec(), expected, all_features);

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    ASSERT_EQ(3, segment.docs_count()); // total count of documents
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "D"; // 'name' value in doc4
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "F"; // 'name' value in doc6
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }

  // merge two segments with different fields
  {
    auto writer = open_writer();
    // add 1st segment
    writer->add(doc2->begin(), doc2->end());
    writer->add(doc4->begin(), doc4->end());
    writer->add(doc6->begin(), doc6->end());
    writer->commit();

    // add 2nd segment
    tests::json_doc_generator gen(
      resource("simple_sequential_upper_case.json"),
      [] (tests::document& doc, const std::string& name, const tests::json::json_value& data) {
        if (data.quoted) {
          doc.add(new tests::templates::string_field(
            ir::string_ref(name),
            ir::string_ref(data.value),
            true, true));
        }
    });

    auto doc1_1 = gen.next();
    auto doc1_2 = gen.next();
    auto doc1_3 = gen.next();
    writer->add(doc1_1->begin(), doc1_1->end());
    writer->add(doc1_2->begin(), doc1_2->end());
    writer->add(doc1_3->begin(), doc1_3->end());
    writer->commit();

    // defragment segments
    writer->defragment(always_merge);
    writer->commit();
    
    // validate merged segment 
    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    ASSERT_EQ(6, segment.docs_count()); // total count of documents

    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());
    auto docsItr = termItr->postings(iresearch::flags());
    ASSERT_TRUE(docsItr->next());
    expected_value = "B"; // 'name' value in doc2
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "D"; // 'name' value in doc4
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "F"; // 'name' value in doc6
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "A"; // 'name' value in doc1_1
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "B"; // 'name' value in doc1_2
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_TRUE(docsItr->next());
    expected_value = "C"; // 'name' value in doc1_3
    ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    ASSERT_FALSE(docsItr->next());
  }
}

TEST_F(memory_index_test, segment_defragment_policy) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
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

  // bytes size policy (merge)
  {
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->add(doc3->begin(), doc3->end());
    writer->add(doc4->begin(), doc4->end());
    writer->commit();
    writer->add(doc5->begin(), doc5->end());
    writer->commit();
    writer->defragment(iresearch::index_utils::defragment_bytes(1)); // value garanteeing merge
    writer->commit();

    std::unordered_set<std::string> expectedName = { "A", "B", "C", "D", "E" };

    std::unordered_map<iresearch::string_ref, std::function<void(iresearch::data_input&)>> codecs{
      { "name", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
      { "same", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
      { "duplicated", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
      { "prefix", [](iresearch::data_input& in)->void{ iresearch::read_string<std::string>(in); } },
      { "seq", [](iresearch::data_input& in)->void{ iresearch::read_zvdouble(in); } },
      { "value", [](iresearch::data_input& in)->void{ iresearch::read_zvdouble(in); } },
    };

    const iresearch::string_ref expectedField = "name";
    iresearch::index_reader::document_visitor_f visitor = [&codecs, &expectedName, &expectedField](
      const iresearch::field_meta& field, iresearch::data_input& in
    ) {
      if (field.name != expectedField) {
        auto it = codecs.find(field.name);
        if (codecs.end() == it) {
          return false; // can't find codec
        }
        it->second(in); // skip field
        return true;
      }
      
      auto value = ir::read_string<std::string>(in);
      if (1 != expectedName.erase(value)) {
        return false;
      }

      return true;
    };

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    ASSERT_EQ(expectedName.size(), segment.docs_count()); // total count of documents
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());

    for (auto docsItr = termItr->postings(iresearch::flags()); docsItr->next();) {
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    }

    ASSERT_TRUE(expectedName.empty());
  }

  // bytes size policy (not modified)
  {
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->add(doc3->begin(), doc3->end());
    writer->add(doc4->begin(), doc4->end());
    writer->commit();
    writer->add(doc5->begin(), doc5->end());
    writer->commit();
    writer->defragment(iresearch::index_utils::defragment_bytes(0)); // value garanteeing non-merge
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(2, reader->size());

    {
      std::unordered_set<std::string> expectedName = { "A", "B", "C", "D" };
      const iresearch::string_ref expectedField = "name";
      iresearch::index_reader::document_visitor_f visitor = [&expectedField, &expectedName](
        const ir::field_meta& field, data_input& in
      ) {
        if (field.name != expectedField) {
          ir::read_string<std::string>(in); // skip value
          return true;
        }

        auto value = ir::read_string<std::string>(in);
        if (1 != expectedName.erase(value)) {
          return false;
        }

        return true;
      };
      auto& segment = (*reader)[0]; // assume 0 is id of first segment
      ASSERT_EQ(expectedName.size(), segment.docs_count()); // total count of documents
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());

      for (auto docsItr = termItr->postings(iresearch::flags()); docsItr->next();) {
        ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      }

      ASSERT_TRUE(expectedName.empty());
    }

    {
      std::unordered_set<std::string> expectedName = { "E" };
      const iresearch::string_ref expectedField = "name";
      iresearch::index_reader::document_visitor_f visitor = [&expectedField, &expectedName](
        const ir::field_meta& field, data_input& in
      ) {
        if (field.name != expectedField) {
          ir::read_string<std::string>(in); // skip value
          return true;
        }

        auto value = ir::read_string<std::string>(in);
        if (1 != expectedName.erase(value)) {
          return false;
        }

        return true;
      };
      auto& segment = (*reader)[1]; // assume 1 is id of second segment
      ASSERT_EQ(expectedName.size(), segment.docs_count()); // total count of documents
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());

      for (auto docsItr = termItr->postings(iresearch::flags()); docsItr->next();) {
        ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      }

      ASSERT_TRUE(expectedName.empty());
    }
  }

  // valid segment bytes_accum policy (merge)
  {
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->commit();
    writer->add(doc2->begin(), doc2->end());
    writer->commit();
    writer->defragment(iresearch::index_utils::defragment_bytes_accum(1)); // value garanteeing merge
    writer->commit();
    // segments merged because segment[0] is a candidate and needs to be merged with something

    std::unordered_set<std::string> expectedName = { "A", "B" };
    const iresearch::string_ref expectedField = "name";
    iresearch::index_reader::document_visitor_f visitor = [&expectedField, &expectedName](
      const ir::field_meta& field, data_input& in
    ) {
      if (field.name != expectedField) {
        ir::read_string<std::string>(in); // skip value
        return true;
      }

      auto value = ir::read_string<std::string>(in);
      if (1 != expectedName.erase(value)) {
        return false;
      }

      return true;
    };
    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    ASSERT_EQ(expectedName.size(), segment.docs_count()); // total count of documents
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());

    for (auto docsItr = termItr->postings(iresearch::flags()); docsItr->next();) {
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    }

    ASSERT_TRUE(expectedName.empty());
  }

  // valid segment bytes_accum policy (not modified)
  {
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->commit();
    writer->add(doc2->begin(), doc2->end());
    writer->commit();
    writer->defragment(iresearch::index_utils::defragment_bytes_accum(0)); // value garanteeing non-merge
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(2, reader->size());

    {
      std::unordered_set<std::string> expectedName = { "A" };
      const iresearch::string_ref expectedField = "name";
      iresearch::index_reader::document_visitor_f visitor = [&expectedField, &expectedName](
        const ir::field_meta& field, data_input& in
      ) {
        if (field.name != expectedField) {
          ir::read_string<std::string>(in); // skip value
          return true;
        }

        auto value = ir::read_string<std::string>(in);
        if (1 != expectedName.erase(value)) {
          return false;
        }

        return true;
      };
      auto& segment = (*reader)[0]; // assume 0 is id of first segment
      ASSERT_EQ(expectedName.size(), segment.docs_count()); // total count of documents
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());

      for (auto docsItr = termItr->postings(iresearch::flags()); docsItr->next();) {
        ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      }

      ASSERT_TRUE(expectedName.empty());
    }

    {
      std::unordered_set<std::string> expectedName = { "B" };
      const iresearch::string_ref expectedField = "name";
      iresearch::index_reader::document_visitor_f visitor = [&expectedField, &expectedName](
        const ir::field_meta& field, data_input& in
      ) {
        if (field.name != expectedField) {
          ir::read_string<std::string>(in); // skip value
          return true;
        }

        auto value = ir::read_string<std::string>(in);
        if (1 != expectedName.erase(value)) {
          return false;
        }

        return true;
      };
      auto& segment = (*reader)[1]; // assume 1 is id of second segment
      ASSERT_EQ(expectedName.size(), segment.docs_count());
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());

      for (auto docsItr = termItr->postings(iresearch::flags()); docsItr->next();) {
        ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      }

      ASSERT_TRUE(expectedName.empty());
    }
  }

  // valid docs count policy (merge)
  {
    auto query_doc2_doc3 = iresearch::iql::query_builder().build("name==B||name==C", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->add(doc3->begin(), doc3->end());
    writer->add(doc4->begin(), doc4->end());
    writer->commit();
    writer->remove(std::move(query_doc2_doc3.filter));
    writer->add(doc5->begin(), doc5->end());
    writer->commit();
    writer->defragment(iresearch::index_utils::defragment_count(1)); // value garanteeing merge
    writer->commit();

    std::unordered_set<std::string> expectedName = { "A", "D", "E" };
    const iresearch::string_ref expectedField = "name";
    iresearch::index_reader::document_visitor_f visitor = [&expectedField, &expectedName](
      const ir::field_meta& field, data_input& in
    ) {
      if (field.name != expectedField) {
        ir::read_string<std::string>(in); // skip value
        return true;
      }

      auto value = ir::read_string<std::string>(in);
      if (1 != expectedName.erase(value)) {
        return false;
      }

      return true;
    };
    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    ASSERT_EQ(expectedName.size(), segment.docs_count()); // total count of documents
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());

    for (auto docsItr = termItr->postings(iresearch::flags()); docsItr->next();) {
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    }

    ASSERT_TRUE(expectedName.empty());
  }

  // valid docs count policy (not modified)
  {
    auto query_doc2_doc3_doc4 = iresearch::iql::query_builder().build("name==B||name==C||name==D", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->add(doc3->begin(), doc3->end());
    writer->add(doc4->begin(), doc4->end());
    writer->commit();
    writer->remove(std::move(query_doc2_doc3_doc4.filter));
    writer->add(doc5->begin(), doc5->end());
    writer->commit();
    writer->defragment(iresearch::index_utils::defragment_count(0)); // value garanteeing non-merge
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(2, reader->size());

    {
      std::unordered_set<std::string> expectedName = { "A" };
      const iresearch::string_ref expectedField = "name";
      iresearch::index_reader::document_visitor_f visitor = [&expectedField, &expectedName](
        const ir::field_meta& field, data_input& in
      ) {
        if (field.name != expectedField) {
          ir::read_string<std::string>(in); // skip value
          return true;
        }

        auto value = ir::read_string<std::string>(in);
        if (1 != expectedName.erase(value)) {
          return false;
        }

        return true;
      };
      auto& segment = (*reader)[0]; // assume 0 is id of first segment
      ASSERT_EQ(expectedName.size() + 3, segment.docs_count()); // total count of documents (+3 == B, C, D masked)
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());

      for (auto docsItr = termItr->postings(iresearch::flags()); docsItr->next();) {
        ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      }

      ASSERT_TRUE(expectedName.empty());
    }

    {
      std::unordered_set<std::string> expectedName = { "E" };
      const iresearch::string_ref expectedField = "name";
      iresearch::index_reader::document_visitor_f visitor = [&expectedField, &expectedName](
        const ir::field_meta& field, data_input& in
      ) {
        if (field.name != expectedField) {
          ir::read_string<std::string>(in); // skip value
          return true;
        }

        auto value = ir::read_string<std::string>(in);
        if (1 != expectedName.erase(value)) {
          return false;
        }

        return true;
      };
      auto& segment = (*reader)[1]; // assume 1 is id of second segment
      ASSERT_EQ(expectedName.size(), segment.docs_count()); // total count of documents
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());

      for (auto docsItr = termItr->postings(iresearch::flags()); docsItr->next();) {
        ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      }

      ASSERT_TRUE(expectedName.empty());
    }
  }

  // valid segment fill policy (merge)
  {
    auto query_doc2_doc4 = iresearch::iql::query_builder().build("name==B||name==D", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->commit();
    writer->add(doc3->begin(), doc3->end());
    writer->add(doc4->begin(), doc4->end());
    writer->commit();
    writer->remove(std::move(query_doc2_doc4.filter));
    writer->commit();
    writer->defragment(iresearch::index_utils::defragment_fill(1)); // value garanteeing merge
    writer->commit();

    std::unordered_set<std::string> expectedName = { "A", "C" };
    const iresearch::string_ref expectedField = "name";
    iresearch::index_reader::document_visitor_f visitor = [&expectedField, &expectedName](
      const ir::field_meta& field, data_input& in
    ) {
      if (field.name != expectedField) {
        ir::read_string<std::string>(in); // skip value
        return true;
      }

      auto value = ir::read_string<std::string>(in);
      if (1 != expectedName.erase(value)) {
        return false;
      }

      return true;
    };
    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(1, reader->size());
    auto& segment = (*reader)[0]; // assume 0 is id of first/only segment
    ASSERT_EQ(expectedName.size(), segment.docs_count()); // total count of documents
    auto terms = segment.terms("same");
    ASSERT_NE(nullptr, terms);
    auto termItr = terms->iterator();
    ASSERT_TRUE(termItr->next());

    for (auto docsItr = termItr->postings(iresearch::flags()); docsItr->next();) {
      ASSERT_TRUE(segment.document(docsItr->value(), visitor));
    }

    ASSERT_TRUE(expectedName.empty());
  }

  // valid segment fill policy (not modified)
  {
    auto query_doc2_doc4 = iresearch::iql::query_builder().build("name==B||name==D", std::locale::classic());
    auto writer = open_writer();

    writer->add(doc1->begin(), doc1->end());
    writer->add(doc2->begin(), doc2->end());
    writer->commit();
    writer->add(doc3->begin(), doc3->end());
    writer->add(doc4->begin(), doc4->end());
    writer->commit();
    writer->remove(std::move(query_doc2_doc4.filter));
    writer->commit();
    writer->defragment(iresearch::index_utils::defragment_fill(0)); // value garanteeing non-merge
    writer->commit();

    auto reader = iresearch::directory_reader::open(dir(), codec());
    ASSERT_EQ(2, reader->size());

    {
      std::unordered_set<std::string> expectedName = { "A" };
      const iresearch::string_ref expectedField = "name";
      iresearch::index_reader::document_visitor_f visitor = [&expectedField, &expectedName](
        const ir::field_meta& field, data_input& in
      ) {
        if (field.name != expectedField) {
          ir::read_string<std::string>(in); // skip value
          return true;
        }

        auto value = ir::read_string<std::string>(in);
        if (1 != expectedName.erase(value)) {
          return false;
        }

        return true;
      };
      auto& segment = (*reader)[0]; // assume 0 is id of first segment
      ASSERT_EQ(expectedName.size() + 1, segment.docs_count()); // total count of documents (+1 == B masked)
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());

      for (auto docsItr = termItr->postings(iresearch::flags()); docsItr->next();) {
        ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      }

      ASSERT_TRUE(expectedName.empty());
    }

    {
      std::unordered_set<std::string> expectedName = { "C" };
      const iresearch::string_ref expectedField = "name";
      iresearch::index_reader::document_visitor_f visitor = [&expectedField, &expectedName](
        const ir::field_meta& field, data_input& in
      ) {
        if (field.name != expectedField) {
          ir::read_string<std::string>(in); // skip value
          return true;
        }

        auto value = ir::read_string<std::string>(in);
        if (1 != expectedName.erase(value)) {
          return false;
        }

        return true;
      };
      auto& segment = (*reader)[1]; // assume 1 is id of second segment
      ASSERT_EQ(expectedName.size() + 1, segment.docs_count()); // total count of documents (+1 == D masked)
      auto terms = segment.terms("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator();
      ASSERT_TRUE(termItr->next());

      for (auto docsItr = termItr->postings(iresearch::flags()); docsItr->next();) {
        ASSERT_TRUE(segment.document(docsItr->value(), visitor));
      }

      ASSERT_TRUE(expectedName.empty());
    }
  }
}

// ----------------------------------------------------------------------------
// --SECTION--                               fs_directory + iresearch_format_10
// ----------------------------------------------------------------------------

class fs_index_test 
  : public tests::cases::tfidf<tests::fs_test_case_base>{
}; // fs_index_test 

TEST_F(fs_index_test, open_writer) {
  open_writer_check_lock();
}

TEST_F(fs_index_test, read_write_doc_attributes) {
  read_write_doc_attributes();
  read_empty_doc_attributes();
}

TEST_F(fs_index_test, writer_transaction_isolation) {
  writer_transaction_isolation();
}

TEST_F(fs_index_test, create_empty_index) {
  writer_check_open_modes();
}

TEST_F(fs_index_test, writer_atomicity_check) {
  writer_atomicity_check();
}

TEST_F(fs_index_test, writer_begin_rollback) {
  writer_begin_rollback();
}

TEST_F(fs_index_test, arango_demo_docs) {
  {
    tests::json_doc_generator gen(
      resource("arango_demo.json"), 
      &tests::generic_json_field_factory
    );
    add_segment(gen);
  }
  assert_index();
}

TEST_F(fs_index_test, europarl_docs) {
  {
    tests::templates::europarl_doc_template doc;
    tests::delim_doc_generator gen(resource("europarl.subset.txt"), doc);
    add_segment(gen);
  }
  assert_index();
}

TEST_F(fs_index_test, writer_close) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"), 
    &tests::generic_json_field_factory
  );
  auto& directory = dir();
  auto* doc = gen.next();
  auto writer = open_writer();

  writer->add(doc->begin(), doc->end());
  writer->commit();
  writer->close();

  iresearch::directory::files files;

  directory.list(files);

  // file removal should pass for all files (especially valid for Microsoft Windows)
  for (auto& file: files) {
    ASSERT_TRUE(directory.remove(file));
  }

  // validate that all files have been removed
  files.clear();
  directory.list(files);
  ASSERT_TRUE(files.empty());
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------