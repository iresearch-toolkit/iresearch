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

#include <unordered_set>

#include "index/index_meta.hpp"
#include "index/index_tests.hpp"
#include "store/memory_directory.hpp"
#include "tests_shared.hpp"
#include "utils/directory_utils.hpp"

namespace {

class directory_utils_tests : public ::testing::Test {
 protected:
  class directory_mock : public irs::directory {
   public:
    directory_mock() noexcept {}

    using directory::attributes;

    irs::directory_attributes& attributes() noexcept override { return attrs_; }

    irs::index_output::ptr create(std::string_view) noexcept override {
      return nullptr;
    }

    bool exists(bool&, std::string_view) const noexcept override {
      return false;
    }

    bool length(uint64_t&, std::string_view) const noexcept override {
      return false;
    }

    irs::index_lock::ptr make_lock(std::string_view) noexcept override {
      return nullptr;
    }

    bool mtime(std::time_t&, std::string_view) const noexcept override {
      return false;
    }

    irs::index_input::ptr open(std::string_view,
                               irs::IOAdvice) const noexcept override {
      return nullptr;
    }

    bool remove(std::string_view) noexcept override { return false; }

    bool rename(std::string_view, std::string_view) noexcept override {
      return false;
    }

    bool sync(std::span<const std::string_view>) noexcept override {
      return false;
    }

    bool visit(const irs::directory::visitor_f&) const override {
      return false;
    }

   private:
    irs::directory_attributes attrs_{};
  };  // directory_mock

  struct callback_directory : tests::directory_mock {
    typedef std::function<void()> AfterCallback;

    explicit callback_directory(irs::directory& impl, AfterCallback&& p)
      : tests::directory_mock(impl), after(p) {}

    irs::index_input::ptr open(std::string_view name,
                               irs::IOAdvice advice) const noexcept override {
      auto stream = tests::directory_mock::open(name, advice);
      after();
      return stream;
    }

    AfterCallback after;
  };  // callback_directory
};

}  // namespace

TEST_F(directory_utils_tests, test_reference) {
  // test add file
  {
    irs::memory_directory dir;
    auto file = dir.create("abc");

    ASSERT_FALSE(!file);
    ASSERT_TRUE(static_cast<bool>(irs::directory_utils::Reference(dir, "abc")));

    auto& attribute = dir.attributes().refs();

    ASSERT_FALSE(attribute.refs().empty());
    ASSERT_TRUE(attribute.refs().contains("abc"));
    ASSERT_FALSE(attribute.refs().contains("def"));
  }
}

TEST_F(directory_utils_tests, test_ref_tracking_dir) {
  // test move constructor
  {
    irs::memory_directory dir;
    irs::RefTrackingDirectory track_dir1(dir);
    irs::RefTrackingDirectory track_dir2(std::move(track_dir1));

    ASSERT_EQ(&dir, &(*track_dir2));
  }

  // test dereference and attributes
  {
    irs::memory_directory dir;
    irs::RefTrackingDirectory track_dir(dir);

    ASSERT_EQ(&dir, &(*track_dir));
    ASSERT_EQ(&(dir.attributes()), &(track_dir.attributes()));
  }

  // test make_lock
  {
    irs::memory_directory dir;
    irs::RefTrackingDirectory track_dir(dir);
    auto lock1 = dir.make_lock("abc");
    ASSERT_FALSE(!lock1);
    auto lock2 = track_dir.make_lock("abc");
    ASSERT_FALSE(!lock2);

    ASSERT_TRUE(lock1->lock());
    ASSERT_FALSE(lock2->lock());
    ASSERT_TRUE(lock1->unlock());
    ASSERT_TRUE(lock2->lock());
  }

  // test open
  {
    irs::memory_directory dir;
    irs::RefTrackingDirectory track_dir(dir);
    const std::string_view file{"abc"};
    auto file1 = dir.create(file);

    ASSERT_FALSE(!file1);
    file1->write_byte(42);
    file1->flush();

    auto file2 = track_dir.open(file, irs::IOAdvice::NORMAL);

    ASSERT_FALSE(!file2);
    // does nothing in memory_directory, but adds line coverage
    ASSERT_TRUE(track_dir.sync({&file, 1}));
    ASSERT_EQ(1, file2->length());
    ASSERT_TRUE(track_dir.rename(file, "def"));
    file1.reset();  // release before remove
    file2.reset();  // release before remove
    ASSERT_FALSE(track_dir.remove(file));
    bool exists;
    ASSERT_TRUE(dir.exists(exists, file) && !exists);
    ASSERT_TRUE(dir.exists(exists, "def") && exists);
    ASSERT_TRUE(track_dir.remove("def"));
    ASSERT_TRUE(dir.exists(exists, "def") && !exists);
  }

  // test visit refs visitor complete
  {
    irs::memory_directory dir;
    irs::RefTrackingDirectory track_dir(dir);
    auto file1 = track_dir.create("abc");
    ASSERT_FALSE(!file1);
    auto file2 = track_dir.create("def");
    ASSERT_FALSE(!file2);
    size_t count = 0;
    auto visitor = [&count](const irs::index_file_refs::ref_t&) -> bool {
      ++count;
      return true;
    };

    ASSERT_TRUE(track_dir.visit_refs(visitor));
    ASSERT_EQ(2, count);
  }

  // test visit refs visitor (no-track-open)
  {
    irs::memory_directory dir;
    irs::RefTrackingDirectory track_dir(dir);
    auto file1 = dir.create("abc");
    ASSERT_FALSE(!file1);
    auto file2 = track_dir.open("abc", irs::IOAdvice::NORMAL);
    ASSERT_FALSE(!file2);
    size_t count = 0;
    auto visitor = [&count](const irs::index_file_refs::ref_t&) -> bool {
      ++count;
      return true;
    };

    ASSERT_TRUE(track_dir.visit_refs(visitor));
    ASSERT_EQ(0, count);
  }

  // test visit refs visitor (track-open)
  {
    irs::memory_directory dir;
    irs::RefTrackingDirectory track_dir(dir, true);
    auto file1 = dir.create("abc");
    ASSERT_FALSE(!file1);
    auto file2 = track_dir.open("abc", irs::IOAdvice::NORMAL);
    ASSERT_FALSE(!file2);
    size_t count = 0;
    auto visitor = [&count](const irs::index_file_refs::ref_t&) -> bool {
      ++count;
      return true;
    };

    ASSERT_TRUE(track_dir.visit_refs(visitor));
    ASSERT_EQ(1, count);
  }

  // test open (track-open)
  {
    irs::memory_directory dir;

    auto clean = [&dir]() {
      irs::directory_utils::RemoveAllUnreferenced(dir);
    };

    callback_directory callback_dir{dir, clean};
    irs::RefTrackingDirectory track_dir(callback_dir, true);
    auto file1 = dir.create("abc");
    ASSERT_NE(nullptr, file1);
    auto file2 = track_dir.open("abc", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, file2);

    size_t count = 0;
    auto visitor = [&count](const irs::index_file_refs::ref_t&) -> bool {
      ++count;
      return true;
    };

    ASSERT_TRUE(track_dir.visit_refs(visitor));
    ASSERT_EQ(1, count);

    auto file3 = dir.open("abc", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, file3);
  }

  // test visit refs visitor terminate
  {
    irs::memory_directory dir;
    irs::RefTrackingDirectory track_dir(dir);
    auto file1 = track_dir.create("abc");
    ASSERT_FALSE(!file1);
    auto file2 = track_dir.create("def");
    ASSERT_FALSE(!file2);
    size_t count = 0;
    auto visitor = [&count](const irs::index_file_refs::ref_t&) -> bool {
      ++count;
      return false;
    };

    ASSERT_FALSE(track_dir.visit_refs(visitor));
    ASSERT_EQ(1, count);
  }

  // ...........................................................................
  // errors during file operations
  // ...........................................................................

  struct error_directory : public irs::directory {
    irs::directory_attributes attrs{};
    irs::directory_attributes& attributes() noexcept override { return attrs; }
    irs::index_output::ptr create(std::string_view) noexcept override {
      return nullptr;
    }
    bool exists(bool&, std::string_view) const noexcept override {
      return false;
    }
    bool length(uint64_t&, std::string_view) const noexcept override {
      return false;
    }
    bool visit(const visitor_f&) const override { return false; }
    irs::index_lock::ptr make_lock(std::string_view) noexcept override {
      return nullptr;
    }
    bool mtime(std::time_t&, std::string_view) const noexcept override {
      return false;
    }
    irs::index_input::ptr open(std::string_view,
                               irs::IOAdvice) const noexcept override {
      return nullptr;
    }
    bool remove(std::string_view) noexcept override { return false; }
    bool rename(std::string_view, std::string_view) noexcept override {
      return false;
    }
    bool sync(std::span<const std::string_view>) noexcept override {
      return false;
    }
  } error_dir;

  // test create failure
  {
    irs::RefTrackingDirectory track_dir(error_dir);

    ASSERT_FALSE(track_dir.create("abc"));

    std::set<std::string> refs;
    auto visitor = [&refs](const irs::index_file_refs::ref_t& ref) -> bool {
      refs.insert(*ref);
      return true;
    };

    ASSERT_TRUE(track_dir.visit_refs(visitor));
    ASSERT_TRUE(refs.empty());
  }

  // test open failure
  {
    irs::RefTrackingDirectory track_dir(error_dir, true);

    ASSERT_FALSE(track_dir.open("abc", irs::IOAdvice::NORMAL));

    std::set<std::string> refs;
    auto visitor = [&refs](const irs::index_file_refs::ref_t& ref) -> bool {
      refs.insert(*ref);
      return true;
    };

    ASSERT_TRUE(track_dir.visit_refs(visitor));
    ASSERT_TRUE(refs.empty());
  }
}

TEST_F(directory_utils_tests, test_tracking_dir) {
  // test dereference and attributes
  {
    irs::memory_directory dir;
    irs::TrackingDirectory track_dir(dir);

    ASSERT_EQ(&dir, &(*track_dir));
    ASSERT_EQ(&(dir.attributes()), &(track_dir.attributes()));
  }

  // test make_lock
  {
    irs::memory_directory dir;
    irs::TrackingDirectory track_dir(dir);
    auto lock1 = dir.make_lock("abc");
    ASSERT_FALSE(!lock1);
    auto lock2 = track_dir.make_lock("abc");
    ASSERT_FALSE(!lock2);

    ASSERT_TRUE(lock1->lock());
    ASSERT_FALSE(lock2->lock());
    ASSERT_TRUE(lock1->unlock());
    ASSERT_TRUE(lock2->lock());
  }

  // test open
  {
    irs::memory_directory dir;
    irs::TrackingDirectory track_dir(dir);
    const std::string_view file{"abc"};
    auto file1 = dir.create(file);
    ASSERT_FALSE(!file1);

    file1->write_byte(42);
    file1->flush();

    auto file2 = track_dir.open("abc", irs::IOAdvice::NORMAL);
    ASSERT_FALSE(!file2);
    // does nothing in memory_directory, but adds line coverage
    ASSERT_TRUE(track_dir.sync({&file, 1}));
    ASSERT_EQ(1, file2->length());
    ASSERT_TRUE(track_dir.rename(file, "def"));
    file1.reset();  // release before remove
    file2.reset();  // release before remove
    ASSERT_FALSE(track_dir.remove(file));
    bool exists;
    ASSERT_TRUE(dir.exists(exists, file) && !exists);
    ASSERT_TRUE(dir.exists(exists, "def") && exists);
    ASSERT_TRUE(track_dir.remove("def"));
    ASSERT_TRUE(dir.exists(exists, "def") && !exists);
  }

  // test open (no-track-open)
  {
    irs::memory_directory dir;
    irs::TrackingDirectory track_dir(dir);
    auto file1 = dir.create("abc");
    ASSERT_FALSE(!file1);
    auto file2 = track_dir.open("abc", irs::IOAdvice::NORMAL);
    ASSERT_FALSE(!file2);
    irs::TrackingDirectory::file_set files;
    track_dir.flush_tracked(files);
    ASSERT_EQ(0, files.size());
  }

  // test open (track-open)
  {
    irs::memory_directory dir;
    irs::TrackingDirectory track_dir(dir, true);
    auto file1 = dir.create("abc");
    ASSERT_FALSE(!file1);
    auto file2 = track_dir.open("abc", irs::IOAdvice::NORMAL);
    ASSERT_FALSE(!file2);
    irs::TrackingDirectory::file_set files;
    track_dir.flush_tracked(files);
    ASSERT_EQ(1, files.size());
    track_dir.flush_tracked(files);  // tracked files were cleared
    ASSERT_EQ(0, files.size());
  }
}
