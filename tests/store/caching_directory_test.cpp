////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

// clang-format off
#include "store/caching_directory.hpp"
#include "store/fs_directory.hpp"
#include "store/mmap_directory.hpp"

#include "tests_param.hpp"
// clang-format on

namespace tests {

template<typename Directory, size_t MaxCount>
class CachingDirectoryTestCase : public test_base {
 protected:
  void SetUp() override {
    test_base::SetUp();
    dir_ = std::static_pointer_cast<Directory>(MakePhysicalDirectory<Directory>(
      this, irs::directory_attributes{}, MaxCount));
  }

  void TearDown() override {
    dir_ = nullptr;
    test_base::TearDown();
  }

  Directory& GetDirectory() noexcept {
    EXPECT_NE(nullptr, dir_);
    return *dir_;
  }

  template<typename IsCached>
  void TestCachingImpl(IsCached&& is_cached);

 private:
  std::shared_ptr<Directory> dir_;
};

template<typename Directory, size_t MaxCount>
template<typename IsCached>
void CachingDirectoryTestCase<Directory, MaxCount>::TestCachingImpl(
  IsCached&& is_cached) {
  auto& dir = GetDirectory();

  auto create_file = [&](std::string_view name, irs::byte_type b) {
    auto stream = dir.create(name);
    ASSERT_NE(nullptr, stream);
    stream->write_byte(b);
  };

  auto check_file = [&](std::string_view name, irs::byte_type b,
                        bool readonce = false) {
    auto stream = dir.open(
      name, readonce ? irs::IOAdvice::READONCE : irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(1, stream->length());
    ASSERT_EQ(0, stream->file_pointer());
    ASSERT_EQ(b, stream->read_byte());
    ASSERT_EQ(1, stream->file_pointer());
  };

  ASSERT_EQ(0, dir.Cache().Count());
  ASSERT_EQ(1, dir.Cache().MaxCount());

  bool exists = true;
  uint64_t length{};

  // File doesn't exist yet
  ASSERT_TRUE(dir.exists(exists, "0"));
  ASSERT_FALSE(exists);
  ASSERT_FALSE(dir.length(length, "0"));
  ASSERT_EQ(nullptr, dir.open("0", irs::IOAdvice::NORMAL));

  ASSERT_FALSE(is_cached("0"));
  create_file("0", 42);
  ASSERT_FALSE(is_cached("0"));
  ASSERT_EQ(0, dir.Cache().Count());
  check_file("0", 42);
  ASSERT_EQ(1, dir.Cache().Count());
  ASSERT_TRUE(is_cached("0"));
  check_file("0", 42);
  ASSERT_EQ(1, dir.Cache().Count());
  ASSERT_TRUE(is_cached("0"));

  // Rename
  ASSERT_TRUE(dir.rename("0", "2"));
  ASSERT_TRUE(is_cached("2"));
  check_file("2", 42);  // Entry is cached after first  check
  ASSERT_EQ(1, dir.Cache().Count());
  ASSERT_TRUE(is_cached("2"));

  // Following entry must not be cached because of cache size
  create_file("1", 24);
  ASSERT_FALSE(is_cached("1"));
  ASSERT_EQ(1, dir.Cache().Count());
  check_file("1", 24);
  ASSERT_FALSE(is_cached("1"));
  ASSERT_EQ(1, dir.Cache().Count());
  check_file("1", 24);
  ASSERT_FALSE(is_cached("1"));
  ASSERT_EQ(1, dir.Cache().Count());

  // Remove
  ASSERT_TRUE(dir.remove("2"));
  ASSERT_EQ(0, dir.Cache().Count());
  ASSERT_FALSE(is_cached("2"));

  // We don't use cache for readonce files
  check_file("1", 24, true);
  ASSERT_EQ(0, dir.Cache().Count());
  ASSERT_FALSE(is_cached("1"));

  // We now can use cache
  check_file("1", 24);
  ASSERT_EQ(1, dir.Cache().Count());
  ASSERT_TRUE(is_cached("1"));

  check_file("1", 24);
  ASSERT_EQ(1, dir.Cache().Count());
  ASSERT_TRUE(is_cached("1"));
}

using CachingMMapDirectoryTest =
  CachingDirectoryTestCase<irs::CachingMMapDirectory, 1>;

TEST_F(CachingMMapDirectoryTest, TestCaching) {
  TestCachingImpl([&](std::string_view name) -> bool {
    std::shared_ptr<irs::mmap_utils::mmap_handle> handle;
    const bool found = GetDirectory().Cache().Visit(name, [&](auto& cached) {
      handle = cached;
      return true;
    });
    return found && 2 == handle.use_count();
  });
}

using CachingFSDirectoryTest =
  CachingDirectoryTestCase<irs::CachingFSDirectory, 1>;

TEST_F(CachingFSDirectoryTest, TestCaching) {
  TestCachingImpl([&](std::string_view name) -> bool {
    uint64_t size = std::numeric_limits<uint64_t>::max();
    const bool found = GetDirectory().Cache().Visit(name, [&](uint64_t cached) {
      size = cached;
      return true;
    });
    return found && 1 == size;
  });
}

}  // namespace tests
