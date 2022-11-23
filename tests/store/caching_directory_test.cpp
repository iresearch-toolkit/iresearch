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

template<typename Impl>
class DirectoryProxy : public Impl {
 public:
  template<typename... Args>
  explicit DirectoryProxy(Args&&... args) noexcept
    : Impl{std::forward<Args>(args)...} {}

  bool exists(bool& result, std::string_view name) const noexcept override {
    EXPECT_TRUE(exists_expected_);
    return Impl::exists(result, name);
  }

  bool length(uint64_t& result, std::string_view name) const noexcept override {
    EXPECT_TRUE(length_expected_);
    return Impl::length(result, name);
  }

  irs::index_input::ptr open(std::string_view name,
                             irs::IOAdvice advice) const noexcept override {
    EXPECT_TRUE(open_expected_);
    return Impl::open(name, advice);
  }

  void ExpectExists(bool v) noexcept { exists_expected_ = v; }
  void ExpectLength(bool v) noexcept { length_expected_ = v; }
  void ExpectOpen(bool v) noexcept { open_expected_ = v; }

 private:
  bool exists_expected_{false};
  bool length_expected_{false};
  bool open_expected_{false};
};

template<typename Impl, typename Acceptor>
struct CachingDirectory : public irs::CachingDirectory<DirectoryProxy<Impl>> {
  template<typename... Args>
  CachingDirectory(Args&&... args)
    : irs::CachingDirectory<DirectoryProxy<Impl>>{
        Acceptor{}, std::forward<Args>(args)...} {}

  using irs::CachingDirectory<DirectoryProxy<Impl>>::GetAcceptor;
};

template<size_t Count>
struct MaxCountAcceptor : irs::MaxCountAcceptor {
  MaxCountAcceptor() noexcept : irs::MaxCountAcceptor{Count} {}
};

template<typename Directory>
class CachingDirectoryTestCase : public test_base {
 public:
  void SetUp() override {
    test_base::SetUp();
    dir_ = std::static_pointer_cast<Directory>(
      MakePhysicalDirectory<Directory>(this, irs::directory_attributes{}));
  }

  void TearDown() override {
    dir_ = nullptr;
    test_base::TearDown();
  }

  Directory& GetDirectory() noexcept {
    EXPECT_NE(nullptr, dir_);
    return *dir_;
  }

 private:
  std::shared_ptr<Directory> dir_;
};

using CachingMMapDirectory =
  CachingDirectory<irs::mmap_directory, MaxCountAcceptor<1>>;
using CachingMMapDirectoryTestCase =
  CachingDirectoryTestCase<CachingMMapDirectory>;

TEST_F(CachingMMapDirectoryTestCase, TestCaching) {
  auto& dir = GetDirectory();

  auto create_file = [&](std::string_view name, irs::byte_type b) {
    auto stream = dir.create(name);
    ASSERT_NE(nullptr, stream);
    stream->write_byte(b);
  };

  auto check_file = [&](std::string_view name, irs::byte_type b) {
    auto stream = dir.open(name, irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(1, stream->length());
    ASSERT_EQ(0, stream->file_pointer());
    ASSERT_EQ(b, stream->read_byte());
    ASSERT_EQ(1, stream->file_pointer());
  };

  ASSERT_EQ(0, dir.Count());
  ASSERT_EQ(1, dir.GetAcceptor().MaxCount());

  dir.ExpectExists(true);
  dir.ExpectLength(true);
  dir.ExpectOpen(true);

  bool exists = true;
  uint64_t length{};

  // File doesn't exist yet
  ASSERT_TRUE(dir.exists(exists, "0"));
  ASSERT_FALSE(exists);
  ASSERT_FALSE(dir.length(length, "0"));
  ASSERT_EQ(nullptr, dir.open("0", irs::IOAdvice::NORMAL));

  create_file("0", 42);  // Entry isn't cached yet
  ASSERT_EQ(0, dir.Count());
  check_file("0", 42);  // Entry is cached after first  check
  ASSERT_EQ(1, dir.Count());

  // Ensure we use cache
  dir.ExpectExists(false);
  dir.ExpectLength(false);
  dir.ExpectOpen(false);
  check_file("0", 42);  // Entry is cached after first  check
  ASSERT_EQ(1, dir.Count());

  // Rename
  ASSERT_TRUE(dir.rename("0", "2"));
  check_file("2", 42);  // Entry is cached after first  check
  ASSERT_EQ(1, dir.Count());

  // Following entry must not be cached because of cache size
  dir.ExpectExists(true);
  dir.ExpectLength(true);
  dir.ExpectOpen(true);

  create_file("1", 24);
  ASSERT_EQ(1, dir.Count());
  check_file("1", 24);
  ASSERT_EQ(1, dir.Count());
  check_file("1", 24);
  ASSERT_EQ(1, dir.Count());

  // Remove
  ASSERT_TRUE(dir.remove("2"));
  ASSERT_EQ(0, dir.Count());

  // We now can use cache
  check_file("1", 24);
  ASSERT_EQ(1, dir.Count());

  dir.ExpectExists(false);
  dir.ExpectLength(false);
  dir.ExpectOpen(false);

  check_file("1", 24);
  ASSERT_EQ(1, dir.Count());
}

}  // namespace tests
