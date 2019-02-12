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

#include "tests_shared.hpp"

#include "store/fs_directory.hpp"
#include "utils/crc.hpp"
#include "utils/process_utils.hpp"
#include "utils/network_utils.hpp"

#include <fstream>

#ifndef _WIN32
#include <sys/file.h>
#endif

NS_LOCAL

using namespace iresearch;

void smoke_store(directory& dir) {
  std::vector<std::string> names{
    "spM42fEO88eDt2","jNIvCMksYwpoxN","Re5eZWCkQexrZn","jjj003oxVAIycv","N9IJuRjFSlO8Pa","OPGG6Ic3JYJyVY","ZDGVji8xtjh9zI","DvBDXbjKgIfPIk",
    "bZyCbyByXnGvlL","pvjGDbNcZGDmQ2","J7by8eYg0ZGbYw","6UZ856mrVW9DeD","Ny6bZIbGQ43LSU","oaYAsO0tXnNBkR","Fr97cjyQoTs9Pf","7rLKaQN4REFIgn",
    "EcFMetqynwG87T","oshFa26WK3gGSl","8keZ9MLvnkec8Q","HuiOGpLtqn79GP","Qnlj0JiQjBR3YW","k64uvviemlfM8p","32X34QY6JaCH3L","NcAU3Aqnn87LJW",
    "Q4LLFIBU9ci40O","M5xpjDYIfos22t","Te9ZhWmGt2cTXD","HYO3hJ1C4n1DvD","qVRj2SyXcKQz3Z","vwt41rzEW7nkoi","cLqv5U8b8kzT2H","tNyCoJEOm0POyC",
    "mLw6cl4HxmOHXa","2eTVXvllcGmZ0e","NFF9SneLv6pX8h","kzCvqOVYlYA3QT","mxCkaGg0GeLxYq","PffuwSr8h3acP0","zDm0rAHgzhHsmv","8LYMjImx00le9c",
    "Ju0FM0mJmqkue1","uNwn8A2SH4OSZW","R1Dm21RTJzb0aS","sUpQGy1n6TiH82","fhkCGcuQ5VnrEa","b6Xsq05brtAr88","NXVkmxvLmhzFRY","s9OuZyZX28uux0",
    "DQaD4HyDMGkbg3","Fr2L3V4UzCZZcJ","7MgRPt0rLo6Cp4","c8lK5hjmKUuc3e","jzmu3ZcP3PF62X","pmUUUvAS00bPfa","lonoswip3le6Hs","TQ1G0ruVvknb8A",
    "4XqPhpJvDazbG1","gY0QFCjckHp1JI","v2a0yfs9yN5lY4","l1XKKtBXtktOs2","AGotoLgRxPe4Pr","x9zPgBi3Bw8DFD","OhX85k7OhY3FZM","riRP6PRhkq0CUi",
    "1ToW1HIephPBlz","C8xo1SMWPZW8iE","tBa3qiFG7c1wiD","BRXFbUYzw646PS","mbR0ECXCash1rF","AVDjHnwujjOGAK","16bmhl4gvDpj44","OLa0D9RlpBLRgK",
    "PgCSXvlxyHQFlQ","sMyrmGRcVTwg53","Fa6Fo687nt9bDV","P0lUFttS64mC7s","rxTZUQIpOPYkPp","oNEsVpak9SNgLh","iHmFTSjGutROen","aTMmlghno9p91a",
    "tpb3rHs9ZWtL5m","iG0xrYN7gXXPTs","KsEl2f8WtF6Ylv","triXFZM9baNltC","MBFTh22Yos3vGt","DTuFyue5f9Mk3x","v2zm4kYxfar0J7","xtpwVgOMT0eIFS",
    "8Wz7MrtXkSH9CA","FuURHWmPLbvFU0","YpIFnExqjgpSh0","2oaIkTM6EJ2zty","s16qvfbrycGnVP","yUb2fcGIDRSujG","9rIfsuCyTCTiLY","HXTg5jWrVZNLNP",
    "maLjUi6Oo6wsJr","C6iHChfoJHGxzO","6LxzytT8iSzNHZ","ex8znLIzbatFCo","HiYTSzZhBHgtaP","H5EpiJw2L5UgD1","ZhPvYoUMMFkoiL","y6014BfgqbE3ke",
    "XXutx8GrPYt7Rq","DjYwLMixhS80an","aQxh91iigWOt4x","1J9ZC2r7CCfGIH","Sg9PzDCOb5Ezym","4PB3SukHVhA6SB","BfVm1XGLDOhabZ","ChEvexTp1CrLUL",
    "M5nlO4VcxIOrxH","YO9rnNNFwzRphV","KzQhfZSnQQGhK9","r7Ez7ZqkXwr0bn","fQipSie8ZKyT62","3yyLqJMcShXG9z","UTb12lz3k5xPPt","JjcWQnBnRFJ2Mv",
    "zsKEX7BLJQTjCx","g0oPvTcOhiev1k","8P6HF4I6t1jwzu","LaOiJIU47kagqu","pyY9sV9WQ5YuQC","MCgpgJhEwrGKWM","Hq5Wgc3Am8cjWw","FnITVHg0jw03Bm",
    "0Jq2YEnFf52861","y0FT03yG9Uvg6I","S6uehKP8uj6wUe","usC8CZtobBmuk6","LrZuchHNpSs282","PsmFFySt7fKFOv","mXe9j6xNYttnSy","al9J6AZYlhAlWU",
    "3v8PsohUeKegJI","QZCwr1URS1OWzX","UVCg1mVWmSBWRT","pO2lnQ4L6yHQic","w5EtZl2gZhj2ca","04B62aNIpnBslQ","0Sz6UCGXBwi7At","l49gEiyDkc3J00",
    "2T9nyWrRwuZj9W","CTtHTPRhSAPRIW","sJZI3K8vP96JPm","HYEy1bBJskEYa2","UKb3uiFuGEi7m9","yeRCnG0EEZ8Vrr"
  };

  // Write contents
  auto it = names.end();
  for (const auto& name : names) {
    --it;
    irs::crc32c crc;

    auto file = dir.create(name);
    ASSERT_FALSE(!file);
    EXPECT_EQ(0, file->file_pointer());

    file->write_bytes(reinterpret_cast<const byte_type*>(it->c_str()), static_cast<uint32_t>(it->size()));
    crc.process_bytes(it->c_str(), it->size());

    // check file_pointer
    EXPECT_EQ(it->size(), file->file_pointer());
    // check checksum
    EXPECT_EQ(crc.checksum(), file->checksum());

    file->flush();
  }

  // Check files count
  std::vector<std::string> files;
  auto list_files = [&files] (std::string& name) {
    files.emplace_back(std::move(name));
    return true;
  };
  ASSERT_TRUE(dir.visit(list_files));
  EXPECT_EQ(files.size(), names.size());

  // Read contents
  it = names.end();
  bstring buf;

  for (const auto& name : names) {
    --it;
    irs::crc32c crc;
    bool exists;

    ASSERT_TRUE(dir.exists(exists, name) && exists);
    uint64_t length;
    EXPECT_TRUE(dir.length(length, name) && length == it->size());

    auto file = dir.open(name, irs::IOAdvice::NORMAL);
    ASSERT_FALSE(!file);
    EXPECT_FALSE(file->eof());
    EXPECT_EQ(0, file->file_pointer());
    EXPECT_EQ(file->length(), it->size());

    const auto checksum = file->checksum(file->length());

    buf.resize(it->size());
    const auto read = file->read_bytes(&(buf[0]), it->size());
    ASSERT_EQ(read, it->size());
    ASSERT_EQ(ref_cast<byte_type>(string_ref(*it)), buf);

    crc.process_bytes(buf.c_str(), buf.size());

    EXPECT_TRUE(file->eof());
    // check checksum
    EXPECT_EQ(crc.checksum(), checksum);
    // check that this is the end of the file
    EXPECT_EQ(file->length(), file->file_pointer());
  }

  for (const auto& name : names) {
    ASSERT_TRUE(dir.remove(name));
    bool exists;
    ASSERT_TRUE(dir.exists(exists, name) && !exists);
  }

  // Check files count
  files.clear();
  ASSERT_TRUE(dir.visit(list_files));
  EXPECT_EQ(0, files.size());

  // Try to open non existing input
  ASSERT_FALSE(dir.open("invalid_file_name", irs::IOAdvice::NORMAL));

  // Check locking logic
  auto l = dir.make_lock("sample_lock");
  ASSERT_FALSE(!l);
  ASSERT_TRUE(l->lock());
  bool locked;
  ASSERT_TRUE(l->is_locked(locked) && locked);
  ASSERT_TRUE(l->unlock());
  ASSERT_TRUE(l->is_locked(locked) && !locked);

  // Check read_bytes on empty file
  {
    byte_type buf[10];

    // create file
    {
      auto out = dir.create("empty_file");
      ASSERT_FALSE(!out);
    }

    // read from file
    {
      auto in = dir.open("empty_file", irs::IOAdvice::NORMAL);
      ASSERT_FALSE(!in);

      size_t read = std::numeric_limits<size_t>::max();
      try {
        read = in->read_bytes(buf, sizeof buf);
        ASSERT_EQ(0, read);
      } catch (const eof_error&) {
        // TODO: rework stream logic, stream should not throw an error
      } catch (...) {
        ASSERT_TRUE(false);
      }
    }

    ASSERT_TRUE(dir.remove("empty_file"));
  }

  // Check read_bytes after the end of file
  {

    // write to file
    {
      byte_type buf[1024]{};
      auto out = dir.create("nonempty_file");
      ASSERT_FALSE(!out);
      out->write_bytes(buf, sizeof buf);
      out->write_bytes(buf, sizeof buf);
      out->write_bytes(buf, 691);
      out->flush();
    }

    // read from file
    {
      byte_type buf[1024 + 691]{}; // 1024 + 691 from above
      auto in = dir.open("nonempty_file", irs::IOAdvice::NORMAL);
      size_t expected = sizeof buf;
      ASSERT_FALSE(!in);
      ASSERT_EQ(expected, in->read_bytes(buf, sizeof buf));

      size_t read = std::numeric_limits<size_t>::max();
      try {
        expected = in->length() - sizeof buf; // 'sizeof buf' already read above
        read = in->read_bytes(buf, sizeof buf);
        ASSERT_EQ(expected, read);
      } catch (const io_error&) {
        // TODO: rework stream logic, stream should not throw an error
      } catch (...) {
        ASSERT_TRUE(false);
      }
    }

    ASSERT_TRUE(dir.remove("nonempty_file"));
  }
}


void check_files(const directory& dir, const utf8_path& path) {
  const std::string file_name = "abcd";

  // create empty file
  {
    auto file = path;

    file /= file_name;

    std::ofstream f(file.native());
  }

  // read files from directory
  std::vector<std::string> files;
  auto list_files = [&files] (std::string& name) {
    files.emplace_back(std::move(name));
    return true;
  };
  ASSERT_TRUE(dir.visit(list_files));
  ASSERT_EQ(1, files.size());
  ASSERT_EQ(file_name, files[0]);
}

class fs_directory_test_case : public test_base {
 public:
  fs_directory_test_case()
    : name_("directory") {
  }

  virtual void SetUp() override {
    test_base::SetUp();
    path_ = test_case_dir();
    path_ /= name_;

    ASSERT_TRUE(path_.mkdir());
    dir_ = std::make_shared<fs_directory>(path_.utf8());
  }

  virtual void TearDown() override {
    dir_ = nullptr;
    ASSERT_TRUE(path_.remove());
    test_base::TearDown();
  }

 protected:
  std::string name_;
  utf8_path path_;
  std::shared_ptr<fs_directory> dir_;
}; // fs_directory_test

TEST_F(fs_directory_test_case, orphaned_lock) {
  // orhpaned lock file with orphaned pid, same hostname
  {
    // create lock file
    {
      char hostname[256] = {};
      ASSERT_EQ(0, get_host_name(hostname, sizeof hostname));

      const std::string pid = std::to_string(integer_traits<int>::const_max);
      auto out = dir_->create("lock");
      ASSERT_FALSE(!out);
      out->write_bytes(reinterpret_cast<const byte_type*>(hostname), strlen(hostname));
      out->write_byte(0);
      out->write_bytes(reinterpret_cast<const byte_type*>(pid.c_str()), pid.size());
    }

    // try to obtain lock
    {
      auto lock = dir_->make_lock("lock");
      ASSERT_FALSE(!lock);
      ASSERT_TRUE(lock->lock());
    }

    bool exists;
    ASSERT_TRUE(dir_->exists(exists, "lock") && !exists);
  }

  // orphaned lock file with invalid pid (not a number), same hostname
  {
    // create lock file
    {
      char hostname[256] = {};
      ASSERT_EQ(0, get_host_name(hostname, sizeof hostname));

      const std::string pid = "invalid_pid";
      auto out = dir_->create("lock");
      ASSERT_FALSE(!out);
      out->write_bytes(reinterpret_cast<const byte_type*>(hostname), strlen(hostname));
      out->write_byte(0);
      out->write_bytes(reinterpret_cast<const byte_type*>(pid.c_str()), pid.size());
    }

    // try to obtain lock
    {
      auto lock = dir_->make_lock("lock");
      ASSERT_FALSE(!lock);
      ASSERT_TRUE(lock->lock());
      ASSERT_TRUE(lock->unlock());
      bool exists;
      ASSERT_TRUE(dir_->exists(exists, "lock") && !exists);
    }
  }

  // orphaned empty lock file 
  {
    // create lock file
    {
      auto out = dir_->create("lock");
      ASSERT_FALSE(!out);
      out->flush();
    }

    // try to obtain lock
    {
      auto lock = dir_->make_lock("lock");
      ASSERT_FALSE(!lock);
      ASSERT_TRUE(lock->lock());
      ASSERT_TRUE(lock->unlock());
      bool exists;
      ASSERT_TRUE(dir_->exists(exists, "lock") && !exists);
    }
  }

  // orphaned lock file with hostname only
  {
    // create lock file
    {
      char hostname[256] = {};
      ASSERT_EQ(0, get_host_name(hostname, sizeof hostname));
      auto out = dir_->create("lock");
      ASSERT_FALSE(!out);
      out->write_bytes(reinterpret_cast<const byte_type*>(hostname), strlen(hostname));
    }

    // try to obtain lock
    {
      auto lock = dir_->make_lock("lock");
      ASSERT_FALSE(!lock);
      ASSERT_TRUE(lock->lock());
      ASSERT_TRUE(lock->unlock());
      bool exists;
      ASSERT_TRUE(dir_->exists(exists, "lock") && !exists);
    }
  }

  // orphaned lock file with valid pid, same hostname
  {
    // create lock file
    {
      char hostname[256] = {};
      ASSERT_EQ(0, get_host_name(hostname, sizeof hostname));
      const std::string pid = std::to_string(iresearch::get_pid());
      auto out = dir_->create("lock");
      ASSERT_FALSE(!out);
      out->write_bytes(reinterpret_cast<const byte_type*>(hostname), strlen(hostname));
      out->write_byte(0);
      out->write_bytes(reinterpret_cast<const byte_type*>(pid.c_str()), pid.size());
    }

    bool exists;
    ASSERT_TRUE(dir_->exists(exists, "lock") && exists);

    // try to obtain lock, after closing stream
    auto lock = dir_->make_lock("lock");
    ASSERT_FALSE(!lock);
    ASSERT_FALSE(lock->lock()); // still have different hostname
  }

  // orphaned lock file with valid pid, different hostname
  {
    // create lock file
    {
      char hostname[] = "not_a_valid_hostname_//+&*(%$#@! }";

      const std::string pid = std::to_string(iresearch::get_pid());
      auto out = dir_->create("lock");
      ASSERT_FALSE(!out);
      out->write_bytes(reinterpret_cast<const byte_type*>(hostname), strlen(hostname));
      out->write_byte(0);
      out->write_bytes(reinterpret_cast<const byte_type*>(pid.c_str()), pid.size());
    }

    bool exists;
    ASSERT_TRUE(dir_->exists(exists, "lock") && exists);

    // try to obtain lock, after closing stream
    auto lock = dir_->make_lock("lock");
    ASSERT_FALSE(!lock);
    ASSERT_FALSE(lock->lock()); // still have different hostname
  }
}

TEST_F(fs_directory_test_case, utf8_chars) {
  std::wstring path_ucs2 = L"\u0442\u0435\u0441\u0442\u043E\u0432\u0430\u044F_\u0434\u0438\u0440\u0435\u043A\u0442\u043E\u0440\u0438\u044F";
  irs::utf8_path path(path_ucs2);

  name_ = path.utf8();
  TearDown();

  // create directory via iResearch functions
  {
    SetUp();
    smoke_store(*dir_);
    // Read files from directory
    check_files(*dir_, path_);
  }
}

TEST_F(fs_directory_test_case, utf8_chars_native) {
  std::wstring path_ucs2 = L"\u0442\u0435\u0441\u0442\u043E\u0432\u0430\u044F_\u0434\u0438\u0440\u0435\u043A\u0442\u043E\u0440\u0438\u044F";
  irs::utf8_path path(path_ucs2);

  name_ = path.utf8();
  TearDown();

  // create directory via native functions
  {
    #ifdef _WIN32
      auto native_path = test_case_dir().native() + L'\\' + path.native();
      ASSERT_EQ(0, _wmkdir(native_path.c_str()));
    #else
      auto native_path = test_case_dir().native() + '/' + path.utf8();
      ASSERT_EQ(0, mkdir(native_path.c_str(), S_IRWXU | S_IRWXG | S_IRWXO));
    #endif

    SetUp();
    smoke_store(*dir_);
    // Read files from directory
    check_files(*dir_, native_path);
  }
}

NS_END

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
