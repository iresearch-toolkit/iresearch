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

#include "shared.hpp"
#include "fs_directory.hpp"
#include "checksum_io.hpp"
#include "error/error.hpp"
#include "utils/utf8_path.hpp"

#ifdef _WIN32
  #include <Windows.h> // for GetLastError()
#endif

#include <boost/filesystem/operations.hpp>
#include <boost/system/error_code.hpp>
#include <boost/locale/encoding.hpp>

#if defined(_MSC_VER)
  #pragma warning(disable : 4244)
  #pragma warning(disable : 4245)
#elif defined (__GNUC__)
  // NOOP
#endif

#include <boost/crc.hpp>

#if defined(_MSC_VER)
  #pragma warning(default: 4244)
  #pragma warning(default: 4245)
#elif defined (__GNUC__)
  // NOOP
#endif

#if defined(_MSC_VER)
  #pragma warning ( disable : 4996 )
#endif

NS_ROOT

//////////////////////////////////////////////////////////////////////////////
/// @class fs_lock
//////////////////////////////////////////////////////////////////////////////

class fs_lock : public index_lock {
 public:
  fs_lock(const std::string& dir, const std::string& file)
    : dir_(dir), file_(file) {
  }

  virtual bool lock() override {
    if (handle_) {
      // don't allow self obtaining
      return false;
    }

    // create directory if it is not exists
    utf8_path path;
    try {
      if (!(path/dir_).exists()) {
        path.mkdir();
      }
    } catch (const boost::filesystem::filesystem_error&) {
      std::stringstream ss;
      ss << "Failed to create directory for lock file, path: " << path.utf8();

      throw detailed_io_error(ss.str());
    }

    // create lock file
    if (!file_utils::verify_lock_file((path/file_).c_str())) {
      if (path.exists()) {
        path.remove();
      }
      handle_ = file_utils::create_lock_file(path.c_str());
    }

    return handle_ != nullptr;
  }

  virtual bool is_locked() const override {
    if (handle_ != nullptr) {
      return true;
    }

    utf8_path path;
    path/dir_/file_;
    return file_utils::verify_lock_file(path.c_str());
  }

  virtual void unlock() NOEXCEPT override {
    if (handle_ != nullptr) {
      handle_ = nullptr;
#ifdef _WIN32
      // file will be automatically removed on close
#else
      (utf8_path()/dir_/file_).remove();
#endif
    }
  }

 private:
  std::string dir_;
  std::string file_;
  file_utils::lock_handle_t handle_;
}; // fs_lock

//////////////////////////////////////////////////////////////////////////////
/// @class fs_index_output
//////////////////////////////////////////////////////////////////////////////
class fs_index_output : public buffered_index_output {
 public:
  static fs_index_output* open(const file_path_t name) {
    assert(name);

    file_utils::handle_t handle(file_open(name, "wb"));

    if (nullptr == handle) {
      auto path = boost::locale::conv::utf_to_utf<char>(name);
          
      std::stringstream ss;

#ifdef _WIN32
      ss << "Failed to open output file, error: " << GetLastError()
         << ", path: " << path;
#else
      ss << "Failed to open output file, error: " << errno
         << ", path: " << path;
#endif

      throw detailed_io_error(ss.str());
    }

    return new fs_index_output(std::move(handle));
  }

  virtual void close() override {
    buffered_index_output::close();
    handle.reset(nullptr);
  }

  virtual int64_t checksum() const override {
    throw not_supported();
  }

 protected:
  virtual void flush_buffer(const byte_type* b, size_t len) override {
    assert(handle);

    auto len_written = fwrite(b, sizeof(byte_type), len, handle.get());

    if (len && len_written != len) {
      std::stringstream ss;
      ss << "Failed to write buffer, written " << len_written 
         << " out of " << len << " bytes.", 
      throw detailed_io_error(ss.str());
    }
  }

 private:
  fs_index_output(file_utils::handle_t&& handle) NOEXCEPT
    : handle(std::move(handle)) {
  }

  file_utils::handle_t handle;
}; // fs_index_output

//////////////////////////////////////////////////////////////////////////////
/// @class fs_index_input
//////////////////////////////////////////////////////////////////////////////
class fs_index_input : public buffered_index_input {
 public:
  static fs_index_input* open(const file_path_t name) {
    assert(name);

    file_handle::ptr handle = file_handle::make<file_handle>();
    handle->handle.reset(file_open(name, "rb"));

    if (nullptr == handle->handle) {
      auto path = boost::locale::conv::utf_to_utf<char>(name);
      std::stringstream ss;

#ifdef _WIN32
      ss << "Failed to open input file, error: " << GetLastError()
         << ", path: " << path;
#else
      ss << "Failed to open input file, error: " << errno
         << ", path: " << path;
#endif

      throw detailed_io_error(ss.str());
    }

    // convert file descriptor to POSIX
    auto size = file_utils::file_size(file_no(*handle));
    if (handle->size < 0) {
      auto path = boost::locale::conv::utf_to_utf<char>(name);
      std::stringstream ss;

#ifdef _WIN32
      ss << "Failed to get stat for input file, error: " << GetLastError()
         << ", path: " << path;
#else
      ss << "Failed to get stat for input file, error: " << errno
         << ", path: " << path;
#endif

      throw detailed_io_error(ss.str());
    }

    handle->size = size;
    return new fs_index_input(std::move(handle));
  }

  virtual index_input::ptr clone() const override {
    return index_input::ptr(new fs_index_input(*this));
  }

  virtual size_t length() const override {
    return handle_->size;
  }

 protected:
  virtual void seek_internal(size_t pos) override {
    if (pos >= handle_->size) {
      std::stringstream ss;
      ss << "Seek out of range for input file, length " << handle_->size 
         << ", position " << pos;
      throw detailed_io_error(ss.str());
    }

    pos_ = pos;
  }

  virtual size_t read_internal(byte_type* b, size_t len) override {
    assert(b);
    assert(handle_->handle);

    FILE* stream = *handle_;

    if (handle_->pos != pos_) {
      if (fseek(stream, static_cast<long>(pos_), SEEK_SET) != 0) {

        std::stringstream ss;
        ss << "Failed to seek to " << pos_ 
           << " for input file, error " << ferror(stream);
        throw detailed_io_error(ss.str());
      }

      handle_->pos = pos_;
    }

    const size_t read = fread(b, sizeof(byte_type), len, stream);
    pos_ = handle_->pos += read;

    if (read != len) {
      if (feof(stream)) {
        //eof(true);
        // read past eof
        throw eof_error();
      }

      // read error
      std::stringstream ss;
      ss << "Failed to read from input file, read " << read 
         << " out of " << len 
         << " bytes, error " << ferror(stream);
      throw detailed_io_error(ss.str());
    }

    assert(handle_->pos == pos_);
    return read;
  }

 private:
  /* use shared wrapper here since we don't want to
  * call "ftell" every time we need to know current 
  * position */
  struct file_handle {
    DECLARE_SPTR(file_handle);
    DECLARE_FACTORY(file_handle);

    operator FILE*() const { return handle.get(); }

    file_utils::handle_t handle; /* native file handle */
    size_t size{}; /* file size */
    size_t pos{}; /* current file position*/
  }; // file_handle

  fs_index_input(file_handle::ptr&& handle) NOEXCEPT
    : handle_(std::move(handle)), pos_(0) {
    assert(handle_);
  }

  fs_index_input(const fs_index_input&) = default;
  fs_index_input& operator=(const fs_index_input&) = delete;

  file_handle::ptr handle_; /* shared file handle */
  size_t pos_; /* current input stream position */
}; // fs_index_input

// -----------------------------------------------------------------------------
// --SECTION--                                       fs_directory implementation
// -----------------------------------------------------------------------------

bool fs_directory::create_directory(const string_ref& dir) {
  return (utf8_path()/dir).mkdir();
}

bool fs_directory::remove_directory(const string_ref& dir) {
  return (utf8_path()/dir).rmdir();
}

fs_directory::fs_directory(const std::string& dir)
  : dir_(dir) {
}

const std::string& fs_directory::directory() const {
  return dir_;
}

bool fs_directory::exists(const std::string& name) const {
  return (utf8_path()/dir_/name).exists();
}

int64_t fs_directory::length(const std::string& name) const {
  return (utf8_path()/dir_/name).file_size();
}

std::time_t fs_directory::mtime(const std::string& name) const {
  return (utf8_path()/dir_/name).file_mtime();
}

bool fs_directory::remove(const std::string& name) {
  return (utf8_path()/dir_/name).remove();
}

void fs_directory::rename(const std::string& src, const std::string& dst) {
  (utf8_path()/dir_/src).rename(utf8_path()/dir_/dst);
}

iresearch::attributes& fs_directory::attributes() {
  return attributes_;
}

void fs_directory::close() { }

bool fs_directory::list( directory::files& names ) const {
  auto directory = (utf8_path()/dir_).native();

  names.clear();

  if (!file_utils::is_directory(directory.c_str())) {
    return false;
  }

  auto visitor = [&names](const file_path_t name)->bool {
#ifdef _WIN32
    boost::filesystem::path::string_type path(name);
    auto u8_path = (utf8_path() / path).utf8();

    names.emplace_back(std::move(u8_path));
#else
    names.emplace_back(name);
#endif

    return true;
  };

  if (!file_utils::visit_directory(directory.c_str(), visitor, false)) {
    names.clear();

    return false;
  }

  return true;
}

void fs_directory::sync(const std::string& name) {
  utf8_path path;
  path/dir_/name;
  if (!file_utils::file_sync(path.c_str())) {
    std::stringstream ss;

#ifdef _WIN32
    ss << "Failed to sync file, error: " << GetLastError() 
       << " path: " << path.utf8();
#else
    ss << "Failed to sync file, error: " << errno 
       << " path: " << path.utf8();
#endif

    throw detailed_io_error(ss.str());
  }
}

index_lock::ptr fs_directory::make_lock(const std::string& name) {
  return index_lock::make<fs_lock>(dir_, name);
}

index_output::ptr fs_directory::create(const std::string& name) {
  typedef checksum_index_output<boost::crc_32_type> checksum_output_t;

  index_output::ptr out(
    fs_index_output::open((utf8_path()/dir_/name).c_str())
  );

  return index_output::make<checksum_output_t>(std::move(out));
}

index_input::ptr fs_directory::open(const std::string& name) const {
  return index_input::ptr(
    fs_index_input::open((utf8_path()/dir_/name).c_str())
  );
}

NS_END

#if defined(_MSC_VER)
  #pragma warning ( default : 4996 )
#endif
