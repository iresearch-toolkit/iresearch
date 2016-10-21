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

#ifndef IRESEARCH_MEMORYDIRECTORY_H
#define IRESEARCH_MEMORYDIRECTORY_H

#include "directory.hpp"
#include "utils/attributes.hpp"
#include "utils/string.hpp"

#include <mutex>
#include <unordered_map>
#include <unordered_set>

NS_ROOT

// -------------------------------------------------------------------
// metadata for a memory_file
// -------------------------------------------------------------------
struct memory_file_meta {
  std::time_t mtime;
};

/* -------------------------------------------------------------------
* memory_file
* ------------------------------------------------------------------*/

class memory_file : util::noncopyable {
 public:
  DECLARE_PTR(memory_file);

  explicit memory_file(size_t buf_size)
    : buf_size_(buf_size) {
  }

  memory_file(memory_file&& rhs)
    : buffers_(std::move(rhs.buffers_)),
      len_(rhs.len_),
      buf_size_(rhs.buf_size_) {
    rhs.len_ = 0;
    rhs.buf_size_ = 0;
  }

  size_t buffer_size() const { return buf_size_; }

  size_t length() const { return len_; }
  void length(size_t length) { len_ = length; }

  size_t buffer_length(size_t i) const {
    auto last_buf = len_ / buf_size_;

    if (i == last_buf) {
      return len_ % buf_size_;
    }

    return i < last_buf ? buf_size_ : 0;
  }

  size_t num_buffers() const { return buffers_.size(); }

  byte_type* push_buffer() {
    buffers_.emplace_back(memory::make_unique< byte_type[] >(buf_size_));
    return buffers_.back().get();
  }

  byte_type* at(size_t i) const { return buffers_.at(i).get(); }

  void reset() { len_ = 0; }

  void clear() {
    buffers_.clear();
    reset();
  }

  template<typename Visitor>
  bool visit(const Visitor& visitor) {
    for (size_t i = 0, size = num_buffers(); i < size; ++i) {
      if (!visitor(at(i), buffer_length(i))) {
        return false;
      }
    }
    return true;
  }

 private:
  typedef std::unique_ptr< byte_type[] > buffer_t;

  std::vector< buffer_t > buffers_;
  size_t len_{};
  size_t buf_size_;
};

/* -------------------------------------------------------------------
 * memory_index_input 
 * ------------------------------------------------------------------*/

struct memory_buffer;

class IRESEARCH_API memory_index_input final : public index_input {
 public:
  explicit memory_index_input(const memory_file& file) NOEXCEPT;

  virtual byte_type read_byte() override;
  virtual size_t read_bytes(byte_type* b, size_t len) override;

  virtual size_t length() const override;

  virtual size_t file_pointer() const override;

  virtual void seek(size_t pos) override;

  virtual bool eof() const override {
    return file_pointer() >= file_->length();
  }

  virtual index_input::ptr clone() const override;

 private:
  memory_index_input(const memory_index_input&) = default;

  void switch_buffer(size_t pos);

  const memory_file* file_; // underline file
  const byte_type* buf_{}; // current buffer
  const byte_type* begin_{ buf_ }; // current position
  const byte_type* end_{ buf_ }; // end of the valid bytes
  size_t start_{}; // buffer offset in file
};

/* -------------------------------------------------------------------
 * memory_index_output
 * ------------------------------------------------------------------*/

class IRESEARCH_API memory_index_output final : public index_output {
 public:
  explicit memory_index_output(memory_file& file, memory_file_meta & meta) NOEXCEPT;
  memory_index_output(const memory_index_output&) = default; 
  memory_index_output& operator=(const memory_index_output&) = delete;

  void reset();

  // data_output

  virtual void close() override;

  virtual void write_byte( byte_type b ) override;

  virtual void write_bytes( const byte_type* b, size_t len ) override;

  // index_output

  virtual void flush() override; // deprecated

  virtual size_t file_pointer() const override;

  virtual int64_t checksum() const override;

  void operator>>( data_output& out );

 private:
  void switch_buffer();

  memory_file& file_; // underlying file
  memory_file_meta& meta_; // metadata for the underlying file
  byte_type* buf_; /* current buffer */
  size_t buf_offset_; /* buffer offset in file */
  size_t pos_; /* position in current buffer */
};

/* -------------------------------------------------------------------
 * memory_directory
 * ------------------------------------------------------------------*/

class IRESEARCH_API memory_directory final : public directory {
 public:
  static const size_t DEF_BUF_SIZE = 1024;

  explicit memory_directory(size_t buf_size = DEF_BUF_SIZE);

  virtual ~memory_directory();
  using directory::attributes;
  virtual iresearch::attributes& attributes() override;
  virtual void close() override;

  virtual bool list(directory::files& names) const override;

  virtual bool exists(const std::string& name) const override;

  virtual std::time_t mtime(const std::string& name) const override;

  virtual bool remove(const std::string& name) override;

  virtual void rename(
    const std::string& src,
    const std::string& dst) override;

  virtual int64_t length(const std::string& name) const override;

  virtual void sync(const std::string& name) override;

  virtual index_lock::ptr make_lock(const std::string& name) override;

  virtual index_output::ptr create(const std::string& name) override;

  virtual index_input::ptr open(const std::string& name) const override;

 private:
  friend class single_instance_lock;
  typedef std::unordered_map<std::string, std::pair<memory_file::ptr, memory_file_meta>> file_map;
  typedef std::unordered_set<std::string> lock_map;

  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  mutable std::mutex flock_;
  std::mutex llock_;
  iresearch::attributes attributes_;
  file_map files_;
  lock_map locks_;
  size_t buf_size_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
};

/* -------------------------------------------------------------------
 * memory_output
 * ------------------------------------------------------------------*/

struct IRESEARCH_API memory_output {
  explicit memory_output(size_t buf_size = memory_directory::DEF_BUF_SIZE);

  memory_output(memory_output&& rhs)
    : file(std::move(rhs.file)),
      meta(std::move(rhs.meta)),
      stream(std::move(rhs.stream)) {
  }

  void reset() {
    file.reset();
    stream.reset();
  }

  memory_file file;
  memory_file_meta meta;
  memory_index_output stream{ file, meta };
};

NS_END

#endif