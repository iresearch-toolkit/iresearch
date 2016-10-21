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
#include "memory_directory.hpp"
#include "checksum_io.hpp"

#include "error/error.hpp"

#include "utils/string.hpp"
#include "utils/thread_utils.hpp"
#include "utils/std.hpp"
#include "utils/utf8_path.hpp"

#include <cassert>
#include <cstring>
#include <algorithm>

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

NS_LOCAL

void touch(std::time_t& time) {
  time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

NS_END

NS_ROOT

//////////////////////////////////////////////////////////////////////////////
/// @class single_instance_lock
//////////////////////////////////////////////////////////////////////////////
class single_instance_lock : public index_lock {
 public:
  single_instance_lock(
      const std::string& name, 
      memory_directory* parent)
    : name(name), parent(parent) {
    assert(parent);
  }
  
  virtual bool lock() override {
    SCOPED_LOCK(parent->llock_);
    return parent->locks_.insert(name).second;
  }

  virtual bool is_locked() const override {
    SCOPED_LOCK(parent->llock_);
    return parent->locks_.find(name) != parent->locks_.end();
  }

  virtual void unlock() NOEXCEPT override{
    SCOPED_LOCK(parent->llock_);
    parent->locks_.erase(name);
  }

 private:
  std::string name;
  memory_directory* parent;
}; // single_instance_lock

/* -------------------------------------------------------------------
 * memory_index_input 
 * ------------------------------------------------------------------*/

memory_index_input::memory_index_input(const memory_file& file) NOEXCEPT
  : file_(&file) {
}

void memory_index_input::switch_buffer(size_t pos) {
  const auto buf_size = file_->buffer_size();

  auto idx = pos / buf_size;
  if (idx >= file_->num_buffers()) {
    throw eof_error(); // read past eof
  }

  auto buf = file_->at(idx);
  if (buf != buf_) {
    buf_ = buf;
    start_ = idx * buf_size;
    end_ = buf_ + std::min(buf_size, file_->length() - start_);
  }

  begin_ = buf_ + (pos % buf_size);
}

size_t memory_index_input::length() const {
  return file_->length();
}

size_t memory_index_input::file_pointer() const {
  return start_ + std::distance(buf_, begin_);
}
    
void memory_index_input::seek(size_t pos) {
  switch_buffer(pos);
}

byte_type memory_index_input::read_byte() {
  if (begin_ >= end_) {
    switch_buffer(file_pointer());
  }

  return *begin_++;
}

size_t memory_index_input::read_bytes(byte_type* b, size_t left) {  
  const size_t length = left; // initial length
  size_t copied;
  while (left) {
    if (begin_ >= end_) {
      if (eof()) {
        break;
      }
      switch_buffer(file_pointer());
    }

    copied = std::min(size_t(std::distance(begin_, end_)), left);
    std::memcpy(b, begin_, sizeof(byte_type) * copied);

    left -= copied;
    begin_ += copied;
    b += copied;
  }
  return length - left;
}

index_input::ptr memory_index_input::clone() const  {
  return index_input::ptr(new memory_index_input(*this));
}

/* -------------------------------------------------------------------
 * memory_index_output
 * ------------------------------------------------------------------*/

memory_index_output::memory_index_output(
  memory_file& file, memory_file_meta& meta) NOEXCEPT:
  file_(file), meta_(meta) {
  reset();
}

void memory_index_output::reset() {
  buf_ = nullptr;
  buf_offset_ = 0;
  buf_offset_ -= file_.buffer_size();
  pos_ = file_.buffer_size();
}

void memory_index_output::switch_buffer() {
  const auto idx = file_pointer() / file_.buffer_size();
  buf_ = idx >= file_.num_buffers() ? file_.push_buffer() : file_.at(idx);
  pos_ = 0;
  buf_offset_ = file_.buffer_size() * idx;
}

void memory_index_output::write_byte( byte_type byte ) {
  if ( pos_ >= file_.buffer_size() ) {
    switch_buffer();
  }
  buf_[pos_++] = byte;
  touch(meta_.mtime);
}

void memory_index_output::write_bytes( const byte_type* b, size_t len ) {
  assert( b );

  for(size_t to_copy = 0; len; len -= to_copy) {
    if (pos_ >= file_.buffer_size()) {
      switch_buffer();
    }

    to_copy = std::min(file_.buffer_size() - pos_, len);
    std::memcpy( buf_ + pos_, b, sizeof( byte_type ) * to_copy );
    b += to_copy;
    pos_ += to_copy;
  }

  touch(meta_.mtime);
}

void memory_index_output::close() {
  flush();
}

void memory_index_output::flush() {
  file_.length(memory_index_output::file_pointer());
}

size_t memory_index_output::file_pointer() const {
  return buf_offset_ + pos_;
}

int64_t memory_index_output::checksum() const {
  throw not_supported();
}

void memory_index_output::operator>>( data_output& out ) {
  auto len = file_.length();
  auto buf_size = file_.buffer_size();

  size_t pos = 0;
  size_t buf = 0;
  size_t to_copy = 0;

  while ( pos < len ) {
    to_copy = std::min( buf_size, len - pos );
    out.write_bytes(file_.at( buf++ ), to_copy);
    pos += to_copy;
  }

  touch(meta_.mtime);
}

/* -------------------------------------------------------------------
 * memory_directory
 * ------------------------------------------------------------------*/

memory_directory::memory_directory(size_t buf_size /* = DEF_BUF_SIZE */)
  : buf_size_(buf_size) {
  assert(buf_size_);
}

memory_directory::~memory_directory() { }

iresearch::attributes& memory_directory::attributes() {
  return attributes_;
}

void memory_directory::close() {
  SCOPED_LOCK(flock_);

  files_.clear();
}

bool memory_directory::list(directory::files& names) const {
  names.clear();
  names.reserve(files_.size());

  SCOPED_LOCK(flock_);

  std::transform(
    files_.begin(), files_.end(),
    irstd::back_emplacer(names),
    [] (const file_map::value_type& p) {
      return p.first;
  });
  return true;
}

bool memory_directory::exists(const std::string& name) const {
  SCOPED_LOCK(flock_);

  return files_.find(name) != files_.end();
}

std::time_t memory_directory::mtime(const std::string& name) const {
  SCOPED_LOCK(flock_);

  file_map::const_iterator it = files_.find(name);

  assert(it != files_.end());
  return it->second.second.mtime;
}

bool memory_directory::remove(const std::string& name) {
  SCOPED_LOCK(flock_);

  return files_.erase(name) > 0;
}

void memory_directory::rename(const std::string& src, const std::string& dst) {
  SCOPED_LOCK(flock_);

  file_map::iterator it = files_.find(src);
  if (it != files_.end()) {
    // noexcept
    files_.erase(dst); // emplace() will not overwrite as per spec
    files_.emplace(dst, std::move(it->second));
    files_.erase(it);
  }
}

int64_t memory_directory::length(const std::string& name) const {
  SCOPED_LOCK(flock_);

  file_map::const_iterator it = files_.find(name);

  assert(it != files_.end());
  return it->second.first->length();
}

void memory_directory::sync(const std::string& /*name*/) {
  // NOOP
}

index_lock::ptr memory_directory::make_lock(const std::string& name) {
  return index_lock::make<single_instance_lock>(name, this);
}

index_output::ptr memory_directory::create(const std::string& name) {
  SCOPED_LOCK(flock_);

  auto res = files_.emplace(
    std::piecewise_construct,
    std::forward_as_tuple(name),
    std::forward_as_tuple()
  );

  auto& it = res.first;
  auto& file = it->second;

  if (!res.second) { // file exists
    file.first->reset();
  } else {
    file.first = std::move(memory::make_unique<memory_file>(buf_size_));
  }

  touch(file.second.mtime);

  typedef checksum_index_output<boost::crc_32_type> checksum_output_t;

  index_output::ptr out(new memory_index_output(*(file.first), file.second));
  return index_output::make<checksum_output_t>(std::move(out));
}

index_input::ptr memory_directory::open(const std::string& name) const {
  SCOPED_LOCK(flock_);

  file_map::const_iterator it = files_.find(name);

  if (it == files_.end()) {
    std::stringstream ss;

    ss << "Failed to open input file, error: File not found, path: " << name;

    throw detailed_io_error(ss.str());
  }

  return index_input::make<memory_index_input>(*it->second.first);
}

/* -------------------------------------------------------------------
* memory_output
* ------------------------------------------------------------------*/

memory_output::memory_output(size_t buf_size /* = memory_directory::DEF_BUF_SIZE */)
  : file(buf_size) { 
}

NS_END