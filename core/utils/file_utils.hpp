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

#ifndef IRESEARCH_FILE_UTILS_H
#define IRESEARCH_FILE_UTILS_H

#include <memory>
#include <cstdio>
#include <functional>
#include <fcntl.h> // open/_wopen

#ifdef _WIN32  
  #include <tchar.h>
  #include <io.h> // _close
  #include "windows.h"
  #define file_blksize_t uint32_t // DWORD (same as GetDriveGeometry(...) DISK_GEOMETRY::BytesPerSector)
  #define file_path_delimiter L'\\'
  #define file_path_t wchar_t*
  #define file_stat _wstat64
 
  #define file_stat_t struct _stat64
  #define file_no(f) f

  #define mode_t unsigned short
  #define posix_create _wcreat
  #define posix_open _wopen
  #define posix_close _close
  #define feof_unlocked feof
  #define file_fstat _fstat64

  #define fwrite_unlocked(buf, size, len, fd) iresearch::file_utils::write((fd), (buf), (len) * (size))
  #define fread_unlocked(buf, size, len, fd) iresearch::file_utils::read((fd), (buf), (len) * (size))
  #define file_fseek iresearch::file_utils::fseek
  #define file_error iresearch::file_utils::ferror
  #define file_ftell iresearch::file_utils::ftell

  #define IR_FADVICE_NORMAL 0
  #define IR_FADVICE_SEQUENTIAL FILE_FLAG_SEQUENTIAL_SCAN
  #define IR_FADVICE_RANDOM FILE_FLAG_RANDOM_ACCESS
  #define IR_FADVICE_DONTNEED 0
  #define IR_FADVICE_NOREUSE 0
  #define IR_WSTR(x) L ## x // cannot use _T(...) macro when _MBCS is defined

  #define IR_FILE void
#else
  #include <unistd.h> // close
  #include <sys/types.h> // for blksize_t
  #define file_blksize_t blksize_t
  #define file_path_delimiter '/'
  #define file_path_t char*
  #define file_stat stat
  #define file_fstat fstat
  #define file_stat_t struct stat    
  #define file_no fileno
  #define file_fseek fseek
  #define file_error ferror
  #define file_ftell ::ftell
  #define posix_create creat
  #define posix_open open
  #define posix_close close
#ifdef __APPLE__
  // MAX doesn't have nolock functions
  #define fwrite_unlocked fwrite
  #define fread_unlocked fread
  #define feof_unlocked feof
#endif

  #define IR_FADVICE_NORMAL POSIX_FADV_NORMAL
  #define IR_FADVICE_SEQUENTIAL POSIX_FADV_SEQUENTIAL
  #define IR_FADVICE_RANDOM POSIX_FADV_RANDOM
  #define IR_FADVICE_DONTNEED POSIX_FADV_DONTNEED
  #define IR_FADVICE_NOREUSE POSIX_FADV_NOREUSE
  #define IR_FILE FILE
#endif

#define file_open_read(name, advice) iresearch::file_utils::open(name, iresearch::file_utils::OpenMode::Read, advice)
#define file_open_write(name, advice) iresearch::file_utils::open(name, iresearch::file_utils::OpenMode::Write, advice) 

#include "shared.hpp"
#include "string.hpp"

NS_ROOT
NS_BEGIN(file_utils)

// -----------------------------------------------------------------------------
// --SECTION--                                                         lock file
// -----------------------------------------------------------------------------

struct lock_file_deleter {
  void operator()(IR_FILE* handle) const;
}; // lock_file_deleter

typedef std::unique_ptr<IR_FILE, lock_file_deleter> lock_handle_t;

lock_handle_t create_lock_file(const file_path_t file);
bool verify_lock_file(const file_path_t file);

// -----------------------------------------------------------------------------
// --SECTION--                                                             stats
// -----------------------------------------------------------------------------

bool absolute(bool& result, const file_path_t path) noexcept;

bool block_size(file_blksize_t& result, const file_path_t file) noexcept;

#ifdef _WIN32
bool block_size(file_blksize_t& result, HANDLE fd) noexcept;
#else
bool block_size(file_blksize_t& result, int fd) noexcept;
#endif

bool byte_size(uint64_t& result, const file_path_t file) noexcept;

#ifdef _WIN32
bool byte_size(uint64_t& result, HANDLE fd) noexcept;
#endif

bool byte_size(uint64_t& result, int fd) noexcept;


bool exists(bool& result, const file_path_t file) noexcept;
bool exists_directory(bool& result, const file_path_t file) noexcept;
bool exists_file(bool& result, const file_path_t file) noexcept;

bool mtime(time_t& result, const file_path_t file) noexcept;
bool mtime(time_t& result, int fd) noexcept;

// -----------------------------------------------------------------------------
// --SECTION--                                                         open file
// -----------------------------------------------------------------------------
enum class OpenMode {
  Read,
  Write
};

struct file_deleter {
  void operator()(IR_FILE* f) const noexcept {
#if _WIN32
    if (f != nullptr && f != INVALID_HANDLE_VALUE) {
      CloseHandle(f);
    }
#else
    if (f) {
      ::fclose(f);
    }
#endif
  }
}; // file_deleter

typedef std::unique_ptr<IR_FILE, file_deleter> handle_t;

handle_t open(const file_path_t path, OpenMode mode, int advice) noexcept;
handle_t open(IR_FILE* file, OpenMode mode, int advice) noexcept;

// -----------------------------------------------------------------------------
// --SECTION--                                                        path utils
// -----------------------------------------------------------------------------

bool mkdir(const file_path_t path, bool createNew) noexcept;  // recursive directory creation

bool move(const file_path_t src_path, const file_path_t dst_path) noexcept;

#ifdef _WIN32 // we do not use CRT files on Win32, so expose native API wrappers
size_t read(HANDLE fd, byte_type* buf, size_t size);
bool write(HANDLE fd, const byte_type* buf, size_t size);
int fseek(HANDLE fd, long pos, int origin);
FORCE_INLINE int ferror(HANDLE) { return static_cast<int>(GetLastError()); }
long ftell(HANDLE fd);
#endif

struct path_parts_t {
  typedef irs::basic_string_ref<std::remove_pointer<file_path_t>::type> ref_t;
  ref_t basename;  // path component after the last path delimiter (ref_t::NIL if not present)
  ref_t dirname;   // path component before the last path delimiter (ref_t::NIL if not present)
  ref_t extension; // basename extension (ref_t::NIL if not present)
  ref_t stem;      // basename without extension (ref_t::NIL if not present)
};

IRESEARCH_API path_parts_t path_parts(const file_path_t path) noexcept;

IRESEARCH_API bool read_cwd(
  std::basic_string<std::remove_pointer<file_path_t>::type>& result
) noexcept;

bool remove(const file_path_t path) noexcept;

bool set_cwd(const file_path_t path) noexcept;

// -----------------------------------------------------------------------------
// --SECTION--                                                   directory utils
// -----------------------------------------------------------------------------

bool visit_directory(
  const file_path_t name,
  const std::function<bool(const file_path_t name)>& visitor,
  bool include_dot_dir = true
);

// -----------------------------------------------------------------------------
// --SECTION--                                                              misc
// -----------------------------------------------------------------------------

bool file_sync(const file_path_t name) noexcept;
bool file_sync(int fd) noexcept;

NS_END
NS_END

#endif
