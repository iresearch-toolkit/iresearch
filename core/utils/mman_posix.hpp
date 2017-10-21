////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_MMAP_POSIX_H
#define IRESEARCH_MMAP_POSIX_H

#if !defined(_MSC_VER)

#ifdef __linux__
#include <linux/version.h>
#endif

#include <sys/mman.h>

////////////////////////////////////////////////////////////////////////////////
/// @brief create a wrapper for MAP_ANONYMOUS / MAP_ANON
///
///               MAP_ANON  MAP_ANONYMOUS
///    OpenBSD         Y         Y(*)
///    Linux           Y(*)      Y
///    FreeBSD         Y         Y
///    NetBSD          Y         N
///    OSX             Y         N
///    Solaris         Y         N
///    HP-UX           N         Y
///    AIX             N         Y
///    IRIX            N         N
///    (*) synonym to other
///
////////////////////////////////////////////////////////////////////////////////
#ifndef MAP_ANON
#ifdef MAP_ANONYMOUS
#define MAP_ANON MAP_ANONYMOUS
#else
#error "System does not support mapping anonymous pages?"
#endif
#endif // MAP_ANON

#ifdef __linux__

#undef IR_MADVISE_NORMAL
#define IR_MADVISE_NORMAL MADV_NORMAL

#undef IR_MADVISE_SEQUENTIAL
#define IR_MADVISE_SEQUENTIAL MADV_SEQUENTIAL

#undef IR_MADVISE_RANDOM
#define IR_MADVISE_RANDOM MADV_RANDOM

#undef IR_MADVISE_WILLNEED
#define IR_MADVISE_WILLNEED MADV_WILLNEED

#undef IR_MADVISE_DONTNEED
#define IR_MADVISE_DONTNEED MADV_DONTNEED

#if LINUX_VERSION_CODE >= KERNEL_VERSION(3,4,0)
// only present since Linux 3.4
#undef IR_MADVISE_DONTDUMP
#define IR_MADVISE_DONTDUMP MADV_DONTDUMP
#endif

#endif // __linux__

#endif // !defined(_MSC_VER)

#endif // IRESEARCH_MMAP_POSIX_H
