////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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

#include "tests_shared.hpp"
#include "utils/file_utils.hpp"

TEST(file_utils_tests, path_parts) {
  typedef irs::file_utils::path_parts_t::ref_t ref_t;

  // nullptr
  {
    auto parts = irs::file_utils::path_parts(nullptr);
    ASSERT_EQ(ref_t::nil, parts.parent);
    ASSERT_EQ(ref_t::nil, parts.stem);
    ASSERT_EQ(ref_t::nil, parts.extension);
  }

  // ...........................................................................
  // no parent
  // ...........................................................................

  // no parent, stem(empty), no extension
  {
    auto data = MSVC_ONLY(L)"";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t::nil, parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.stem);
    ASSERT_EQ(ref_t::nil, parts.extension);
  }

  // no parent, stem(empty), extension(empty)
  {
    auto data = MSVC_ONLY(L)".";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t::nil, parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.stem);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.extension);
  }

  // no parent, stem(empty), extension(non-empty)
  {
    auto data = MSVC_ONLY(L)".xyz";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t::nil, parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.stem);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"xyz"), parts.extension);
  }

  // no parent, stem(non-empty), no extension
  {
    auto data = MSVC_ONLY(L)"abc";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t::nil, parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc"), parts.stem);
    ASSERT_EQ(ref_t::nil, parts.extension);
  }
  
  // no parent, stem(non-empty), extension(empty)
  {
    auto data = MSVC_ONLY(L)"abc.";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t::nil, parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc"), parts.stem);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.extension);
  }

  // no parent, stem(non-empty), extension(non-empty)
  {
    auto data = MSVC_ONLY(L)"abc.xyz";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t::nil, parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc"), parts.stem);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"xyz"), parts.extension);
  }

  // no parent, stem(non-empty), extension(non-empty) (multi-extension)
  {
    auto data = MSVC_ONLY(L)"abc.def..xyz";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t::nil, parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc.def."), parts.stem);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"xyz"), parts.extension);
  }

  // ...........................................................................
  // empty parent
  // ...........................................................................

  // parent(empty), stem(empty), no extension
  {
    auto data = MSVC_ONLY(L)"/";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.stem);
    ASSERT_EQ(ref_t::nil, parts.extension);
  }

  // parent(empty), stem(empty), extension(empty)
  {
    auto data = MSVC_ONLY(L)"/.";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.stem);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.extension);
  }

  // parent(empty), stem(empty), extension(non-empty)
  {
    auto data = MSVC_ONLY(L)"/.xyz";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.stem);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"xyz"), parts.extension);
  }

  // parent(empty), stem(non-empty), no extension
  {
    auto data = MSVC_ONLY(L)"/abc";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc"), parts.stem);
    ASSERT_EQ(ref_t::nil, parts.extension);
  }

  // parent(empty), stem(non-empty), extension(empty)
  {
    auto data = MSVC_ONLY(L)"/abc.";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc"), parts.stem);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.extension);
  }

  // parent(empty), stem(non-empty), extension(non-empty)
  {
    auto data = MSVC_ONLY(L)"/abc.xyz";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc"), parts.stem);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"xyz"), parts.extension);
  }

  // parent(empty), stem(non-empty), extension(non-empty) (multi-extension)
  {
    auto data = MSVC_ONLY(L)"/abc.def..xyz";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc.def."), parts.stem);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"xyz"), parts.extension);
  }

  // ...........................................................................
  // non-empty parent
  // ...........................................................................

  // parent(non-empty), stem(empty), no extension
  {
    auto data = MSVC_ONLY(L)"klm/";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"klm"), parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.stem);
    ASSERT_EQ(ref_t::nil, parts.extension);
  }

  // parent(non-empty), stem(empty), extension(empty)
  {
    auto data = MSVC_ONLY(L)"klm/.";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"klm"), parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.stem);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.extension);
  }

  // parent(non-empty), stem(empty), extension(non-empty)
  {
    auto data = MSVC_ONLY(L)"klm/.xyz";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"klm"), parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.stem);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"xyz"), parts.extension);
  }

  // parent(non-empty), stem(non-empty), no extension
  {
    auto data = MSVC_ONLY(L)"klm/abc";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"klm"), parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc"), parts.stem);
    ASSERT_EQ(ref_t::nil, parts.extension);
  }

  // parent(non-empty), stem(non-empty), extension(empty)
  {
    auto data = MSVC_ONLY(L)"klm/abc.";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"klm"), parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc"), parts.stem);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.extension);
  }

  // parent(non-empty), stem(non-empty), extension(non-empty)
  {
    auto data = MSVC_ONLY(L)"klm/abc.xyz";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"klm"), parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc"), parts.stem);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"xyz"), parts.extension);
  }

  // parent(non-empty), stem(non-empty), extension(non-empty) (multi-extension)
  {
    auto data = MSVC_ONLY(L)"klm/abc.def..xyz";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"klm"), parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc.def."), parts.stem);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"xyz"), parts.extension);
  }

  // parent(non-empty), stem(non-empty), extension(non-empty) (multi-parent, multi-extension)
  {
    auto data = MSVC_ONLY(L)"/123/klm/abc.def..xyz";
    auto parts = irs::file_utils::path_parts(data);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"/123/klm"), parts.parent);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc.def."), parts.stem);
    ASSERT_EQ(ref_t(MSVC_ONLY(L)"xyz"), parts.extension);
  }

  #ifdef _WIN32
    // ...........................................................................
    // win32 non-empty parent
    // ...........................................................................

    // parent(non-empty), stem(empty), no extension
    {
      auto data = MSVC_ONLY(L)"klm\\";
      auto parts = irs::file_utils::path_parts(data);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)"klm"), parts.parent);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.stem);
      ASSERT_EQ(ref_t::nil, parts.extension);
    }

    // parent(non-empty), stem(empty), extension(empty)
    {
      auto data = MSVC_ONLY(L)"klm\\.";
      auto parts = irs::file_utils::path_parts(data);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)"klm"), parts.parent);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.stem);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.extension);
    }

    // parent(non-empty), stem(empty), extension(non-empty)
    {
      auto data = MSVC_ONLY(L)"klm\\.xyz";
      auto parts = irs::file_utils::path_parts(data);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)"klm"), parts.parent);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.stem);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)"xyz"), parts.extension);
    }

    // parent(non-empty), stem(non-empty), no extension
    {
      auto data = MSVC_ONLY(L)"klm\\abc";
      auto parts = irs::file_utils::path_parts(data);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)"klm"), parts.parent);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc"), parts.stem);
      ASSERT_EQ(ref_t::nil, parts.extension);
    }

    // parent(non-empty), stem(non-empty), extension(empty)
    {
      auto data = MSVC_ONLY(L)"klm\\abc.";
      auto parts = irs::file_utils::path_parts(data);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)"klm"), parts.parent);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc"), parts.stem);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)""), parts.extension);
    }

    // parent(non-empty), stem(non-empty), extension(non-empty)
    {
      auto data = MSVC_ONLY(L)"klm\\abc.xyz";
      auto parts = irs::file_utils::path_parts(data);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)"klm"), parts.parent);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc"), parts.stem);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)"xyz"), parts.extension);
    }

    // parent(non-empty), stem(non-empty), extension(non-empty) (multi-extension)
    {
      auto data = MSVC_ONLY(L)"klm\\abc.def..xyz";
      auto parts = irs::file_utils::path_parts(data);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)"klm"), parts.parent);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc.def."), parts.stem);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)"xyz"), parts.extension);
    }

    // parent(non-empty), stem(non-empty), extension(non-empty) (multi-parent, multi-extension)
    {
      auto data = MSVC_ONLY(L)"/123\\klm/abc.def..xyz";
      auto parts = irs::file_utils::path_parts(data);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)"/123\\klm"), parts.parent);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)"abc.def."), parts.stem);
      ASSERT_EQ(ref_t(MSVC_ONLY(L)"xyz"), parts.extension);
    }
  #endif
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------