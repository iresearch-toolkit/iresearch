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

class CachingDirectoryTestCase : public tests::directory_test_case_base<> {};

TEST_P(CachingDirectoryTestCase, TestCaching) {}

template<size_t Size>
struct MaxSizeAcceptor : irs::MaxSizeAcceptor {
  MaxSizeAcceptor() noexcept : irs::MaxSizeAcceptor{Size} {}
};

INSTANTIATE_TEST_SUITE_P(
  CachingDirectoryTest, CachingDirectoryTestCase,
  ::testing::Values(
    &tests::directory<
      &tests::MakePhysicalDirectory<irs::fs_directory, MaxSizeAcceptor<10>>>,
    &tests::directory<
      &tests::MakePhysicalDirectory<irs::mmap_directory, MaxSizeAcceptor<10>>>),
  CachingDirectoryTestCase::to_string);

}  // namespace tests
