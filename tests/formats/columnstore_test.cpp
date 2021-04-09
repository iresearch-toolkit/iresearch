////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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

#include "tests_shared.hpp"
#include "tests_param.hpp"

#include "formats/columnstore.hpp"

class columnstore_test_case : public tests::directory_test_case_base { };

TEST_P(columnstore_test_case, test) {
  irs::columns::writer writer(false);
  irs::segment_meta meta("test", nullptr);

  writer.prepare(dir(), meta);

  auto [id, column] = writer.push_column({
    irs::type<irs::compression::none>::get(),
    {}, false });

  {
    auto& stream = column(1);
    stream.write_bytes(reinterpret_cast<const irs::byte_type*>("foo"), 3);

  }

}

INSTANTIATE_TEST_SUITE_P(
  columnstore_test,
  columnstore_test_case,
  ::testing::Values(
    &tests::memory_directory,
    &tests::fs_directory,
    &tests::mmap_directory),
  &columnstore_test_case::to_string
);
