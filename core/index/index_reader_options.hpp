////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023 ArangoDB GmbH, Cologne, Germany
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

#pragma once

#include <function2/function2.hpp>
#include <functional>

namespace irs {

struct SegmentMeta;
struct field_reader;
struct column_reader;

using ColumnWarmupCallback =
  std::function<bool(const SegmentMeta& meta, const field_reader& fields,
                     const column_reader& column)>;

// should never throw as may be used in dtors
using MemoryAccountingFunc = fu2::function<bool(int64_t) noexcept>;

struct IndexReaderOptions {
  ColumnWarmupCallback warmup_columns{};
  MemoryAccountingFunc pinned_memory_accounting{};

  // Open inverted index
  bool index{true};

  // Open columnstore
  bool columnstore{true};

  // Read document mask
  bool doc_mask{true};
};

}  // namespace irs
