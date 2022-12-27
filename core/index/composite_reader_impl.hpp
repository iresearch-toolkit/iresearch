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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "index_reader.hpp"
#include "shared.hpp"

namespace irs {

// Common implementation for readers composied of multiple other readers
// for use/inclusion into cpp files
template<typename Readers>
class CompositeReaderImpl : public index_reader {
 public:
  using ReadersType = Readers;

  using ReaderType = typename std::enable_if_t<
    std::is_base_of_v<index_reader, typename Readers::value_type>,
    typename Readers::value_type>;

  CompositeReaderImpl(ReadersType&& readers, uint64_t docs_count,
                      uint64_t docs_max) noexcept
    : readers_{std::move(readers)},
      docs_count_{docs_count},
      docs_max_{docs_max} {}

  // returns corresponding sub-reader
  const ReaderType& operator[](size_t i) const noexcept override {
    IRS_ASSERT(i < readers_.size());
    return *(readers_[i]);
  }

  // maximum number of documents
  uint64_t docs_count() const noexcept override { return docs_max_; }

  // number of live documents
  uint64_t live_docs_count() const noexcept override { return docs_count_; }

  // returns total number of opened writers
  size_t size() const noexcept override { return readers_.size(); }

 private:
  ReadersType readers_;
  uint64_t docs_count_;
  uint64_t docs_max_;
};  // composite_reader

}  // namespace irs
