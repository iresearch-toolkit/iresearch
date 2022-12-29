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

#include "directory_reader.hpp"

#include "index/directory_reader_impl.hpp"
#include "index/segment_reader.hpp"
#include "utils/hash_utils.hpp"
#include "utils/singleton.hpp"
#include "utils/type_limits.hpp"

#include <absl/container/flat_hash_map.h>

namespace irs {

DirectoryReader::DirectoryReader(
  std::shared_ptr<const DirectoryReaderImpl>&& impl) noexcept
  : impl_{std::move(impl)} {}

DirectoryReader::DirectoryReader(const DirectoryReader& other) noexcept
  : impl_{std::atomic_load(&other.impl_)} {}

DirectoryReader& DirectoryReader::operator=(
  const DirectoryReader& other) noexcept {
  if (this != &other) {
    // make a copy
    auto impl = std::atomic_load(&other.impl_);

    std::atomic_store(&impl_, impl);
  }

  return *this;
}

const SubReader& DirectoryReader::operator[](size_t i) const {
  return (*impl_)[i];
}

uint64_t DirectoryReader::docs_count() const { return impl_->docs_count(); }

uint64_t DirectoryReader::live_docs_count() const {
  return impl_->live_docs_count();
}

size_t DirectoryReader::size() const { return impl_->size(); }

DirectoryReader::operator IndexReader::ptr() const noexcept { return impl_; }

const DirectoryMeta& DirectoryReader::Meta() const {
  auto impl = std::atomic_load(&impl_);  // make a copy

  return impl->Meta();
}

/*static*/ DirectoryReader DirectoryReader::open(
  const directory& dir, format::ptr codec /*= nullptr*/,
  const IndexReaderOptions& opts /*= directory_reader_options()*/) {
  return DirectoryReader{
    DirectoryReaderImpl::Open(dir, opts, codec.get(), nullptr)};
}

DirectoryReader DirectoryReader::reopen(format::ptr codec /*= nullptr*/) const {
  // make a copy
  auto impl = std::atomic_load(&impl_);

  return DirectoryReader{
    DirectoryReaderImpl::Open(impl->Dir(), impl->Options(), codec.get(), impl)};
}

}  // namespace irs
