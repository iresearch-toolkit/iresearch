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

#include "pyresearch.h"
#include "store/mmap_directory.hpp"
#include "index/directory_reader.hpp"

std::vector<std::string> field_reader::features() const {
  std::vector<std::string> result;
  for (const auto* type_id : field_->meta().features) {
    auto& type_name = type_id->name();
    result.emplace_back(type_name.c_str(), type_name.size())    ;
  }
  return result;
}

/*static*/ index_reader index_reader::open(const char* path) {
  auto dir = std::make_shared<irs::mmap_directory>(path);
  auto index = irs::directory_reader::open(*dir, irs::formats::get("1_0"));

  return std::shared_ptr<irs::index_reader>(
    index.get(),
    [dir, index](irs::index_reader*) { }
  );
}

segment_reader index_reader::segment(size_t i) const {
  auto begin = reader_->begin();
  std::advance(begin, i);

  return std::shared_ptr<irs::sub_reader>(
    &*begin,
    [](irs::sub_reader*){}
  );
}

bool column_values_reader::value(uint64_t key, std::basic_string<uint8_t>& out) {
  irs::bytes_ref value;

  if (reader_(key, value)) {
    out = value;
    return true;
  }

  return false;
}
