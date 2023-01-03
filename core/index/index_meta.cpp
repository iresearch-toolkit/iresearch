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

#include "index_meta.hpp"

#include "formats/formats.hpp"
#include "shared.hpp"
#include "utils/type_limits.hpp"

namespace irs {

bool SegmentMeta::operator==(const SegmentMeta& other) const noexcept {
  return name == other.name && version == other.version &&
         docs_count == other.docs_count &&
         live_docs_count == other.live_docs_count && codec == other.codec &&
         size_in_bytes == other.size_in_bytes &&
         column_store == other.column_store && files == other.files &&
         sort == other.sort;
}

bool IndexMeta::operator==(const IndexMeta& other) const noexcept {
  if (gen_ != other.gen_ || last_gen_ != other.last_gen_ ||
      seg_counter_ != other.seg_counter_ ||
      segments_.size() != other.segments_.size() ||
      payload_ != other.payload_) {
    return false;
  }

  for (size_t i = 0, count = segments_.size(); i < count; ++i) {
    if (segments_[i] != other.segments_[i]) {
      return false;
    }
  }

  return true;
}

uint64_t IndexMeta::next_generation() const noexcept {
  return index_gen_limits::valid(gen_) ? (gen_ + 1) : 1;
}

}  // namespace irs
