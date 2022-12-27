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

#pragma once

#include "index/index_writer.hpp"

namespace irs::index_utils {

// merge segment if:
//   {threshold} > segment_bytes / (all_segment_bytes / #segments)
struct consolidate_bytes {
  float threshold = 0;
};

ConsolidationPolicy consolidation_policy(const consolidate_bytes& options);

// merge segment if:
//   {threshold} >= (segment_bytes + sum_of_merge_candidate_segment_bytes) /
//   all_segment_bytes
struct consolidate_bytes_accum {
  float threshold = 0;
};

ConsolidationPolicy consolidation_policy(
  const consolidate_bytes_accum& options);

// merge first {threshold} segments
struct consolidate_count {
  size_t threshold = std::numeric_limits<size_t>::max();
};

ConsolidationPolicy consolidation_policy(const consolidate_count& options);

// merge segment if:
//   {threshold} >= segment_docs{valid} / (all_segment_docs{valid} / #segments)
struct consolidate_docs_live {
  float threshold = 0;
};

ConsolidationPolicy consolidation_policy(const consolidate_docs_live& options);

// merge segment if:
//   {threshold} > #segment_docs{valid} / (#segment_docs{valid} +
//   #segment_docs{removed})
struct consolidate_docs_fill {
  float threshold = 0;
};

ConsolidationPolicy consolidation_policy(const consolidate_docs_fill& options);

struct consolidate_tier {
  // minimum allowed number of segments to consolidate at once
  size_t min_segments = 1;
  // maximum allowed number of segments to consolidate at once
  size_t max_segments = 10;
  // maxinum allowed size of all consolidated segments
  size_t max_segments_bytes = size_t(5) * (1 << 30);
  // treat all smaller segments as equal for consolidation selection
  size_t floor_segment_bytes = size_t(2) * (1 << 20);
  // filter out candidates with score less than min_score
  double_t min_score = 0.;
};

ConsolidationPolicy consolidation_policy(const consolidate_tier& options);

void read_document_mask(document_mask& docs_mask, const directory& dir,
                        const SegmentMeta& meta);

// Writes segment_meta to the supplied directory
// updates index_meta::index_segment_t::filename to the segment filename
// updates segment_meta::size to the size of files written
void flush_index_segment(directory& dir, IndexSegment& segment);

}  // namespace irs::index_utils
