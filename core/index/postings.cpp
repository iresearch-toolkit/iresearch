//
// IResearch search engine 
// 
// Copyright © 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#include "utils/timer_utils.hpp"
#include "utils/type_limits.hpp"
#include "postings.hpp"

NS_LOCAL

const iresearch::posting DUMMY{};

NS_END

NS_ROOT

// -----------------------------------------------------------------------------
// --SECTION--                                           postings implementation
// -----------------------------------------------------------------------------

postings::postings(writer_t& writer):
  writer_(writer) {
}

postings::emplace_result postings::emplace(const bytes_ref& term) {
  REGISTER_TIMER_DETAILED();
  auto& parent = writer_.parent();
 
  // maximum number to bytes needed for storage of term length and data
  const auto max_term_len = term.size(); // + vencode_size(term.size());

  if (writer_t::container::block_type::SIZE < max_term_len) {
    // TODO: maybe move big terms it to a separate storage
    // reject terms that do not fit in a block
    return std::make_pair(map_.end(), false);
  }

  const auto slice_end = writer_.pool_offset() + max_term_len;
  const auto next_block_start = writer_.pool_offset() < parent.size()
                        ? writer_.position().block_offset() + writer_t::container::block_type::SIZE
                        : writer_t::container::block_type::SIZE * parent.count();

  // do not span slice over 2 blocks, start slice at the start of the next block
  if (slice_end > next_block_start) {
    writer_.seek(next_block_start);
  }

  assert(size() < type_limits<type_t::doc_id_t>::eof()); // not larger then the static flag
  const auto result = map_.emplace(make_hashed_ref(term, bytes_ref_hash_t()), DUMMY);

  // for new terms also write out their value
  if (result.second) {
    auto& key = const_cast<hashed_bytes_ref&>(result.first->first);

    // reuse hash but point ref at data in pool
    writer_.write(term.c_str(), term.size());
    key = hashed_bytes_ref(key.hash(), (writer_.position() - term.size()).buffer(), term.size());
  }

  return result;
}

NS_END