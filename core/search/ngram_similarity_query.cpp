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
/// @author Andrei Abramov
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#include "ngram_similarity_query.hpp"

#include "search/min_match_disjunction.hpp"
#include "search/ngram_similarity_filter.hpp"

namespace iresearch {
namespace {

using NGramApprox = min_match_disjunction<doc_iterator::ptr, NoopAggregator>;

class SerialPositionsChecker {
 public:
  template<typename Iterator>
  SerialPositionsChecker(Iterator begin, Iterator end, size_t total_terms_count,
                         size_t min_match_count = 1,
                         bool collect_all_states = false)
      : pos_(begin, end),
        min_match_count_{min_match_count},
        // avoid runtime conversion
        total_terms_count_{static_cast<score_t>(total_terms_count)},
        collect_all_states_{collect_all_states} {}

  bool Check(size_t potential, irs::doc_id_t doc);

  attribute* GetMutable(irs::type_info::type_id type) noexcept {
    if (type == irs::type<frequency>::id()) {
      return &seq_freq_;
    }

    if (type == irs::type<filter_boost>::id()) {
      return &filter_boost_;
    }

    return nullptr;
  }

 private:
  struct Position {
    template<typename Iterator>
    explicit Position(Iterator& itr) noexcept
        : pos(&position::get_mutable(itr)),
          doc(irs::get<document>(itr)),
          scr(&irs::score::get(itr)) {
      assert(pos);
      assert(doc);
      assert(scr);
    }

    position* pos;
    const document* doc;
    const score* scr;
  };

  struct SearchState {
    SearchState(size_t p, const score* s)
        : parent{nullptr}, scr{s}, pos{p}, len(1) {}
    SearchState(SearchState&&) = default;
    SearchState(const SearchState&) = default;
    SearchState& operator=(const SearchState&) = default;

    // appending constructor
    SearchState(std::shared_ptr<SearchState>& other, size_t p, const score* s)
        : parent{other}, scr{s}, pos{p}, len(other->len + 1) {}

    std::shared_ptr<SearchState> parent;
    const score* scr;
    size_t pos;
    size_t len;
  };

  using SearchStates =
      std::map<uint32_t, std::shared_ptr<SearchState>, std::greater<uint32_t>>;
  using PosTemp =
      std::vector<std::pair<uint32_t, std::shared_ptr<SearchState>>>;

  std::vector<Position> pos_;
  std::set<size_t> used_pos_;  // longest sequence positions overlaping detector
  std::vector<const score*> longest_sequence_;
  std::vector<size_t> pos_sequence_;
  size_t min_match_count_;
  SearchStates search_buf_;
  score_t total_terms_count_;
  filter_boost filter_boost_;
  frequency seq_freq_;
  bool collect_all_states_;
};

bool SerialPositionsChecker::Check(size_t potential, doc_id_t doc) {
  // how long max sequence could be in the best case
  search_buf_.clear();
  size_t longest_sequence_len = 0;

  seq_freq_.value = 0;
  for (const auto& pos_iterator : pos_) {
    if (pos_iterator.doc->value == doc) {
      position& pos = *(pos_iterator.pos);
      if (potential <= longest_sequence_len || potential < min_match_count_) {
        // this term could not start largest (or long enough) sequence.
        // skip it to first position to append to any existing candidates
        assert(!search_buf_.empty());
        pos.seek(search_buf_.rbegin()->first + 1);
      } else {
        pos.next();
      }
      if (!pos_limits::eof(pos.value())) {
        PosTemp swap_cache;
        auto last_found_pos = pos_limits::invalid();
        do {
          auto current_pos = pos.value();
          auto found = search_buf_.lower_bound(current_pos);
          if (found != search_buf_.end()) {
            if (last_found_pos != found->first) {
              last_found_pos = found->first;
              const auto* found_state = found->second.get();
              assert(found_state);
              auto current_sequence = found;
              // if we hit same position - set length to 0 to force checking
              // candidates to the left
              size_t current_found_len = (found->first == current_pos ||
                                          found_state->scr == pos_iterator.scr)
                                             ? 0
                                             : found_state->len + 1;
              auto initial_found = found;
              if (current_found_len > longest_sequence_len) {
                longest_sequence_len = current_found_len;
              } else {
                // maybe some previous candidates could produce better results.
                // lets go leftward and check if there are any candidates which
                // could became longer if we stick this ngram to them rather
                // than the closest one found
                for (++found; found != search_buf_.end(); ++found) {
                  found_state = found->second.get();
                  assert(found_state);
                  if (found_state->scr != pos_iterator.scr &&
                      found_state->len + 1 > current_found_len) {
                    // we have better option. Replace this match!
                    current_sequence = found;
                    current_found_len = found_state->len + 1;
                    if (current_found_len > longest_sequence_len) {
                      longest_sequence_len = current_found_len;
                      break;  // this match is the best - nothing to search
                              // further
                    }
                  }
                }
              }
              if (current_found_len) {
                auto new_candidate = std::make_shared<SearchState>(
                    current_sequence->second, current_pos, pos_iterator.scr);
                const auto res = search_buf_.try_emplace(
                    current_pos, std::move(new_candidate));
                if (!res.second) {
                  // pos already used. This could be if same ngram used several
                  // times. replace with new length through swap cache - to not
                  // spoil candidate for following positions of same ngram
                  swap_cache.emplace_back(current_pos,
                                          std::move(new_candidate));
                }
              } else if (initial_found->second->scr == pos_iterator.scr &&
                         potential > longest_sequence_len &&
                         potential >= min_match_count_) {
                // we just hit same iterator and found no better place to join,
                // so it will produce new candidate
                search_buf_.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(current_pos),
                    std::forward_as_tuple(std::make_shared<SearchState>(
                        current_pos, pos_iterator.scr)));
              }
            }
          } else if (potential > longest_sequence_len &&
                     potential >= min_match_count_) {
            // this ngram at this position  could potentially start a long
            // enough sequence so add it to candidate list
            search_buf_.emplace(
                std::piecewise_construct, std::forward_as_tuple(current_pos),
                std::forward_as_tuple(std::make_shared<SearchState>(
                    current_pos, pos_iterator.scr)));
            if (!longest_sequence_len) {
              longest_sequence_len = 1;
            }
          }
        } while (pos.next());
        for (auto& p : swap_cache) {
          auto res = search_buf_.find(p.first);
          assert(res != search_buf_.end());
          std::swap(res->second, p.second);
        }
      }
      --potential;  // we are done with this term.
                    // next will have potential one less as less matches left

      if (!potential) {
        break;  // all further terms will not add anything
      }

      if (longest_sequence_len + potential < min_match_count_) {
        break;  // all further terms will not let us build long enough sequence
      }

      // if we have no scoring - we could stop searh once we got enough matches
      if (longest_sequence_len >= min_match_count_ && !collect_all_states_) {
        break;
      }
    }
  }

  if (longest_sequence_len >= min_match_count_ && collect_all_states_) {
    uint32_t freq = 0;
    size_t count_longest{0};
    // try to optimize case with one longest candidate
    // performance profiling shows it is majority of cases
    for (auto i = search_buf_.begin(), end = search_buf_.end(); i != end; ++i) {
      if (i->second->len == longest_sequence_len) {
        ++count_longest;
        if (count_longest > 1) {
          break;
        }
      }
    }

    if (count_longest > 1) {
      longest_sequence_.clear();
      used_pos_.clear();
      longest_sequence_.reserve(longest_sequence_len);
      pos_sequence_.reserve(longest_sequence_len);
      for (auto i = search_buf_.begin(), end = search_buf_.end(); i != end;) {
        pos_sequence_.clear();
        const auto* state = i->second.get();
        assert(state && state->len <= longest_sequence_len);
        if (state->len == longest_sequence_len) {
          bool delete_candidate = false;
          // only first longest sequence will contribute to frequency
          if (longest_sequence_.empty()) {
            longest_sequence_.push_back(state->scr);
            pos_sequence_.push_back(state->pos);
            auto cur_parent = state->parent;
            while (cur_parent) {
              longest_sequence_.push_back(cur_parent->scr);
              pos_sequence_.push_back(cur_parent->pos);
              cur_parent = cur_parent->parent;
            }
          } else {
            if (used_pos_.find(state->pos) != used_pos_.end() ||
                state->scr != longest_sequence_[0]) {
              delete_candidate = true;
            } else {
              pos_sequence_.push_back(state->pos);
              auto cur_parent = state->parent;
              size_t j = 1;
              while (cur_parent) {
                assert(j < longest_sequence_.size());
                if (longest_sequence_[j] != cur_parent->scr ||
                    used_pos_.find(cur_parent->pos) != used_pos_.end()) {
                  delete_candidate = true;
                  break;
                }
                pos_sequence_.push_back(cur_parent->pos);
                cur_parent = cur_parent->parent;
                ++j;
              }
            }
          }
          if (!delete_candidate) {
            ++freq;
            used_pos_.insert(std::begin(pos_sequence_),
                             std::end(pos_sequence_));
          }
        }
        ++i;
      }
    } else {
      freq = 1;
    }
    seq_freq_.value = freq;
    assert(!pos_.empty());
    filter_boost_.value =
        static_cast<score_t>(longest_sequence_len) / total_terms_count_;
  }
  return longest_sequence_len >= min_match_count_;
}

// Adapter for min_match_disjunction with honor of terms orderings

class NGramSimilarityDocIterator final : public doc_iterator,
                                         private score_ctx {
 public:
  NGramSimilarityDocIterator(NGramApprox::doc_iterators_t&& itrs,
                             size_t total_terms_count, size_t min_match_count,
                             bool collect_all_states)
      : checker_{std::begin(itrs), std::end(itrs), total_terms_count,
                 min_match_count, collect_all_states},
        // we are not interested in disjunction`s // scoring
        approx_(std::move(itrs), min_match_count, NoopAggregator{}) {
    std::get<attribute_ptr<document>>(attrs_) =
        irs::get_mutable<document>(&approx_);

    // FIXME find a better estimation
    std::get<cost>(attrs_).reset([this]() { return cost::extract(approx_); });
  }

  NGramSimilarityDocIterator(NGramApprox::doc_iterators_t&& itrs,
                             const sub_reader& segment,
                             const term_reader& field, score_t boost,
                             const byte_type* stats, size_t total_terms_count,
                             size_t min_match_count = 1,
                             const Order& ord = Order::kUnordered)
      : NGramSimilarityDocIterator{std::move(itrs), total_terms_count,
                                   min_match_count, !ord.empty()} {
    if (!ord.empty()) {
      auto& score = std::get<irs::score>(attrs_);
      score = CompileScore(ord.buckets(), segment, field, stats, *this, boost);
    }
  }

  virtual attribute* get_mutable(type_info::type_id type) noexcept override {
    auto* attr = irs::get_mutable(attrs_, type);

    return attr ? attr : checker_.GetMutable(type);
  }

  virtual bool next() override {
    bool next = false;
    while ((next = approx_.next()) &&
           !checker_.Check(approx_.match_count(), value())) {
    }
    return next;
  }

  virtual doc_id_t value() const override {
    return std::get<attribute_ptr<document>>(attrs_).ptr->value;
  }

  virtual doc_id_t seek(doc_id_t target) override {
    auto* doc_ = std::get<attribute_ptr<document>>(attrs_).ptr;

    if (doc_->value >= target) {
      return doc_->value;
    }
    const auto doc = approx_.seek(target);

    if (doc_limits::eof(doc) ||
        checker_.Check(approx_.match_count(), doc_->value)) {
      return doc;
    }

    next();
    return doc_->value;
  }

 private:
  using attributes = std::tuple<attribute_ptr<document>, cost, score>;

  SerialPositionsChecker checker_;
  NGramApprox approx_;
  attributes attrs_;
};

NGramApprox::doc_iterators_t Execute(const NGramState& query_state,
                                     IndexFeatures required_features,
                                     IndexFeatures extra_features) {
  auto* field = query_state.field;

  if (!field ||
      required_features != (field->meta().index_features & required_features)) {
    return {};
  }

  required_features |= extra_features;

  NGramApprox::doc_iterators_t itrs;
  itrs.reserve(query_state.terms.size());

  for (auto& term_state : query_state.terms) {
    if (IRS_UNLIKELY(term_state == nullptr)) {
      continue;
    }

    if (auto docs = field->postings(*term_state, required_features); docs) {
      auto& it = itrs.emplace_back(std::move(docs));

      if (IRS_UNLIKELY(!it)) {
        itrs.pop_back();
      }
    }
  }

  return itrs;
}

}  // namespace

doc_iterator::ptr NGramSimilarityQuery::execute(
    const ExecutionContext& ctx) const {
  auto& ord = ctx.scorers;
  assert(1 != min_match_count_ || !ord.empty());

  auto& segment = ctx.segment;
  auto query_state = states_.find(segment);

  if (!query_state) {
    return doc_iterator::empty();
  }

  auto itrs = Execute(*query_state, kRequiredFeatures, ord.features());

  if (itrs.size() < min_match_count_) {
    return doc_iterator::empty();
  }

  return memory::make_managed<NGramSimilarityDocIterator>(
      std::move(itrs), segment, *query_state->field, boost(), stats_.c_str(),
      query_state->terms.size(), min_match_count_, ord);
}

doc_iterator::ptr NGramSimilarityQuery::ExecuteWithOffsets(
    const sub_reader& rdr) const {
  auto query_state = states_.find(rdr);

  if (!query_state) {
    return doc_iterator::empty();
  }

  auto itrs = Execute(*query_state, kRequiredFeatures | IndexFeatures::OFFS,
                      IndexFeatures::NONE);

  if (itrs.size() < min_match_count_) {
    return doc_iterator::empty();
  }

  return memory::make_managed<NGramSimilarityDocIterator>(
      std::move(itrs), rdr, *query_state->field, boost(), stats_.c_str(),
      query_state->terms.size(), min_match_count_, Order::kUnordered);
}

}  // namespace iresearch
