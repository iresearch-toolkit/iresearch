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
////////////////////////////////////////////////////////////////////////////////

#include "multi_delimited_token_stream.hpp"

#include <fst/union.h>
#include <fstext/determinize-star.h>
#include <velocypack/Iterator.h>

#include <string_view>

#include "utils/automaton_utils.hpp"
#include "utils/fstext/fst_draw.hpp"
#include "utils/vpack_utils.hpp"
#include "velocypack/Builder.h"
#include "velocypack/Parser.h"
#include "velocypack/Slice.h"
#include "velocypack/velocypack-aliases.h"

using namespace irs;
using namespace irs::analysis;

namespace {

template<typename Derived>
class multi_delimited_token_stream_single_chars_base
  : public multi_delimited_token_stream {
 public:
  bool next() override {
    while (true) {
      if (data_.begin() == data_.end()) {
        return false;
      }

      auto next = static_cast<Derived*>(this)->find_next_delim();

      if (next == data_.begin()) {
        // skip empty terms
        data_ = bytes_view{next + 1, data_.end()};
        continue;
      }

      auto& term = std::get<term_attribute>(attrs_);
      term.value = bytes_view{data_.begin(), next};

      if (next == data_.end()) {
        data_ = {};
      } else {
        data_ = bytes_view{next + 1, data_.end()};
      }

      return true;
    }
  }
};

template<std::size_t N>
class multi_delimited_token_stream_single_chars final
  : public multi_delimited_token_stream_single_chars_base<
      multi_delimited_token_stream_single_chars<N>> {
 public:
  explicit multi_delimited_token_stream_single_chars(
    const multi_delimited_token_stream::options& opts) {
    IRS_ASSERT(opts.delimiters.size() == N);
    std::size_t k = 0;
    for (const auto& delim : opts.delimiters) {
      IRS_ASSERT(delim.size() == 1);
      bytes_[k++] = delim[0];
    }
  }

  auto find_next_delim() {
    return std::search(this->data_.begin(), this->data_.end(), bytes_.begin(),
                       bytes_.end());
  }

  std::array<byte_type, N> bytes_;
};

template<>
class multi_delimited_token_stream_single_chars<1> final
  : public multi_delimited_token_stream_single_chars_base<
      multi_delimited_token_stream_single_chars<1>> {
 public:
  explicit multi_delimited_token_stream_single_chars(
    const multi_delimited_token_stream::options& opts) {
    IRS_ASSERT(opts.delimiters.size() == 1);
    IRS_ASSERT(opts.delimiters[0].size() == 1);
    delim_ = opts.delimiters[0][0];
  }

  auto find_next_delim() {
    if (auto pos = this->data_.find(delim_); pos != bstring::npos) {
      return this->data_.begin() + pos;
    }
    return this->data_.end();
  }

  byte_type delim_;
};

template<>
class multi_delimited_token_stream_single_chars<0> final
  : public multi_delimited_token_stream_single_chars_base<
      multi_delimited_token_stream_single_chars<0>> {
 public:
  explicit multi_delimited_token_stream_single_chars(
    const multi_delimited_token_stream::options& opts) {
    IRS_ASSERT(opts.delimiters.size() == 0);
  }

  auto find_next_delim() { return this->data_.end(); }
};

class multi_delimited_token_stream_generic_single_chars final
  : public multi_delimited_token_stream_single_chars_base<
      multi_delimited_token_stream_generic_single_chars> {
 public:
  explicit multi_delimited_token_stream_generic_single_chars(
    const options& opts) {
    for (const auto& delim : opts.delimiters) {
      IRS_ASSERT(delim.size() == 1);
      bytes_[delim[0]] = true;
    }
  }

  auto find_next_delim() {
    return std::find_if(data_.begin(), data_.end(), [&](auto c) {
      if (c > CHAR_MAX) {
        return false;
      }
      IRS_ASSERT(c <= CHAR_MAX);
      return bytes_[c];
    });
  }
  // TODO maybe use a bitset instead?
  std::array<bool, CHAR_MAX + 1> bytes_;
};

struct TrieNode {
  explicit TrieNode(int32_t stateId, int32_t depth)
    : state_id(stateId), depth(depth) {}
  int32_t state_id;
  int32_t depth;
  bool is_leaf{false};
  std::array<TrieNode*, 256> simple_outgoing{};
  std::array<TrieNode*, 256> real_outgoing{};
};

bytes_view find_longest_prefix_that_is_suffix(bytes_view s, bytes_view str) {
  // TODO this algorithm is quadratic. Probably OK for small strings.
  for (std::size_t n = s.length() - 1; n > 0; n--) {
    auto prefix = s.substr(0, n);
    if (str.ends_with(prefix)) {
      return prefix;
    }
  }
  return {};
}

bytes_view find_longest_prefix_that_is_suffix(
  const std::vector<bstring>& strings, std::string_view str) {
  bytes_view result = {};
  for (const auto& s : strings) {
    auto other =
      find_longest_prefix_that_is_suffix(s, ViewCast<byte_type>(str));
    if (other.length() > result.length()) {
      result = other;
    }
  }
  return result;
}

void insert_error_conditions(const std::vector<bstring>& strings,
                             std::string& matched_word, TrieNode* node,
                             TrieNode* root) {
  if (node->is_leaf) {
    return;
  }

  for (size_t k = 0; k < 256; k++) {
    if (node->simple_outgoing[k] != nullptr) {
      node->real_outgoing[k] = node->simple_outgoing[k];
      matched_word.push_back(k);
      insert_error_conditions(strings, matched_word, node->simple_outgoing[k],
                              root);
      matched_word.pop_back();
    } else {
      // if we find a character c that we don't expect, we have to find
      // the longest prefix of `str` that is a suffix of the already matched
      // text including c. then go to that state.
      matched_word.push_back(k);
      auto prefix = find_longest_prefix_that_is_suffix(strings, matched_word);

      auto* dest = root;
      for (auto c : prefix) {
        dest = dest->simple_outgoing[c];
        IRS_ASSERT(dest != nullptr);
      }
      node->real_outgoing[k] = dest;
      matched_word.pop_back();
    }
  }
}

automaton make_string_trie(const std::vector<bstring>& strings) {
  std::vector<std::unique_ptr<TrieNode>> nodes;
  nodes.emplace_back(std::make_unique<TrieNode>(0, 0));

  for (const auto& str : strings) {
    TrieNode* current = nodes.front().get();

    for (size_t k = 0; k < str.length(); k++) {
      auto c = str[k];
      if (current->is_leaf) {
        break;
      }

      if (current->simple_outgoing[c] != nullptr) {
        current = current->simple_outgoing[c];
        continue;
      }

      auto& new_node =
        nodes.emplace_back(std::make_unique<TrieNode>(nodes.size(), k));
      current->simple_outgoing[c] = new_node.get();
      current = new_node.get();
    }

    current->is_leaf = true;
  }

  std::string matched_word;
  auto* root = nodes.front().get();
  insert_error_conditions(strings, matched_word, root, root);

  automaton a;
  a.AddStates(nodes.size());
  a.SetStart(0);

  for (auto& n : nodes) {
    size_t last_state = -1;
    size_t last_char = 0;

    if (n->is_leaf) {
      a.SetFinal(n->state_id, {1, static_cast<byte_type>(n->depth)});
      continue;
    }

    for (size_t k = 0; k < 256; k++) {
      size_t next_state = n->real_outgoing[k]->state_id;
      if (last_state == -1) {
        last_state = next_state;
        last_char = k;
      } else if (last_state != next_state) {
        a.EmplaceArc(n->state_id, range_label::fromRange(last_char, k - 1),
                     last_state);
        last_state = next_state;
        last_char = k;
      }
    }

    a.EmplaceArc(n->state_id, range_label::fromRange(last_char, 255),
                 last_state);
  }

  return a;
}

class multi_delimited_token_stream_generic final
  : public multi_delimited_token_stream {
 public:
  explicit multi_delimited_token_stream_generic(options&& opts)
    : automaton_(make_string_trie(opts.delimiters)),
      matcher_(make_automaton_matcher(automaton_)) {
    // fst::drawFst(nfa, std::cout);

#ifdef IRESEARCH_DEBUG
    // ensure nfa is sorted
    static constexpr auto EXPECTED_NFA_PROPERTIES =
      fst::kILabelSorted | fst::kOLabelSorted | fst::kAcceptor |
      fst::kUnweighted;

    IRS_ASSERT(EXPECTED_NFA_PROPERTIES ==
               automaton_.Properties(EXPECTED_NFA_PROPERTIES, true));
#endif
  }

  auto find_next_delim() {
    auto state = matcher_.GetFst().Start();
    matcher_.SetState(state);
    for (size_t k = 0; k < data_.length(); k++) {
      matcher_.Find(data_[k]);

      state = matcher_.Value().nextstate;

      if (matcher_.Final(state)) {
        auto length = matcher_.Final(state).Payload();
        IRS_ASSERT(length <= k);

        return std::make_pair(data_.begin() + (k - length),
                              static_cast<size_t>(length + 1));
      }

      matcher_.SetState(state);
    }

    return std::make_pair(data_.end(), size_t{0});
  }

  bool next() override {
    while (true) {
      if (data_.begin() == data_.end()) {
        return false;
      }

      auto [next, skip] = find_next_delim();

      if (next == data_.begin()) {
        // skip empty terms
        data_ = bytes_view{next + skip, data_.end()};
        continue;
      }

      auto& term = std::get<term_attribute>(attrs_);
      term.value = bytes_view{data_.begin(), next};

      if (next == data_.end()) {
        data_ = {};
      } else {
        data_ = bytes_view{next + skip, data_.end()};
      }

      return true;
    }
  }

  automaton automaton_;
  automaton_table_matcher matcher_;
};

class multi_delimited_token_stream_single final
  : public multi_delimited_token_stream {
 public:
  explicit multi_delimited_token_stream_single(options&& opts)
    : delim_(std::move(opts.delimiters[0])),
      searcher_(delim_.begin(), delim_.end()) {}

  bool next() override {
    while (true) {
      if (data_.begin() == data_.end()) {
        return false;
      }

      auto next = std::search(data_.begin(), data_.end(), searcher_);
      if (next == data_.begin()) {
        // skip empty terms
        data_ = bytes_view{next + delim_.size(), data_.end()};
        continue;
      }

      auto& term = std::get<term_attribute>(attrs_);
      term.value = bytes_view{data_.begin(), next};

      if (next == data_.end()) {
        data_ = {};
      } else {
        data_ = bytes_view{next + delim_.size(), data_.end()};
      }

      return true;
    }
  }

  bstring delim_;
  std::boyer_moore_searcher<bstring::iterator> searcher_;
};

template<std::size_t N>
irs::analysis::analyzer::ptr make_single_char(
  multi_delimited_token_stream::options&& opts) {
  if constexpr (N >= 4) {
    return std::make_unique<multi_delimited_token_stream_generic_single_chars>(
      std::move(opts));
  } else if (opts.delimiters.size() == N) {
    return std::make_unique<multi_delimited_token_stream_single_chars<N>>(
      std::move(opts));
  } else {
    return make_single_char<N + 1>(std::move(opts));
  }
}

irs::analysis::analyzer::ptr make(
  multi_delimited_token_stream::options&& opts) {
  const bool single_character_case =
    std::all_of(opts.delimiters.begin(), opts.delimiters.end(),
                [](const auto& delim) { return delim.size() == 1; });
  if (single_character_case) {
    return make_single_char<0>(std::move(opts));
  } else if (opts.delimiters.size() == 1) {
    return std::make_unique<multi_delimited_token_stream_single>(
      std::move(opts));
  } else {
    return std::make_unique<multi_delimited_token_stream_generic>(
      std::move(opts));
  }
}

constexpr std::string_view DELIMITER_PARAM_NAME{"delimiter"};

bool parse_vpack_options(VPackSlice slice,
                         multi_delimited_token_stream::options& options) {
  if (!slice.isObject()) {
    IRS_LOG_ERROR(
      "Slice for multi_delimited_token_stream is not an object or string");
    return false;
  }

  if (auto delim_array_slice = slice.get(DELIMITER_PARAM_NAME);
      !delim_array_slice.isNone()) {
    if (!delim_array_slice.isArray()) {
      IRS_LOG_WARN(
        absl::StrCat("Invalid type '", DELIMITER_PARAM_NAME,
                     "' (array expected) for multi_delimited_token_stream from "
                     "VPack arguments"));
      return false;
    }

    for (auto delim : VPackArrayIterator(delim_array_slice)) {
      if (!delim.isString()) {
        IRS_LOG_WARN(absl::StrCat(
          "Invalid type in '", DELIMITER_PARAM_NAME,
          "' (string expected) for multi_delimited_token_stream from "
          "VPack arguments"));
        return false;
      }
      auto view = ViewCast<byte_type>(delim.stringView());
      options.delimiters.emplace_back(view);
    }
  }

  return true;
}

bool make_vpack_config(const multi_delimited_token_stream::options& options,
                       VPackBuilder* vpack_builder) {
  VPackObjectBuilder object(vpack_builder);
  {
    VPackArrayBuilder array(vpack_builder, DELIMITER_PARAM_NAME);
    for (const auto& delim : options.delimiters) {
      auto view = ViewCast<char>(bytes_view{delim});
      vpack_builder->add(VPackValue(view));
    }
  }

  return true;
}

irs::analysis::analyzer::ptr make_vpack(VPackSlice slice) {
  multi_delimited_token_stream::options options;
  if (parse_vpack_options(slice, options)) {
    return irs::analysis::multi_delimited_token_stream::make(
      std::move(options));
  } else {
    return nullptr;
  }
}

irs::analysis::analyzer::ptr make_vpack(std::string_view args) {
  VPackSlice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return make_vpack(slice);
}

bool normalize_vpack_config(VPackSlice slice, VPackBuilder* vpack_builder) {
  multi_delimited_token_stream::options options;
  if (parse_vpack_options(slice, options)) {
    return make_vpack_config(options, vpack_builder);
  } else {
    return false;
  }
}

bool normalize_vpack_config(std::string_view args, std::string& definition) {
  VPackSlice slice(reinterpret_cast<const uint8_t*>(args.data()));
  VPackBuilder builder;
  bool res = normalize_vpack_config(slice, &builder);
  if (res) {
    definition.assign(builder.slice().startAs<char>(),
                      builder.slice().byteSize());
  }
  return res;
}

REGISTER_ANALYZER_VPACK(irs::analysis::multi_delimited_token_stream, make_vpack,
                        normalize_vpack_config);
/*
REGISTER_ANALYZER_JSON(irs::analysis::multi_delimited_token_stream, make_json,
                       normalize_json_config);
*/
}  // namespace

namespace irs {
namespace analysis {

void multi_delimited_token_stream::init() {
  REGISTER_ANALYZER_VPACK(multi_delimited_token_stream, make_vpack,
                          normalize_vpack_config);  // match registration above
  // REGISTER_ANALYZER_JSON(multi_delimited_token_stream, make_json,
  //                        normalize_json_config);  // match registration above
}

analyzer::ptr multi_delimited_token_stream::make(
  multi_delimited_token_stream::options&& opts) {
  return ::make(std::move(opts));
}

}  // namespace analysis
}  // namespace irs
