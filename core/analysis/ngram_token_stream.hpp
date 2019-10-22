////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_NGRAM_TOKEN_STREAM_H
#define IRESEARCH_NGRAM_TOKEN_STREAM_H

#include "analyzers.hpp"
#include "token_attributes.hpp"

NS_ROOT
NS_BEGIN(analysis)

enum class InputType {
  Binary, // input is treaten as generic bytes 
  UTF8,   // input is treaten as ut8-encoded symbols
};

struct ngram_token_stream_options_t {
  ngram_token_stream_options_t() : min_gram(0), max_gram(0), preserve_original(true),
    stream_bytes_type(InputType::Binary) {}
  ngram_token_stream_options_t(size_t min, size_t max, bool original) : min_gram(min), max_gram(max),
    stream_bytes_type(InputType::Binary), preserve_original(original) {}
  ngram_token_stream_options_t(size_t min, size_t max, bool original, InputType stream_type,
    const irs::bytes_ref start, const irs::bytes_ref end)
    : start_marker(start), end_marker(end), min_gram(min), max_gram(max),
    stream_bytes_type(stream_type), preserve_original(original) {}

  irs::bstring start_marker; // marker of ngrams at the beginning of stream
  irs::bstring end_marker; // marker of ngrams at the end of strem
  size_t min_gram;
  size_t max_gram;
  InputType stream_bytes_type;
  bool preserve_original; // emit input data as a token
};

////////////////////////////////////////////////////////////////////////////////
/// @class ngram_token_stream
/// @brief produces ngram from a specified input in a range of 
//         [min_gram;max_gram]. Can optionally preserve the original input.
////////////////////////////////////////////////////////////////////////////////

template<InputType StreamType>
class ngram_token_stream: public analyzer, util::noncopyable {
 public:
  DECLARE_ANALYZER_TYPE();

  DECLARE_FACTORY(const ngram_token_stream_options_t& options);

  static void init(); // for trigering registration in a static build

  //ngram_token_stream(size_t n, bool preserve_original)
  //  : ngram_token_stream(n, n, preserve_original) {
 //}

  ngram_token_stream(const ngram_token_stream_options_t& options);

  virtual const attribute_view& attributes() const noexcept override {
    return attrs_;
  }

  virtual bool next() noexcept override;
  virtual bool reset(const string_ref& data) noexcept override;

  size_t min_gram() const noexcept { return options_.min_gram; }
  size_t max_gram() const noexcept { return options_.max_gram; }
  bool preserve_original() const noexcept { return options_.preserve_original; }

 private:
  inline bool next_symbol(const byte_type*& it) const noexcept;
  void emit_original() noexcept;

  class term_attribute final: public irs::term_attribute {
   public:
    void value(const bytes_ref& value) { value_ = value; }
  };

  ngram_token_stream_options_t options_;
  attribute_view attrs_;
  bytes_ref data_; // data to process
  increment inc_;
  offset offset_;
  term_attribute term_;
  const byte_type* begin_{};
  const byte_type* ngram_end_{};
  size_t length_{};
  const byte_type* data_end_{};

  enum class emit_original_t {
    None,
    WithoutMarkers,
    WithStartMarker,
    WithEndMarker
  };

  emit_original_t emit_original_{ emit_original_t::None };

  // buffer for emitting ngram with start/stop marker
  // we need continious memory for value so can not use
  // pointers to input memory block
  bstring marked_term_buffer_;

  // increment value for next token
  uint32_t next_inc_val_{ 0 };

  // Aux flags to speed up marker properties access;
  bool start_marker_empty_;
  bool end_marker_empty_;
}; // ngram_token_stream


NS_END
NS_END

#endif // IRESEARCH_NGRAM_TOKEN_STREAM_H
