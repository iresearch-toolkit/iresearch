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

////////////////////////////////////////////////////////////////////////////////
/// @class ngram_token_stream
/// @brief produces ngram from a specified input in a range of 
//         [min_gram;max_gram]. Can optionally preserve the original input.
////////////////////////////////////////////////////////////////////////////////
class ngram_token_stream: public analyzer, util::noncopyable {
 public:
  

  struct options_t {
    enum class stream_bytes_t {
      BinaryStream, // input is treaten as generic bytes 
      Ut8Stream,    // input is treaten as ut8-encoded string
    };
    options_t() : min_gram(0), max_gram(0), preserve_original(true),
      stream_bytes_type(stream_bytes_t::BinaryStream) {}
    options_t(size_t min, size_t max, bool original) : min_gram(min), max_gram(max), 
      stream_bytes_type(stream_bytes_t::BinaryStream), preserve_original(original) {}
    options_t(size_t min, size_t max, bool original, stream_bytes_t stream_type, 
              const irs::bytes_ref start, const irs::bytes_ref end)
      : start_marker(start), end_marker(end), min_gram(min), max_gram(max), 
      stream_bytes_type(stream_type), preserve_original(original) {}

    irs::bstring start_marker; // marker of ngrams at the beginning of stream
    irs::bstring end_marker; // marker of ngrams at the end of strem
    size_t min_gram;
    size_t max_gram;
    stream_bytes_t stream_bytes_type;
    bool preserve_original; // emit input data as a token
  };

  DECLARE_ANALYZER_TYPE();

  DECLARE_FACTORY(const options_t& options);

  static void init(); // for trigering registration in a static build

  //ngram_token_stream(size_t n, bool preserve_original)
  //  : ngram_token_stream(n, n, preserve_original) {
 //}

  ngram_token_stream(const options_t& options);

  virtual const attribute_view& attributes() const noexcept override {
    return attrs_;
  }

  virtual bool next() noexcept override;
  virtual bool reset(const string_ref& data) noexcept override;

  size_t min_gram() const noexcept { return options_.min_gram; }
  size_t max_gram() const noexcept { return options_.max_gram; }
  bool preserve_original() const noexcept { return options_.preserve_original; }

 private:
  bool ngram_token_stream::next_symbol(const byte_type*& it) noexcept;
  void emit_original() noexcept;
  void emit_ngram() noexcept;

  class term_attribute final: public irs::term_attribute {
   public:
    void value(const bytes_ref& value) { value_ = value; }
  };

  struct ngram_word_state_t {
    const byte_type* ngram_word_start{};
    const byte_type* ngram_word_end{};
  };

  ngram_word_state_t ngram_word_state_;
  options_t options_;
  attribute_view attrs_;
  bytes_ref data_; // data to process
  increment inc_;
  offset offset_;
  term_attribute term_;
  const byte_type* begin_{};
  const byte_type* ngram_end_{};
  size_t length_{};

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

  // 
  uint32_t next_inc_val_{ 0 };

  // keep position for next ngram  - is used 
  // for emitting same ngram with different start/end markers
  bool keep_ngram_position_{ false };
}; // ngram_token_stream


NS_END
NS_END

#endif // IRESEARCH_NGRAM_TOKEN_STREAM_H
