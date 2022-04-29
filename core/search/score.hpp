////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_SCORE_H
#define IRESEARCH_SCORE_H

#include "sort.hpp"
#include "utils/attributes.hpp"

namespace iresearch {

// Represents a score related for the particular document
class score : public attribute {
 public:
  static const score kNoScore;
  static const score_f kDefaultScoreFunc;

  static constexpr string_ref type_name() noexcept {
    return "iresearch::score";
  }

  template<typename Provider>
  static const score& get(const Provider& attrs) {
    const auto* score = irs::get<irs::score>(attrs);
    return score ? *score : kNoScore;
  }

  // cppcheck-suppress shadowFunction
  score() noexcept;
  explicit score(const Order& ord);

  bool is_default() const noexcept {
    return reinterpret_cast<score_ctx*>(data()) == func_.ctx()
           && func_.func() == kDefaultScoreFunc;
  }

  [[nodiscard]] FORCE_INLINE const float_t* evaluate() const {
    assert(func_);
    return func_();
  }

  // Reset score to default value
  void reset() noexcept;

  void reset(const score& score) noexcept {
    assert(score.func_);
    func_.reset(const_cast<score_ctx*>(score.func_.ctx()),
                score.func_.func());
  }

  void reset(std::unique_ptr<score_ctx>&& ctx, const score_f func) noexcept {
    assert(func);
    func_.reset(std::move(ctx), func);
  }

  void reset(score_ctx* ctx, const score_f func) noexcept {
    assert(func);
    func_.reset(ctx, func);
  }

  void reset(score_function&& func) noexcept {
    assert(func);
    func_ = std::move(func);
  }

  void resize(const Order& order) {
    buf_.resize(order.score_size);
  }

  score_t* data() const noexcept {
    return reinterpret_cast<score_t*>(buf_.data());
  }

  size_t size() const noexcept {
    return buf_.size();
  }

  void clear() noexcept {
    assert(!buf_.empty());
    std::memset(const_cast<byte_type*>(buf_.data()), 0, buf_.size());
  }

 private:
  mutable bstring buf_;
  score_function func_;
};

void reset(irs::score& score, Scorers&& scorers);

} // ROOT

#endif // IRESEARCH_SCORE_H

