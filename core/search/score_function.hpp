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
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_SCORE_FUNCTION_H
#define IRESEARCH_SCORE_FUNCTION_H

#include "utils/memory.hpp"
#include "utils/noncopyable.hpp"

namespace iresearch {

// Stateful object used for computing the document score based on the
// stored state.
struct score_ctx {
  score_ctx() = default;
  score_ctx(score_ctx&&) = default;
  score_ctx& operator=(score_ctx&&) = default;
  virtual ~score_ctx() = default;
};

using score_f = void (*)(score_ctx* ctx, score_t* res);

// Convenient wrapper around score_f and score_ctx.
class ScoreFunction : util::noncopyable {
 public:
  // Default implementation sets result to 0.
  static const score_f kDefault;

  // Returns default scoring function setting `size` score buckets to 0.
  static ScoreFunction Default(size_t size) noexcept {
    // FIXME(gnusi): use std::bit_cast when avaibale
    return {reinterpret_cast<score_ctx*>(sizeof(score_t) * size), kDefault};
  }

  // Returns scoring function setting `size` score buckets to `value`.
  static ScoreFunction Constant(score_t value, uint32_t size) noexcept;

  // Returns scoring function setting a single score bucket to `value`.
  static ScoreFunction Constant(score_t value) noexcept;

  // Returns invalid scoring function.
  static ScoreFunction Invalid() noexcept { return {nullptr, nullptr}; }

  ScoreFunction() noexcept;
  ScoreFunction(memory::managed_ptr<score_ctx>&& ctx,
                const score_f func) noexcept
    : ctx_(std::move(ctx)), func_(func) {}
  ScoreFunction(std::unique_ptr<score_ctx>&& ctx, const score_f func) noexcept
    : ScoreFunction(memory::to_managed<score_ctx>(std::move(ctx)), func) {}
  ScoreFunction(score_ctx* ctx, const score_f func) noexcept
    : ScoreFunction(memory::to_managed<score_ctx, false>(std::move(ctx)),
                    func) {}
  ScoreFunction(ScoreFunction&& rhs) noexcept;
  ScoreFunction& operator=(ScoreFunction&& rhs) noexcept;

  [[nodiscard]] score_ctx* Ctx() const noexcept { return ctx_.get(); }
  [[nodiscard]] score_f Func() const noexcept { return func_; }

  void Reset(memory::managed_ptr<score_ctx>&& ctx,
             const score_f func) noexcept {
    ctx_ = std::move(ctx);
    func_ = func;
  }

  void Reset(std::unique_ptr<score_ctx>&& ctx, const score_f func) noexcept {
    ctx_ = memory::to_managed<score_ctx>(std::move(ctx));
    func_ = func;
  }

  void Reset(score_ctx* ctx, const score_f func) noexcept {
    ctx_ = memory::to_managed<score_ctx, false>(ctx);
    func_ = func;
  }

  bool IsNoop() const noexcept { return nullptr == ctx_ && kDefault == func_; }

  FORCE_INLINE void operator()(score_t* res) const noexcept {
    assert(func_);
    return func_(ctx_.get(), res);
  }

  bool operator==(std::nullptr_t) const noexcept {
    return !static_cast<bool>(*this);
  }

  bool operator!=(std::nullptr_t) const noexcept { return !(*this == nullptr); }

  bool operator==(const ScoreFunction& rhs) const noexcept {
    return ctx_ == rhs.ctx_ && func_ == rhs.func_;
  }

  bool operator!=(const ScoreFunction& rhs) const noexcept {
    return !(*this == rhs);
  }

  bool operator==(score_f rhs) const noexcept { return func_ == rhs; }

  bool operator!=(score_f rhs) const noexcept { return !(*this == rhs); }

  explicit operator bool() const noexcept { return nullptr != func_; }

 private:
  memory::managed_ptr<score_ctx> ctx_;
  score_f func_;
};

}  // namespace iresearch

#endif  // IRESEARCH_SCORE_FUNCTION_H
