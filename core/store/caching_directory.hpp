
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

#pragma once

#include <absl/container/flat_hash_map.h>

#include <shared_mutex>

#include "store/directory.hpp"

namespace iresearch {

class MaxCountAcceptor {
 public:
  explicit constexpr MaxCountAcceptor(size_t max_count = 1024) noexcept
    : max_count_{max_count} {}

  bool operator()(size_t count, std::string_view, IOAdvice) const noexcept {
    return count < max_count_;
  }

  size_t MaxCount() const noexcept { return max_count_; }

 private:
  size_t max_count_;
};

template<typename Impl, typename Acceptor = MaxCountAcceptor>
class CachingDirectory : public Impl, private Acceptor {
 public:
  template<typename... Args>
  explicit CachingDirectory(const Acceptor& acceptor, Args&&... args)
    : Impl{std::forward<Args>(args)...}, Acceptor{acceptor} {}

  bool exists(bool& result, std::string_view name) const noexcept override {
    if (Find(name, [&](auto&) noexcept { result = true; })) {
      return true;
    }

    return Impl::exists(result, name);
  }

  bool length(uint64_t& result, std::string_view name) const noexcept override {
    if (Find(name, [&](auto& stream) noexcept { result = stream.length(); })) {
      return true;
    }

    return Impl::length(result, name);
  }

  bool remove(std::string_view name) noexcept override {
    if (Impl::remove(name)) {
      std::lock_guard lock{mutex_};
      cache_.erase(name);
      return true;
    }
    return false;
  }

  bool rename(std::string_view src, std::string_view dst) noexcept override {
    if (Impl::rename(src, dst)) {
      const auto src_hash = cache_.hash_function()(src);

      std::lock_guard lock{mutex_};
      if (auto src_it = cache_.find(src, src_hash); src_it != cache_.end()) {
        auto tmp = std::move(src_it->second);
        cache_.erase(src_it);
        try {
          cache_[dst] = std::move(tmp);
        } catch (...) {
        }
      }
      return true;
    }
    return false;
  }

  index_input::ptr open(std::string_view name,
                        IOAdvice advice) const noexcept override {
    index_input::ptr stream;

    if (Find(name, [&](auto& cached) noexcept { stream = cached.reopen(); }) &&
        stream) {
      return stream;
    }

    stream = Impl::open(name, advice);

    if (!stream) {
      return nullptr;
    }

    {
      std::unique_lock lock{mutex_};
      if (GetAcceptor()(cache_.size(), name, advice)) {
        try {
          const auto [it, is_new] = cache_.try_emplace(name, std::move(stream));
          if (!is_new) {
            return it->second->reopen();
          }
        } catch (...) {
        }
      }
    }

    return stream;
  }

  size_t Count() const noexcept {
    std::shared_lock lock{mutex_};
    return cache_.size();
  }

  const Acceptor& GetAcceptor() const noexcept {
    return static_cast<const Acceptor&>(*this);
  }

 private:
  template<typename Visitor>
  bool Find(std::string_view key, Visitor&& visitor) const noexcept {
    const size_t key_hash = cache_.hash_function()(key);

    {
      std::shared_lock lock{mutex_};
      if (auto it = cache_.find(key, key_hash); it != cache_.end()) {
        assert(it->second);
        visitor(*it->second);
        return true;
      }
    }

    return false;
  }

  mutable std::shared_mutex mutex_;
  mutable absl::flat_hash_map<std::string, index_input::ptr> cache_;
};

}  // namespace iresearch
