
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

class MaxSizeAcceptor {
 public:
  explicit constexpr MaxSizeAcceptor(size_t max_size = 1024) noexcept
    : max_size_{max_size} {}

  bool operator()(size_t size, std::string_view, IOAdvice) const noexcept {
    return size < max_size_;
  }

  size_t MaxSize() const noexcept { return max_size_; }

 private:
  size_t max_size_;
};

template<typename Impl, typename Acceptor = MaxSizeAcceptor>
class CachingDirectory : public Impl, private Acceptor {
 public:
  template<typename... Args>
  explicit CachingDirectory(const Acceptor& acceptor, Args&&... args)
    : Impl{std::forward<Args>(args)...}, Acceptor{acceptor} {}

  // FIXME(gnusi): cache existence in "create"?

  bool exists(bool& result, std::string_view name) const noexcept override {
    if (std::shared_lock lock{mutex_}; cache_.contains(name)) {
      result = true;
      return true;
    }

    return Impl::exists(result, name);
  }

  bool length(uint64_t& result, std::string_view name) const noexcept override {
    {
      std::shared_lock lock{mutex_};
      if (auto it = cache_.find(name); it != cache_.end()) {
        result = it->second->length();
        return true;
      }
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
      std::lock_guard lock{mutex_};
      if (auto src_it = cache_.find(src); src_it != cache_.end()) {
        auto tmp = std::move(src_it->second);
        cache_.erase(src_it);
        try {
          cache_[dst] = std::move(tmp);
        } catch (...) {
        }
      }
      return true;
    }
    return true;
  }

  index_input::ptr open(std::string_view name,
                        IOAdvice advice) const noexcept override {
    {
      std::shared_lock lock{mutex_};
      if (auto it = cache_.find(name); it != cache_.end()) {
        assert(it->second);
        return it->second->reopen();
      }
    }

    auto stream = Impl::open(name, advice);

    if (!stream) {
      return nullptr;
    }

    {
      std::lock_guard lock{mutex_};
      if (Acceptor::operator()(cache_.size(), name, advice)) {
        try {
          const auto [it, _] = cache_.try_emplace(name, std::move(stream));
          return it->second->reopen();
        } catch (...) {
        }
      }
    }

    return stream;
  }

  size_t size() const noexcept {
    std::shared_lock lock{mutex_};
    return cache_.size();
  }

  const Acceptor& GetAcceptor() const noexcept {
    return static_cast<const Acceptor&>(*this);
  }

 private:
  mutable std::shared_mutex mutex_;
  mutable absl::flat_hash_map<std::string, index_input::ptr> cache_;
};

}  // namespace iresearch
