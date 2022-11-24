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

#include "shared.hpp"
#include "store/directory.hpp"

namespace iresearch {

template<typename Value, typename Acceptor>
class CachingHelper {
 public:
  explicit CachingHelper(const Acceptor& acceptor) : acceptor_{acceptor} {}

  template<typename Visitor>
  bool Visit(std::string_view key, Visitor&& visitor) const noexcept {
    const size_t key_hash = cache_.hash_function()(key);

    {
      std::shared_lock lock{mutex_};
      if (auto it = cache_.find(key, key_hash); it != cache_.end()) {
        return visitor(it->second);
      }
    }

    return false;
  }

  template<typename Constructor>
  bool Put(std::string_view key, IOAdvice advice, Constructor&& ctor) {
    bool is_new = false;

    if (std::unique_lock lock{mutex_}; acceptor_(cache_.size(), key, advice)) {
      try {
        cache_.lazy_emplace(key, [&](const auto& map_ctor) {
          is_new = true;
          map_ctor(key, ctor());
        });
      } catch (...) {
      }
    }

    return is_new;
  }

  void Remove(std::string_view key) {
    std::lock_guard lock{mutex_};
    cache_.erase(key);
  }

  void Rename(std::string_view src, std::string_view dst) {
    const auto src_hash = cache_.hash_function()(src);

    std::lock_guard lock{mutex_};
    if (auto src_it = cache_.find(src, src_hash); src_it != cache_.end()) {
      auto tmp = std::move(src_it->second);
      cache_.erase(src_it);
      try {
        assert(!cache_.contains(dst));
        cache_[dst] = std::move(tmp);
      } catch (...) {
      }
    }
  }

  size_t Count() const {
    std::lock_guard lock{mutex_};
    return cache_.size();
  }

  const Acceptor& GetAcceptor() const noexcept { return acceptor_; }

 private:
  mutable std::shared_mutex mutex_;
  mutable absl::flat_hash_map<std::string, Value> cache_;
  IRS_NO_UNIQUE_ADDRESS Acceptor acceptor_;
};

template<typename Impl, typename Value, typename Acceptor>
class CachingDirectoryBase : public Impl {
 public:
  bool remove(std::string_view name) noexcept override {
    if (Impl::remove(name)) {
      cache_.Remove(name);
      return true;
    }
    return false;
  }

  bool rename(std::string_view src, std::string_view dst) noexcept override {
    if (Impl::rename(src, dst)) {
      cache_.Rename(src, dst);
      return true;
    }
    return false;
  }

  bool exists(bool& result, std::string_view name) const noexcept override {
    if (cache_.Visit(name, [&](auto&) noexcept {
          result = true;
          return true;
        })) {
      return true;
    }

    return Impl::exists(result, name);
  }

  size_t Count() const noexcept { return cache_.Count(); }

  const Acceptor& GetAcceptor() const noexcept { return cache_.GetAcceptor(); }

 protected:
  template<typename... Args>
  explicit CachingDirectoryBase(const Acceptor& acceptor, Args&&... args)
    : Impl{std::forward<Args>(args)...}, cache_{acceptor} {}

  mutable CachingHelper<Value, Acceptor> cache_;
};

template<typename Impl, typename Acceptor>
class CachingDirectory
  : public CachingDirectoryBase<Impl, index_input::ptr, Acceptor> {
 public:
  template<typename... Args>
  explicit CachingDirectory(Args&&... args)
    : CachingDirectoryBase<Impl, index_input::ptr, Acceptor>{
        std::forward<Args>(args)...} {}

  bool length(uint64_t& result, std::string_view name) const noexcept override {
    if (this->cache_.Visit(name, [&](auto& stream) noexcept {
          if (stream) {
            result = stream->length();
            return true;
          }
          return true;
        })) {
      return true;
    }

    return Impl::length(result, name);
  }

  index_input::ptr open(std::string_view name,
                        IOAdvice advice) const noexcept override {
    index_input::ptr stream;

    if (this->cache_.Visit(name, [&](const auto& cached) noexcept {
          stream = cached->reopen();
          return stream != nullptr;
        })) {
      return stream;
    }

    stream = Impl::open(name, advice);

    if (!stream) {
      return nullptr;
    }

    this->cache_.Put(name, advice, [&]() { return stream->reopen(); });

    return stream;
  }
};

}  // namespace iresearch
