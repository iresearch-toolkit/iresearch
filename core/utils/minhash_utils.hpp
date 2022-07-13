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

#include <absl/container/flat_hash_set.h>

#include <span>
#include <vector>

#include "utils/math_utils.hpp"

namespace iresearch {

// Implementation of MinHash variant with a single hash function.
class MinHash {
 public:
  // Returns number of hashes required to preserve probabilistic error
  // threshold.
  static constexpr size_t MaxSize(double_t err) noexcept {
    return err > 0. && err < 1.
               ? math::ceil64(1. / (err * err))
               : (err < 1. ? std::numeric_limits<size_t>::max() : 0);
  }

  // Returns expected probabilistic error according
  // the size of MinHash signature.
  static constexpr double_t Error(size_t size) noexcept {
    return size ? 1. / std::sqrt(size)
                : std::numeric_limits<double_t>::infinity();
  }

  explicit MinHash(size_t size)
      : max_size_{std::max(size, size_t{1})}, left_{max_size_} {
    min_hashes_.reserve(left_);

    // +1 because we insert a new hash
    // value before removing an old one.
    dedup_.reserve(left_ + 1);
  }

  // Update MinHash with the new value.
  // `noexcept` because we reserved enough space in constructor already.
  void Insert(size_t hash_value) {
    if (left_ && dedup_.emplace(hash_value).second) {
      min_hashes_.emplace_back(hash_value);
      if (0 == --left_) {
        std::make_heap(std::begin(min_hashes_), std::end(min_hashes_));
      }
    } else if (hash_value < min_hashes_.front() &&
               dedup_.emplace(hash_value).second) {
      std::pop_heap(std::begin(min_hashes_), std::end(min_hashes_));
      dedup_.erase(min_hashes_.back());
      min_hashes_.back() = hash_value;
      std::push_heap(std::begin(min_hashes_), std::end(min_hashes_));
    }
  }

  // Provides access to the accumulated MinHash signature.
  auto begin() const noexcept { return std::begin(min_hashes_); }
  auto end() const noexcept { return std::end(min_hashes_); }

  // Return `true` if MinHash signature is empty, false - otherwise.
  size_t Empty() const noexcept { return min_hashes_.empty(); }

  // Return actual size of accumulated MinHash signature.
  size_t Size() const noexcept { return dedup_.size(); }

  // Return the expected size of MinHash signature.
  size_t MaxSize() const noexcept { return max_size_; }

  // Return Jaccard coefficient of 2 MinHash signatures.
  // `rhs` members are meant to be unique.
  double Jaccard(std::span<const size_t> rhs) const noexcept {
    const size_t intersect =
        std::accumulate(std::begin(rhs), std::end(rhs), size_t{0},
                        [&](size_t acc, size_t hash_value) noexcept {
                          return acc + size_t{dedup_.contains(hash_value)};
                        });
    const size_t cardinality = Size() + rhs.size() - intersect;

    return cardinality ? static_cast<double_t>(intersect) / cardinality : 1.0;
  }

  // Return Jaccard coefficient of 2 MinHash signatures.
  double Jaccard(const MinHash& rhs) const noexcept {
    if (Size() > rhs.Size()) {
      return Jaccard(std::span<const size_t>{rhs.min_hashes_});
    } else {
      return rhs.Jaccard(std::span<const size_t>{min_hashes_});
    }
  }

  // Reset MinHash to the initial state.
  void Clear() noexcept {
    min_hashes_.clear();
    dedup_.clear();
    left_ = MaxSize();
  }

 private:
  std::vector<size_t> min_hashes_;
  absl::flat_hash_set<size_t> dedup_;  // guard against duplicated hash values
  size_t max_size_;
  size_t left_;
};

}  // namespace iresearch
