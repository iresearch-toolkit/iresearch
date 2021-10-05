////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_ITERATORS_H
#define IRESEARCH_ITERATORS_H

#include "shared.hpp"
#include "utils/attributes.hpp"
#include "utils/attribute_provider.hpp"
#include "utils/iterator.hpp"
#include "utils/memory.hpp"
#include "index/index_features.hpp"

namespace iresearch {

//////////////////////////////////////////////////////////////////////////////
/// @struct doc_iterator
/// @brief an iterator providing sequential and random access to a posting list
///
/// @note After creation iterator is in uninitialized state:
///   - `value()` returns `type_limits<type_t>::invalid()` or
///     `type_limits<type_t>::eof()`
/// @note `seek()` to:
///   - `type_limits<type_t>::invalid()` is undefined
///      and implementation dependent
///   - `type_limits<type_t>::eof()` must always return
///     `type_limits<type_t>::eof()`
/// @note Once iterator is exhausted:
///   - `next()` must constantly return `false`
///   - `seek()` to any value must return `type_limits<type_t>::eof()`
///   - `value()` must return `type_limits<type_t>::eof()`
///
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API doc_iterator
    : iterator<doc_id_t>,
      attribute_provider {
  using ptr = memory::managed_ptr<doc_iterator>;

  ////////////////////////////////////////////////////////////////////////////
  /// @return an empty iterator
  ////////////////////////////////////////////////////////////////////////////
  static doc_iterator::ptr empty();

  //////////////////////////////////////////////////////////////////////////////
  /// @brief position iterator at a specified target and returns current value
  /// (for more information see class description)
  //////////////////////////////////////////////////////////////////////////////
  virtual doc_id_t seek(doc_id_t target) = 0;
}; // doc_iterator

//////////////////////////////////////////////////////////////////////////////
/// @struct resettable_doc_iterator
/// @brief same as `doc_iterator` but also support `reset()` operation
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API resettable_doc_iterator : doc_iterator {
  ////////////////////////////////////////////////////////////////////////////
  /// @brief reset iterator to initial state
  ////////////////////////////////////////////////////////////////////////////
  virtual void reset() = 0;
}; // resettable_doc_iterator

struct term_reader;

//////////////////////////////////////////////////////////////////////////////
/// @struct field_iterator
/// @brief an iterator providing sequential and random access to indexed fields
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API field_iterator : iterator<const term_reader&> {
  using ptr = memory::managed_ptr<field_iterator>;

  ////////////////////////////////////////////////////////////////////////////
  /// @return an empty iterator
  ////////////////////////////////////////////////////////////////////////////
  static field_iterator::ptr empty();

  //////////////////////////////////////////////////////////////////////////////
  /// @brief position iterator at a specified target
  /// @return if the target is found, false otherwise
  //////////////////////////////////////////////////////////////////////////////
  virtual bool seek(const string_ref& target) = 0;
}; // field_iterator

struct column_meta;

//////////////////////////////////////////////////////////////////////////////
/// @struct column_iterator
/// @brief an iterator providing sequential and random access to stored columns
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API column_iterator : iterator<const column_meta&> {
  using ptr = memory::managed_ptr<column_iterator>;

  ////////////////////////////////////////////////////////////////////////////
  /// @return an empty iterator
  ////////////////////////////////////////////////////////////////////////////
  static column_iterator::ptr empty();

  //////////////////////////////////////////////////////////////////////////////
  /// @brief position iterator at a specified target
  /// @return if the target is found, false otherwise
  //////////////////////////////////////////////////////////////////////////////
  virtual bool seek(const string_ref& name) = 0;
}; // column_iterator

//////////////////////////////////////////////////////////////////////////////
/// @struct term_iterator
/// @brief an iterator providing sequential access to term dictionary
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API term_iterator
    : iterator<const bytes_ref&>,
      public attribute_provider {
  using ptr = memory::managed_ptr<term_iterator>;

  ////////////////////////////////////////////////////////////////////////////
  /// @return an empty iterator
  ////////////////////////////////////////////////////////////////////////////
  static term_iterator::ptr empty();

  ////////////////////////////////////////////////////////////////////////////
  /// @brief read term attributes
  ////////////////////////////////////////////////////////////////////////////
  virtual void read() = 0;

  ////////////////////////////////////////////////////////////////////////////
  /// @param features requested index features
  /// @return iterator over the associated posting list
  ////////////////////////////////////////////////////////////////////////////
  virtual doc_iterator::ptr postings(IndexFeatures features) const = 0;
}; // term_iterator

//////////////////////////////////////////////////////////////////////////////
/// @enum SeekResult
/// @brief represents a result of seek operation
//////////////////////////////////////////////////////////////////////////////
enum class SeekResult {
  ////////////////////////////////////////////////////////////////////////////
  /// @brief exact value is found
  ////////////////////////////////////////////////////////////////////////////
  FOUND = 0,

  ////////////////////////////////////////////////////////////////////////////
  /// @brief exact value is not found, an iterator is positioned at the next
  ///        greatest value
  ////////////////////////////////////////////////////////////////////////////
  NOT_FOUND,

  ////////////////////////////////////////////////////////////////////////////
  /// @brief no value greater than a target found, eof
  ////////////////////////////////////////////////////////////////////////////
  END
}; // SeekResult

//////////////////////////////////////////////////////////////////////////////
/// @struct seek_cookie
/// @brief implementation defined term value state
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API seek_cookie : attribute_provider {
  using ptr = std::unique_ptr<seek_cookie>;
}; // seek_cookie

//////////////////////////////////////////////////////////////////////////////
/// @struct seek_term_iterator
/// @brief an iterator providing random and sequential access to term
///        dictionary
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API seek_term_iterator : term_iterator {
  using ptr = memory::managed_ptr<seek_term_iterator>;
  using cookie_ptr = seek_cookie::ptr;

  ////////////////////////////////////////////////////////////////////////////
  /// @return an empty iterator
  ////////////////////////////////////////////////////////////////////////////
  static seek_term_iterator::ptr empty();

  ////////////////////////////////////////////////////////////////////////////
  /// @brief position iterator at a value that is not less than the specified
  ///        one
  /// @return seek result
  ////////////////////////////////////////////////////////////////////////////
  virtual SeekResult seek_ge(const bytes_ref& value) = 0;

  ////////////////////////////////////////////////////////////////////////////
  /// @brief position iterator at a value that is not less than the specified
  ///        one
  /// @return `true` on success, `false` otherwise
  ////////////////////////////////////////////////////////////////////////////
  virtual bool seek(const bytes_ref& value) = 0;

  ////////////////////////////////////////////////////////////////////////////
  /// @brief position iterator at a value denoted by a specified `cookie`
  /// @return `true` on success, `false` otherwise
  ////////////////////////////////////////////////////////////////////////////
  virtual bool seek(
    const bytes_ref& term,
    const seek_cookie& cookie) = 0;

  ////////////////////////////////////////////////////////////////////////////
  /// @return seek cookie of the current term value
  ////////////////////////////////////////////////////////////////////////////
  virtual seek_cookie::ptr cookie() const = 0;
}; // seek_term_iterator

//////////////////////////////////////////////////////////////////////////////
/// @brief jumps iterator to the specified target and returns current value
/// of the iterator
/// @returns `false` if iterator exhausted, `true` otherwise
//////////////////////////////////////////////////////////////////////////////
template<typename Iterator, typename T, typename Less = std::less<T>>
bool seek(Iterator& it, const T& target, Less less = Less()) {
  bool next = true;
  while (less(it.value(), target) && true == (next = it.next()));
  return next;
}

//////////////////////////////////////////////////////////////////////////////
/// @brief position iterator to the specified min term or to the next term
///        after the min term depending on the specified `Include` value
/// @returns true in case if iterator has been succesfully positioned,
///          false otherwise
//////////////////////////////////////////////////////////////////////////////
template<bool Include>
inline bool seek_min(seek_term_iterator& it, const bytes_ref& min) {
  const auto res = it.seek_ge(min);

  return SeekResult::END != res
      && (Include || SeekResult::FOUND != res || it.next());
}

//////////////////////////////////////////////////////////////////////////////
/// @brief position iterator `count` items after the current position
/// @return if the iterator has been succesfully positioned
//////////////////////////////////////////////////////////////////////////////
template<typename Iterator>
inline bool skip(Iterator& itr, size_t count) {
  while (count--) {
    if (!itr.next()) {
      return false;
    }
  }

  return true;
}

}

#endif
