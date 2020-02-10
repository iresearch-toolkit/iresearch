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

#ifndef IRESEARCH_PHRASE_FILTER_H
#define IRESEARCH_PHRASE_FILTER_H

#include <map>

#include "filter.hpp"
#include "levenshtein_filter.hpp"
#include "utils/string.hpp"

NS_ROOT

//////////////////////////////////////////////////////////////////////////////
/// @class by_phrase
/// @brief user-side phrase filter
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API by_phrase : public filter {
 public:
  struct info_t {
    enum class Type {
      TERM, PREFIX, WILDCARD, LEVENSHTEIN
    } type;

    struct simple_term {
      bool operator==(const simple_term& /*other*/) const noexcept {
        return true;
      }
    };

    struct general_term {
      bool operator==(const general_term& other) const noexcept {
        return scored_terms_limit == other.scored_terms_limit;
      }

      size_t scored_terms_limit{1024};
    };

    struct prefix_term : general_term {
    };

    struct wildcard_term : general_term {
    };

    struct levenstein_term : general_term {
      byte_type max_distance;
      by_edit_distance::pdp_f provider;
      bool with_transpositions;
    };

    union {
      simple_term st;
      prefix_term pt;
      wildcard_term wt;
      levenstein_term lt;
    };

    info_t();
    info_t(const info_t& other);
    info_t(info_t&& other) noexcept;
    info_t(const simple_term& st);
    info_t(simple_term&& st) noexcept;
    info_t(const prefix_term& pt);
    info_t(prefix_term&& pt) noexcept;
    info_t(const wildcard_term& wt);
    info_t(wildcard_term&& wt) noexcept;
    info_t(const levenstein_term& lt);
    info_t(levenstein_term&& lt) noexcept;

    info_t& operator=(const info_t& other) noexcept;
    info_t& operator=(info_t&& other) noexcept;

    bool operator==(const info_t& other) const noexcept;
  };

  // positions and terms
  typedef std::pair<info_t, bstring> term_info_t;
  typedef std::map<size_t, term_info_t> terms_t;
  typedef terms_t::const_iterator const_iterator;
  typedef terms_t::iterator iterator;
  typedef terms_t::value_type term_t;

  // returns set of features required for filter
  static const flags& required();

  DECLARE_FILTER_TYPE();
  DECLARE_FACTORY();

  by_phrase();

  by_phrase& field(std::string fld) {
    fld_ = std::move(fld);
    return *this;
  }

  const std::string& field() const { return fld_; }

  // inserts term to the specified position
  template<typename T>
  by_phrase& insert(const T& t, size_t pos, const bytes_ref& term) {
    is_not_term_only |= !std::is_same<T, info_t::simple_term>::value; // constexpr
    phrase_[pos] = {info_t(t), term};
    return *this;
  }

  template<typename T>
  by_phrase& insert(const T& t, size_t pos, const string_ref& term) {
    return insert(t, pos, ref_cast<byte_type>(term));
  }

  template<typename T>
  by_phrase& insert(T&& t, size_t pos, bstring&& term) {
    is_not_term_only |= !std::is_same<T, info_t::simple_term>::value; // constexpr
    phrase_[pos] = {std::forward<T>(t), std::move(term)};
    return *this;
  }

  // inserts term to the end of the phrase with 
  // the specified offset from the last term
  template<typename T>
  by_phrase& push_back(const T& t, const bytes_ref& term, size_t offs = 0) {
    return insert(t, next_pos() + offs, term);
  }

  template<typename T>
  by_phrase& push_back(const T& t, const string_ref& term, size_t offs = 0) {
    return push_back(t, ref_cast<byte_type>(term), offs);
  }

  template<typename T>
  by_phrase& push_back(const T& t, bstring&& term, size_t offs = 0) {
    return insert(t, next_pos() + offs, std::move(term));
  }

  term_info_t& operator[](size_t pos) { return phrase_[pos]; }
  const term_info_t& operator[](size_t pos) const {
    return phrase_.at(pos); 
  }

  bool empty() const { return phrase_.empty(); }
  size_t size() const { return phrase_.size(); }

  const_iterator begin() const { return phrase_.begin(); }
  const_iterator end() const { return phrase_.end(); }

  iterator begin() { return phrase_.begin(); }
  iterator end() { return phrase_.end(); }

  using filter::prepare;

  virtual filter::prepared::ptr prepare(
    const index_reader& rdr,
    const order::prepared& ord,
    boost_t boost,
    const attribute_view& ctx
  ) const override;

  virtual size_t hash() const noexcept override;

 protected:
  virtual bool equals(const filter& rhs) const noexcept override;
 
 private:
  size_t next_pos() const {
    return phrase_.empty() ? 0 : 1 + phrase_.rbegin()->first;
  }
  
  size_t first_pos() const {
    return phrase_.empty() ? 0 : phrase_.begin()->first;
  }

  filter::prepared::ptr fixed_prepare_collect(
      const index_reader& rdr,
      const order::prepared& ord,
      boost_t boost,
      order::prepared::fixed_terms_collectors collectors) const;

  filter::prepared::ptr variadic_prepare_collect(
      const index_reader& rdr,
      const order::prepared& ord,
      boost_t boost,
      order::prepared::variadic_terms_collectors collectors) const;

  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  std::string fld_;
  terms_t phrase_;
  bool is_not_term_only = false;
  IRESEARCH_API_PRIVATE_VARIABLES_END
}; // by_phrase

NS_END // ROOT

#endif
