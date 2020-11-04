////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_REGEX_SCANNER_H
#define IRESEARCH_REGEX_SCANNER_H

#include <cstring>
#include <cassert>
#include <functional>
#include <ostream>
#include <locale>

#include "error/error.hpp"

namespace iresearch {
namespace detail {

struct scanner_base {
  /// Token types returned from the scanner.
  enum class Token : unsigned {
    ANYCHAR,
    ORD_CHAR,
    OCT_NUM,
    HEX_NUM,
    BACKREF,
    SUBEXPR_BEGIN,
    SUBEXPR_NO_GROUP_BEGIN,
    SUBEXPR_NO_LOOKAHEAD_BEGIN, // neg if value_[0] == 'n'
    SUBEXPR_END,
    BRACKET_BEGIN,
    BRACKET_NEG_BEGIN,
    BRACKET_END,
    INTERVAL_BEGIN,
    INTERVAL_END,
    QUOTED_CLASS,
    CHAR_CLASS_NAME,
    COLLSYMBOL,
    EQUIV_CLASS_NAME,
    OPT,
    OR,
    CLOSURE0,
    CLOSURE1,
    LINE_BEGIN,
    LINE_END,
    WORD_BOUND, // neg if value_[0] == 'n'
    COMMA,
    DUP_COUNT,
    END_OF_FILE,
    BRACKET_DASH,
    UNKNOWN = -1u
  };

  enum class State {
    NORMAL,
    IN_BRACE,
    IN_BRACKET,
  };

  static constexpr std::pair<char, Token> TOKEN_TBL[9] = {
    {'^', Token::LINE_BEGIN},
    {'$', Token::LINE_END},
    {'.', Token::ANYCHAR},
    {'*', Token::CLOSURE0},
    {'+', Token::CLOSURE1},
    {'?', Token::OPT},
    {'|', Token::OR},
    {'\n', Token::OR}, // grep and egrep
    {'\0', Token::OR},
  };

  static constexpr std::pair<char, char> ECMA_ESCAPE_TBL[8] = {
    {'0', '\0'},
    {'b', '\b'},
    {'f', '\f'},
    {'n', '\n'},
    {'r', '\r'},
    {'t', '\t'},
    {'v', '\v'},
    {'\0', '\0'},
  };

  static constexpr char ECMA_SPEC_CHAR[] = "^$\\.*+?()[]{}|";

  static const char* find_escape(char c) {
    auto it = ECMA_ESCAPE_TBL;
    for (; it->first != '\0'; ++it) {
      if (it->first == c) {
        return &it->second;
      }
    }
    return nullptr;
  }
}; // scanner_base

}

enum class RegexErrorCode {
  ERROR_ESCAPE,
  ERROR_PARENTHESIS,
  ERROR_BRACKET,
  ERROR_BRACE,
  ERROR_BADBRACE,
  ERROR_CTYPE,
  ERROR_COLLATE
};

class regex_error final : public error_base {
 public:
  regex_error(RegexErrorCode code, std::string&& what)
    : what_(std::move(what)),
      regex_error_(code) {
  }

  virtual ErrorCode code() const noexcept override { return ErrorCode::illegal_argument; }
  virtual const char* what() const noexcept override { return what_.c_str(); }
  RegexErrorCode regex_error_code() const noexcept { return regex_error_; }

 private:
  std::string what_;
  RegexErrorCode regex_error_;
};

template<typename Char>
class scanner : public detail::scanner_base {
 public:
  using iterator_type = const Char*;
  using string_type = std::basic_string<Char>;
  using ctype_type = const std::ctype<Char>;

  scanner(iterator_type begin, iterator_type end, std::locale loc)
    : current_(begin), end_(end),
      ctype_(std::use_facet<ctype_type>(loc)) {
    next();
  }

  void next();

  Token token() const noexcept { return token_; }

  const string_type& value() const noexcept { return value_; }

#ifdef IRESEARCH_DEBUG
  std::ostream& print(std::ostream&);
#endif

 private:
  void scan_normal();

  void scan_in_bracket();

  void scan_in_brace();

  void eat_escape_ecma();

  void eat_class(char);

  State state_{State::NORMAL};
  Token token_;
  iterator_type current_;
  iterator_type end_;
  ctype_type& ctype_;
  string_type value_;
  bool at_bracket_start_{false};
}; // scanner

template<typename Char>
void scanner<Char>::next() {
  if (current_ == end_) {
    token_ = Token::END_OF_FILE;
    return;
  }

  switch (state_) {
    case State::NORMAL:
      scan_normal();
      break;
    case State::IN_BRACKET:
      scan_in_bracket();
      break;
    case State::IN_BRACE:
      scan_in_brace();
      break;
  }
}

// Differences between styles:
// 1) "\(", "\)", "\{" in basic. It's not escaping.
// 2) "(?:", "(?=", "(?!" in ECMAScript.
template<typename Char>
void scanner<Char>::scan_normal() {
  auto c = *current_++;

  if (std::strchr(ECMA_SPEC_CHAR, ctype_.narrow(c, ' ')) == nullptr) {
    token_ = Token::ORD_CHAR;
    value_.assign(1, c);
    return;
  }

  if (c == '\\') {
    if (current_ == end_) {
      throw regex_error(
        RegexErrorCode::ERROR_ESCAPE,
        "Unexpected end of regex when escaping.");
    }

    if (*current_ != '(' && *current_ != ')' && *current_ != '{') {
      eat_escape_ecma();
      return;
    }
    c = *current_++;
  }

  if (c == '(') {
    if (*current_ == '?') {
      if (++current_ == end_) {
        throw regex_error(
          RegexErrorCode::ERROR_PARENTHESIS,
          "Unexpected end of regex when in an open parenthesis.");
      }

      if (*current_ == ':') {
        ++current_;
        token_ = Token::SUBEXPR_NO_GROUP_BEGIN;
      } else if (*current_ == '=') {
        ++current_;
        token_ = Token::SUBEXPR_NO_LOOKAHEAD_BEGIN;
        value_.assign(1, 'p');
      } else if (*current_ == '!') {
        ++current_;
        token_ = Token::SUBEXPR_NO_LOOKAHEAD_BEGIN;
        value_.assign(1, 'n');
      } else {
        throw regex_error(
          RegexErrorCode::ERROR_PARENTHESIS,
          "Invalid special open parenthesis.");
      }
    } else if (_M_flags & regex_constants::nosubs) {
      token_ = Token::SUBEXPR_NO_GROUP_BEGIN;
    } else {
      token_ = Token::SUBEXPR_BEGIN;
    }
  } else if (c == ')') {
    token_ = Token::SUBEXPR_END;
  } else if (c == '[') {
    state_ = State::IN_BRACKET;
    at_bracket_start_ = true;
    if (current_ != end_ && *current_ == '^') {
      token_ = Token::BRACKET_NEG_BEGIN;
      ++current_;
    } else {
      token_ = Token::BRACKET_BEGIN;
    }
  } else if (c == '{') {
    state_ = State::IN_BRACE;
    token_ = Token::INTERVAL_BEGIN;
  } else if (c != ']' && c != '}') {
    auto __it = TOKEN_TBL;
    auto __narrowc = ctype_.narrow(c, '\0');
    for (; __it->first != '\0'; ++__it)
      if (__it->first == __narrowc) {
        token_ = __it->second;
        return;
      }
      assert(false);
  } else {
    token_ = Token::ORD_CHAR;
    value_.assign(1, c);
  }
}

// Differences between styles:
// 1) different semantics of "[]" and "[^]".
// 2) Escaping in bracket expr.
template<typename Char>
void scanner<Char>::scan_in_bracket() {
  if (current_ == end_) {
    throw regex_error(
      RegexErrorCode::ERROR_BRACKET,
      "Unexpected end of regex when in bracket expression.");
  }

  auto c = *current_++;

  if (c == '-') {
    token_ = Token::BRACKET_DASH;
  } else if (c == '[') {
    if (current_ == end_) {
      throw regex_error(
        RegexErrorCode::ERROR_BRACKET,
        "Unexpected character class open bracket.");
    }

    if (*current_ == '.') {
      token_ = Token::COLLSYMBOL;
      eat_class(*current_++);
    } else if (*current_ == ':') {
      token_ = Token::CHAR_CLASS_NAME;
      eat_class(*current_++);
    } else if (*current_ == '=') {
      token_ = Token::EQUIV_CLASS_NAME;
      eat_class(*current_++);
    } else {
      token_ = Token::ORD_CHAR;
      value_.assign(1, c);
    }
  }
  // In POSIX, when encountering "[]" or "[^]", the ']' is interpreted
  // literally. So "[]]" and "[^]]" are valid regexes. See the testcases
  // `*/empty_range.cc`.
  else if (c == ']' && (!at_bracket_start_)) {
    token_ = Token::BRACKET_END;
    state_ = State::NORMAL;
  } else if (c == '\\') {
    // ECMAScript
    eat_escape_ecma();
  } else {
    token_ = Token::ORD_CHAR;
    value_.assign(1, c);
  }
  at_bracket_start_ = false;
}

// Differences between styles:
// 1) "\}" in basic style.
template<typename Char>
void scanner<Char>::scan_in_brace() {
  if (current_ == end_) {
    throw regex_error(
      RegexErrorCode::ERROR_BRACE,
      "Unexpected end of regex when in brace expression.");
  }

  auto c = *current_++;

  if (ctype_.is(ctype_type::digit, c)) {
    token_ = Token::DUP_COUNT;
    value_.assign(1, c);
    while (current_ != end_ && ctype_.is(ctype_type::digit, *current_)) {
      value_ += *current_++;
    }
  } else if (c == ',') {
    token_ = Token::COMMA;
  // basic use \}.
  } else if (c == '}') {
    state_ = State::NORMAL;
    token_ = Token::INTERVAL_END;
  } else {
    throw regex_error(
      RegexErrorCode::ERROR_BADBRACE,
      "Unexpected character in brace expression.");
  }
}

template<typename Char>
void scanner<Char>::eat_escape_ecma() {
  if (current_ == end_) {
    throw regex_error(
      RegexErrorCode::ERROR_ESCAPE,
      "Unexpected end of regex when escaping.");
  }

  auto c = *current_++;
  auto __pos = find_escape(ctype_.narrow(c, '\0'));

  if (__pos != nullptr && (c != 'b' || state_ == State::IN_BRACKET)) {
    token_ = Token::ORD_CHAR;
    value_.assign(1, *__pos);
  } else if (c == 'b') {
    token_ = Token::WORD_BOUND;
    value_.assign(1, 'p');
  } else if (c == 'B') {
    token_ = Token::WORD_BOUND;
    value_.assign(1, 'n');
  }
  // N3376 28.13
  else if (c == 'd' || c == 'D' || c == 's' ||
           c == 'S' || c == 'w' || c == 'W') {
    token_ = Token::QUOTED_CLASS;
    value_.assign(1, c);
  }
  else if (c == 'c') {
    if (current_ == end_) {
      throw regex_error(
        RegexErrorCode::ERROR_ESCAPE,
        "Unexpected end of regex when reading control code.");
    }

    token_ = Token::ORD_CHAR;
    value_.assign(1, *current_++);
  } else if (c == 'x' || c == 'u') {
    value_.erase();
    for (int __i = 0; __i < (c == 'x' ? 2 : 4); __i++) {
      if (current_ == end_
          || !ctype_.is(ctype_type::xdigit, *current_)) {
        throw regex_error(
          RegexErrorCode::ERROR_ESCAPE,
          "Unexpected end of regex when ascii character.");
      }
      value_ += *current_++;
    }
    token_ = Token::HEX_NUM;
  }
  // ECMAScript recognizes multi-digit back-references.
  else if (ctype_.is(ctype_type::digit, c)) {
    value_.assign(1, c);
    while (current_ != end_ && ctype_.is(ctype_type::digit, *current_)) {
      value_ += *current_++;
    }
    token_ = Token::BACKREF;
  } else {
    token_ = Token::ORD_CHAR;
    value_.assign(1, c);
  }
}

// Eats a character class or throws an exception.
// __ch could be ':', '.' or '=', _M_current is the char after ']' when
// returning.
template<typename Char>
void scanner<Char>::eat_class(char ch) {
  for (value_.clear(); current_ != end_ && *current_ != ch;) {
    value_ += *current_++;
  }

  if (current_ == end_
      || *current_++ != ch
      || current_ == end_ // skip ch
      || *current_++ != ']') // skip ']'
  {
    if (ch == ':') {
      throw regex_error(
        RegexErrorCode::ERROR_CTYPE,
        "Unexpected end of character class.");
    } else {
      throw regex_error(
        RegexErrorCode::ERROR_COLLATE,
        "Unexpected end of character class.");
    }
  }
}

#ifdef IRESEARCH_DEBUG
template<typename _CharT>
std::ostream& scanner<_CharT>::print(std::ostream& ostr) {
  switch (token_) {
    case Token::ANYCHAR:
      ostr << "any-character\n";
      break;
    case Token::BACKREF:
      ostr << "backref\n";
      break;
    case Token::BRACKET_BEGIN:
      ostr << "bracket-begin\n";
      break;
    case Token::BRACKET_NEG_BEGIN:
      ostr << "bracket-neg-begin\n";
      break;
    case Token::BRACKET_END:
      ostr << "bracket-end\n";
      break;
    case Token::CHAR_CLASS_NAME:
      ostr << "char-class-name \"" << value_ << "\"\n";
      break;
    case Token::CLOSURE0:
      ostr << "closure0\n";
      break;
    case Token::CLOSURE1:
      ostr << "closure1\n";
      break;
    case Token::COLLSYMBOL:
      ostr << "collsymbol \"" << value_ << "\"\n";
      break;
    case Token::COMMA:
      ostr << "comma\n";
      break;
    case Token::DUP_COUNT:
      ostr << "dup count: " << value_ << "\n";
      break;
    case Token::END_OF_FILE:
      ostr << "EOF\n";
      break;
    case Token::EQUIV_CLASS_NAME:
      ostr << "equiv-class-name \"" << value_ << "\"\n";
      break;
    case Token::INTERVAL_BEGIN:
      ostr << "interval begin\n";
      break;
    case Token::INTERVAL_END:
      ostr << "interval end\n";
      break;
    case Token::LINE_BEGIN:
      ostr << "line begin\n";
      break;
    case Token::LINE_END:
      ostr << "line end\n";
      break;
    case Token::OPT:
      ostr << "opt\n";
      break;
    case Token::OR:
      ostr << "or\n";
      break;
    case Token::ORD_CHAR:
      ostr << "ordinary character: \"" << value_ << "\"\n";
      break;
    case Token::SUBEXPR_BEGIN:
      ostr << "subexpr begin\n";
      break;
    case Token::SUBEXPR_NO_GROUP_BEGIN:
      ostr << "no grouping subexpr begin\n";
      break;
    case Token::SUBEXPR_NO_LOOKAHEAD_BEGIN:
      ostr << "lookahead subexpr begin\n";
      break;
    case Token::SUBEXPR_END:
      ostr << "subexpr end\n";
      break;
    case Token::UNKNOWN:
      ostr << "-- unknown token --\n";
      break;
    case Token::OCT_NUM:
      ostr << "oct number " << value_ << "\n";
      break;
    case Token::HEX_NUM:
      ostr << "hex number " << value_ << "\n";
      break;
    case Token::QUOTED_CLASS:
      ostr << "quoted class " << "\\" << value_ << "\n";
      break;
    default:
      assert(false);
  }
  return ostr;
}
#endif

} // iresearch

#endif // IRESEARCH_REGEX_SCANNER_H
