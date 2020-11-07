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

#include "regex_utils.hpp"

#include "shared.hpp"
#include "regex_scanner.h"

namespace {

class automaton_builder {
 public:
 private:
  using Token = irs::scanner<irs::byte_type>::Token;

  bool match(Token token) {
    if (token == scanner_.token()) {
      scanner_.next();
      return true;
    }
    return false;
  }

  void alternative();
  bool assertion();
  bool atom();
  bool disjunction();
  bool term();
  bool quantifier();
  bool bracket_expression();
  bool try_char();

  irs::scanner<irs::byte_type> scanner_;
};

bool automaton_builder::disjunction() {
  alternative();
  while (match(Token::OR)) {
    // pop
    alternative();
    // pop
  }
}

void automaton_builder::alternative() {
  if (term()) {
    // pop
    alternative();
  }
}

bool automaton_builder::term() {
  if (assertion()) {
    return true;
  }

  if (atom()) {
    while (quantifier());
    return true;
  }

  return false;
}

bool automaton_builder::assertion() {
  if (match(Token::LINE_BEGIN)) {

  } else if (match(Token::LINE_END)) {

  } else if (match(Token::WORD_BOUND)) {
  // _M_value[0] == 'n' means it's negative, say "not word boundary".
  }
  else if (match(Token::SUBEXPR_LOOKAHEAD_BEGIN)) {
    disjunction();
  } else {
    return false;
  }

  return true;
}

bool automaton_builder::atom() {
  if (match(Token::ANYCHAR)) {

  } else if (try_char()) {

  } else if (match(Token::BACKREF)) {

  } else if (match(Token::QUOTED_CLASS)) {

  } else if (match(Token::SUBEXPR_NO_GROUP_BEGIN)) {

  } else if (match(Token::SUBEXPR_BEGIN)) {

  } else if (!bracket_expression()) {
    return false;
  }

  return true;
}

bool automaton_builder::quantifier() {
  // check stack is not empty

  if (match(Token::CLOSURE_STAR)) {

  } else if (match(Token::CLOSURE_PLUS)) {

  } else if (match(Token::OPT)) {

  } else if (match(Token::INTERVAL_BEGIN)) {

  } else {
    return false;
  }

  return true;
}

bool automaton_builder::try_char() {
  bool is_char = false;
  if (match(Token::OCT_NUM)) {
    is_char = true;
  } else if (match(Token::HEX_NUM)) {
    is_char = true;
  } else if (match(Token::ORD_CHAR)) {
    is_char = true;
  }

  return is_char;
}

bool automaton_builder::bracket_expression() {
  const bool neg = match(Token::BRACKET_NEG_BEGIN);
  if (!(neg || match(Token::BRACKET_BEGIN))) {

    return false;
  }

  return true;
}

}

namespace iresearch {

automaton from_regex(const bytes_ref& expr) {
  return {};
}

}
