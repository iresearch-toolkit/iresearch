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

#include "tests_shared.hpp"
#include "utils/regex_scanner.h"

TEST(regex_scanner_test, test) {
  const char regex[] = "\\whuypuya[b-c]d*(f|lll)";
  irs::scanner<char> s(std::begin(regex), std::end(regex), std::locale());
  while (irs::detail::scanner_base::Token::END_OF_FILE != s.token()) {
    auto& v = s.value();
    auto t = s.token();
    s.next();
  }
}

