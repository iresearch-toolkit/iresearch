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

#include "types.hpp"

namespace iresearch {

struct multiterm_state;
struct term_state;
struct term_reader;

struct PreparedStateVisitor {
  virtual ~PreparedStateVisitor() = default;

  virtual bool Visit(const term_reader& field, score_t boost,
                     const term_state& state) = 0;
  virtual bool Visit(const term_reader& field, score_t boost,
                     const multiterm_state& state) = 0;
};

}  // namespace iresearch