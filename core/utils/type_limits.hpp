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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "types.hpp"

namespace irs {

struct address_limits {
  constexpr static uint64_t invalid() noexcept {
    return std::numeric_limits<uint64_t>::max();
  }
  constexpr static bool valid(uint64_t addr) noexcept {
    return invalid() != addr;
  }
};

struct doc_limits {
  constexpr static doc_id_t eof() noexcept {
    return std::numeric_limits<doc_id_t>::max();
  }
  constexpr static bool eof(doc_id_t id) noexcept { return eof() == id; }
  constexpr static doc_id_t invalid() noexcept { return 0; }
  constexpr static doc_id_t(min)() noexcept {
    return 1;  // > invalid()
  }
  constexpr static bool valid(doc_id_t id) noexcept { return invalid() != id; }
};

struct field_limits {
  constexpr static field_id invalid() noexcept {
    return std::numeric_limits<field_id>::max();
  }
  constexpr static bool valid(field_id id) noexcept { return invalid() != id; }
};

struct index_gen_limits {
  constexpr static uint64_t invalid() noexcept { return 0; }
  constexpr static bool valid(uint64_t id) noexcept { return invalid() != id; }
};

struct pos_limits {
  constexpr static uint32_t invalid() noexcept { return 0; }
  constexpr static bool valid(uint32_t pos) noexcept {
    return invalid() != pos;
  }
  constexpr static uint32_t eof() noexcept {
    return std::numeric_limits<uint32_t>::max();
  }
  constexpr static bool eof(uint32_t pos) noexcept { return eof() == pos; }
  constexpr static uint32_t(min)() noexcept { return 1; }
};

}  // namespace irs
