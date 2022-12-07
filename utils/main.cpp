////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2016 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#if defined(_MSC_VER)
#pragma warning(disable : 4101)
#pragma warning(disable : 4267)
#endif

#include <cmdline.h>

#if defined(_MSC_VER)
#pragma warning(default : 4267)
#pragma warning(default : 4101)
#endif

#if defined(_MSC_VER)
#pragma warning(disable : 4229)
#endif

#include <unicode/uclean.h>  // for u_cleanup

#if defined(_MSC_VER)
#pragma warning(default : 4229)
#endif

#include <absl/container/flat_hash_map.h>

#include <functional>
#include <iostream>

#include "index-put.hpp"
#include "index-search.hpp"
#include "utils/log.hpp"
#include "utils/misc.hpp"
#include "utils/timer_utils.hpp"

using handlers_t =
  absl::flat_hash_map<std::string, std::function<int(int argc, char* argv[])>>;

bool init_handlers(handlers_t&);

namespace {

void AssertCallback(std::string_view file, std::size_t line,
                    std::string_view function,
                    std::string_view condition) noexcept {
  std::cerr << file << ":" << line << ": " << function << ": Condition '"
            << condition << "' is true.\n";
  assert(false);
}

const std::string HELP = "help";
const std::string MODE = "mode";

std::string get_modes_description(const handlers_t& handlers) {
  std::string message = "Select mode:";
  for (auto& entry : handlers) {
    message.append(" ");
    message += entry.first;
  }
  return message;
}

}  // namespace

int main(int argc, char* argv[]) {
  irs::SetCallback(irs::LogLevel::Assert, AssertCallback);
  handlers_t handlers;

  // initialize supported modes
  if (!init_handlers(handlers)) {
    return 1;
  }

  // init timers
  irs::timer_utils::init_stats(true);
  irs::Finally output_stats = []() noexcept {
    irs::timer_utils::visit(
      [](const std::string& key, size_t count, size_t time_us) -> bool {
        std::cout << key << " calls:" << count << ", time: " << time_us
                  << " us, avg call: " << time_us / double(count) << " us"
                  << std::endl;
        return true;
      });
  };

  // set error level
  irs::logger::output_le(iresearch::logger::IRL_ERROR, stderr);

  // general description
  cmdline::parser cmdroot;
  cmdroot.add(HELP, '?', "Produce help message");
  cmdroot.add<std::string>(MODE, 'm', get_modes_description(handlers), true);
  cmdroot.parse(argc, argv);

  if (!cmdroot.exist(MODE) || cmdroot.exist(HELP)) {
    std::cout << cmdroot.usage() << std::endl;
  }

  const auto& mode = cmdroot.get<std::string>(MODE);
  const auto entry = handlers.find(mode);

  if (handlers.end() != entry) {
    return entry->second(argc, argv);
  }

  u_cleanup();  // cleanup ICU resources

  return 0;
}
