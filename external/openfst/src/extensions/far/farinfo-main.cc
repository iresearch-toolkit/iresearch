// Copyright 2005-2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// See www.openfst.org for extensive documentation on this weighted
// finite-state transducer library.
//
// Prints some basic information about the FSTs in an FST archive.

#include <string>
#include <vector>

#include <fst/flags.h>
#include <fst/extensions/far/farscript.h>
#include <fst/extensions/far/getters.h>

DECLARE_string(begin_key);
DECLARE_string(end_key);
DECLARE_bool(list_fsts);

int farinfo_main(int argc, char **argv) {
  namespace s = fst::script;

  std::string usage = "Prints some basic information about the FSTs in an FST ";
  usage += "archive.\n\n  Usage:";
  usage += argv[0];
  usage += " [in1.far in2.far...]\n";
  usage += "  Flags: begin_key end_key list_fsts";

  std::set_new_handler(FailedNewHandler);
  SET_FLAGS(usage.c_str(), &argc, &argv, true);
  s::ExpandArgs(argc, argv, &argc, &argv);

  std::vector<std::string> sources;
  for (int i = 1; i < argc; ++i) sources.push_back(argv[i]);
  if (sources.empty()) sources.push_back("");

  const auto arc_type = s::LoadArcTypeFromFar(sources[0]);
  if (arc_type.empty()) return 1;

  s::Info(sources, arc_type, FST_FLAGS_begin_key,
          FST_FLAGS_end_key, FST_FLAGS_list_fsts);

  return 0;
}
