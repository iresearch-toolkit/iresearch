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
// Extracts component FSTs from an finite-state archive.

#include <memory>
#include <string>
#include <vector>

#include <fst/flags.h>
#include <fst/extensions/far/farscript.h>
#include <fst/extensions/far/getters.h>

DECLARE_string(filename_prefix);
DECLARE_string(filename_suffix);
DECLARE_int32(generate_filenames);
DECLARE_string(keys);
DECLARE_string(key_separator);
DECLARE_string(range_delimiter);

int farextract_main(int argc, char **argv) {
  namespace s = fst::script;
  using fst::script::FarReaderClass;

  std::string usage = "Extracts FSTs from a finite-state archive.\n\n Usage:";
  usage += argv[0];
  usage += " [in1.far in2.far...]\n";

  std::set_new_handler(FailedNewHandler);
  SET_FLAGS(usage.c_str(), &argc, &argv, true);
  s::ExpandArgs(argc, argv, &argc, &argv);

  std::vector<std::string> sources;
  for (int i = 1; i < argc; ++i) sources.push_back(argv[i]);
  if (sources.empty()) sources.push_back("");
  std::unique_ptr<FarReaderClass> reader(FarReaderClass::Open(sources));
  if (!reader) return 1;

  s::Extract(*reader, FST_FLAGS_generate_filenames,
             FST_FLAGS_keys, FST_FLAGS_key_separator,
             FST_FLAGS_range_delimiter,
             FST_FLAGS_filename_prefix,
             FST_FLAGS_filename_suffix);

  if (reader->Error()) {
    FSTERROR() << "Error reading FAR(s)";
    return 1;
  }

  return 0;
}
