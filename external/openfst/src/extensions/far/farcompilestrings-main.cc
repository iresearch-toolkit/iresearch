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
// Compiles a set of stings as FSTs and stores them in a finite-state archive.

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <fst/flags.h>
#include <fst/extensions/far/farscript.h>
#include <fstream>
#include <fst/script/getters.h>

DECLARE_string(key_prefix);
DECLARE_string(key_suffix);
DECLARE_int32(generate_keys);
DECLARE_string(far_type);
DECLARE_bool(allow_negative_labels);
DECLARE_string(arc_type);
DECLARE_string(entry_type);
DECLARE_string(fst_type);
DECLARE_string(token_type);
DECLARE_string(symbols);
DECLARE_string(unknown_symbol);
DECLARE_bool(file_list_input);
DECLARE_bool(keep_symbols);
DECLARE_bool(initial_symbols);

int farcompilestrings_main(int argc, char **argv) {
  namespace s = fst::script;
  using fst::script::FarWriterClass;

  std::string usage = "Compiles a set of strings as FSTs and stores them in";
  usage += " a finite-state archive.\n\n  Usage:";
  usage += argv[0];
  usage += " [in1.txt [[in2.txt ...] out.far]]\n";

  std::set_new_handler(FailedNewHandler);
  SET_FLAGS(usage.c_str(), &argc, &argv, true);
  s::ExpandArgs(argc, argv, &argc, &argv);

  std::vector<std::string> sources;
  if (FST_FLAGS_file_list_input) {
    for (int i = 1; i < argc - 1; ++i) {
      std::ifstream istrm(argv[i]);
      std::string str;
      while (std::getline(istrm, str)) sources.push_back(str);
    }
  } else {
    for (int i = 1; i < argc - 1; ++i)
      sources.push_back(strcmp(argv[i], "-") != 0 ? argv[i] : "");
    if (sources.empty()) {
      // argc == 1 || argc == 2. This cleverly handles both the no-file case
      // and the one (input) file case together.
      sources.push_back(argc == 2 && strcmp(argv[1], "-") != 0 ? argv[1] : "");
    }
  }

  // argc <= 2 means the file (if any) is an input file, so write to stdout.
  const std::string out_far =
      argc > 2 && strcmp(argv[argc - 1], "-") != 0 ? argv[argc - 1] : "";

  fst::FarEntryType entry_type;
  if (!s::GetFarEntryType(FST_FLAGS_entry_type, &entry_type)) {
    LOG(ERROR) << "Unknown or unsupported FAR entry type: "
               << FST_FLAGS_entry_type;
    return 1;
  }

  fst::TokenType token_type;
  if (!s::GetTokenType(FST_FLAGS_token_type, &token_type)) {
    LOG(ERROR) << "Unknown or unsupported FAR token type: "
               << FST_FLAGS_token_type;
    return 1;
  }

  fst::FarType far_type;
  if (!s::GetFarType(FST_FLAGS_far_type, &far_type)) {
    LOG(ERROR) << "Unknown or unsupported FAR type: "
               << FST_FLAGS_far_type;
    return 1;
  }

  // Empty fst_type means vector for farcompilestrings, but "input FST type"
  // for farconvert.
  const std::string fst_type = FST_FLAGS_fst_type.empty()
                                   ? "vector"
                                   : FST_FLAGS_fst_type;

  const auto arc_type = FST_FLAGS_arc_type;
  if (arc_type.empty()) return 1;

  std::unique_ptr<FarWriterClass> writer(
      FarWriterClass::Create(out_far, arc_type, far_type));
  if (!writer) return 1;

  s::CompileStrings(
      sources, *writer, fst_type, FST_FLAGS_generate_keys,
      entry_type, token_type, FST_FLAGS_symbols,
      FST_FLAGS_unknown_symbol, FST_FLAGS_keep_symbols,
      FST_FLAGS_initial_symbols,
      FST_FLAGS_allow_negative_labels,
      FST_FLAGS_key_prefix, FST_FLAGS_key_suffix);

  if (writer->Error()) {
    FSTERROR() << "Error writing FAR: " << out_far;
    return 1;
  }

  return 0;
}
