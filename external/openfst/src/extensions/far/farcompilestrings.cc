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
#include <fst/flags.h>

DEFINE_string(key_prefix, "", "Prefix to append to keys");
DEFINE_string(key_suffix, "", "Suffix to append to keys");
DEFINE_int32(generate_keys, 0,
             "Generate N digit numeric keys (def: use file basenames)");
DEFINE_string(far_type, "default",
              "FAR file format type: one of: \"default\", \"fst\", "
              "\"stlist\", \"sttable\"");
DEFINE_bool(allow_negative_labels, false,
            "Allow negative labels (not recommended; may cause conflicts)");
DEFINE_string(arc_type, "standard", "Output arc type");
DEFINE_string(entry_type, "line",
              "Entry type: one of : "
              "\"file\" (one FST per file), \"line\" (one FST per line)");
DEFINE_string(fst_type, "", "Output FST type");
DEFINE_string(token_type, "symbol",
              "Token type: one of : "
              "\"symbol\", \"byte\", \"utf8\"");
DEFINE_string(symbols, "",
              "Label symbol table. Only applies to \"symbol\" tokens.");
DEFINE_string(unknown_symbol, "", "");
DEFINE_bool(file_list_input, false,
            "Each input file contains a list of files to be processed");
DEFINE_bool(keep_symbols, false, "Store symbol table in the FAR file");
DEFINE_bool(initial_symbols, true,
            "When keep_symbols is true, stores symbol table only for the first"
            " FST in archive.");

int farcompilestrings_main(int argc, char **argv);

int main(int argc, char **argv) { return farcompilestrings_main(argc, argv); }
