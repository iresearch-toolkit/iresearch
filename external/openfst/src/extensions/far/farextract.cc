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

DEFINE_string(filename_prefix, "", "Prefix to append to filenames");
DEFINE_string(filename_suffix, "", "Suffix to append to filenames");
DEFINE_int32(generate_filenames, 0,
             "Generate N digit numeric filenames (def: use keys)");
DEFINE_string(keys, "",
              "Extract set of keys separated by comma (default) "
              "including ranges delimited by dash (default)");
DEFINE_string(key_separator, ",", "Separator for individual keys");
DEFINE_string(range_delimiter, "-", "Delimiter for ranges of keys");

int farextract_main(int argc, char **argv);

int main(int argc, char **argv) { return farextract_main(argc, argv); }
