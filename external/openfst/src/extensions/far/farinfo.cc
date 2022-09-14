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

DEFINE_string(begin_key, "",
              "First key to extract (default: first key in archive)");
DEFINE_string(end_key, "",
              "Last key to extract (default: last key in archive)");

DEFINE_bool(list_fsts, false, "Display FST information for each key");

int farinfo_main(int argc, char **argv);

int main(int argc, char **argv) { return farinfo_main(argc, argv); }
