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
// Composes a PDT and an FST.

#include <fst/flags.h>

DEFINE_string(pdt_parentheses, "", "PDT parenthesis label pairs");
DEFINE_bool(left_pdt, true, "Is the first argument the PDT?");
DEFINE_bool(connect, true, "Trim output?");
DEFINE_string(compose_filter, "paren",
              "Composition filter, one of: \"expand\", \"expand_paren\", "
              "\"paren\"");

int pdtcompose_main(int argc, char **argv);

int main(int argc, char **argv) { return pdtcompose_main(argc, argv); }
