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
// Converts an RTN represented by FSTs and non-terminal labels into a PDT.

#include <fst/flags.h>
#include <fst/fst.h>

DEFINE_string(pdt_parentheses, "", "PDT parenthesis label pairs");
DEFINE_string(pdt_parser_type, "left",
              "Construction method, one of: \"left\", \"left_sr\"");
DEFINE_int64(start_paren_labels, fst::kNoLabel,
             "Index to use for the first inserted parentheses; if not "
             "specified, the next available label beyond the highest output "
             "label is used");
DEFINE_string(left_paren_prefix, "(_",
              "Prefix to attach to SymbolTable "
              "labels for inserted left parentheses");
DEFINE_string(right_paren_prefix, ")_",
              "Prefix to attach to SymbolTable "
              "labels for inserted right parentheses");

int pdtreplace_main(int argc, char **argv);

int main(int argc, char **argv) { return pdtreplace_main(argc, argv); }
