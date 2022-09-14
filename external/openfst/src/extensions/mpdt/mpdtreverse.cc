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
// Reverses an MPDT.

#include <fst/flags.h>

DEFINE_string(mpdt_parentheses, "",
              "MPDT parenthesis label pairs with assignments.");
DEFINE_string(mpdt_new_parentheses, "",
              "Output for reassigned parentheses and stacks");

int mpdtreverse_main(int argc, char **argv);

int main(int argc, char **argv) { return mpdtreverse_main(argc, argv); }
