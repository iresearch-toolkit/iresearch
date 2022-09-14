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

#include <fst/flags.h>
#include <fst/extensions/linear/linearscript.h>

DECLARE_string(arc_type);
DECLARE_string(epsilon_symbol);
DECLARE_string(unknown_symbol);
DECLARE_string(vocab);
DECLARE_string(out);
DECLARE_string(save_isymbols);
DECLARE_string(save_fsymbols);
DECLARE_string(save_osymbols);

int fstlinear_main(int argc, char **argv) {
  // TODO(wuke): more detailed usage
  std::set_new_handler(FailedNewHandler);
  SET_FLAGS(argv[0], &argc, &argv, true);
  fst::script::ValidateDelimiter();
  fst::script::ValidateEmptySymbol();

  if (argc == 1) {
    ShowUsage();
    return 1;
  }

  fst::script::LinearCompile(
      FST_FLAGS_arc_type, FST_FLAGS_epsilon_symbol,
      FST_FLAGS_unknown_symbol, FST_FLAGS_vocab, argv + 1,
      argc - 1, FST_FLAGS_out, FST_FLAGS_save_isymbols,
      FST_FLAGS_save_fsymbols, FST_FLAGS_save_osymbols);

  return 0;
}
