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

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <fst/flags.h>
#include <fst/log.h>
#include <fst/extensions/mpdt/mpdtscript.h>
#include <fst/extensions/mpdt/read_write_utils.h>
#include <fst/util.h>

DECLARE_string(mpdt_parentheses);
DECLARE_string(mpdt_new_parentheses);

int mpdtreverse_main(int argc, char **argv) {
  namespace s = fst::script;
  using fst::ReadLabelTriples;
  using fst::WriteLabelTriples;
  using fst::script::FstClass;
  using fst::script::VectorFstClass;

  std::string usage = "Reverse an MPDT.\n\n  Usage: ";
  usage += argv[0];
  usage += " in.pdt [out.fst]\n";

  std::set_new_handler(FailedNewHandler);
  SET_FLAGS(usage.c_str(), &argc, &argv, true);
  if (argc > 3) {
    ShowUsage();
    return 1;
  }

  const std::string in_name =
      (argc > 1 && (strcmp(argv[1], "-") != 0)) ? argv[1] : "";
  const std::string out_name =
      (argc > 2 && (strcmp(argv[2], "-") != 0)) ? argv[2] : "";

  std::unique_ptr<FstClass> ifst(FstClass::Read(in_name));
  if (!ifst) return 1;

  if (FST_FLAGS_mpdt_parentheses.empty()) {
    LOG(ERROR) << argv[0] << ": No MPDT parenthesis label pairs provided";
    return 1;
  }

  if (FST_FLAGS_mpdt_new_parentheses.empty()) {
    LOG(ERROR) << argv[0] << ": No MPDT output parenthesis label file provided";
    return 1;
  }

  std::vector<std::pair<int64_t, int64_t>> parens;
  std::vector<int64_t> assignments;
  if (!ReadLabelTriples(FST_FLAGS_mpdt_parentheses, &parens,
                        &assignments, false)) {
    return 1;
  }

  VectorFstClass ofst(ifst->ArcType());

  s::Reverse(*ifst, parens, &assignments, &ofst);

  if (!ofst.Write(out_name)) return 1;

  return !WriteLabelTriples(FST_FLAGS_mpdt_new_parentheses, parens,
                            assignments);
}
