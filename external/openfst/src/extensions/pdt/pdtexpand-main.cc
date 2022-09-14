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
// Expands a (bounded-stack) PDT as an FST.

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <fst/flags.h>
#include <fst/log.h>
#include <fst/extensions/pdt/pdtscript.h>
#include <fst/util.h>

DECLARE_string(pdt_parentheses);
DECLARE_bool(connect);
DECLARE_bool(keep_parentheses);
DECLARE_string(weight);

int pdtexpand_main(int argc, char **argv) {
  namespace s = fst::script;
  using fst::ReadLabelPairs;
  using fst::script::FstClass;
  using fst::script::VectorFstClass;
  using fst::script::WeightClass;

  std::string usage = "Expand a (bounded-stack) PDT as an FST.\n\n  Usage: ";
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

  if (FST_FLAGS_pdt_parentheses.empty()) {
    LOG(ERROR) << argv[0] << ": No PDT parenthesis label pairs provided";
    return 1;
  }

  std::vector<std::pair<int64_t, int64_t>> parens;
  if (!ReadLabelPairs(FST_FLAGS_pdt_parentheses, &parens, false))
    return 1;

  const auto weight_threshold =
      FST_FLAGS_weight.empty()
          ? WeightClass::Zero(ifst->WeightType())
          : WeightClass(ifst->WeightType(), FST_FLAGS_weight);

  VectorFstClass ofst(ifst->ArcType());
  s::Expand(*ifst, parens, &ofst,
            s::PdtExpandOptions(FST_FLAGS_connect,
                                FST_FLAGS_keep_parentheses,
                                weight_threshold));

  return !ofst.Write(out_name);
}
