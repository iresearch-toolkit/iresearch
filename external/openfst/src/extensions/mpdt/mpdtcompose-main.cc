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
// Composes an MPDT and an FST.

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
#include <fst/extensions/pdt/getters.h>
#include <fst/util.h>

DECLARE_string(mpdt_parentheses);
DECLARE_bool(left_mpdt);
DECLARE_bool(connect);
DECLARE_string(compose_filter);

int mpdtcompose_main(int argc, char **argv) {
  namespace s = fst::script;
  using fst::MPdtComposeOptions;
  using fst::PdtComposeFilter;
  using fst::ReadLabelTriples;
  using fst::script::FstClass;
  using fst::script::VectorFstClass;

  std::string usage = "Compose an MPDT and an FST.\n\n  Usage: ";
  usage += argv[0];
  usage += " in.pdt in.fst [out.mpdt]\n";
  usage += " in.fst in.pdt [out.mpdt]\n";

  std::set_new_handler(FailedNewHandler);
  SET_FLAGS(usage.c_str(), &argc, &argv, true);
  if (argc < 3 || argc > 4) {
    ShowUsage();
    return 1;
  }

  const std::string in1_name = strcmp(argv[1], "-") == 0 ? "" : argv[1];
  const std::string in2_name = strcmp(argv[2], "-") == 0 ? "" : argv[2];
  const std::string out_name =
      argc > 3 && strcmp(argv[3], "-") != 0 ? argv[3] : "";

  if (in1_name.empty() && in2_name.empty()) {
    LOG(ERROR) << argv[0] << ": Can't take both inputs from standard input.";
    return 1;
  }

  std::unique_ptr<FstClass> ifst1(FstClass::Read(in1_name));
  if (!ifst1) return 1;
  std::unique_ptr<FstClass> ifst2(FstClass::Read(in2_name));
  if (!ifst2) return 1;

  if (FST_FLAGS_mpdt_parentheses.empty()) {
    LOG(ERROR) << argv[0] << ": No MPDT parenthesis label pairs provided";
    return 1;
  }

  std::vector<std::pair<int64_t, int64_t>> parens;
  std::vector<int64_t> assignments;
  if (!ReadLabelTriples(FST_FLAGS_mpdt_parentheses, &parens,
                        &assignments, false)) {
    return 1;
  }

  VectorFstClass ofst(ifst1->ArcType());

  PdtComposeFilter compose_filter;
  if (!s::GetPdtComposeFilter(FST_FLAGS_compose_filter,
                              &compose_filter)) {
    LOG(ERROR) << argv[0] << ": Unknown or unsupported compose filter type: "
               << FST_FLAGS_compose_filter;
    return 1;
  }

  const MPdtComposeOptions opts(FST_FLAGS_connect, compose_filter);

  s::Compose(*ifst1, *ifst2, parens, assignments, &ofst, opts,
             FST_FLAGS_left_mpdt);

  return !ofst.Write(out_name);
}
