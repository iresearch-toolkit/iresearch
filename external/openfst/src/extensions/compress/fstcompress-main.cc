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
// Compresses/decompresses an FST.

#include <cstring>
#include <memory>
#include <string>

#include <fst/flags.h>
#include <fst/log.h>
#include <fst/extensions/compress/compressscript.h>
#include <fst/script/arg-packs.h>
#include <fst/script/fst-class.h>

DECLARE_string(arc_type);
DECLARE_bool(decode);

int fstcompress_main(int argc, char **argv) {
  namespace s = fst::script;
  using fst::script::FstClass;
  using fst::script::VectorFstClass;

  std::string usage = "Compresses/decompresses an FST.\n\n  Usage: ";
  usage += argv[0];
  usage += " [in.fst [out.fstz]]\n";
  usage += " --decode [in.fstz [out.fst]]\n";

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

  if (FST_FLAGS_decode) {
    VectorFstClass fst(FST_FLAGS_arc_type);
    if (!s::Decompress(in_name, &fst)) {
      FSTERROR() << "Decompression failed";
      return 1;
    }
    return !fst.Write(out_name);
  } else {
    std::unique_ptr<FstClass> ifst(FstClass::Read(in_name));
    if (!ifst) return 1;
    if (!s::Compress(*ifst, out_name)) {
      FSTERROR() << "Compression failed";
      return 1;
    }
    return 0;
  }
}
