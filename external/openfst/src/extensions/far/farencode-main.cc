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
// Encodes FAR labels and/or weights.

#include <cstring>
#include <memory>
#include <string>

#include <fst/flags.h>
#include <fst/log.h>
#include <fst/extensions/far/farscript.h>
#include <fst/extensions/far/getters.h>

DECLARE_bool(decode);
DECLARE_bool(encode_labels);
DECLARE_bool(encode_weights);
DECLARE_bool(encode_reuse);
DECLARE_string(far_type);

int farencode_main(int argc, char **argv) {
  namespace s = fst::script;
  using fst::script::EncodeMapperClass;
  using fst::script::FarReaderClass;
  using fst::script::FarWriterClass;

  std::string usage = "Encodes FAR labels and/or weights.\n\n Usage: ";
  usage += argv[0];
  usage += " [in.far mapper [out.far]]\n";

  std::set_new_handler(FailedNewHandler);
  SET_FLAGS(usage.c_str(), &argc, &argv, true);
  if (argc < 3 || argc > 4) {
    ShowUsage();
    return 1;
  }

  const std::string in_name = (strcmp(argv[1], "-") != 0) ? argv[1] : "";
  const std::string mapper_name = argv[2];
  const std::string out_name =
      argc > 3 && strcmp(argv[3], "-") != 0 ? argv[3] : "";

  std::unique_ptr<FarReaderClass> reader(FarReaderClass::Open(in_name));
  if (!reader) return 1;

  fst::FarType far_type;
  if (!s::GetFarType(FST_FLAGS_far_type, &far_type)) {
    LOG(ERROR) << "Unknown --far_type " << FST_FLAGS_far_type;
    return 1;
  }

  // This uses a different meaning of far_type; since DEFAULT means "same as
  // input", we must determine the input FarType.
  if (far_type == fst::FarType::DEFAULT) far_type = reader->Type();

  const auto arc_type = reader->ArcType();
  if (arc_type.empty()) return 1;

  std::unique_ptr<FarWriterClass> writer(
      FarWriterClass::Create(out_name, arc_type, far_type));
  if (!writer) return 1;

  if (FST_FLAGS_decode) {
    std::unique_ptr<EncodeMapperClass> mapper(
        EncodeMapperClass::Read(mapper_name));
    if (!mapper) return 1;
    s::Decode(*reader, *writer, *mapper);
  } else if (FST_FLAGS_encode_reuse) {
    std::unique_ptr<EncodeMapperClass> mapper(
        EncodeMapperClass::Read(mapper_name));
    if (!mapper) return 1;
    s::Encode(*reader, *writer, mapper.get());
  } else {
    const auto flags = s::GetEncodeFlags(FST_FLAGS_encode_labels,
                                         FST_FLAGS_encode_weights);
    EncodeMapperClass mapper(reader->ArcType(), flags);
    s::Encode(*reader, *writer, &mapper);
    if (!mapper.Write(mapper_name)) return 1;
  }

  if (reader->Error()) {
    FSTERROR() << "Error reading FAR: " << in_name;
    return 1;
  }
  if (writer->Error()) {
    FSTERROR() << "Error writing FAR: " << out_name;
    return 1;
  }

  return 0;
}
