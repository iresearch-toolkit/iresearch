////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "minhash_token_stream.hpp"

#include <velocypack/Parser.h>
#include <velocypack/Slice.h>

#include "analysis/analyzers.hpp"
#include "utils/log.hpp"

namespace {

using namespace arangodb;
using namespace irs;
using namespace irs::analysis;

bool ParseVPack(velocypack::Slice slice, analyzer::ptr* a) { return false; }

analyzer::ptr MakeVPack(velocypack::Slice slice) {
  analyzer::ptr a;
  if (ParseVPack(slice, &a)) {
    return std::make_unique<MinHashTokenStream>(std::move(a));
  }
  return nullptr;
}

irs::analysis::analyzer::ptr MakeVPack(irs::string_ref args) {
  VPackSlice slice(reinterpret_cast<const uint8_t*>(args.c_str()));
  return MakeVPack(slice);
}

analyzer::ptr MakeJson(irs::string_ref args) {
  try {
    if (args.null()) {
      IR_FRMT_ERROR("Null arguments while constructing MinHashAnalyzer");
      return nullptr;
    }
    auto vpack = velocypack::Parser::fromJson(args.c_str(), args.size());
    return MakeVPack(vpack->slice());
  } catch (const VPackException& ex) {
    IR_FRMT_ERROR(
        "Caught error '%s' while constructing MinHashAnalyzer from JSON",
        ex.what());
  } catch (...) {
    IR_FRMT_ERROR("Caught error while constructing MinHashAnalyzer from JSON");
  }
  return nullptr;
}

bool NormalizeVPack(velocypack::Slice slice, velocypack::Builder* out) {
  return false;
}

bool NormalizeVPack(irs::string_ref args, std::string& definition) {
  VPackSlice slice(reinterpret_cast<const uint8_t*>(args.c_str()));
  VPackBuilder builder;
  bool res = NormalizeVPack(slice, &builder);
  if (res) {
    definition.assign(builder.slice().startAs<char>(),
                      builder.slice().byteSize());
  }
  return res;
}

bool NormalizeJson(irs::string_ref args, std::string& definition) {
  try {
    if (args.null()) {
      IR_FRMT_ERROR("Null arguments while normalizing MinHashAnalyzer");
      return false;
    }
    auto vpack = velocypack::Parser::fromJson(args.c_str(), args.size());
    VPackBuilder builder;
    if (NormalizeVPack(vpack->slice(), &builder)) {
      definition = builder.toString();
      return !definition.empty();
    }
  } catch (const VPackException& ex) {
    IR_FRMT_ERROR(
        "Caught error '%s' while normalizing MinHashAnalyzer from JSON",
        ex.what());
  } catch (...) {
    IR_FRMT_ERROR(
        "Caught error while normalizing MinHashAnalyzerfrom from JSON");
  }
  return false;
}

auto sRegisterTypes = []() {
  MinHashTokenStream::init();
  return std::nullopt;
}();

}  // namespace

namespace iresearch::analysis {

/*static*/ void MinHashTokenStream::init() {
  REGISTER_ANALYZER_VPACK(irs::analysis::MinHashTokenStream, MakeVPack,
                          NormalizeVPack);
  REGISTER_ANALYZER_JSON(irs::analysis::MinHashTokenStream, MakeJson,
                         NormalizeJson);
}

MinHashTokenStream::MinHashTokenStream(analyzer::ptr&& analyzer)
    : analysis::analyzer{irs::type<MinHashTokenStream>::get()},
      a_{std::move(analyzer)} {
  assert(a_);
}

bool next() { return false; }

bool MinHashTokenStream::reset(string_ref) { return false; }

}  // namespace iresearch::analysis
