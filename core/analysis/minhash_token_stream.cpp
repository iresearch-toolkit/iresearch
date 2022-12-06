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

#include <absl/strings/internal/escaping.h>
#include <velocypack/Parser.h>
#include <velocypack/Slice.h>

#include "analysis/analyzers.hpp"
#include "analysis/token_streams.hpp"
#include "utils/log.hpp"
#include "utils/vpack_utils.hpp"

namespace {

// CityHash64 implementation from Abseil library (city.cc).
//
// The code is copied to avoid potential problems related to Abseil update.
// That is important because produced tokens are stored in the index as terms.

// Some primes between 2^63 and 2^64 for various uses.
static const uint64_t k0 = 0xc3a5c85c97cb3127ULL;
static const uint64_t k1 = 0xb492b66fbe98f273ULL;
static const uint64_t k2 = 0x9ae16a3b2f90404fULL;

#ifdef ABSL_IS_BIG_ENDIAN
#define uint32_in_expected_order(x) (absl::gbswap_32(x))
#define uint64_in_expected_order(x) (absl::gbswap_64(x))
#else
#define uint32_in_expected_order(x) (x)
#define uint64_in_expected_order(x) (x)
#endif

static uint64_t Fetch64(const char* p) {
  return uint64_in_expected_order(ABSL_INTERNAL_UNALIGNED_LOAD64(p));
}

static uint32_t Fetch32(const char* p) {
  return uint32_in_expected_order(ABSL_INTERNAL_UNALIGNED_LOAD32(p));
}

static uint64_t ShiftMix(uint64_t val) { return val ^ (val >> 47); }

// Bitwise right rotate.  Normally this will compile to a single
// instruction, especially if the shift is a manifest constant.
static uint64_t Rotate(uint64_t val, int shift) {
  // Avoid shifting by 64: doing so yields an undefined result.
  return shift == 0 ? val : ((val >> shift) | (val << (64 - shift)));
}

static uint64_t HashLen16(uint64_t u, uint64_t v, uint64_t mul) {
  // Murmur-inspired hashing.
  uint64_t a = (u ^ v) * mul;
  a ^= (a >> 47);
  uint64_t b = (v ^ a) * mul;
  b ^= (b >> 47);
  b *= mul;
  return b;
}

static uint64_t HashLen16(uint64_t u, uint64_t v) {
  const uint64_t kMul = 0x9ddfea08eb382d69ULL;
  return HashLen16(u, v, kMul);
}

static uint64_t HashLen0to16(const char* s, size_t len) {
  if (len >= 8) {
    uint64_t mul = k2 + len * 2;
    uint64_t a = Fetch64(s) + k2;
    uint64_t b = Fetch64(s + len - 8);
    uint64_t c = Rotate(b, 37) * mul + a;
    uint64_t d = (Rotate(a, 25) + b) * mul;
    return HashLen16(c, d, mul);
  }
  if (len >= 4) {
    uint64_t mul = k2 + len * 2;
    uint64_t a = Fetch32(s);
    return HashLen16(len + (a << 3), Fetch32(s + len - 4), mul);
  }
  if (len > 0) {
    uint8_t a = s[0];
    uint8_t b = s[len >> 1];
    uint8_t c = s[len - 1];
    uint32_t y = static_cast<uint32_t>(a) + (static_cast<uint32_t>(b) << 8);
    uint32_t z = len + (static_cast<uint32_t>(c) << 2);
    return ShiftMix(y * k2 ^ z * k0) * k2;
  }
  return k2;
}

// This probably works well for 16-byte strings as well, but it may be overkill
// in that case.
static uint64_t HashLen17to32(const char* s, size_t len) {
  uint64_t mul = k2 + len * 2;
  uint64_t a = Fetch64(s) * k1;
  uint64_t b = Fetch64(s + 8);
  uint64_t c = Fetch64(s + len - 8) * mul;
  uint64_t d = Fetch64(s + len - 16) * k2;
  return HashLen16(Rotate(a + b, 43) + Rotate(c, 30) + d,
                   a + Rotate(b + k2, 18) + c, mul);
}

// Return a 16-byte hash for 48 bytes.  Quick and dirty.
// Callers do best to use "random-looking" values for a and b.
static std::pair<uint64_t, uint64_t> WeakHashLen32WithSeeds(
  uint64_t w, uint64_t x, uint64_t y, uint64_t z, uint64_t a, uint64_t b) {
  a += w;
  b = Rotate(b + a + z, 21);
  uint64_t c = a;
  a += x;
  a += y;
  b += Rotate(a, 44);
  return std::make_pair(a + z, b + c);
}

// Return a 16-byte hash for s[0] ... s[31], a, and b.  Quick and dirty.
static std::pair<uint64_t, uint64_t> WeakHashLen32WithSeeds(const char* s,
                                                            uint64_t a,
                                                            uint64_t b) {
  return WeakHashLen32WithSeeds(Fetch64(s), Fetch64(s + 8), Fetch64(s + 16),
                                Fetch64(s + 24), a, b);
}

// Return an 8-byte hash for 33 to 64 bytes.
static uint64_t HashLen33to64(const char* s, size_t len) {
  uint64_t mul = k2 + len * 2;
  uint64_t a = Fetch64(s) * k2;
  uint64_t b = Fetch64(s + 8);
  uint64_t c = Fetch64(s + len - 24);
  uint64_t d = Fetch64(s + len - 32);
  uint64_t e = Fetch64(s + 16) * k2;
  uint64_t f = Fetch64(s + 24) * 9;
  uint64_t g = Fetch64(s + len - 8);
  uint64_t h = Fetch64(s + len - 16) * mul;
  uint64_t u = Rotate(a + g, 43) + (Rotate(b, 30) + c) * 9;
  uint64_t v = ((a + g) ^ d) + f + 1;
  uint64_t w = absl::gbswap_64((u + v) * mul) + h;
  uint64_t x = Rotate(e + f, 42) + c;
  uint64_t y = (absl::gbswap_64((v + w) * mul) + g) * mul;
  uint64_t z = e + f + c;
  a = absl::gbswap_64((x + z) * mul + y) + b;
  b = ShiftMix((z + a) * mul + d + h) * mul;
  return b + x;
}

uint64_t CityHash64(const char* s, size_t len) {
  if (len <= 32) {
    if (len <= 16) {
      return HashLen0to16(s, len);
    } else {
      return HashLen17to32(s, len);
    }
  } else if (len <= 64) {
    return HashLen33to64(s, len);
  }

  // For strings over 64 bytes we hash the end first, and then as we
  // loop we keep 56 bytes of state: v, w, x, y, and z.
  uint64_t x = Fetch64(s + len - 40);
  uint64_t y = Fetch64(s + len - 16) + Fetch64(s + len - 56);
  uint64_t z = HashLen16(Fetch64(s + len - 48) + len, Fetch64(s + len - 24));
  std::pair<uint64_t, uint64_t> v =
    WeakHashLen32WithSeeds(s + len - 64, len, z);
  std::pair<uint64_t, uint64_t> w =
    WeakHashLen32WithSeeds(s + len - 32, y + k1, x);
  x = x * k1 + Fetch64(s);

  // Decrease len to the nearest multiple of 64, and operate on 64-byte chunks.
  len = (len - 1) & ~static_cast<size_t>(63);
  do {
    x = Rotate(x + y + v.first + Fetch64(s + 8), 37) * k1;
    y = Rotate(y + v.second + Fetch64(s + 48), 42) * k1;
    x ^= w.second;
    y += v.first + Fetch64(s + 40);
    z = Rotate(z + w.first, 33) * k1;
    v = WeakHashLen32WithSeeds(s, v.second * k1, x + w.first);
    w = WeakHashLen32WithSeeds(s + 32, z + w.second, y + Fetch64(s + 16));
    std::swap(z, x);
    s += 64;
    len -= 64;
  } while (len != 0);
  return HashLen16(HashLen16(v.first, w.first) + ShiftMix(y) * k1 + z,
                   HashLen16(v.second, w.second) + x);
}

}  // namespace

namespace {

using namespace arangodb;
using namespace irs;
using namespace irs::analysis;

constexpr uint32_t kMinHashes = 1;
constexpr std::string_view kTypeParam{"type"};
constexpr std::string_view kPropertiesParam{"properties"};
constexpr std::string_view kAnalyzerParam{"analyzer"};
constexpr std::string_view kNumHashes{"numHashes"};

const offset kEmptyOffset;

std::pair<std::string_view, velocypack::Slice> ParseAnalyzer(
  velocypack::Slice slice) {
  if (!slice.isObject()) {
    return {};
  }

  const auto typeSlice = slice.get(kTypeParam);

  if (!typeSlice.isString()) {
    IR_FRMT_ERROR(
      "Failed to read '%s' attribute of '%s' member as string while "
      "constructing MinHashTokenStream from VPack arguments",
      kTypeParam.data(), kAnalyzerParam.data());
    return {};
  }

  return {typeSlice.stringView(), slice.get(kPropertiesParam)};
}

bool ParseVPack(velocypack::Slice slice, MinHashTokenStream::Options* opts) {
  IRS_ASSERT(opts);

  if (const auto num_hashesSlice = slice.get(kNumHashes);
      !num_hashesSlice.isNumber()) {
    IR_FRMT_ERROR(
      "Failed to read '%s' attribute as number while "
      "constructing MinHashTokenStream from VPack arguments",
      kNumHashes.data());
    return false;
  } else {
    opts->num_hashes = num_hashesSlice.getNumber<decltype(opts->num_hashes)>();
  }

  if (opts->num_hashes < kMinHashes) {
    IR_FRMT_ERROR(
      "Number of hashes must be at least 1, failed to "
      "construct MinHashTokenStream from VPack arguments",
      kNumHashes.data());
    return false;
  }

  if (const auto analyzerSlice = slice.get(kAnalyzerParam);
      analyzerSlice.isNone() || analyzerSlice.isNull()) {
    opts->analyzer.reset();
    return true;
  } else {
    auto [type, props] = ParseAnalyzer(analyzerSlice);

    if (IsNull(type)) {
      return false;
    }

    if (props.isNone()) {
      props = velocypack::Slice::emptyObjectSlice();
    }

    auto analyzer =
      analyzers::get(type, irs::type<irs::text_format::vpack>::get(),
                     {props.startAs<char>(), props.byteSize()});

    if (!analyzer) {
      // fallback to json format if vpack isn't available
      analyzer = analyzers::get(type, irs::type<irs::text_format::json>::get(),
                                irs::slice_to_string(props));
    }

    if (analyzer) {
      opts->analyzer = std::move(analyzer);
      return true;
    } else {
      IR_FRMT_ERROR(
        "Failed to create analyzer of type '%s' with properties '%s' while "
        "constructing "
        "MinHashTokenStream pipeline_token_stream from VPack arguments",
        type.data(), irs::slice_to_string(props).c_str());
    }
  }

  return false;
}

analyzer::ptr MakeVPack(velocypack::Slice slice) {
  MinHashTokenStream::Options opts;
  if (ParseVPack(slice, &opts)) {
    return std::make_unique<MinHashTokenStream>(std::move(opts));
  }
  return nullptr;
}

irs::analysis::analyzer::ptr MakeVPack(std::string_view args) {
  VPackSlice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return MakeVPack(slice);
}

// `args` is a JSON encoded object with the following attributes:
// "analyzer"(object) the analyzer definition containing "type"(string) and
// optional "properties"(object)
analyzer::ptr MakeJson(std::string_view args) {
  try {
    if (IsNull(args)) {
      IR_FRMT_ERROR("Null arguments while constructing MinHashAnalyzer");
      return nullptr;
    }
    auto vpack = velocypack::Parser::fromJson(args.data(), args.size());
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

bool MakeVPackOptions(const MinHashTokenStream::Options& opts,
                      VPackSlice analyzerSlice, velocypack::Builder* out) {
  velocypack::Slice props = velocypack::Slice::emptyObjectSlice();

  if (analyzerSlice.isObject()) {
    props = analyzerSlice.get(kPropertiesParam);
    if (props.isNone()) {
      props = velocypack::Slice::emptyObjectSlice();
    }
  } else if (!analyzerSlice.isNone()) {
    IR_FRMT_ERROR(
      "Failed to normalize definition of MinHashAnalyzer, 'properties' field "
      "must be object");
    return false;
  }

  velocypack::ObjectBuilder root_scope{out};
  out->add(kNumHashes, velocypack::Value{opts.num_hashes});

  if (props.isObject() && opts.analyzer) {
    const auto type = opts.analyzer->type()().name();
    std::string normalized;

    velocypack::ObjectBuilder analyzer_scope{out, kAnalyzerParam};
    out->add(kTypeParam, velocypack::Value{type});

    if (analyzers::normalize(normalized, type,
                             irs::type<irs::text_format::vpack>::get(),
                             {props.startAs<char>(), props.byteSize()})) {
      out->add(kPropertiesParam,
               velocypack::Slice{
                 reinterpret_cast<const uint8_t*>(normalized.c_str())});

      return true;
    }

    // fallback to json format if vpack isn't available
    if (analyzers::normalize(normalized, type,
                             irs::type<irs::text_format::json>::get(),
                             irs::slice_to_string(props))) {
      auto vpack = velocypack::Parser::fromJson(normalized);
      out->add(kPropertiesParam, vpack->slice());
      return true;
    }
  } else if (!opts.analyzer) {
    out->add(kAnalyzerParam, velocypack::Slice::emptyObjectSlice());
    return true;
  }

  return false;
}

bool NormalizeVPack(velocypack::Slice slice, velocypack::Builder* out) {
  MinHashTokenStream::Options opts;
  if (ParseVPack(slice, &opts)) {
    return MakeVPackOptions(opts, slice.get(kAnalyzerParam), out);
  }
  return false;
}

bool NormalizeVPack(std::string_view args, std::string& definition) {
  VPackSlice slice(reinterpret_cast<const uint8_t*>(args.data()));
  VPackBuilder builder;
  bool res = NormalizeVPack(slice, &builder);
  if (res) {
    definition.assign(builder.slice().startAs<char>(),
                      builder.slice().byteSize());
  }
  return res;
}

bool NormalizeJson(std::string_view args, std::string& definition) {
  try {
    if (IsNull(args)) {
      IR_FRMT_ERROR("Null arguments while normalizing MinHashAnalyzer");
      return false;
    }
    auto vpack = velocypack::Parser::fromJson(args.data(), args.size());
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

MinHashTokenStream::MinHashTokenStream(Options&& opts)
  : analysis::analyzer{irs::type<MinHashTokenStream>::get()},
    opts_{std::move(opts)},
    minhash_{opts.num_hashes} {
  if (!opts_.analyzer) {
    // Fallback to default implementation
    opts_.analyzer = std::make_unique<string_token_stream>();
  }

  term_ = irs::get<term_attribute>(*opts_.analyzer);

  if (IRS_UNLIKELY(!term_)) {
    opts_.analyzer = std::make_unique<empty_analyzer>();
  }

  offset_ = irs::get<offset>(*opts_.analyzer);

  std::get<term_attribute>(attrs_).value = {
    reinterpret_cast<const byte_type*>(buf_.data()), buf_.size()};
}

bool MinHashTokenStream::next() {
  if (begin_ == end_) {
    return false;
  }

  const size_t value = [value = *begin_]() noexcept -> size_t {
    if constexpr (is_big_endian()) {
      return absl::gbswap_64(value);
    } else {
      return value;
    }
  }();

  [[maybe_unused]] const size_t length =
    absl::strings_internal::Base64EscapeInternal(
      reinterpret_cast<const byte_type*>(&value), sizeof value, buf_.data(),
      buf_.size(), absl::strings_internal::kBase64Chars, false);
  IRS_ASSERT(length == buf_.size());

  std::get<increment>(attrs_).value = std::exchange(next_inc_.value, 0);
  ++begin_;

  return true;
}

bool MinHashTokenStream::reset(std::string_view data) {
  begin_ = end_ = {};
  if (opts_.analyzer->reset(data)) {
    ComputeSignature();
    return true;
  }
  return false;
}

void MinHashTokenStream::ComputeSignature() {
  minhash_.Clear();
  next_inc_.value = 1;

  if (opts_.analyzer->next()) {
    IRS_ASSERT(term_);

    const offset* offs = offset_ ? offset_ : &kEmptyOffset;
    auto& [start, end] = std::get<offset>(attrs_);
    start = offs->start;
    end = offs->end;

    do {
      const std::string_view value = ViewCast<char>(term_->value);
      const size_t hash_value = ::CityHash64(value.data(), value.size());

      minhash_.Insert(hash_value);
      end = offs->end;
    } while (opts_.analyzer->next());

    begin_ = std::begin(minhash_);
    end_ = std::end(minhash_);
  }
}

}  // namespace iresearch::analysis
