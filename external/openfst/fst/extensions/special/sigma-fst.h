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

#ifndef FST_EXTENSIONS_SPECIAL_SIGMA_FST_H_
#define FST_EXTENSIONS_SPECIAL_SIGMA_FST_H_

#include <cstdint>
#include <istream>
#include <memory>
#include <ostream>
#include <string>

#include <fst/const-fst.h>
#include <fst/matcher-fst.h>
#include <fst/matcher.h>

DECLARE_int64(sigma_fst_sigma_label);
DECLARE_string(sigma_fst_rewrite_mode);

namespace fst {
namespace internal {

template <class Label>
class SigmaFstMatcherData {
 public:
  explicit SigmaFstMatcherData(
      Label sigma_label = FST_FLAGS_sigma_fst_sigma_label,
      MatcherRewriteMode rewrite_mode =
          RewriteMode(FST_FLAGS_sigma_fst_rewrite_mode))
      : sigma_label_(sigma_label), rewrite_mode_(rewrite_mode) {}

  SigmaFstMatcherData(const SigmaFstMatcherData &data)
      : sigma_label_(data.sigma_label_), rewrite_mode_(data.rewrite_mode_) {}

  static SigmaFstMatcherData<Label> *Read(std::istream &istrm,
                                          const FstReadOptions &read) {
    auto *data = new SigmaFstMatcherData<Label>();
    ReadType(istrm, &data->sigma_label_);
    int32_t rewrite_mode;
    ReadType(istrm, &rewrite_mode);
    data->rewrite_mode_ = static_cast<MatcherRewriteMode>(rewrite_mode);
    return data;
  }

  bool Write(std::ostream &ostrm, const FstWriteOptions &opts) const {
    WriteType(ostrm, sigma_label_);
    WriteType(ostrm, static_cast<int32_t>(rewrite_mode_));
    return !ostrm ? false : true;
  }

  Label SigmaLabel() const { return sigma_label_; }

  MatcherRewriteMode RewriteMode() const { return rewrite_mode_; }

 private:
  static MatcherRewriteMode RewriteMode(const std::string &mode) {
    if (mode == "auto") return MATCHER_REWRITE_AUTO;
    if (mode == "always") return MATCHER_REWRITE_ALWAYS;
    if (mode == "never") return MATCHER_REWRITE_NEVER;
    LOG(WARNING) << "SigmaFst: Unknown rewrite mode: " << mode << ". "
                 << "Defaulting to auto.";
    return MATCHER_REWRITE_AUTO;
  }

  Label sigma_label_;
  MatcherRewriteMode rewrite_mode_;
};

}  // namespace internal

inline constexpr uint8_t kSigmaFstMatchInput =
    0x01;  // Input matcher is SigmaMatcher.
inline constexpr uint8_t kSigmaFstMatchOutput =
    0x02;  // Output matcher is SigmaMatcher.

template <class M, uint8_t flags = kSigmaFstMatchInput | kSigmaFstMatchOutput>
class SigmaFstMatcher : public SigmaMatcher<M> {
 public:
  using FST = typename M::FST;
  using Arc = typename M::Arc;
  using StateId = typename Arc::StateId;
  using Label = typename Arc::Label;
  using Weight = typename Arc::Weight;
  using MatcherData = internal::SigmaFstMatcherData<Label>;

  static constexpr uint8_t kFlags = flags;

  // This makes a copy of the FST.
  SigmaFstMatcher(
      const FST &fst, MatchType match_type,
      std::shared_ptr<MatcherData> data = std::make_shared<MatcherData>())
      : SigmaMatcher<M>(
            fst, match_type,
            SigmaLabel(match_type,
                       data ? data->SigmaLabel() : MatcherData().SigmaLabel()),
            data ? data->RewriteMode() : MatcherData().RewriteMode()),
        data_(data) {}

  // This doesn't copy the FST.
  SigmaFstMatcher(
      const FST *fst, MatchType match_type,
      std::shared_ptr<MatcherData> data = std::make_shared<MatcherData>())
      : SigmaMatcher<M>(
            fst, match_type,
            SigmaLabel(match_type,
                       data ? data->SigmaLabel() : MatcherData().SigmaLabel()),
            data ? data->RewriteMode() : MatcherData().RewriteMode()),
        data_(data) {}

  // This makes a copy of the FST.
  SigmaFstMatcher(const SigmaFstMatcher<M, flags> &matcher, bool safe = false)
      : SigmaMatcher<M>(matcher, safe), data_(matcher.data_) {}

  SigmaFstMatcher<M, flags> *Copy(bool safe = false) const override {
    return new SigmaFstMatcher<M, flags>(*this, safe);
  }

  const MatcherData *GetData() const { return data_.get(); }

  std::shared_ptr<MatcherData> GetSharedData() const { return data_; }

 private:
  static Label SigmaLabel(MatchType match_type, Label label) {
    if (match_type == MATCH_INPUT && flags & kSigmaFstMatchInput) return label;
    if (match_type == MATCH_OUTPUT && flags & kSigmaFstMatchOutput)
      return label;
    return kNoLabel;
  }

  std::shared_ptr<MatcherData> data_;
};

inline constexpr char sigma_fst_type[] = "sigma";
inline constexpr char input_sigma_fst_type[] = "input_sigma";
inline constexpr char output_sigma_fst_type[] = "output_sigma";

template <class Arc>
using SigmaFst =
    MatcherFst<ConstFst<Arc>, SigmaFstMatcher<SortedMatcher<ConstFst<Arc>>>,
               sigma_fst_type>;

using StdSigmaFst = SigmaFst<StdArc>;

template <class Arc>
using InputSigmaFst = MatcherFst<
    ConstFst<Arc>,
    SigmaFstMatcher<SortedMatcher<ConstFst<Arc>>, kSigmaFstMatchInput>,
    input_sigma_fst_type>;

using StdInputSigmaFst = InputSigmaFst<StdArc>;

template <class Arc>
using OutputSigmaFst = MatcherFst<
    ConstFst<Arc>,
    SigmaFstMatcher<SortedMatcher<ConstFst<Arc>>, kSigmaFstMatchOutput>,
    output_sigma_fst_type>;

using StdOutputSigmaFst = OutputSigmaFst<StdArc>;

}  // namespace fst

#endif  // FST_EXTENSIONS_SPECIAL_SIGMA_FST_H_
