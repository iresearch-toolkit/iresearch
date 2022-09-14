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

#ifndef FST_EXTENSIONS_SPECIAL_RHO_FST_H_
#define FST_EXTENSIONS_SPECIAL_RHO_FST_H_

#include <cstdint>
#include <istream>
#include <memory>
#include <ostream>
#include <string>

#include <fst/const-fst.h>
#include <fst/matcher-fst.h>
#include <fst/matcher.h>

DECLARE_int64(rho_fst_rho_label);
DECLARE_string(rho_fst_rewrite_mode);

namespace fst {
namespace internal {

template <class Label>
class RhoFstMatcherData {
 public:
  explicit RhoFstMatcherData(
      Label rho_label = FST_FLAGS_rho_fst_rho_label,
      MatcherRewriteMode rewrite_mode =
          RewriteMode(FST_FLAGS_rho_fst_rewrite_mode))
      : rho_label_(rho_label), rewrite_mode_(rewrite_mode) {}

  RhoFstMatcherData(const RhoFstMatcherData &data)
      : rho_label_(data.rho_label_), rewrite_mode_(data.rewrite_mode_) {}

  static RhoFstMatcherData<Label> *Read(std::istream &istrm,
                                        const FstReadOptions &read) {
    auto *data = new RhoFstMatcherData<Label>();
    ReadType(istrm, &data->rho_label_);
    int32_t rewrite_mode;
    ReadType(istrm, &rewrite_mode);
    data->rewrite_mode_ = static_cast<MatcherRewriteMode>(rewrite_mode);
    return data;
  }

  bool Write(std::ostream &ostrm, const FstWriteOptions &opts) const {
    WriteType(ostrm, rho_label_);
    WriteType(ostrm, static_cast<int32_t>(rewrite_mode_));
    return !ostrm ? false : true;
  }

  Label RhoLabel() const { return rho_label_; }

  MatcherRewriteMode RewriteMode() const { return rewrite_mode_; }

 private:
  static MatcherRewriteMode RewriteMode(const std::string &mode) {
    if (mode == "auto") return MATCHER_REWRITE_AUTO;
    if (mode == "always") return MATCHER_REWRITE_ALWAYS;
    if (mode == "never") return MATCHER_REWRITE_NEVER;
    LOG(WARNING) << "RhoFst: Unknown rewrite mode: " << mode << ". "
                 << "Defaulting to auto.";
    return MATCHER_REWRITE_AUTO;
  }

  Label rho_label_;
  MatcherRewriteMode rewrite_mode_;
};

}  // namespace internal

inline constexpr uint8_t kRhoFstMatchInput =
    0x01;  // Input matcher is RhoMatcher.
inline constexpr uint8_t kRhoFstMatchOutput =
    0x02;  // Output matcher is RhoMatcher.

template <class M, uint8_t flags = kRhoFstMatchInput | kRhoFstMatchOutput>
class RhoFstMatcher : public RhoMatcher<M> {
 public:
  using FST = typename M::FST;
  using Arc = typename M::Arc;
  using StateId = typename Arc::StateId;
  using Label = typename Arc::Label;
  using Weight = typename Arc::Weight;
  using MatcherData = internal::RhoFstMatcherData<Label>;

  static constexpr uint8_t kFlags = flags;

  // This makes a copy of the FST.
  RhoFstMatcher(
      const FST &fst, MatchType match_type,
      std::shared_ptr<MatcherData> data = std::make_shared<MatcherData>())
      : RhoMatcher<M>(fst, match_type,
                      RhoLabel(match_type, data ? data->RhoLabel()
                                                : MatcherData().RhoLabel()),
                      data ? data->RewriteMode() : MatcherData().RewriteMode()),
        data_(data) {}

  // This doesn't copy the FST.
  RhoFstMatcher(
      const FST *fst, MatchType match_type,
      std::shared_ptr<MatcherData> data = std::make_shared<MatcherData>())
      : RhoMatcher<M>(fst, match_type,
                      RhoLabel(match_type, data ? data->RhoLabel()
                                                : MatcherData().RhoLabel()),
                      data ? data->RewriteMode() : MatcherData().RewriteMode()),
        data_(data) {}

  // This makes a copy of the FST.
  RhoFstMatcher(const RhoFstMatcher<M, flags> &matcher, bool safe = false)
      : RhoMatcher<M>(matcher, safe), data_(matcher.data_) {}

  RhoFstMatcher<M, flags> *Copy(bool safe = false) const override {
    return new RhoFstMatcher<M, flags>(*this, safe);
  }

  const MatcherData *GetData() const { return data_.get(); }

  std::shared_ptr<MatcherData> GetSharedData() const { return data_; }

 private:
  static Label RhoLabel(MatchType match_type, Label label) {
    if (match_type == MATCH_INPUT && flags & kRhoFstMatchInput) return label;
    if (match_type == MATCH_OUTPUT && flags & kRhoFstMatchOutput) return label;
    return kNoLabel;
  }

  std::shared_ptr<MatcherData> data_;
};

inline constexpr char rho_fst_type[] = "rho";
inline constexpr char input_rho_fst_type[] = "input_rho";
inline constexpr char output_rho_fst_type[] = "output_rho";

template <class Arc>
using RhoFst =
    MatcherFst<ConstFst<Arc>, RhoFstMatcher<SortedMatcher<ConstFst<Arc>>>,
               rho_fst_type>;

using StdRhoFst = RhoFst<StdArc>;

template <class Arc>
using InputRhoFst =
    MatcherFst<ConstFst<Arc>,
               RhoFstMatcher<SortedMatcher<ConstFst<Arc>>, kRhoFstMatchInput>,
               input_rho_fst_type>;

using StdInputRhoFst = InputRhoFst<StdArc>;

template <class Arc>
using OutputRhoFst =
    MatcherFst<ConstFst<Arc>,
               RhoFstMatcher<SortedMatcher<ConstFst<Arc>>, kRhoFstMatchOutput>,
               output_rho_fst_type>;

using StdOutputRhoFst = OutputRhoFst<StdArc>;

}  // namespace fst

#endif  // FST_EXTENSIONS_SPECIAL_RHO_FST_H_
