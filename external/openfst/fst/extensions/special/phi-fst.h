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

#ifndef FST_EXTENSIONS_SPECIAL_PHI_FST_H_
#define FST_EXTENSIONS_SPECIAL_PHI_FST_H_

#include <cstdint>
#include <istream>
#include <memory>
#include <ostream>
#include <string>

#include <fst/const-fst.h>
#include <fst/matcher-fst.h>
#include <fst/matcher.h>

DECLARE_int64(phi_fst_phi_label);
DECLARE_bool(phi_fst_phi_loop);
DECLARE_string(phi_fst_rewrite_mode);

namespace fst {
namespace internal {

template <class Label>
class PhiFstMatcherData {
 public:
  explicit PhiFstMatcherData(
      Label phi_label = FST_FLAGS_phi_fst_phi_label,
      bool phi_loop = FST_FLAGS_phi_fst_phi_loop,
      MatcherRewriteMode rewrite_mode =
          RewriteMode(FST_FLAGS_phi_fst_rewrite_mode))
      : phi_label_(phi_label),
        phi_loop_(phi_loop),
        rewrite_mode_(rewrite_mode) {}

  PhiFstMatcherData(const PhiFstMatcherData &data)
      : phi_label_(data.phi_label_),
        phi_loop_(data.phi_loop_),
        rewrite_mode_(data.rewrite_mode_) {}

  static PhiFstMatcherData<Label> *Read(std::istream &istrm,
                                        const FstReadOptions &read) {
    auto *data = new PhiFstMatcherData<Label>();
    ReadType(istrm, &data->phi_label_);
    ReadType(istrm, &data->phi_loop_);
    int32_t rewrite_mode;
    ReadType(istrm, &rewrite_mode);
    data->rewrite_mode_ = static_cast<MatcherRewriteMode>(rewrite_mode);
    return data;
  }

  bool Write(std::ostream &ostrm, const FstWriteOptions &opts) const {
    WriteType(ostrm, phi_label_);
    WriteType(ostrm, phi_loop_);
    WriteType(ostrm, static_cast<int32_t>(rewrite_mode_));
    return !ostrm ? false : true;
  }

  Label PhiLabel() const { return phi_label_; }

  bool PhiLoop() const { return phi_loop_; }

  MatcherRewriteMode RewriteMode() const { return rewrite_mode_; }

 private:
  static MatcherRewriteMode RewriteMode(const std::string &mode) {
    if (mode == "auto") return MATCHER_REWRITE_AUTO;
    if (mode == "always") return MATCHER_REWRITE_ALWAYS;
    if (mode == "never") return MATCHER_REWRITE_NEVER;
    LOG(WARNING) << "PhiFst: Unknown rewrite mode: " << mode << ". "
                 << "Defaulting to auto.";
    return MATCHER_REWRITE_AUTO;
  }

  Label phi_label_;
  bool phi_loop_;
  MatcherRewriteMode rewrite_mode_;
};

}  // namespace internal

inline constexpr uint8_t kPhiFstMatchInput =
    0x01;  // Input matcher is PhiMatcher.
inline constexpr uint8_t kPhiFstMatchOutput =
    0x02;  // Output matcher is PhiMatcher.

template <class M, uint8_t flags = kPhiFstMatchInput | kPhiFstMatchOutput>
class PhiFstMatcher : public PhiMatcher<M> {
 public:
  using FST = typename M::FST;
  using Arc = typename M::Arc;
  using StateId = typename Arc::StateId;
  using Label = typename Arc::Label;
  using Weight = typename Arc::Weight;
  using MatcherData = internal::PhiFstMatcherData<Label>;

  static constexpr uint8_t kFlags = flags;

  // This makes a copy of the FST.
  PhiFstMatcher(
      const FST &fst, MatchType match_type,
      std::shared_ptr<MatcherData> data = std::make_shared<MatcherData>())
      : PhiMatcher<M>(fst, match_type,
                      PhiLabel(match_type, data ? data->PhiLabel()
                                                : MatcherData().PhiLabel()),
                      data ? data->PhiLoop() : MatcherData().PhiLoop(),
                      data ? data->RewriteMode() : MatcherData().RewriteMode()),
        data_(data) {}

  // This doesn't copy the FST.
  PhiFstMatcher(
      const FST *fst, MatchType match_type,
      std::shared_ptr<MatcherData> data = std::make_shared<MatcherData>())
      : PhiMatcher<M>(fst, match_type,
                      PhiLabel(match_type, data ? data->PhiLabel()
                                                : MatcherData().PhiLabel()),
                      data ? data->PhiLoop() : MatcherData().PhiLoop(),
                      data ? data->RewriteMode() : MatcherData().RewriteMode()),
        data_(data) {}

  // This makes a copy of the FST.
  PhiFstMatcher(const PhiFstMatcher<M, flags> &matcher, bool safe = false)
      : PhiMatcher<M>(matcher, safe), data_(matcher.data_) {}

  PhiFstMatcher<M, flags> *Copy(bool safe = false) const override {
    return new PhiFstMatcher<M, flags>(*this, safe);
  }

  const MatcherData *GetData() const { return data_.get(); }

  std::shared_ptr<MatcherData> GetSharedData() const { return data_; }

 private:
  static Label PhiLabel(MatchType match_type, Label label) {
    if (match_type == MATCH_INPUT && flags & kPhiFstMatchInput) return label;
    if (match_type == MATCH_OUTPUT && flags & kPhiFstMatchOutput) return label;
    return kNoLabel;
  }

  std::shared_ptr<MatcherData> data_;
};

inline constexpr char phi_fst_type[] = "phi";
inline constexpr char input_phi_fst_type[] = "input_phi";
inline constexpr char output_phi_fst_type[] = "output_phi";

template <class Arc>
using PhiFst =
    MatcherFst<ConstFst<Arc>, PhiFstMatcher<SortedMatcher<ConstFst<Arc>>>,
               phi_fst_type>;

using StdPhiFst = PhiFst<StdArc>;

template <class Arc>
using InputPhiFst =
    MatcherFst<ConstFst<Arc>,
               PhiFstMatcher<SortedMatcher<ConstFst<Arc>>, kPhiFstMatchInput>,
               input_phi_fst_type>;

using StdInputPhiFst = InputPhiFst<StdArc>;

template <class Arc>
using OutputPhiFst =
    MatcherFst<ConstFst<Arc>,
               PhiFstMatcher<SortedMatcher<ConstFst<Arc>>, kPhiFstMatchOutput>,
               output_phi_fst_type>;

using StdOutputPhiFst = OutputPhiFst<StdArc>;

}  // namespace fst

#endif  // FST_EXTENSIONS_SPECIAL_PHI_FST_H_
