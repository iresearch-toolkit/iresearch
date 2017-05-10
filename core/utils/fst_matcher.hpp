//
// IResearch search engine 
// 
// Copyright (c) 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#ifndef IRESEARCH_FST_MATCHER_H
#define IRESEARCH_FST_MATCHER_H

#include "shared.hpp"

#include <fst/matcher.h>

NS_BEGIN(fst)

// This class discards any implicit matches (e.g., the implicit epsilon
// self-loops in the SortedMatcher). Matchers are most often used in
// composition/intersection where the implicit matches are needed
// e.g. for epsilon processing. However, if a matcher is simply being
// used to look-up explicit label matches, this class saves the user
// from having to check for and discard the unwanted implicit matches
// themselves.
template <class M>
class explicit_matcher : public MatcherBase<typename M::Arc> {
 public:
  using FST = typename M::FST;
  using Arc = typename FST::Arc;
  using Label = typename Arc::Label;
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;

  explicit explicit_matcher(M& matcher)
    : matcher_(&matcher),
#ifdef IRESEARCH_DEBUG
      match_type_(matcher.Type(true)), // test fst properties
#else
      match_type_(matcher.Type(false)),
#endif
      error_(false) {
  }

  virtual explicit_matcher<M>* Copy(bool safe = false) const {
    UNUSED(safe);
    return new explicit_matcher<M>(*this);
  }

  virtual MatchType Type(bool test) const { return matcher_->Type(test); }

  void SetState(StateId s) {
    matcher_->SetState(s);
  }

  bool Find(Label match_label) {
    matcher_->Find(match_label);
    CheckArc();
    return !Done();
  }

  bool Done() const { return matcher_->Done(); }

  const Arc& Value() const { return matcher_->Value(); }

  void Next() {
    matcher_->Next();
    CheckArc();
  }

  Weight Final(StateId s) const { return matcher_->Final(s); }

  ssize_t Priority(StateId s) { return  matcher_->Priority(s); }

  virtual const FST &GetFst() const { return matcher_->GetFst(); }

  virtual uint64 Properties(uint64 inprops) const {
    return matcher_->Properties(inprops);
  }

  virtual uint32 Flags() const {
    return matcher_->Flags();
  }

 private:
  // Checks current arc if available and explicit. If not available, stops. If
  // not explicit, checks next ones.
  void CheckArc() {
    for (; !matcher_->Done(); matcher_->Next()) {
      const auto label = match_type_ == MATCH_INPUT ? matcher_->Value().ilabel
                                                    : matcher_->Value().olabel;
      if (label != kNoLabel) return;
    }
  }

  virtual void SetState_(StateId s) { SetState(s); }
  virtual bool Find_(Label label) { return Find(label); }
  virtual bool Done_() const { return Done(); }
  virtual const Arc& Value_() const { return Value(); }
  virtual void Next_() { Next(); }
  virtual Weight Final_(StateId s) const { return Final(s); }
  virtual ssize_t Priority_(StateId s) { return Priority(s); }

  M* matcher_;
  MatchType match_type_;  // Type of match requested.
  bool error_;            // Error encountered?
}; // explicit_matcher

NS_END // fst

#endif  // IRESEARCH_FST_MATHCER_H
