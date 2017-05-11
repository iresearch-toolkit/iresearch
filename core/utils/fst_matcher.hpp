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

NS_BEGIN(fst)

// This class discards any implicit matches (e.g., the implicit epsilon
// self-loops in the SortedMatcher). Matchers are most often used in
// composition/intersection where the implicit matches are needed
// e.g. for epsilon processing. However, if a matcher is simply being
// used to look-up explicit label matches, this class saves the user
// from having to check for and discard the unwanted implicit matches
// themselves.
template <class MatcherBase>
class explicit_matcher final : public MatcherBase {
 public:
  typedef typename MatcherBase::FST FST;
  typedef typename FST::Arc Arc;
  typedef typename Arc::Label Label;
  typedef typename Arc::StateId StateId;
  typedef typename Arc::Weight Weight;

  template<typename... Args>
  explicit explicit_matcher(Args&&... args)
    : MatcherBase(std::forward<Args>(args)...),
#ifdef IRESEARCH_DEBUG
      match_type_(MatcherBase::Type(true)) // test fst properties
#else
      match_type_(MatcherBase::Type(false)) // read type without checks
#endif
  { }

  explicit_matcher(const explicit_matcher& rhs, bool safe = false)
    : MatcherBase(rhs, safe),
      match_type_(rhs.match_type_) {
  }

  explicit_matcher* Copy(bool safe = false) const {
    return new explicit_matcher(*this, safe);
  }

  MatchType Type(bool test) const {
    return MatcherBase::Type(test);
  }

  void SetState(StateId s) {
    MatcherBase::SetState(s);
  }

  bool Find(Label match_label) {
    MatcherBase::Find(match_label);
    CheckArc();
    return !Done();
  }

  bool Done() const { return MatcherBase::Done(); }

  const Arc& Value() const { return MatcherBase::Value(); }

  void Next() {
    MatcherBase::Next();
    CheckArc();
  }

  Weight Final(StateId s) const {
    return MatcherBase::Final(s);
  }

  ssize_t Priority(StateId s) {
    return  MatcherBase::Priority(s);
  }

  const FST& GetFst() const {
    return MatcherBase::GetFst();
  }

  uint64 Properties(uint64 inprops) const {
    return MatcherBase::Properties(inprops);
  }

  uint32 Flags() const {
    return MatcherBase::Flags();
  }

 private:
  // Checks current arc if available and explicit. If not available, stops. If
  // not explicit, checks next ones.
  void CheckArc() {
    for (; !MatcherBase::Done(); MatcherBase::Next()) {
      const auto label = match_type_ == MATCH_INPUT ? MatcherBase::Value().ilabel
                                                    : MatcherBase::Value().olabel;
      if (label != kNoLabel) return;
    }
  }

  MatchType match_type_;  // Type of match requested.
}; // explicit_matcher

NS_END // fst

#endif  // IRESEARCH_FST_MATHCER_H
