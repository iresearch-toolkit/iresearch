////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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

#include <fst/fst.h>
#include <fst/vector-fst.h>
#include <fst/expanded-fst.h>

#include "shared.hpp"
#include "store/data_output.hpp"
#include "store/data_input.hpp"
#include "utils/automaton.hpp"
#include "utils/misc.hpp"
#include "utils/fstext/fst_string_ref_weight.h"

#ifndef IRESEARCH_CONST_FST_H
#define IRESEARCH_CONST_FST_H

namespace fst {
namespace fstext {

namespace detail {
DEFINE_HAS_MEMBER(FinalRef);
}

template<typename W, typename L = int32_t>
struct Transition {
  using Weight = W;
  using Label = L;
  using StateId = int32_t;

  static const std::string &Type() {
    static const std::string type("fst::Transition");
    return type;
  }

  Label ilabel{fst::kNoLabel};
  StateId nextstate{fst::kNoStateId};
  union {
    Weight weight{};
    fsa::EmptyLabel<Label> olabel;
  };

  constexpr Transition() = default;

  constexpr Transition(Label ilabel, StateId nextstate)
    : ilabel(ilabel),
      nextstate(nextstate) {
  }

  // satisfy openfst API
  constexpr Transition(Label ilabel, Label, Weight, StateId nextstate)
    : ilabel(ilabel),
      nextstate(nextstate) {
  }

  // satisfy openfst API
  constexpr Transition(Label ilabel, Label, StateId nextstate)
    : ilabel(ilabel),
      nextstate(nextstate) {
  }
}; // Transition


template<typename Arc> class ImmutableFst;

constexpr static const char kImmutableFstType[] = "fst::fstext::immutable";

// States and arcs each implemented by single arrays, templated on the
// Arc definition
template <class A>
class ImmutableFstImpl : public internal::FstImpl<A> {
 public:
  using Arc = A;
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;

  using internal::FstImpl<A>::SetInputSymbols;
  using internal::FstImpl<A>::SetOutputSymbols;
  using internal::FstImpl<A>::SetType;
  using internal::FstImpl<A>::SetProperties;
  using internal::FstImpl<A>::Properties;

  ImmutableFstImpl()
      : narcs_(0),
        nstates_(0),
        start_(kNoStateId) {
    SetType(kImmutableFstType);
    SetProperties(kNullProperties | kStaticProperties);
  }

  StateId Start() const noexcept { return start_; }

  Weight Final(StateId s) const noexcept { return states_[s].weight; }

  StateId NumStates() const noexcept { return nstates_; }

  size_t NumArcs(StateId s) const noexcept { return states_[s].narcs; }

  size_t NumInputEpsilons(StateId) const noexcept { return 0; }

  size_t NumOutputEpsilons(StateId) const noexcept { return 0; }

  static ImmutableFstImpl<Arc>* Read(std::istream &strm, const FstReadOptions &opts);

  const Arc* Arcs(StateId s) const noexcept { return states_[s].arcs; }

  // Provide information needed for generic state iterator.
  void InitStateIterator(StateIteratorData<Arc> *data) const noexcept {
    data->base = nullptr;
    data->nstates = nstates_;
  }

  // Provide information needed for the generic arc iterator.
  void InitArcIterator(StateId s, ArcIteratorData<Arc> *data) const noexcept {
    data->base = nullptr;
    data->arcs = states_[s].arcs;
    data->narcs = states_[s].narcs;
    data->ref_count = nullptr;
  }

 private:
  // Used to find narcs_ and nstates_ in Write.
  friend class ImmutableFst<Arc>;

  // States implemented by array *states_ below, arcs by (single) *arcs_.
  struct State {
    const Arc* arcs; // Start of state's arcs in *arcs_.
    size_t narcs;    // Number of arcs (per state).
    Weight weight;   // Final weight.
  };

  // Properties always true of this FST class.
  static constexpr uint64 kStaticProperties = kExpanded;

  std::vector<State> states_;
  std::vector<Arc> arcs_;
  std::unique_ptr<irs::byte_type[]> weights_;
  size_t narcs_;                               // Number of arcs.
  StateId nstates_;                            // Number of states.
  StateId start_;                              // Initial state.

  ImmutableFstImpl(const ImmutableFstImpl &) = delete;
  ImmutableFstImpl &operator=(const ImmutableFstImpl &) = delete;
};

template <class Arc>
ImmutableFstImpl<Arc>* ImmutableFstImpl<Arc>::Read(
    std::istream &strm,
    const FstReadOptions& /*opts*/) {
#ifdef IRESEARCH_DEBUG
  auto* strmImpl = dynamic_cast<irs::input_buf*>(strm.rdbuf());
#else
  auto* strmImpl = static_cast<irs::input_buf*>(strm.rdbuf());
#endif
  assert(strmImpl && strmImpl->internal());

  auto* stream = strmImpl->internal();

  auto impl = std::make_unique<ImmutableFstImpl<Arc>>();

  // read header
  const size_t total_weight_size = stream->read_long();
  const uint64_t props = stream->read_long();
  const StateId start = stream->read_vint();
  const size_t nstates = stream->read_vlong();
  const size_t narcs = stream->read_vlong();

  std::vector<State> states(nstates);
  std::vector<Arc> arcs(narcs);
  auto weights = std::make_unique<irs::byte_type[]>(total_weight_size);

  // read states & arcs
  auto* weight = weights.get();
  auto* arc = arcs.data();
  for (auto& state : states) {
    state.arc = arc;
    state.narcs = stream->read_vlong(); // FIXME total number of arcs can be encoded with 1 byte
    state.weight = { weight, stream->read_vlong() };

    weight += state.weight.Size();

    for (auto* end = arc + state.narcs; arc != end; ++arc) {
      arc->ilabel = stream->read_byte();
      arc->nextstate = stream->read_vint();
      arc->weight = { weight, stream->read_vlong() };
      weight += arc->weight.Size();
    }
  }

  // read weights
  stream->read_bytes(weight, total_weight_size);

  // noexcept block
  impl->properties_ = props;
  impl->start_ = start;
  impl->nstates_ = nstates;
  impl->narcs_ = narcs;
  impl->states_ = std::move(states);
  impl->arcs_ = std::move(arcs);
  impl->weights_ = std::move(weights);

  return impl.release();
}

// Simple concrete immutable FST. This class attaches interface to
// implementation and handles reference counting, delegating most methods to
// ImplToExpandedFst. The unsigned type U is used to represent indices into the
// arc array (default declared in fst-decl.h).
template <class A>
class ImmutableFst : public ImplToExpandedFst<ImmutableFstImpl<A>> {
 public:
  using Arc = A;
  using StateId = typename Arc::StateId;

  using Impl = ImmutableFstImpl<A>;

  friend class StateIterator<ImmutableFst<Arc>>;
  friend class ArcIterator<ImmutableFst<Arc>>;

  template <class F, class G>
  void friend Cast(const F &, G *);

  ImmutableFst() : ImplToExpandedFst<Impl>(std::make_shared<Impl>()) {}

  // Reads a ConstFst from an input stream, returning nullptr on error.
  static ImmutableFst<A> *Read(std::istream &strm,
                           const FstReadOptions &opts) {
    auto *impl = Impl::Read(strm, opts);
    return impl ? new ImmutableFst<A>(std::shared_ptr<Impl>(impl))
                : nullptr;
  }

  // Read a ConstFst from a file; return nullptr on error; empty filename reads
  // from standard input.
  static ImmutableFst<A> *Read(const std::string &filename) {
    auto *impl = ImplToExpandedFst<Impl>::Read(filename);
    return impl ? new ImmutableFst<A>(std::shared_ptr<Impl>(impl))
                : nullptr;
  }

  bool Write(std::ostream& strm, const FstWriteOptions& opts) const override {
    return WriteFst(*this, strm, opts);
  }

  bool Write(const std::string &filename) const override {
    return Fst<Arc>::WriteFile(filename);
  }

  template<typename FST>
  static bool WriteFst(const FST& fst,
                       std::ostream &strm,
                       const FstWriteOptions &opts);

  void InitStateIterator(StateIteratorData<Arc> *data) const override {
    GetImpl()->InitStateIterator(data);
  }

  void InitArcIterator(StateId s, ArcIteratorData<Arc> *data) const override {
    GetImpl()->InitArcIterator(s, data);
  }

 private:
  explicit ImmutableFst(std::shared_ptr<Impl> impl)
      : ImplToExpandedFst<Impl>(impl) {}

  using ImplToFst<Impl, ExpandedFst<Arc>>::GetImpl;

  ImmutableFst &operator=(const ImmutableFst &) = delete;
};

// Writes FST in Const format, potentially with a pass over the machine before
// writing to compute number of states and arcs.
template <class A>
template <class FST>
bool ImmutableFst<A>::WriteFst(
    const FST& fst,
    std::ostream &strm,
    const FstWriteOptions& /*opts*/) {
#ifdef IRESEARCH_DEBUG
  auto* strmImpl = dynamic_cast<irs::output_buf*>(strm.rdbuf());
#else
  auto* strmImpl = static_cast<irs::output_buf*>(strm.rdbuf());
#endif
  assert(strmImpl && strmImpl->internal());
  auto* fstImpl = fst.GetImpl();

  const auto properties =
      fst.Properties(kCopyProperties, true) |
      fstext::ImmutableFstImpl<Arc>::kStaticProperties;

  // FIXME we can precompute 'num_arcs'
  size_t num_arcs = 0;
  for (StateIterator<FST> siter(fst); !siter.Done(); siter.Next()) {
    num_arcs += fstImpl->NumArcs(siter.Value());
  }

  auto* stream = strmImpl->internal();

  // write header
  stream->write_long(properties);
  stream->write_long(total_weight_size);
  stream->write_vint(fst.Start());
  stream->write_vlong(fstImpl->NumStates());
  stream->write_vlong(num_arcs);
  // FIXME write total weight size

  // FIXME SIMD???
  // write states & arcs
  for (StateIterator<FST> siter(fst); !siter.Done(); siter.Next()) {
    const StateId s = siter.Value();
    const size_t narcs = fstImpl->NumArcs(s);

    size_t weight_size = 0;
    if constexpr (detail::has_member_FinalRef_v<typename FST::Impl>) {
      weight_size = fstImpl->FinalRef(s).Size();
    } else {
      weight_size = fstImpl->Final(s).Size();
    }

    stream->write_byte(static_cast<irs::byte_type>(narcs & 0xFF)); // FIXME make it optional
    stream->write_vlong(weight_size);

    for (ArcIterator<FST> aiter(fst, s); !aiter.Done(); aiter.Next()) {
      const auto& arc = aiter.Value();
      const size_t weight_size = arc.weight.Size();

      assert(arc.ilabel <= std::numeric_limits<irs::byte_type>::max());
      stream->write_byte(static_cast<irs::byte_type>(arc.ilabel & 0xFF)); // FIXME make it optional?
      stream->write_vint(arc.nextstate);
      stream->write_vlong(weight_size);
    }
  }

  // write weights
  for (StateIterator<FST> siter(fst); !siter.Done(); siter.Next()) {
    const StateId s = siter.Value();

    if constexpr (detail::has_member_FinalRef_v<typename FST::Impl>) {
      const auto& weight = fstImpl->FinalRef(s);
      if (!weight.Empty()) {
        stream->write_bytes(weight.c_str(), weight.Size());
      }
    } else {
      const auto weight = fstImpl->Final(s);
      if (!weight.Empty()) {
        stream->write_bytes(weight.c_str(), weight.Size());
      }
    }

    for (ArcIterator<FST> aiter(fst, s); !aiter.Done(); aiter.Next()) {
      const auto& arc = aiter.Value();

      if (!arc.weight.Empty()) {
        stream->write_bytes(arc.weight.c_str(), arc.weight.Size());
      }
    }
  }

  return true;
}

} // fstext

// Specialization for ConstFst; see generic version in fst.h for sample usage
// (but use the ConstFst type instead). This version should inline.
template <class Arc>
class StateIterator<fstext::ImmutableFst<Arc>> {
 public:
  using StateId = typename Arc::StateId;

  explicit StateIterator(const fstext::ImmutableFst<Arc> &fst)
      : nstates_(fst.GetImpl()->NumStates()), s_(0) {}

  bool Done() const noexcept { return s_ >= nstates_; }

  StateId Value() const noexcept { return s_; }

  void Next() noexcept { ++s_; }

  void Reset() noexcept { s_ = 0; }

 private:
  const StateId nstates_;
  StateId s_;
};

// Specialization for ConstFst; see generic version in fst.h for sample usage
// (but use the ConstFst type instead). This version should inline.
template <class Arc>
class ArcIterator<fstext::ImmutableFst<Arc>> {
 public:
  using StateId = typename Arc::StateId;

  ArcIterator(const fstext::ImmutableFst<Arc> &fst, StateId s)
      : arcs_(fst.GetImpl()->Arcs(s)),
        begin_(arcs_),
        end_(arcs_ + fst.GetImpl()->NumArcs(s)) {
  }

  bool Done() const noexcept { return begin_ >= end_; }

  const Arc& Value() const noexcept { return *begin_; }

  void Next() noexcept { ++begin_; }

  size_t Position() const noexcept {
    return size_t(std::distance(arcs_, begin_));
  }

  void Reset() noexcept { begin_ = arcs_; }

  void Seek(size_t a) noexcept { begin_ = arcs_ + a; }

  constexpr uint32 Flags() const { return kArcValueFlags; }

  void SetFlags(uint32, uint32) {}

 private:
  const Arc* arcs_;
  const Arc* begin_;
  const Arc* end_;
};

} // fst

#endif // IRESEARCH_CONST_FST_H

