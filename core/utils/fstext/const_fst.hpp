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

  explicit ImmutableFstImpl(const Fst<Arc> &fst);

  StateId Start() const noexcept { return start_; }

  Weight Final(StateId s) const noexcept { return states_[s].weight; }

  StateId NumStates() const noexcept { return nstates_; }

  size_t NumArcs(StateId s) const noexcept { return states_[s].narcs; }

  size_t NumInputEpsilons(StateId) const noexcept { return 0; }

  size_t NumOutputEpsilons(StateId) const noexcept { return 0; }

  static ImmutableFstImpl<Arc>* Read(std::istream &strm, const FstReadOptions &opts);

  const Arc* Arcs(StateId s) const noexcept { return arcs_.data() + states_[s].pos; }

  // Provide information needed for generic state iterator.
  void InitStateIterator(StateIteratorData<Arc> *data) const noexcept {
    data->base = nullptr;
    data->nstates = nstates_;
  }

  // Provide information needed for the generic arc iterator.
  void InitArcIterator(StateId s, ArcIteratorData<Arc> *data) const noexcept {
    data->base = nullptr;
    data->arcs = arcs_.data() + states_[s].pos;
    data->narcs = states_[s].narcs;
    data->ref_count = nullptr;
  }

 private:
  // Used to find narcs_ and nstates_ in Write.
  friend class ImmutableFst<Arc>;

  // States implemented by array *states_ below, arcs by (single) *arcs_.
  struct State {
    Weight weight{Weight::Zero()}; // Final weight.
    size_t pos;                    // Start of state's arcs in *arcs_.
    size_t narcs;                  // Number of arcs (per state).
    size_t weight_offset;
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
ImmutableFstImpl<Arc>::ImmutableFstImpl(const Fst<Arc> &fst)
    : narcs_(0), nstates_(0) {
  SetType(kImmutableFstType);
  SetInputSymbols(fst.InputSymbols());
  SetOutputSymbols(fst.OutputSymbols());
  start_ = fst.Start();
  // Counts states and arcs.
  for (StateIterator<Fst<Arc>> siter(fst); !siter.Done(); siter.Next()) {
    ++nstates_;
    narcs_ += fst.NumArcs(siter.Value());
  }
  //states_region_.reset(MappedFile::Allocate(nstates_ * sizeof(*states_)));
  //arcs_region_.reset(MappedFile::Allocate(narcs_ * sizeof(*arcs_)));
  //states_ = reinterpret_cast<ConstState *>(states_region_->mutable_data());
  //arcs_ = reinterpret_cast<Arc *>(arcs_region_->mutable_data());
  size_t pos = 0;
  for (StateId s = 0; s < nstates_; ++s) {
    states_[s].weight = fst.Final(s);
    states_[s].pos = pos;
    states_[s].narcs = 0;
    for (ArcIterator<Fst<Arc>> aiter(fst, s); !aiter.Done(); aiter.Next()) {
      const auto &arc = aiter.Value();
      ++states_[s].narcs;
      arcs_[pos] = arc;
      ++pos;
    }
  }
  const auto props =
      fst.Properties(kMutable, false)
          ? fst.Properties(kCopyProperties, true)
          : CheckProperties(
                fst, kCopyProperties & ~kWeightedCycles & ~kUnweightedCycles,
                kCopyProperties);
  SetProperties(props | kStaticProperties);
}

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
  const uint64_t props = stream->read_long();
  const StateId start = stream->read_vint();
  const size_t nstates = stream->read_vlong();
  const size_t narcs = stream->read_vlong();

  // read states & arcs
  std::vector<State> states(nstates);
  std::vector<Arc> arcs(narcs);

  auto begin = arcs.begin();
  for (auto& state : states) {
    state.pos = stream->read_vlong();
    state.narcs = stream->read_vlong();
    state.weight = { nullptr, stream->read_vlong() };

    for (auto end = begin + state.narcs; begin != end; ++begin) {
      begin->ilabel = stream->read_byte();
      begin->nextstate = stream->read_vint();
      begin->weight = { nullptr, stream->read_vlong() };
    }
  }

  // read weights
  const size_t total_weight_size = stream->read_vlong();
  auto weights = std::make_unique<irs::byte_type[]>(total_weight_size);
  auto* weight = weights.get();
  stream->read_bytes(weight, total_weight_size);

  begin = arcs.begin();
  for (auto& state : states) {
    state.weight = { weight, state.weight.Size() };
    weight += state.weight.Size();

    for (auto end = begin + state.narcs; begin != end; ++begin) {
      begin->weight = { weight, begin->weight.Size() };
      weight += begin->weight.Size();
    }
  }

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

  explicit ImmutableFst(const Fst<Arc> &fst)
      : ImplToExpandedFst<Impl>(std::make_shared<Impl>(fst)) {}

  ImmutableFst(const ImmutableFst<A> &fst, bool safe = false)
      : ImplToExpandedFst<Impl>(fst) {}

  // Gets a copy of this ConstFst. See Fst<>::Copy() for further doc.
  ImmutableFst<A> *Copy(bool safe = false) const override {
    return new ImmutableFst<A>(*this, safe);
  }

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
  stream->write_vint(fst.Start());
  stream->write_vlong(fstImpl->NumStates());
  stream->write_vlong(num_arcs);
  // FIXME write total weight size

  // FIXME SIMD???
  // write states & arcs
  size_t pos = 0;
  size_t total_weight_size = 0;
  for (StateIterator<FST> siter(fst); !siter.Done(); siter.Next()) {
    const StateId s = siter.Value();
    const size_t narcs = fstImpl->NumArcs(s);

    size_t weight_size = 0;
    if constexpr (detail::has_member_FinalRef_v<typename FST::Impl>) {
      weight_size = fstImpl->FinalRef(s).Size();
    } else {
      weight_size = fstImpl->Final(s).Size();
    }

    stream->write_vlong(pos);
    stream->write_vlong(narcs);
    stream->write_vlong(weight_size);

    total_weight_size += weight_size;

    for (ArcIterator<FST> aiter(fst, s); !aiter.Done(); aiter.Next()) {
      const auto& arc = aiter.Value();
      const size_t weight_size = arc.weight.Size();

      assert(arc.ilabel <= std::numeric_limits<irs::byte_type>::max());
      stream->write_byte(static_cast<irs::byte_type>(arc.ilabel & 0xFF)); // FIXME make it optional?
      stream->write_vint(arc.nextstate);
      stream->write_vlong(weight_size);

      total_weight_size += weight_size;
    }

    pos += narcs;
  }

  stream->write_vlong(total_weight_size);

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

