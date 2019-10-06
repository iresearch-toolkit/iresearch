#ifndef IRESEARCH_AUTOMATON_DECL_H
#define IRESEARCH_AUTOMATON_DECL_H

NS_BEGIN(fst)

template <class Arc, class Allocator>
class VectorState;

template <class Arc, class State>
class VectorFst;

NS_BEGIN(fsa)

class Transition;

using AutomatonState = VectorState<Transition, std::allocator<Transition>>;
using Automaton = VectorFst<Transition, AutomatonState>;

NS_END // fsa
NS_END // fst

NS_ROOT

using automaton = fst::fsa::Automaton;

NS_END

#endif // IRESEARCH_AUTOMATON_DECL_H

