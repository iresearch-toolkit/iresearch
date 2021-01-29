////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_AUTOMATON_DECL_H
#define IRESEARCH_AUTOMATON_DECL_H

#include <cstddef>

namespace fst {

template <class Arc, class Allocator>
class VectorState;

template <class Arc, class State>
class VectorFst;

template<typename F, size_t CacheSize, bool MatchInput, bool ByteLabel>
class TableMatcher;

namespace fsa {

class BooleanWeight;

template<typename T>
struct RangeLabel;

template<typename L, typename W>
struct Transition;

template<typename L, typename W>
using AutomatonState = VectorState<Transition<L, W>, std::allocator<Transition<L, W>>>;

template<typename L = int32_t, typename W = BooleanWeight>
using Automaton = VectorFst<Transition<L, W>, AutomatonState<L, W>>;

} // fsa
} // fst

namespace iresearch {

using automaton = fst::fsa::Automaton<int32_t>;
using rautomaton = fst::fsa::Automaton<int64_t>;
using range_label = fst::fsa::RangeLabel<int64_t>;

using automaton_table_matcher = fst::TableMatcher<automaton, 256, true, true>;
using rautomaton_table_matcher = fst::TableMatcher<rautomaton, 256, true, true>;

}

#endif // IRESEARCH_AUTOMATON_DECL_H

