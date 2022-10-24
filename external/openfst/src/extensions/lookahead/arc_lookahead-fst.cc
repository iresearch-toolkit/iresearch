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

#include <fst/fst.h>
#include <fst/matcher-fst.h>

namespace fst {

static FstRegisterer<StdArcLookAheadFst> ArcLookAheadFst_StdArc_registerer;
static FstRegisterer<MatcherFst<
    ConstFst<LogArc>, ArcLookAheadMatcher<SortedMatcher<ConstFst<LogArc>>>,
    arc_lookahead_fst_type>>
    ArcLookAheadFst_LogArc_registerer;
static FstRegisterer<MatcherFst<
    ConstFst<Log64Arc>, ArcLookAheadMatcher<SortedMatcher<ConstFst<Log64Arc>>>,
    arc_lookahead_fst_type>>
    ArcLookAheadFst_Log64Arc_registerer;

}  // namespace fst
