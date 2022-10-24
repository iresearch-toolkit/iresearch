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
//
// This is an experimental push-down transducer (PDT) library. A PDT is
// encoded as an FST, where some transitions are labeled with open or close
// parentheses. To be interpreted as a PDT, the parentheses must balance on a
// path.

#ifndef FST_EXTENSIONS_PDT_PDTLIB_H_
#define FST_EXTENSIONS_PDT_PDTLIB_H_

#include <fst/extensions/pdt/compose.h>
#include <fst/extensions/pdt/expand.h>
#include <fst/extensions/pdt/pdt.h>
#include <fst/extensions/pdt/replace.h>
#include <fst/extensions/pdt/reverse.h>
#include <fst/extensions/pdt/shortest-path.h>

#endif  // FST_EXTENSIONS_PDT_PDTLIB_H_
