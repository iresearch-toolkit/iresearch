////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
/// @author Alex Geenen
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_FASTTEXT_MODEL_PROVIDER_H
#define IRESEARCH_FASTTEXT_MODEL_PROVIDER_H

#include <string>
#include <functional>

#include "fasttext.h"

namespace iresearch {
namespace analysis {

inline std::function<std::shared_ptr<fasttext::FastText>(const std::string &)> fasttext_model_provider = nullptr;

} // analysis
} // iresearch

#endif //IRESEARCH_FASTTEXT_MODEL_PROVIDER_H
