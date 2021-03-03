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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////
#include "segmentation_token_stream.hpp"

#include <rapidjson/rapidjson/document.h> // for rapidjson::Document
#include <rapidjson/rapidjson/writer.h> // for rapidjson::Writer
#include <rapidjson/rapidjson/stringbuffer.h> // for rapidjson::StringBuffer

#include "unicorn/segment.hpp"
#include "unicorn/string.hpp"

namespace iresearch {
namespace analysis {

/*static*/ void segmentation_token_stream::init() {
 // REGISTER_ANALYZER_JSON(pipeline_token_stream, make_json,
 //   normalize_json_config);  // match registration above
}

segmentation_token_stream::segmentation_token_stream(segmentation_token_stream::options_t&& options)
	: analyzer{ irs::type<segmentation_token_stream>::get() } {
}

bool segmentation_token_stream::next() {
	return true;
}
bool segmentation_token_stream::reset(const string_ref& data) {
	for (auto word : RS::Unicorn::word_range(static_cast<std::basic_string_view<char>>(data), RS::Unicorn::Segment::alpha)) {
		std::cout <<  RS::Unicorn::str_lowercase(RS::Unicorn::u_str(word)) << std::endl;
	}
	return true;
}

} // namespace analysis
} // namespace iresearch