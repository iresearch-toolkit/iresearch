////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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

#include "gtest/gtest.h"
#include "tests_config.hpp"

#include "analysis/pipeline_token_stream.hpp"
#include "analysis/text_token_stream.hpp"
#include "analysis/ngram_token_stream.hpp"
#include "analysis/delimited_token_stream.hpp"
#include "analysis/text_token_stream.hpp"

#include "analysis/token_attributes.hpp"
#include "analysis/token_stream.hpp"
#include "utils/locale_utils.hpp"
#include "utils/runtime_utils.hpp"
#include "utils/utf8_path.hpp"

#include <rapidjson/document.h> // for rapidjson::Document, rapidjson::Value

#ifndef IRESEARCH_DLL


void check_pipeline(irs::analysis::pipeline_token_stream& pipe, const std::string& data, bool no_modifications) {
	SCOPED_TRACE(data);
	auto* offset = irs::get<irs::offset>(pipe);
	auto* term = irs::get<irs::term_attribute>(pipe);
	auto* inc = irs::get<irs::increment>(pipe);
	pipe.reset(data);
	uint32_t pos { irs::integer_traits<uint32_t>::const_max }; 
	while (pipe.next()) {
		auto old_pos = pos;
		pos += inc->value;
		auto term_value = std::string(irs::ref_cast<char>(term->value).c_str(), term->value.size());
#ifdef IRESEARCH_DEBUG //TODO: remove me
		std::cerr << term_value << "(" << pos << ")" <<"|";
#endif
		ASSERT_EQ(term->value.size(), offset->end - offset->start);
		ASSERT_GE(pos - old_pos, 0);
		if (no_modifications) {
			ASSERT_EQ(data.substr(offset->start, offset->end - offset->start), term_value);
		}
	}
	std::cerr << std::endl;
}

TEST(pipeline_token_stream_test, many_tokenizers) {
	auto delimiter = irs::analysis::analyzers::get("delimiter",
		irs::type<irs::text_format::json>::get(),
		"{\"delimiter\":\",\"}");

	auto delimiter2 = irs::analysis::analyzers::get("delimiter",
		irs::type<irs::text_format::json>::get(),
		"{\"delimiter\":\" \"}");

	auto text = irs::analysis::analyzers::get("text",
		irs::type<irs::text_format::json>::get(),
		"{\"locale\":\"en_US.UTF-8\", \"stopwords\":[], \"case\":\"none\", \"stemming\":false }");

	auto ngram = irs::analysis::analyzers::get("ngram",
		irs::type<irs::text_format::json>::get(),
		"{\"min\":2, \"max\":2, \"preserveOriginal\":true }");

	irs::analysis::pipeline_token_stream::options_t pipeline_options;
	pipeline_options.pipeline.push_back(delimiter);
	pipeline_options.pipeline.push_back(delimiter2);
	pipeline_options.pipeline.push_back(text);
	pipeline_options.pipeline.push_back(ngram);

	irs::analysis::pipeline_token_stream pipe(pipeline_options);

	std::string data = "quick broWn,, FOX  jumps,  over lazy dog";
	check_pipeline(pipe, data, true);
}

TEST(pipeline_token_stream_test, overlapping_ngrams) {

	auto ngram = irs::analysis::analyzers::get("ngram",
		irs::type<irs::text_format::json>::get(),
		"{\"min\":6, \"max\":10, \"preserveOriginal\":false }");
	auto ngram2 = irs::analysis::analyzers::get("ngram",
		irs::type<irs::text_format::json>::get(),
		"{\"min\":2, \"max\":3, \"preserveOriginal\":false }");

	irs::analysis::pipeline_token_stream::options_t pipeline_options;
	pipeline_options.pipeline.push_back(ngram);
	pipeline_options.pipeline.push_back(ngram2);
	irs::analysis::pipeline_token_stream pipe(pipeline_options);

	std::string data = "ABCDEFJHIJKLMNOP";
	check_pipeline(pipe, data, true);
}


TEST(pipeline_token_stream_test, case_ngrams) {

	auto ngram = irs::analysis::analyzers::get("ngram",
		irs::type<irs::text_format::json>::get(),
		"{\"min\":3, \"max\":3, \"preserveOriginal\":false }");
	auto norm = irs::analysis::analyzers::get("norm",
		irs::type<irs::text_format::json>::get(),
		"{\"locale\":\"en\", \"case\":\"upper\"}");
	std::string data = "QuIck BroWN FoX";
	{
		irs::analysis::pipeline_token_stream::options_t pipeline_options;
		pipeline_options.pipeline.push_back(ngram);
		pipeline_options.pipeline.push_back(norm);
		irs::analysis::pipeline_token_stream pipe(pipeline_options);
		check_pipeline(pipe, data, false);
	}
	{
		irs::analysis::pipeline_token_stream::options_t pipeline_options;
		pipeline_options.pipeline.push_back(norm);
		pipeline_options.pipeline.push_back(ngram);
		irs::analysis::pipeline_token_stream pipe(pipeline_options);
		check_pipeline(pipe, data, false);
	}
}

#endif