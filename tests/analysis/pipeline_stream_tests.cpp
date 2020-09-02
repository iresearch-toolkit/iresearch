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

// #include "analysis/pipeline_token_stream.hpp"
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

NS_ROOT
NS_BEGIN(analysis)

class pipeline_token_stream final
	: public frozen_attributes<3, analyzer>,  private util::noncopyable {
 public:
  struct options_t {
		std::vector<irs::analysis::analyzer::ptr> pipeline;
  };

	static constexpr string_ref type_name() noexcept { return "pipeline"; }

	pipeline_token_stream(const options_t& options) 
		: attributes{ {
		 { irs::type<increment>::id(), &inc_ },
		 { irs::type<offset>::id(), &offs_ },
		 { irs::type<term_attribute>::id(), &term_ }},
	   irs::type<pipeline_token_stream>::get()} {
		pipeline_.reserve(options.pipeline.size());
		for (const auto& p : options.pipeline) {
			pipeline_.emplace_back(p);
		}
	}

	virtual bool next() override {
		while (!current_->analyzer->next()) {
			if (current_ == pipeline_.rbegin()) {
				return false;
			}
			current_--;
		}
		return true;
	}

	virtual bool reset(const string_ref& data) override {
		current_ = pipeline_.rbegin();
		return pipeline_.front().analyzer->reset(data);
	}

 private:
	 struct sub_analyzer_t {
	   explicit sub_analyzer_t(const irs::analysis::analyzer::ptr& a) : analyzer(a) {}
		 
		 void reset(const string_ref& data) {
			 analyzer->reset(data);
			 sub_pos = integer_traits<uint32_t>::const_max;
		 }
		 uint32_t sub_pos{ integer_traits<uint32_t>::const_max };
		 irs::analysis::analyzer::ptr analyzer;
	};
	using pipeline_t = std::vector<sub_analyzer_t>;
	pipeline_t pipeline_;
	pipeline_t::reverse_iterator current_;
	offset offs_;
	increment inc_;
	// FIXME: find way to wire attribute directly from last pipeline member
	term_attribute term_;
};

NS_END
NS_END

#ifndef IRESEARCH_DLL

TEST(pipeline_token_stream_test, construct) {
	auto text = irs::analysis::analyzers::get("text",
		irs::type<irs::text_format::json>::get(),
		"{\"locale\":\"en_US.UTF-8\", \"stopwords\":[] }");

	auto ngram = irs::analysis::analyzers::get("ngram",
		irs::type<irs::text_format::json>::get(),
		"{\"min\":2, \"max\":2, \"preserveOriginal\":true }");

	auto delimiter = irs::analysis::analyzers::get("delimiter",
		irs::type<irs::text_format::json>::get(),
		"{\"delimiter\":\",\"}");

	irs::analysis::pipeline_token_stream::options_t pipeline_options;
	pipeline_options.pipeline.push_back(delimiter);
	pipeline_options.pipeline.push_back(text);
	pipeline_options.pipeline.push_back(ngram);

	irs::analysis::pipeline_token_stream pipe(pipeline_options);

	std::string data = "quick brown fox jumps over lazy dog";
	auto* offset = irs::get<irs::offset>(pipe);
	auto* term = irs::get<irs::term_attribute>(pipe);
	auto* inc = irs::get<irs::increment>(pipe);
	pipe.reset(data);
	uint32_t pos { irs::integer_traits<uint32_t>::const_max }; 
	while (pipe.next()) {
		std::cerr << "===================" << std::endl;
		std::cerr << "Offset:" << offset->start << ":" << offset->end << std::endl;
		std::cerr << "Inc:" << inc->value << std::endl;
		pos += inc->value;
		ASSERT_EQ(term->value.size(), offset->end - offset->start);
		ASSERT_EQ(offset->start, inc->value);
		std::cerr << term->value.c_str() << std::endl;
	}
}
#endif