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

#include "pipeline_token_stream.hpp"

NS_LOCAL

REGISTER_ANALYZER_JSON(irs::analysis::pipeline_token_stream, make_json, 
	normalize_json_config);

NS_END

NS_ROOT
NS_BEGIN(analysis)

pipeline_token_stream::pipeline_token_stream(const pipeline_token_stream::options_t& options) 
	: attributes{ {
		{ irs::type<increment>::id(), &inc_ },
		{ irs::type<offset>::id(), &offs_ },
		{ irs::type<term_attribute>::id(), &term_ }}, // TODO: use get_mutable
		irs::type<pipeline_token_stream>::get()} {
	pipeline_.reserve(options.pipeline.size());
	for (const auto& p : options.pipeline) {
		pipeline_.emplace_back(p);
	}
	top_ = pipeline_.begin();
	bottom_ = --pipeline_.end();
}



inline bool pipeline_token_stream::next() {
	upstream_inc_ = 0;
	while (!current_->analyzer->next()) {
		if (current_ == top_) { // reached pipeline top and next has failed - we are done
			return false;
		}
		--current_;
	}
	upstream_inc_ += current_->inc->value;
	// go down to lowest pipe to get actual tokens
	while (current_ != bottom_) {
		auto prev = current_;
		++current_;
		current_->analyzer->reset(irs::ref_cast<char>(prev->term->value));
		while (!current_->analyzer->next()) { // empty one found. Move upstream.
			if (current_ == top_) { // reached pipeline top and next has failed - we are done
				return false;
			}
			--current_;
		}
		upstream_inc_ += current_->inc->value;
		assert(current_->inc->value > 0); // first increment after reset should be positive to give 0 or farther pos!
		upstream_inc_--; // compensate pacing sub_analyzer from -1 to 0
										 // a this step actually does not move whole pipeline
										 // sub analyzer just stays same pos as it`s parent
	}
	term_.value = current_->term->value;

	// FIXME: get rid of full recalc. Use incremental approach
	uint32_t start{ 0 };
	for (const auto& p : pipeline_) {
		start += p.offs->start;
	}
	inc_.value = upstream_inc_;
	offs_.start = start;
	offs_.end = offs_.start + (current_->offs->end - current_->offs->start);
	return true;
}

inline bool pipeline_token_stream::reset(const string_ref& data) {
	current_ = top_;
	return pipeline_.front().analyzer->reset(data);
}

/*static*/ void pipeline_token_stream::init() {
	REGISTER_ANALYZER_JSON(pipeline_token_stream, make_json,
		normalize_json_config);  // match registration above

}

NS_END
NS_END