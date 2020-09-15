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

#include <rapidjson/rapidjson/document.h> // for rapidjson::Document
#include <rapidjson/rapidjson/writer.h> // for rapidjson::Writer
#include <rapidjson/rapidjson/stringbuffer.h> // for rapidjson::StringBuffer

NS_LOCAL

////////////////////////////////////////////////////////////////////////////////
/// @brief args is a jSON encoded object with the following attributes:
/// pipeline: Array of objects containing analyzers definition inside pipeline.
/// Each definition is an object with the following attributes:
/// type: analyzer type name (one of registered analyzers type)
/// properties: object with properties for corresponding analyzer
////////////////////////////////////////////////////////////////////////////////
irs::analysis::analyzer::ptr make_json(const irs::string_ref& args) {
	return nullptr;
}

constexpr irs::string_ref PIPELINE_PARAM_NAME   = "pipeline";
constexpr irs::string_ref TYPE_PARAM_NAME       = "type";
constexpr irs::string_ref PROPERTIES_PARAM_NAME = "properties";

bool parse_json_config(const irs::string_ref& args,
	irs::analysis::pipeline_token_stream::options_t& options) {
	rapidjson::Document json;
	if (json.Parse(args.c_str(), args.size()).HasParseError()) {
		IR_FRMT_ERROR(
			"Invalid jSON arguments passed while constructing pipeline_token_stream, "
			"arguments: %s",
			args.c_str());

		return false;
	}

	if (rapidjson::kObjectType != json.GetType()) {
		IR_FRMT_ERROR(
			"Not a jSON object passed while constructing pipeline_token_stream, "
			"arguments: %s",
			args.c_str());

		return false;
	}

	if (json.HasMember(PIPELINE_PARAM_NAME.c_str())) {
		auto& pipeline = json[PIPELINE_PARAM_NAME.c_str()];
		if (pipeline.IsArray()) {
			for (auto pipe = pipeline.Begin(), end = pipeline.End(); pipe != end; ++pipe) {
				if (pipe->IsObject()) {
					irs::string_ref type;
					if (pipe->HasMember(TYPE_PARAM_NAME.c_str())) {
						auto& type_atr = (*pipe)[TYPE_PARAM_NAME.c_str()];
						if (type_atr.IsString()) {
							type = type_atr.GetString();
						}	else {
							IR_FRMT_ERROR(
								"Failed to read '%s' attribute of  '%s' member as string while constructing "
								"pipeline_token_stream from jSON arguments: %s",
								TYPE_PARAM_NAME.c_str(), PIPELINE_PARAM_NAME.c_str(), args.c_str());
							return false;
						}
					} else {
						IR_FRMT_ERROR(
							"Failed to get '%s' attribute of  '%s' member while constructing "
							"pipeline_token_stream from jSON arguments: %s",
							TYPE_PARAM_NAME.c_str(), PIPELINE_PARAM_NAME.c_str(), args.c_str());
						return false;
					}
					if (pipe->HasMember(PROPERTIES_PARAM_NAME.c_str())) {
						auto& properties_atr = (*pipe)[PROPERTIES_PARAM_NAME.c_str()];
						rapidjson::StringBuffer properties_buffer;
						rapidjson::Writer< rapidjson::StringBuffer> writer(properties_buffer);
						properties_atr.Accept(writer);
						auto analyzer = irs::analysis::analyzers::get(
							                type.c_str(), 
							                irs::type<irs::text_format::json>::get(),
							                properties_buffer.GetString());
						if (analyzer) {
							options.pipeline.push_back(std::move(analyzer));
						} else {
							IR_FRMT_ERROR(
								"Failed to create pipeline member of type '%s' with properties '%s' while constructing "
								"pipeline_token_stream from jSON arguments: %s",
								type.c_str(), properties_buffer.GetString(), args.c_str());
							return false;
						}
					} else {
						IR_FRMT_ERROR(
							"Failed to get '%s' attribute of  '%s' member while constructing "
							"pipeline_token_stream from jSON arguments: %s",
							PROPERTIES_PARAM_NAME.c_str(), PIPELINE_PARAM_NAME.c_str(), args.c_str());
						return false;
					}
				}	else {
					IR_FRMT_ERROR(
						"Failed to read '%s' member as object while constructing "
						"pipeline_token_stream from jSON arguments: %s",
						PIPELINE_PARAM_NAME.c_str(), args.c_str());
					return false;
				}
			}
		} else {
			IR_FRMT_ERROR(
				"Failed to read '%s' attribute as array while constructing "
				"pipeline_token_stream from jSON arguments: %s",
				PIPELINE_PARAM_NAME.c_str(), args.c_str());
			return false;
		}
	} else {
		IR_FRMT_ERROR(
			"Not found parameter '%s' while constructing pipeline_token_stream, "
			"arguments: %s",
			PIPELINE_PARAM_NAME.c_str(),
			args.c_str());
		return false;
	}
	return true;
}



bool normalize_json_config(const irs::string_ref& args, std::string& definition) {
	return false;
}


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
	uint32_t upstream_inc = 0;
	while (!current_->analyzer->next()) {
		if (current_ == top_) { // reached pipeline top and next has failed - we are done
			return false;
		}
		--current_;
	}
	upstream_inc += current_->inc->value;
	// if upstream holds postions than all downstream resets will be just one step forward
	// other possible gaps will be counted below in downstream loop
	upstream_inc += (current_->inc->value == 0 && current_ != bottom_);
	// go down to lowest pipe to get actual tokens
	while (current_ != bottom_) {
		const auto prev_term = current_->term->value;
		++current_;
		if (!current_->reset(irs::ref_cast<char>(prev_term))) {
			return false;
		}
		while (!current_->analyzer->next()) { // empty one found. Move upstream.
			if (current_ == top_) { // reached pipeline top and next has failed - we are done
				return false;
			}
			--current_;
		}
		upstream_inc += current_->inc->value;
		assert(current_->inc->value > 0); // first increment after reset should be positive to give 0 or next pos!
		assert(upstream_inc > 0);
		upstream_inc --; // compensate placing sub_analyzer from -1 to 0
										 // as this step actually does not move whole pipeline
										 // sub analyzer just stays same pos as it`s parent
	}
	term_.value = current_->term->value;

	// FIXME: get rid of full recalc. Use incremental approach
	uint32_t start{ 0 };
	uint32_t upstream_end{ static_cast<uint32_t>(pipeline_.front().data_size) };
	for (const auto& p : pipeline_) {
		start += p.offs->start;
		if (p.offs->end != p.data_size && p.analyzer != bottom_->analyzer) {
			// this analyzer is not last and eaten not all its data.
			// so it will mark new pipeline offset end.
			upstream_end = start +  (p.offs->end - p.offs->start);
		}
	}
	inc_.value = upstream_inc;
	offs_.start = start;
	offs_.end = current_->offs->end == current_->data_size ? 
		          upstream_end : // all data eaten - actual end is defined by upstream
              (offs_.start + (current_->offs->end - current_->offs->start));
	return true;
}

inline bool pipeline_token_stream::reset(const string_ref& data) {
	current_ = top_;
	return pipeline_.front().reset(data);
}

/*static*/ void pipeline_token_stream::init() {
	REGISTER_ANALYZER_JSON(pipeline_token_stream, make_json,
		normalize_json_config);  // match registration above
}

NS_END
NS_END