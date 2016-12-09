//
// IResearch search engine 
// 
// Copyright © 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#include <memory>

#include "shared.hpp"
#include "singleton.hpp"

#include "log.hpp"

NS_LOCAL

class logger: public iresearch::singleton<std::ostream*> {
  logger(): singleton() { instance() = &std::cerr; }
};

NS_END

NS_ROOT

int32_t VERBOSITY = 0;

std::ostream& log_message::stream() { return *logger::instance(); }

/*static*/ void log_message::stream(std::ostream& stream) {
  logger::instance() = &stream;
}

NS_END