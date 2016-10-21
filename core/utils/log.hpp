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

#ifndef IRESEARCH_LOG_H
#define IRESEARCH_LOG_H

#include <string>
#include <iostream>
#include "shared.hpp"

NS_ROOT

/* verbosity level */
IRESEARCH_API extern int32_t VERBOSITY;

class log_message {
 public:
  log_message(const std::string& type)
    : fatal_(type == "FATAL") {
    std::cerr << type << ": ";
  }

  ~log_message() {
    std::cerr << std::endl;
    if (fatal_) {
      exit(1);
    }
  }

  std::ostream& stream() { return std::cerr; }

 private:
  bool fatal_;
}; // log_message

NS_END

#define IR_LOG(type) log_message(#type).stream()
#define IR_LOG_DETAILED(type) IR_LOG(type) << __FILE__ << ":" << __LINE__ << " "

#define IR_ERROR() IR_LOG_DETAILED(ERROR) 
#define IR_INFO() IR_LOG_DETAILED(INFO) 
#define IR_INFO_LEVEL(level) if ((level) <= iresearch::VERBOSITY) IR_INFO()


#endif