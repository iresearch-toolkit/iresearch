//
// IResearch search engine 
// 
// Copyright (c) 2016 by EMC Corporation, All Rights Reserved
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

NS_BEGIN(logger)

// use a prefx that does not ckash with any predefined macros (e.g. win32 'ERROR')
enum level_t {
  IRL_NONE = 0,
  IRL_FATAL,
  IRL_ERROR,
  IRL_WARN,
  IRL_INFO,
  IRL_DEBUG,
  IRL_TRACE
};

level_t IRESEARCH_API level();
level_t IRESEARCH_API level(level_t min_level);
void IRESEARCH_API stack_trace();
IRESEARCH_API std::ostream& stream();

NS_END

class IRESEARCH_API log_message {
 public:
  log_message(const std::string& type): fatal_(type == "FATAL") {
    stream() << type << ": ";
  }

  ~log_message() {
    stream() << std::endl;

    if (fatal_) {
      exit(1);
    }
  }

  std::ostream& stream();

 private:
  bool fatal_;
}; // log_message

NS_END

#define IR_LOG(prefix) iresearch::log_message(prefix).stream()
#define IR_LOG_DETAILED(prefix) IR_LOG(prefix) << __FILE__ << ":" << __LINE__ << " "
#define IR_LOG_LEVEL(v_level, v_prefix) if ((v_level) && (v_level) <= iresearch::logger::level()) IR_LOG_DETAILED(v_prefix)

#define IR_FATAL() IR_LOG_LEVEL(iresearch::logger::IRL_FATAL, "FATAL")
#define IR_ERROR() IR_LOG_LEVEL(iresearch::logger::IRL_ERROR, "ERROR")
#define IR_WARN() IR_LOG_LEVEL(iresearch::logger::IRL_WARN, "WARN")
#define IR_INFO() IR_LOG_LEVEL(iresearch::logger::IRL_INFO, "INFO")
#define IR_DEBUG() IR_LOG_LEVEL(iresearch::logger::IRL_DEBUG, "DEBUG")
#define IR_TRACE() IR_LOG_LEVEL(iresearch::logger::IRL_TRACE, "TRACE")

#define IR_STACK_TRACE() iresearch::logger::stack_trace()
#define IR_EXCEPTION() IR_LOG_DETAILED("EXCEPTION") << "@" << __FUNCTION__ << " stack trace:" << std::endl; IR_STACK_TRACE()

#endif