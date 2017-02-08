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

// use a prefx that does not clash with any predefined macros (e.g. win32 'ERROR')
enum level_t {
  IRL_FATAL,
  IRL_ERROR,
  IRL_WARN,
  IRL_INFO,
  IRL_DEBUG,
  IRL_TRACE
};

IRESEARCH_API FILE* output(level_t level);
IRESEARCH_API void output(level_t level, FILE* out); // nullptr == /dev/null
IRESEARCH_API void output_le(level_t level, FILE* out); // nullptr == /dev/null
IRESEARCH_API void stack_trace(level_t level);
IRESEARCH_API void stack_trace(level_t level, const std::exception_ptr& eptr);
IRESEARCH_API std::ostream& stream(level_t level);

#ifndef _MSC_VER
  // +1 to skip stack_trace_nomalloc(...)
  void IRESEARCH_API stack_trace_nomalloc(level_t level, size_t skip = 1);
#endif

NS_END

class IRESEARCH_API log_message {
 public:
  log_message(logger::level_t level, const std::string& type)
    : fatal_(type == "FATAL"), level_(level) {
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
  logger::level_t level_;
}; // log_message

NS_END

NS_LOCAL

FORCE_INLINE CONSTEXPR iresearch::logger::level_t exception_stack_trace_level() {
  return iresearch::logger::IRL_DEBUG;
}

NS_END

#define IR_LOG(level, prefix) iresearch::log_message(level, prefix).stream()
#define IR_LOG_DETAILED(level, prefix) IR_LOG(level, prefix) << __FILE__ << ":" << __LINE__ << " "
#define IR_LOG_FORMATED(level, prefix, format, ...) \
  (0&fprintf(iresearch::logger::output(level), "%s: %s:%u ", prefix, __FILE__, __LINE__)) \
  +fprintf(iresearch::logger::output(level), format, __VA_ARGS__)
#define IR_FATAL() IR_LOG_DETAILED(iresearch::logger::IRL_FATAL, "FATAL")
#define IR_ERROR() IR_LOG_DETAILED(iresearch::logger::IRL_ERROR, "ERROR")
#define IR_WARN() IR_LOG_DETAILED(iresearch::logger::IRL_WARN, "WARN")
#define IR_INFO() IR_LOG_DETAILED(iresearch::logger::IRL_INFO, "INFO")
#define IR_DEBUG() IR_LOG_DETAILED(iresearch::logger::IRL_DEBUG, "DEBUG")
#define IR_TRACE() IR_LOG_DETAILED(iresearch::logger::IRL_TRACE, "TRACE")

#define IR_STACK_TRACE() \
  iresearch::logger::stack_trace(exception_stack_trace_level()); \
  IR_LOG_DETAILED(exception_stack_trace_level(), "STACK_TRACE")
#define IR_EXCEPTION() \
  IR_LOG_DETAILED(exception_stack_trace_level(), "EXCEPTION") << "@" << __FUNCTION__ << " stack trace:" << std::endl; \
  iresearch::logger::stack_trace(exception_stack_trace_level(), std::current_exception()); \
  IR_LOG_DETAILED(exception_stack_trace_level(), "EXCEPTION")

#endif