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

#if defined(_MSC_VER)
  #include <mutex>

  #include <Windows.h> // must be included before DbgHelp.h
  #include <Psapi.h>
  #include <DbgHelp.h>

  #include "thread_utils.hpp"
#else
  #include <thread>

  #include <execinfo.h>
  #include <unistd.h> // for STDIN_FILENO/STDOUT_FILENO/STDERR_FILENO
#endif

#include "shared.hpp"
#include "singleton.hpp"

#include "log.hpp"

NS_LOCAL

class logger_ctx: public iresearch::singleton<logger_ctx> {
 public:
  logger_ctx()
    : singleton(), level_(iresearch::logger::IRL_INFO), stream_(&std::cerr) {
  }

  iresearch::logger::level_t level() { return level_; }
  logger_ctx& level(iresearch::logger::level_t level) { level_ = level; return *this; }
  std::ostream& stream() { return *stream_; }
  logger_ctx& stream(std::ostream& stream) { stream_ = &stream; return *this; }

 private:
  iresearch::logger::level_t level_;
  std::ostream* stream_;
};

#if defined(_MSC_VER)
  DWORD stack_trace_win32(struct _EXCEPTION_POINTERS* ex) {
    auto& stream = iresearch::logger::stream();
    static std::mutex mutex;

    SCOPED_LOCK(mutex); // win32 stack trace API is not thread safe

    if (!ex || !ex->ContextRecord) {
      stream << "No stack_trace available" << std::endl;
      return EXCEPTION_EXECUTE_HANDLER;
    }

    CONTEXT ctx = *(ex->ContextRecord); // create a modifiable copy
    STACKFRAME frame = { 0 };

    frame.AddrPC.Offset = ctx.Rip;
    frame.AddrPC.Mode = AddrModeFlat;
    frame.AddrFrame.Offset = ctx.Rbp;
    frame.AddrFrame.Mode = AddrModeFlat;

    auto process = GetCurrentProcess();

    // SYMOPT_DEFERRED_LOADS required to avoid win32 GUI exception
    //SymSetOptions(SymGetOptions() | SYMOPT_DEFERRED_LOADS | SYMOPT_LOAD_LINES | SYMOPT_UNDNAME);
    // always load symbols
    SymInitialize(process, NULL, TRUE);

    HMODULE module_handle;
    DWORD module_handle_size;

    if (!EnumProcessModules(process, &module_handle, sizeof(HMODULE), &module_handle_size)) {
      stream << "Failed to enumerate modules for current process" << std::endl;
      return EXCEPTION_EXECUTE_HANDLER;
    }

    MODULEINFO module_info;

    GetModuleInformation(process, module_handle, &module_info, sizeof(MODULEINFO));

    auto image_type = ImageNtHeader(module_info.lpBaseOfDll)->FileHeader.Machine;
    auto thread = GetCurrentThread();
    char symbol_buf[sizeof(IMAGEHLP_SYMBOL) + 256];
    auto& symbol = reinterpret_cast<IMAGEHLP_SYMBOL&>(symbol_buf);

    symbol.SizeOfStruct = sizeof(IMAGEHLP_SYMBOL);
    symbol.MaxNameLength = 256;

    IMAGEHLP_LINE line = { 0 };

    line.SizeOfStruct = sizeof(IMAGEHLP_LINE);

    IMAGEHLP_MODULE module = { 0 };

    module.SizeOfStruct = sizeof(IMAGEHLP_MODULE);

    DWORD offset_from_line = 0;
    DWORD64 offset_from_symbol = 0;
    bool skip_frame = true;
    size_t frame_count = size_t(-1);

    while (StackWalk64(image_type, process, thread, &frame, &ctx, NULL, NULL, NULL, NULL)) {
      static const std::string stack_trace_fn_symbol("iresearch::logger::stack_trace");
      auto has_module = SymGetModuleInfo(process, frame.AddrPC.Offset, &module);
      auto has_symbol = SymGetSymFromAddr(process, frame.AddrPC.Offset, &offset_from_symbol, &symbol);
      auto has_line = SymGetLineFromAddr(process, frame.AddrPC.Offset, &offset_from_line, &line);

      // skip frames until stack_trace entry point is encountered
      if (skip_frame && has_symbol && stack_trace_fn_symbol == symbol.Name) {
        skip_frame = false;  // next frame is start of exception stack trace
        continue;
      }

      stream << "#" << ++frame_count << " ";

      if (has_module) {
        stream << module.ModuleName;
      }

      if (has_symbol) {
        stream << "(" << symbol.Name << "+0x" << std::hex << offset_from_symbol << ")" << std::dec;
      }

      if (has_line) {
        stream << ": " << line.FileName << ":" << line.LineNumber << "+0x" << std::hex << offset_from_line << std::dec;
      }

      stream << std::endl;
    }

    if (skip_frame) {
      stream << "No stack_trace available outside of logger" << std::endl;
    }

    return EXCEPTION_EXECUTE_HANDLER;
  }
#else
  void stack_trace_posix() {
    auto& stream = iresearch::logger::stream();
    static const size_t frames_max = 128; // arbitrary size
    void* frames_buf[frames_max];
    auto frames_count = backtrace(frames_buf, frames_max);

    if (frames_count < 1) {
      return; // nothing to log
    }

    // skip current fn frame
    auto frames_buf_ptr = frames_buf + 1;
    --frames_count;

    int pipefd[2];

    if (pipe(pipefd)) {
      stream << "Failed to output stack trace to stream, redirecting stack trace to STDERR" << std::endl;
      backtrace_symbols_fd(frames_buf_ptr, frames_count, STDERR_FILENO); // fallback to stderr
      return;
    }

    std::thread thread([&pipefd, &stream]()->void {
      for (char buf; read(pipefd[0], &buf, 1) > 0;) {
        stream << buf;
      }
    });

    backtrace_symbols_fd(frames_buf, frames_count, pipefd[1]);
    close(pipefd[1]);
    thread.join();
    close(pipefd[0]);
    stream << std::endl;
  }
#endif

NS_END

NS_ROOT

NS_BEGIN(logger)

level_t level() {
  return logger_ctx::instance().level();
}

level_t level(level_t min_level) {
  auto old_level = level();

  logger_ctx::instance().level(min_level);

  return old_level;
}

std::ostream& stream() {
  return logger_ctx::instance().stream();
}

void stack_trace() {
  #if defined(_MSC_VER)
    __try {
      RaiseException(1, 0, 0, NULL);
    } __except(stack_trace_win32(GetExceptionInformation())) {
      return;
    }

    stack_trace_win32(nullptr);
  #else
    stack_trace_posix();
  #endif
}

NS_END

std::ostream& log_message::stream() { return logger_ctx::instance().stream(); }

NS_END