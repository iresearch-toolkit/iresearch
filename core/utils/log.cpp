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

#include <memory>

#if defined(_MSC_VER)
  #include <mutex>

  #include <Windows.h> // must be included before DbgHelp.h
  #include <Psapi.h>
  #include <DbgHelp.h>

  #include "thread_utils.hpp"
#else
  #include <thread>

  #include <cxxabi.h> // for abi::__cxa_demangle(...)
  #include <dlfcn.h> // for dladdr(...)
  #include <execinfo.h>
  #include <string.h> // for strlen(...)
  #include <unistd.h> // for STDIN_FILENO/STDOUT_FILENO/STDERR_FILENO
  #include <sys/wait.h> // for waitpid(...)
#endif

#if defined(USE_LIBUNWIND)
  #include <libunwind.h>
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
  bool file_line_addr2line(const char* obj, const char* addr) {
    auto pid = fork();

    if (!pid) {
      size_t pid_size = sizeof(pid_t)*3 + 1; // aproximately 3 chars per byte +1 for \0
      size_t name_size = strlen("/proc//exe") + pid_size + 1; // +1 for \0
      char pid_buf[pid_size];
      char name_buf[name_size];
      auto ppid = getppid();

      snprintf(pid_buf, pid_size, "%d", ppid);
      snprintf(name_buf, name_size, "/proc/%d/exe", ppid);

      // The exec() family of functions replaces the current process image with a new process image.
      // The exec() functions only return if an error has occurred.
      execlp("addr2line", "addr2line", "-e", obj, addr, NULL);
      exit(1);
    }

    int status;

    return 0 < waitpid(pid, &status, 0) && !WEXITSTATUS(status);
  }

  std::unique_ptr<char> proc_name_demangle(const char* symbol) {
    int status;

    // abi::__cxa_demangle(...) expects 'output_buffer' to be malloc()ed and does realloc()/free() internally
    std::unique_ptr<char> buf(abi::__cxa_demangle(symbol, nullptr, nullptr, &status));

    return buf && !status ? std::move(buf) : nullptr;
  }

  bool stack_trace_gdb() {
    auto pid = fork();

    if (!pid) {
      size_t pid_size = sizeof(pid_t)*3 + 1; // approximately 3 chars per byte +1 for \0
      size_t name_size = strlen("/proc//exe") + pid_size + 1; // +1 for \0
      char pid_buf[pid_size];
      char name_buf[name_size];
      auto ppid = getppid();

      snprintf(pid_buf, pid_size, "%d", ppid);
      snprintf(name_buf, name_size, "/proc/%d/exe", ppid);

      // The exec() family of functions replaces the current process image with a new process image.
      // The exec() functions only return if an error has occurred.
      execlp("gdb", "gdb", "-n", "-nx", "-return-child-result", "-batch", "-ex", "thread", "-ex", "bt", name_buf, pid_buf, NULL);
      exit(1);
    }

    int status;

    return 0 < waitpid(pid, &status, 0) && !WEXITSTATUS(status);
  }

  void stack_trace_posix() {
    auto& stream = iresearch::logger::stream();
    static const size_t frames_max = 128; // arbitrary size
    void* frames_buf[frames_max];
    auto frames_count = backtrace(frames_buf, frames_max);

    if (frames_count < 2) {
      return; // nothing to log
    }

    frames_count -= 2; // -2 to skip stack_trace(...) + stack_trace_posix(...)

    auto frames_buf_ptr = frames_buf + 2; // +2 to skip backtrace(...) + stack_trace_posix(...)
    int pipefd[2];

    if (pipe(pipefd)) {
      stream << "Failed to output stack trace to stream, redirecting stack trace to STDERR" << std::endl;
      backtrace_symbols_fd(frames_buf_ptr, frames_count, STDERR_FILENO); // fallback to stderr
      return;
    }

    size_t buf_len = 0;
    size_t buf_size = 1024; // arbitrary size
    char buf[buf_size];
    std::thread thread([&pipefd, &stream, &buf, &buf_len, buf_size]()->void {
      for (char ch; read(pipefd[0], &ch, 1) > 0;) {
        if (ch != '\n') {
          if (buf_len < buf_size - 1) {
            buf[buf_len++] = ch;
            continue;
          }

          if (buf_len < buf_size) {
            buf[buf_len++] = '\0';
            stream << buf;
          }

          stream << ch; // line longer than buf, output line directly
          continue;
        }

        if (buf_len >= buf_size) {
          buf_len = 0;
          stream << std::endl;
          continue;
        }

        char* addr_start = nullptr;
        char* addr_end = nullptr;
        char* fn_start = nullptr;
        char* offset_start = nullptr;
        char* offset_end = nullptr;
        char* path_start = buf;

        for (size_t i = 0; i < buf_len; ++i) {
          switch(buf[i]) {
           case '(':
            fn_start = &buf[i + 1];
            continue;
           case '+':
            offset_start = &buf[i + 1];
            continue;
           case ')':
            offset_end = &buf[i];
            continue;
           case '[':
            addr_start = &buf[i + 1];
            continue;
           case ']':
            addr_end = &buf[i];
            continue;
          }
        }

        buf[buf_len] = '\0';
        buf_len = 0;

        auto fn_end = offset_start ? offset_start - 1 : nullptr;
        auto path_end = fn_start ? fn_start - 1 : (addr_start ? addr_start - 1 : nullptr);

        if (path_start < path_end) {
          if (offset_start < offset_end) {
            stream.write(path_start, path_end - path_start);

            if (fn_start < fn_end) {
              stream.put('(');
              *fn_end = '\0';

              auto fn_name = proc_name_demangle(fn_start);

              if(fn_name) {
                stream << fn_name.get();
              } else {
                stream.write(fn_start, fn_end - fn_start);
              }

              stream.put('+') << offset_start << std::endl;
            } else {
              stream << path_end << " ";
              *offset_end = '\0';
              *path_end = '\0';
              file_line_addr2line(path_start, offset_start);
            }

            continue;
          }

          if (addr_start < addr_end) {
            stream << path_start << " ";
            *addr_end = '\0';
            *path_end = '\0';
            file_line_addr2line(path_start, addr_start);

            continue;
          }
        }

        stream << buf << std::endl;
      }
    });

    backtrace_symbols_fd(frames_buf_ptr, frames_count, pipefd[1]);
    close(pipefd[1]);
    thread.join();
    close(pipefd[0]);
    stream << std::endl;
  }
#endif

#if defined(USE_LIBUNWIND)
  bool file_line_addr2line(const char* obj, unw_word_t addr) {
    size_t addr_size = sizeof(unw_word_t)*3 + 2 + 1; // aproximately 3 chars per byte +2 for 0x, +1 for \0
    char addr_buf[addr_size];

    snprintf(addr_buf, addr_size, "0x%lx", addr);

    return file_line_addr2line(obj, addr_buf);
  }

  bool stack_trace_libunwind() {
    unw_context_t ctx;
    unw_cursor_t cursor;

    if (0 != unw_getcontext(&ctx) || 0 != unw_init_local(&cursor, &ctx)) {
      return false;
    }

    // skip backtrace(...) + stack_trace_libunwind(...)
    if (unw_step(&cursor) <= 0) {
      return true; // nothing to log
    }

    auto& stream = iresearch::logger::stream();
    unw_word_t instruction_pointer;

    while (unw_step(&cursor) > 0) {
      if (0 != unw_get_reg(&cursor, UNW_REG_IP, &instruction_pointer)) {
        stream << "<unknown>" << std::endl;
        continue; // no instruction pointer available
      }

      Dl_info dl_info;

      // resolve function/flie/line via dladdr() + addr2line
      if (0 != dladdr((void*)instruction_pointer, &dl_info) || !dl_info.dli_fname) {
        stream << (dl_info.dli_fname ? dl_info.dli_fname : "\?\?") << "(";

        if (dl_info.dli_sname) {
          auto proc_name = proc_name_demangle(dl_info.dli_sname);

          stream << (proc_name ? proc_name.get() : dl_info.dli_sname);
        }

        stream << "+0x" << std::hex << instruction_pointer - unw_word_t(dl_info.dli_saddr) << std::dec << ")";
        stream << "[0x" << std::hex << instruction_pointer << std::dec << "] ";

        // there appears to be a magic number base address which should not be subtracted from the instruction_pointer
        static const void* static_fbase = (void*)0x400000;
        auto addr = instruction_pointer - (static_fbase == dl_info.dli_fbase ? unw_word_t(dl_info.dli_saddr) : unw_word_t(dl_info.dli_fbase));

        if (!file_line_addr2line(dl_info.dli_fname, addr)) {
          stream << std::endl;
        }

        continue;
      }

      size_t proc_size = 1024; // arbitrary size
      char proc_buf[proc_size];
      unw_word_t offset;

      if (0 != unw_get_proc_name(&cursor, proc_buf, proc_size, &offset)) {
        stream << "\?\?[0x" << std::hex << instruction_pointer << std::dec << "]" << std::endl;
        continue; // no function info available
      }

      auto proc_name = proc_name_demangle(proc_buf);

      stream << "\?\?(" << (proc_name ? proc_name.get() : proc_buf) << "+0x" << std::hex << offset << std::dec << ")";
      stream << "[0x" << std::hex << instruction_pointer << std::dec << "]" << std::endl;
    }

    return true;
  }
#else
  bool stack_trace_libunwind() {
    return false;
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
    if (!stack_trace_libunwind() && !stack_trace_gdb()) {
      stack_trace_posix();
    }
  #endif
}

NS_END

std::ostream& log_message::stream() { return logger_ctx::instance().stream(); }

NS_END