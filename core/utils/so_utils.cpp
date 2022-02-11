////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////
#include "so_utils.hpp"

#include "log.hpp"
#include "utils/file_utils.hpp"
#include "utils/utf8_path.hpp"

#if defined(_MSC_VER)  // Microsoft compiler

#define WIN32_LEAN_AND_MEAN
// https://stackoverflow.com/questions/1394910/how-to-tame-the-windows-headers-useful-defines
#define NOGDICAPMASKS      // CC_*, LC_*, PC_*, CP_*, TC_*, RC_
#define NOVIRTUALKEYCODES  // VK_*
#define NOWINMESSAGES      // WM_*, EM_*, LB_*, CB_*
#define NOWINSTYLES        // WS_*, CS_*, ES_*, LBS_*, SBS_*, CBS_*
#define NOSYSMETRICS       // SM_*
#define NOMENUS            // MF_*
#define NOICONS            // IDI_*
#define NOKEYSTATES        // MK_*
#define NOSYSCOMMANDS      // SC_*
#define NORASTEROPS        // Binary and Tertiary raster ops
#define NOSHOWWINDOW       // SW_*
// #define OEMRESOURCE     // OEM Resource values
#define NOATOM        // Atom Manager routines
#define NOCLIPBOARD   // Clipboard routines
#define NOCOLOR       // Screen colors
#define NOCTLMGR      // Control and Dialog routines
#define NODRAWTEXT    // DrawText() and DT_*
#define NOGDI         // All GDI defines and routines
#define NOKERNEL      // All KERNEL defines and routines
#define NOUSER        // All USER defines and routines
#define NONLS         // All NLS defines and routines
#define NOMB          // MB_*and MessageBox()
#define NOMEMMGR      // GMEM_*, LMEM_*, GHND, LHND, associated routines
#define NOMETAFILE    // typedef METAFILEPICT
#define NOMINMAX      // Macros min(a, b) and max(a, b)
#define NOMSG         // typedef MSG and associated routines
#define NOOPENFILE    // OpenFile(), OemToAnsi, AnsiToOem, and OF_*
#define NOSCROLL      // SB_*and scrolling routines
#define NOSERVICE     // All Service Controller routines, SERVICE_ equates, etc.
#define NOSOUND       // Sound driver routines
#define NOTEXTMETRIC  // typedef TEXTMETRIC and associated routines
#define NOWH          // SetWindowsHook and WH_*
#define NOWINOFFSETS  // GWL_*, GCL_*, associated routines
#define NOCOMM        // COMM driver routines
#define NOKANJI       // Kanji support stuff.
#define NOHELP        // Help engine interface.
#define NOPROFILER    // Profiler interface.
#define NODEFERWINDOWPOS  // DeferWindowPos routines
#define NOMCX             // Modem Configuration Extensions
#include <windows.h>

namespace {

[[nodiscard]] std::string dlerror() {
  auto error = GetLastError();
  if (!error) {
    return {};
  }
  LPVOID lp_msg_buf;
  auto buf_len = FormatMessage(
      FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM |
          FORMAT_MESSAGE_IGNORE_INSERTS,
      NULL, error, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
      (LPTSTR)&lp_msg_buf, 0, NULL);
  if (!buf_len) {
    return {};
  }
  auto lp_msg_str = (LPTSTR)lp_msg_buf;
  std::string result{lp_msg_str, lp_msg_str + buf_len};
  LocalFree(lp_msg_buf);
  return result;
}

}  // namespace

#elif defined(__GNUC__)  // GNU compiler or CLang or ???
#include <dlfcn.h>
#else
#error define your compiler
#endif

namespace {

#if defined(_MSC_VER)  // Microsoft compiler
constexpr std::wstring_view kFileNameExtension = L".dll";
#elif defined(__APPLE__)  // MacOS
constexpr std::string_view kFileNameExtension = ".dylib";
#elif defined(__GNUC__)   // GNU compiler or CLang or ???
constexpr std::string_view kFileNameExtension = ".so";
#else
constexpr std::string_view kFileNameExtension;
#endif

}  // namespace
namespace iresearch {

void* load_library(const char* soname, [[maybe_unused]] int mode /* = 2 */) {
  if (soname == nullptr) {
    return nullptr;
  }
  irs::utf8_path name{soname};
  name += kFileNameExtension;
#if defined(_MSC_VER)  // Microsoft compiler
  auto* handle = static_cast<void*>(::LoadLibraryW(name.c_str()));
#elif defined(__GNUC__)  // GNU compiler
  auto* handle = dlopen(name.c_str(), mode);
#endif
  if (handle == nullptr) {
#ifdef _WIN32
    IR_FRMT_ERROR("load failed of shared object: %s error: %s",
                  name.u8string().c_str(), dlerror().c_str());
#else
    IR_FRMT_ERROR("load failed of shared object: %s error: %s", name.c_str(),
                  dlerror());
#endif
  }
  return handle;
}

void* get_function(void* handle, const char* fname) {
#if defined(_MSC_VER)  // Microsoft compiler
  return static_cast<void*>(
      ::GetProcAddress(static_cast<HINSTANCE>(handle), fname));
#elif defined(__GNUC__)  // GNU compiler
  return dlsym(handle, fname);
#endif
}

bool free_library(void* handle) {
#if defined(_MSC_VER)  // Microsoft compiler
  return TRUE == ::FreeLibrary(static_cast<HINSTANCE>(handle));
#elif defined(__GNUC__)  // GNU compiler
  return dlclose(handle) != 0;
#endif
}

void load_libraries(std::string_view path, std::string_view prefix,
                    std::string_view suffix) {
  irs::utf8_path plugin_path{path};
  bool result;

  if (!file_utils::exists_directory(result, plugin_path.c_str()) || !result) {
    IR_FRMT_INFO("library load failed, not a plugin path: %s",
                 plugin_path.u8string().c_str());

    return;  // no plugins directory
  }

  auto visitor = [&plugin_path, &prefix, &suffix](auto name) {
    bool result;
    const auto path = plugin_path / name;
    if (!file_utils::exists_file(result, path.c_str())) {
      IR_FRMT_ERROR("Failed to identify plugin file: %s",
                    path.u8string().c_str());
      return false;
    }
    if (!result) {
      return true;  // skip non-files
    }
    const auto path_parts = irs::file_utils::path_parts(name);
    if (irs::basic_string_ref{kFileNameExtension} != path_parts.extension) {
      return true;  // skip non-library extensions
    }
    auto stem =
        irs::utf8_path{irs::utf8_path::string_type(path_parts.stem)}.string();
    if (stem.size() < prefix.size() + suffix.size() ||
        strncmp(stem.c_str(), prefix.data(), prefix.size()) != 0 ||
        strncmp(stem.c_str() + stem.size() - suffix.size(), suffix.data(),
                suffix.size()) != 0) {
      return true;  // filename does not match
    }
    const auto path_stem = plugin_path / stem;  // strip extension
    // FIXME TODO check double-load of same dll
    const void* handle = load_library(path_stem.string().c_str(), 1);
    if (!handle) {
      IR_FRMT_ERROR("library load failed for path: %s",
                    path_stem.string().c_str());
    }
    return true;
  };
  file_utils::visit_directory(plugin_path.c_str(), visitor, false);
}

}  // namespace iresearch
