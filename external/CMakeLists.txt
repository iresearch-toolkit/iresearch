include(UpdateModule)

### Suppressing all warnings from external libs
if (SUPPRESS_EXTERNAL_WARNINGS)
  if (MSVC)
    add_compile_options(/W0)
  else ()
    add_compile_options(-Wno-all)
  endif ()
endif()

################################################################################
### Highway library
################################################################################

set(BUILD_TESTING OFF CACHE INTERNAL "" FORCE)
add_subdirectory(highway)

################################################################################
### sse2neon
### need this after highway and before simdcomp
################################################################################

add_library(sse2neon INTERFACE)

if (IS_ARM)
  set(SSE2NEON_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/sse2neon)

  target_include_directories(
    sse2neon
    SYSTEM
    INTERFACE ${SSE2NEON_ROOT}
  )
endif()

################################################################################
### CmdLine library
################################################################################

add_library(
  iresearch-cmdline
  INTERFACE
)

target_include_directories(
  iresearch-cmdline
  SYSTEM
  INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/cmdline
)

################################################################################
### OpenFST + Kaldi library
################################################################################

# We build OpenFST extensions provided by Kaldi as a part of OpenFST
set(Kaldi_sources
  ${CMAKE_CURRENT_SOURCE_DIR}/kaldi/src/base/io-funcs.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/kaldi/src/base/kaldi-error.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/kaldi/src/base/kaldi-utils.cc
)

set(OpenFST_sources
  ${CMAKE_CURRENT_SOURCE_DIR}/openfst/compat.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/openfst/flags.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/openfst/fst.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/openfst/fst-types.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/openfst/weight.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/openfst/mapped-file.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/openfst/properties.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/openfst/symbol-table.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/openfst/symbol-table-ops.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/openfst/util.cc
  ${Kaldi_sources}
)

add_library(
  iresearch-ofst
  OBJECT
  ${OpenFST_sources}
)

set_ipo(iresearch-ofst)

target_compile_definitions(
  iresearch-ofst
  PUBLIC "FST_NO_DYNAMIC_LINKING"
)

# disable warnings for 3rd-party libs for a cleaner build
if (MSVC)
  target_compile_options(
    iresearch-ofst
    # MSVC2015-2017 require "/bigobj" to compile debug build
    PRIVATE "$<$<CONFIG:Debug>:/bigobj>"
  )
else()
  CHECK_C_COMPILER_FLAG(-fPIC SUPPORT_FPIC)
  if (SUPPORT_FPIC)
    target_compile_options(
      iresearch-ofst
      PRIVATE "-fPIC"
    )
  endif ()
endif()

# force c++17 for ofst as transitional step
set_target_properties(iresearch-ofst
    PROPERTIES
        CXX_STANDARD 17
        CXX_STANDARD_REQUIRED YES
)

target_include_directories(
  iresearch-ofst
  SYSTEM
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/openfst
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/kaldi/src
)

################################################################################
### RapidJSON library
################################################################################

add_library(
  iresearch-rapidjson
  INTERFACE
)

target_include_directories(
  iresearch-rapidjson
  SYSTEM
  INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/rapidjson
)

################################################################################
### SIMDCOMP library
################################################################################

add_subdirectory(simdcomp)

################################################################################
### utfcpp library
################################################################################

add_library(
  iresearch-utfcpp
  INTERFACE
)

target_include_directories(
  iresearch-utfcpp
  SYSTEM
  INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/utfcpp
)

################################################################################
### frozen library
################################################################################

set(FROZEN_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/frozen/include)
set(FROZEN_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/frozen/include PARENT_SCOPE)

################################################################################
### abseil-cpp library
################################################################################
if (NOT TARGET absl::base)
  if ("${ABSL_ROOT}" STREQUAL "")
    set(ABSL_ROOT "$ENV{ABSL_ROOT}")
  endif ()

  if ("${ABSL_ROOT}" STREQUAL "")
    message("ABSL_ROOT not set. Pulling abseil-cpp submodule.")
    UpdateModule(${GIT_EXECUTABLE} "external/abseil-cpp" ${CMAKE_SOURCE_DIR} "README.md")
    set(ABSL_ROOT ${CMAKE_SOURCE_DIR}/external/abseil-cpp)
  endif ()

  if ("${ABSL_ROOT}" STREQUAL "")
    message(FATAL_ERROR "ABSL_ROOT not set")
  else ()
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON CACHE BOOL "" FORCE)
    set(ABSL_BUILD_TESTING OFF CACHE BOOL "" FORCE)
    set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "" FORCE)
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
    expand_path(${ABSL_ROOT} ABSL_ROOT_EXP)
    add_subdirectory(${ABSL_ROOT_EXP}
      ${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/iresearch-abseil-cpp.dir
      EXCLUDE_FROM_ALL)
    set(CMAKE_CXX_STANDARD 20)
  endif ()
endif ()

################################################################################
### velocypack library
################################################################################
if (NOT TARGET velocypack)
  if ("${VPACK_ROOT}" STREQUAL "")
    set(VPACK_ROOT "$ENV{VPACK_ROOT}")
  endif ()

  if ("${VPACK_ROOT}" STREQUAL "")
    message("VPACK_ROOT not set. Pulling velocypack submodule.")
    UpdateModule(${GIT_EXECUTABLE} "external/velocypack" ${CMAKE_SOURCE_DIR} "README.md")
    set(VPACK_ROOT ${CMAKE_SOURCE_DIR}/external/velocypack)
  endif ()

  if ("${VPACK_ROOT}" STREQUAL "")
    message(FATAL_ERROR "VPACK_ROOT not set")
  else ()
    expand_path(${VPACK_ROOT} VPACK_ROOT_EXP)
    add_subdirectory(${VPACK_ROOT_EXP}
      ${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/iresearch-velocypack.dir
      EXCLUDE_FROM_ALL)
  endif ()
endif ()

################################################################################
### Boost::Text library
################################################################################
# Force C++17 for boost::text
set(CXX_STD 17 CACHE STRING "Set to 14, 17, etc., to enable C++14, C++17, etc." FORCE)
add_subdirectory(text)
# force c++17
set_target_properties(text
    PROPERTIES
        CXX_STANDARD 17
        CXX_STANDARD_REQUIRED YES
)

################################################################################
### fastText library
################################################################################

add_subdirectory(fastText)
