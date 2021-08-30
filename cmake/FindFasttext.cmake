# Find fasttext (libfasttext.a, libfasttext.dylib)
# This module defines
# Fasttext_SHARED_LIB, path to libfasttext*.dylib/.so/.lib
# Fasttext_STATIC_LIB, path to libfasttext*.a/*.lib
# Fasttext_INCLUDE_DIR
# Fasttext_FOUND, whether fasttext has been found

if ("${FASTTEXT_ROOT}" STREQUAL "")
    set(FASTTEXT_ROOT "$ENV{FASTTEXT_ROOT}")
    if (NOT "${FASTTEXT_ROOT}" STREQUAL "")
        string(REPLACE "\"" "" FASTTEXT_ROOT ${FASTTEXT_ROOT})
    endif()
endif()

if (NOT "${FASTTEXT_ROOT}" STREQUAL "")
    set(Fasttext_SEARCH_LIB_PATHS
      ${FASTTEXT_ROOT}
      ${FASTTEXT_ROOT}/build
      ${FASTTEXT_ROOT}/lib
      ${FASTTEXT_ROOT}/Release
      ${FASTTEXT_ROOT}/build/Release
      ${FASTTEXT_ROOT}/Debug
      ${FASTTEXT_ROOT}/build/Debug
    )

    set(Fasttext_SEARCH_SRC_PATHS
      ${FASTTEXT_ROOT}
      ${FASTTEXT_ROOT}/src
    )

    set(Fasttext_SEARCH_HEADER_PATHS
      ${FASTTEXT_ROOT}
      ${FASTTEXT_ROOT}/src
    )
endif()

find_path(Fasttext_INCLUDE_DIR
    fasttext.h
    PATHS ${Fasttext_SEARCH_HEADER_PATHS}
    NO_DEFAULT_PATH
)

find_path(Fasttext_SRC_DIR
    fasttext.cc
    PATHS ${Fasttext_SEARCH_SRC_PATHS}
    NO_DEFAULT_PATH
)

if (Fasttext_INCLUDE_DIR AND Fasttext_SRC_DIR)
    get_filename_component(Fasttext_SRC_DIR_PARENT ${Fasttext_SRC_DIR} DIRECTORY)

    find_path(Fasttext_SRC_DIR_CMAKE
      CMakeLists.txt
      PATHS ${Fasttext_SRC_DIR} ${Fasttext_SRC_DIR_PARENT}
      NO_DEFAULT_PATH
    )
endif()

if (Fasttext_SRC_DIR AND Fasttext_SRC_DIR_CMAKE)
    set(Fasttext_FOUND TRUE)

    add_subdirectory(
      ${Fasttext_SRC_DIR_CMAKE}
      ${CMAKE_CURRENT_BINARY_DIR}/CmakeFiles/iresearch-fasttext.dir
      EXCLUDE_FROM_ALL # don't build unused
    )
    set(Fasttext_LIBRARY_DIR ${Fasttext_SEARCH_LIB_PATHS})
    set(Fasttext_SHARED_LIB fasttext-shared)
    set(Fasttext_STATIC_LIB fasttext-static)

    message(${Fasttext_STATIC_LIB})
    message(${Fasttext_SHARED_LIB})
    return()
endif()

include(Utils)

# set options for: shared
if (MSVC)
    set(FASTTEXT_LIBRARY_PREFIX "")
    set(FASTTEXT_LIBRARY_SUFFIX ".lib")
elseif(APPLE)
    set(FASTTEXT_LIBRARY_PREFIX "lib")
    set(FASTTEXT_LIBRARY_SUFFIX ".dylib")
else()
    set(FASTTEXT_LIBRARY_PREFIX "lib")
    set(FASTTEXT_LIBRARY_SUFFIX ".so")
endif()
set_find_library_options("${FASTTEXT_LIBRARY_PREFIX}" "${FASTTEXT_LIBRARY_SUFFIX}")

# find library
find_library(Fasttext_SHARED_LIB
        NAMES fasttext
        PATHS ${FASTTEXT_SEARCH_LIB_PATHS}
        NO_DEFAULT_PATH
        )

# restore initial options
restore_find_library_options()


# set options for: static
if (MSVC)
    set(FASTTEXT_LIBRARY_PREFIX "")
    set(FASTTEXT_LIBRARY_SUFFIX ".lib")
else()
    set(FASTTEXT_LIBRARY_PREFIX "lib")
    set(FASTTEXT_LIBRARY_SUFFIX ".a")
endif()
set_find_library_options("${FASTTEXT_LIBRARY_PREFIX}" "${FASTTEXT_LIBRARY_SUFFIX}")

# find library
find_library(Fasttext_STATIC_LIB
        NAMES fasttext libfasttext
        PATHS ${FASTTEXT_SEARCH_LIB_PATHS}
        NO_DEFAULT_PATH
        )

# restore initial options
restore_find_library_options()

message("${FASTTEXT_SEARCH_LIB_PATHS}")
message("${Fasttext_SHARED_LIB}")
message("${Fasttext_STATIC_LIB}")
if (Fasttext_SHARED_LIB AND Fasttext_STATIC_LIB)
    set(Fasttext_FOUND TRUE)

    message("FOUND FASTTEXT")
    message("${Fasttext_SHARED_LIB}")
    message("${Fasttext_STATIC_LIB}")
else()
    set(Fasttext_FOUND FALSE)
endif()
