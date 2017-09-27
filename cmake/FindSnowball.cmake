# - Find Snowball (libstemmer.h, stemmer.lib libstemmer.a, libstemmer.so, and libstemmer.so.0d)
# This module defines
#  Snowball_INCLUDE_DIR, directory containing headers
#  Snowball_LIBRARY_DIR, directory containing Snowball libraries
#  Snowball_SHARED_LIB, path to stemmer.so/stemmer.dll
#  Snowball_STATIC_LIB, path to stemmer.lib
#  Snowball_FOUND, whether Snowball has been found

if ("${SNOWBALL_ROOT}" STREQUAL "")
  set(SNOWBALL_ROOT "$ENV{SNOWBALL_ROOT}")
  if (NOT "${SNOWBALL_ROOT}" STREQUAL "") 
    string(REPLACE "\"" "" SNOWBALL_ROOT ${SNOWBALL_ROOT})
  endif()
endif()

set(Snowball_SEARCH_HEADER_PATHS
  ${SNOWBALL_ROOT}/include
)

set(Snowball_SEARCH_LIB_PATH
  ${SNOWBALL_ROOT}/lib
  ${SNOWBALL_ROOT}/build
  ${SNOWBALL_ROOT}/Release
  ${SNOWBALL_ROOT}/build/Release
  ${SNOWBALL_ROOT}/Debug
  ${SNOWBALL_ROOT}/build/Debug
)

if(NOT MSVC AND "${SNOWBALL_ROOT}" STREQUAL "")
  set(UNIX_DEFAULT_INCLUDE
      "/usr/include"
      "/usr/include/x86_64-linux-gnu"
  )
endif()

find_path(Snowball_INCLUDE_DIR
  libstemmer.h
  PATHS ${Snowball_SEARCH_HEADER_PATHS} ${UNIX_DEFAULT_INCLUDE}
  NO_DEFAULT_PATH # make sure we don't accidentally pick up a different version
)

include(Utils)

if(NOT MSVC AND "${SNOWBALL_ROOT}" STREQUAL "")
  set(UNIX_DEFAULT_LIB
      "/lib"
      "/lib/x86_64-linux-gnu"
      "/usr/lib"
      "/usr/lib/x86_64-linux-gnu"
  )
endif()


# set options for: shared
if (MSVC)
  set(Snowball_LIBRARY_PREFIX "")
  set(Snowball_LIBRARY_SUFFIX ".lib")
elseif(APPLE)
  set(Snowball_LIBRARY_PREFIX "lib")
  set(Snowball_LIBRARY_SUFFIX ".dylib")
else()
  set(Snowball_LIBRARY_PREFIX "lib")
  set(Snowball_LIBRARY_SUFFIX ".so")
endif()
set_find_library_options("${Snowball_LIBRARY_PREFIX}" "${Snowball_LIBRARY_SUFFIX}")

# find library
find_library(Snowball_SHARED_LIB
  NAMES stemmer
  PATHS ${Snowball_SEARCH_LIB_PATH} ${UNIX_DEFAULT_LIB}
  NO_DEFAULT_PATH
)

# restore initial options
restore_find_library_options()


# set options for: static
if (MSVC)
  set(Snowball_LIBRARY_PREFIX "")
  set(Snowball_LIBRARY_SUFFIX ".lib")
else()
  set(Snowball_LIBRARY_PREFIX "lib")
  set(Snowball_LIBRARY_SUFFIX ".a")
endif()
set_find_library_options("${Snowball_LIBRARY_PREFIX}" "${Snowball_LIBRARY_SUFFIX}")

# find library
find_library(Snowball_STATIC_LIB
  NAMES stemmer
  PATHS ${Snowball_SEARCH_LIB_PATH} ${UNIX_DEFAULT_LIB}
  NO_DEFAULT_PATH
)

# restore initial options
restore_find_library_options()


if (Snowball_INCLUDE_DIR AND Snowball_SHARED_LIB AND Snowball_STATIC_LIB)
  set(Snowball_FOUND TRUE)
  set(Snowball_LIBRARY_DIR
    "${Snowball_SEARCH_LIB_PATH}"
    CACHE PATH
    "Directory containing Snowball libraries"
    FORCE
  )

  add_library(snowball-shared IMPORTED SHARED)
  set_target_properties(snowball-shared PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${Snowball_INCLUDE_DIR}"
    IMPORTED_LOCATION "${Snowball_SHARED_LIB}"
  )

  add_library(snowball-static IMPORTED STATIC)
  set_target_properties(snowball-static PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${Snowball_INCLUDE_DIR}"
    IMPORTED_LOCATION "${Snowball_STATIC_LIB}"
  )
else ()
  set(Snowball_FOUND FALSE)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Snowball
  DEFAULT_MSG
  Snowball_INCLUDE_DIR
  Snowball_SHARED_LIB
  Snowball_STATIC_LIB
)
message("Snowball_INCLUDE_DIR: " ${Snowball_INCLUDE_DIR})
message("Snowball_LIBRARY_DIR: " ${Snowball_LIBRARY_DIR})
message("Snowball_SHARED_LIB: " ${Snowball_SHARED_LIB})
message("Snowball_STATIC_LIB: " ${Snowball_STATIC_LIB})

mark_as_advanced(
  Snowball_INCLUDE_DIR
  Snowball_LIBRARY_DIR
  Snowball_SHARED_LIB
  Snowball_STATIC_LIB
)
