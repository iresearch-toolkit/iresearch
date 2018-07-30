cmake_minimum_required(VERSION 3.3)
project(SIMDCOMP VERSION "0.0.3")
cmake_policy(SET CMP0065 OLD)
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -v" )
set(BASE_FLAGS
  "-std=c99"
  "-fPIC"
  "-Wextra"
  "-pedantic"
  "-Wshadow"
)
if(CMAKE_BUILD_TYPE MATCHES Debug)
  set(BASE_FLAGS
    ${BASE_FLAGS}
    "-O0"
    "-ggdb"
    )
else()
  set(BASE_FLAGS
    ${BASE_FLAGS}
    "-O3"
    "-g"
    )
endif()
# test for arm
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*)")
   set(BASE_FLAGS
    ${BASE_FLAGS}
    "-D__ARM_NEON__"
    )
endif()
set(SIMDCOMP_SRCS
  ${PROJECT_SOURCE_DIR}/src/avx512bitpacking.c
  ${PROJECT_SOURCE_DIR}/src/avxbitpacking.c
  ${PROJECT_SOURCE_DIR}/src/simdfor.c
  ${PROJECT_SOURCE_DIR}/src/simdcomputil.c
  ${PROJECT_SOURCE_DIR}/src/simdbitpacking.c
  ${PROJECT_SOURCE_DIR}/src/simdintegratedbitpacking.c
  ${PROJECT_SOURCE_DIR}/src/simdpackedsearch.c
  ${PROJECT_SOURCE_DIR}/src/simdpackedselect.c
)
add_library(simdcomp_static STATIC "${SIMDCOMP_SRCS}")
target_link_libraries(simdcomp_static ${BASE_FLAGS})
add_library(simdcomp SHARED "${SIMDCOMP_SRCS}")
target_link_libraries(simdcomp ${BASE_FLAGS})
target_include_directories(
  simdcomp
  PUBLIC ${PROJECT_SOURCE_DIR}/include
)
target_include_directories(
  simdcomp_static
  PUBLIC ${PROJECT_SOURCE_DIR}/include
)
