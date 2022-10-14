################################################################################
### iresearch-microbench
################################################################################

if (NOT TARGET benchmark)
  if ("${MICROBENCH_ROOT}" STREQUAL "")
    set(MICROBENCH_ROOT "$ENV{MICROBENCH_ROOT}")
  endif ()

  if (NOT ("${MICROBENCH_ROOT}" STREQUAL ""))
    set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE INTERNAL "" FORCE)
    set(BENCHMARK_ENABLE_TESTING OFF CACHE INTERNAL "" FORCE)
    add_subdirectory(${MICROBENCH_ROOT}
      ${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/iresearch-microbench.dir
      EXCLUDE_FROM_ALL)
  else ()
    find_package(benchmark REQUIRED)
  endif ()
endif ()

add_executable(iresearch-microbench
  ./top_term_collector_benchmark.cpp
  ./segmentation_stream_benchmark.cpp
  ./simd_utils_benchmark.cpp
  ./lower_bound_benchmark.cpp
  ./microbench_main.cpp
  )

set_ipo(iresearch-microbench)
add_sanitizers(iresearch-microbench)

add_dependencies(iresearch-microbench
  iresearch-static
  )

target_include_directories(iresearch-microbench
  PRIVATE ${PROJECT_BINARY_DIR}/core
  )

set_target_properties(iresearch-microbench
  PROPERTIES
  OUTPUT_NAME iresearch-microbench
  COMPILE_DEFINITIONS "$<$<CONFIG:Coverage>:IRESEARCH_DEBUG>;$<$<CONFIG:Debug>:IRESEARCH_DEBUG>"
  )

target_link_libraries(iresearch-microbench
  iresearch-analyzer-nearest-neighbors-static
  iresearch-static
  ${PTHREAD_LIBRARY}
  benchmark::benchmark
  )

include_directories(iresearch-microbench
  ${EXTERNAL_INCLUDE_DIRS}
  ${IResearchTests_INCLUDE_DIR}
  ${IReSearch_generated_INCLUDE_DIR}
  ${IResearch_INCLUDE_DIR}
  ${GTEST_INCLUDE_DIR}
  ${Boost_INCLUDE_DIRS}
  ${FROZEN_INCLUDE_DIR}
  $<TARGET_PROPERTY:iresearch-cmdline,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:iresearch-ofst,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:iresearch-utfcpp,INTERFACE_INCLUDE_DIRECTORIES>
  )
