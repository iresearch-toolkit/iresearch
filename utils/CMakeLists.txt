# set cmdline directories
set(CMDLINE_INCLUDE_DIR
  ${EXTERNAL_INCLUDE_DIRS}/cmdline
  )

if (MSVC)
  # disable "checked iterators" feature
  add_definitions(-D_SCL_SECURE_NO_WARNINGS)
endif ()

include_directories(
  ${PROJECT_BINARY_DIR}/core
)

if (SUPPRESS_EXTERNAL_WARNINGS)
  include_directories(SYSTEM
    ${EXTERNAL_INCLUDE_DIRS}
    ${FROZEN_INCLUDE_DIR})
else ()
  include_directories(${EXTERNAL_INCLUDE_DIRS}
    ${FROZEN_INCLUDE_DIR})
endif ()

################################################################################
### iresearch-util
################################################################################

add_executable(iresearch-index-util
  ./common.cpp
  ./index-dump.cpp
  ./index-convert.cpp
  ./index-util.cpp
  ./main.cpp
  )

add_sanitizers(iresearch-index-util)
set_ipo(iresearch-index-util)

target_link_libraries(iresearch-index-util
  iresearch-static
  iresearch-cmdline
  ${PTHREAD_LIBRARY}
  ${ATOMIC_LIBRARY}
  )

include_directories(iresearch-index-util
  ${IReSearch_generated_INCLUDE_DIR}
  ${IResearch_INCLUDE_DIR}
  ${Boost_INCLUDE_DIRS}
  ${CMDLINE_INCLUDE_DIR}
  )

################################################################################
### iresearch-benchmarks
################################################################################

add_executable(iresearch-benchmarks
  ./common.cpp
  ./index-put.cpp
  ./index-search.cpp
  ./index-benchmarks.cpp
  ./main.cpp
  )

add_sanitizers(iresearch-benchmarks)
set_ipo(iresearch-benchmarks)

add_dependencies(iresearch-benchmarks
  iresearch-static
  )

target_link_libraries(iresearch-benchmarks
  iresearch-static
  iresearch-cmdline
  ${PTHREAD_LIBRARY}
  ${ATOMIC_LIBRARY}
  )

include_directories(iresearch-benchmarks
  ${IResearch_INCLUDE_DIR}
  ${ICU_INCLUDE_DIR}
  ${Boost_INCLUDE_DIRS}
  ${CMDLINE_INCLUDE_DIR}
  )

################################################################################
### parametric-description-dump
################################################################################

add_executable(iresearch-pdd
  ./common.cpp
  ./pdd.cpp
  ./main.cpp
  )

add_sanitizers(iresearch-pdd)
set_ipo(iresearch-pdd)

target_link_libraries(iresearch-pdd
  iresearch-static
  iresearch-cmdline
  ${PTHREAD_LIBRARY}
  ${ATOMIC_LIBRARY}
  )

include_directories(iresearch-pdd
  ${IResearch_INCLUDE_DIR}
  ${Boost_INCLUDE_DIRS}
  ${CMDLINE_INCLUDE_DIR}
  )
