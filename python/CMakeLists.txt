add_library(iresearch-python
  SHARED
  pyresearch.cpp
)

set_ipo(iresearch-python)

target_include_directories(iresearch-python
  PRIVATE ${IResearch_INCLUDE_DIR}
)

target_link_libraries(iresearch-python
  ${IResearch_TARGET_NAME}-shared
)

add_subdirectory(swig)
