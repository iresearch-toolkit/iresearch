add_definitions(-DNDEBUG -DIRS_DISABLE_LOG)
add_compile_options(--coverage -g -O0 -fno-inline)
add_link_options(--coverage)
