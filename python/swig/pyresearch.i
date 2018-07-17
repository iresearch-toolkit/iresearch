%module pyresearch

%{
#define SWIG_FILE_WITH_INIT
#include "pyresearch.h"
#include "index/index_reader.hpp"
%}

%include "std_string.i"
%include "std_shared_ptr.i"
%include "index/index_reader.hpp"

%shared_ptr(iresearch::index_reader);

int test(int i);
std::shared_ptr<iresearch::index_reader> open_index(const std::string& path);

