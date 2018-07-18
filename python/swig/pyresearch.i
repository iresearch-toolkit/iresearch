%module pyresearch

%{
#define SWIG_FILE_WITH_INIT
#include "pyresearch.h"
%}

%include "stdint.i"
%include "std_string.i"
%include "std_vector.i"

%template(StringVector) std::vector<std::string>;

%typemap(out) iresearch::bytes_ref {
  $result = PyBytes_FromStringAndSize(
    reinterpret_cast<const char*>($1.c_str()),
    $1.size()
  );
}

%include "../pyresearch.h"

