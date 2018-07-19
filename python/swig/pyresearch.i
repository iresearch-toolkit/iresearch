%module pyresearch

%{
#define SWIG_FILE_WITH_INIT
#include "pyresearch.h"
%}

%include "stdint.i"
%include "std_string.i"
%include "std_vector.i"
%include "constraints.i"

%template(StringVector) std::vector<std::string>;

%typemap(out) irs::bytes_ref {
  $result = PyBytes_FromStringAndSize(
    reinterpret_cast<const char*>($1.c_str()),
    $1.size()
  );
}

%typemap(out) irs::string_ref {
  $result = PyBytes_FromStringAndSize($1.c_str(), $1.size());
}

%apply (char* STRING, size_t LENGTH) { (const char* data, size_t size ) }

%include "../pyresearch.h"

