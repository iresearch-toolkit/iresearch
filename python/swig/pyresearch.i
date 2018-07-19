%module pyresearch

%{
#define SWIG_FILE_WITH_INIT
#include "pyresearch.h"
%}

%include "stdint.i"
%include "cstring.i"
%include "std_string.i"
%include "std_vector.i"
%include "std_pair.i"

%template(StringVector) std::vector<std::string>;
%template(ColumnValue) std::pair<bool, irs::bytes_ref>;

%typemap(out) irs::bytes_ref {
  $result = PyBytes_FromStringAndSize(
    reinterpret_cast<const char*>($1.c_str()),
    $1.size()
  );
}

%typemap(out) irs::string_ref {
  $result = PyBytes_FromStringAndSize($1.c_str(), $1.size());
}

%typemap(out) std::pair<bool, irs::bytes_ref> {
  $result = PyTuple_New(2);
  PyTuple_SetItem($result, 0, $1.first ? Py_True : Py_False);
  PyTuple_SetItem($result, 1, PyBytes_FromStringAndSize(reinterpret_cast<const char*>($1.second.c_str()), $1.second.size()));
}

%apply (char* STRING, size_t LENGTH) { (const char* data, size_t size ) }

%include "../pyresearch.h"

