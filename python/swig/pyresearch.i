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
  // create tuple of size 2
  $result = PyTuple_New(2);

  // set first value
  PyObject* first_value = $1.first
    ? Py_True
    : Py_False;
  Py_INCREF(first_value);
  PyTuple_SetItem($result, 0, first_value);

  // set second value
  const auto& second = $1.second;

  PyObject* second_value = second.null()
    ? Py_None
    : PyBytes_FromStringAndSize(
        reinterpret_cast<const char*>($1.second.c_str()),
        $1.second.size()
      );
  Py_INCREF(second_value);
  PyTuple_SetItem($result, 1, second_value);
}

%apply (char* STRING, size_t LENGTH) { (const char* data, size_t size ) }

%include "../pyresearch.h"

