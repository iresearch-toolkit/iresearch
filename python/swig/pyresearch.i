%module pyresearch

%{
#define SWIG_FILE_WITH_INIT
#include "pyresearch.hpp"
%}

%include "stdint.i"
%include "std_pair.i"
%include "std_string.i"
%include "std_vector.i"

// FIXME check for invalid return values

%template(StringVector) std::vector<std::string>;
%template(ColumnValue) std::pair<bool, irs::bytes_ref>;

%typemap(in) (irs::string_ref) {
  if (!PyBytes_Check($input)) {
    PyErr_SetString(PyExc_ValueError, "Expected a string");
    SWIG_fail;
  }

  $1 = irs::string_ref(
    PyBytes_AsString($input),
    PyBytes_Size($input)
  );
}

%typemap(typecheck, precedence=0) irs::string_ref {
  $1 = PyBytes_Check($input) ? 1 : 0;
}

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

%include "../pyresearch.hpp"
