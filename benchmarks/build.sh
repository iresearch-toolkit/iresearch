#!/usr/bin/env sh
set -x
inc="../core"
lib="../build/bin"
g++ -g -std=c++11 index-search.cpp -I $inc -L$lib -liresearch -lscorer-bm25-s -lboost_filesystem -lboost_program_options -lboost_system -lboost_thread -lboost_locale -lboost_regex -licuuc -lpthread -o index-search -Wl,-rpath=":$lib"
g++ -g -std=c++11 index-put.cpp -I $inc -L$lib -liresearch -lanalyzer-text -lboost_filesystem -lboost_program_options -lboost_system -lboost_thread -lboost_locale -licui18n -licuuc -lstemmer -lpthread -o index-put -Wl,-rpath=":$lib"

