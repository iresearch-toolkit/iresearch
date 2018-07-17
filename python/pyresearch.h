#ifndef PYRESEARCH_H
#define PYRESEARCH_H

#include <string>
#include <memory>

namespace iresearch {
struct index_reader;
}

std::shared_ptr<iresearch::index_reader> open_index(const std::string& path);
int test(int);

#endif
