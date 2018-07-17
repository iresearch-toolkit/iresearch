#include "pyresearch.h"
#include "store/mmap_directory.hpp"
#include "index/directory_reader.hpp"
#include "index/index_reader.hpp"

std::shared_ptr<iresearch::index_reader> open_index(const std::string& path) {
  auto dir = std::make_shared<irs::mmap_directory>(path);
  auto index = irs::directory_reader::open(*dir, irs::formats::get("1_0"));

  return std::shared_ptr<irs::index_reader>(
    index.get(),
    [dir, index](irs::index_reader*) { }
  );
}

int test(int n) {
    if (n < 0){ /* This should probably return an error, but this is simpler */
        return 0;
    }
    if (n == 0) {
        return 1;
    }
    else {
        /* testing for overflow would be a good idea here */
        return n * test(n-1);
    }
}
